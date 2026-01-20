from __future__ import annotations

import os
import uuid
from dataclasses import asdict
from datetime import datetime, timedelta, time
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from . import db, ibkr, rvol, output
from .float_provider import FmpFloatProvider
from .scoring import Metrics, grade_and_score
from .settings import RuntimeSettings

NY = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


def _parse_time_hhmm(v: str) -> time:
    hh, mm = v.split(":")
    return time(int(hh), int(mm))


def _iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat()


def build_watchlist(settings: RuntimeSettings) -> dict:
    now_ny = datetime.now(tz=NY)
    now_utc = datetime.now(tz=UTC)
    asof_date_ny = now_ny.date().isoformat()
    anchor = _parse_time_hhmm(settings.rvol_anchor_ny)

    conn = db.connect(settings.db_path)
    db.init_schema(conn, schema_file=os.path.join("config", "schema.sql"))

    ib = ibkr.connect(settings.ib_host, settings.ib_port, settings.ib_client_id, settings.ib_timeout_s)

    scan = ibkr.scan_top_perc_gainers(
        ib,
        price_min=settings.filters.price_min,
        price_max=settings.filters.price_max,
        volume_min=settings.filters.volume_min,
        max_rows=settings.filters.max_candidates,
    )
    scan_candidates_count = len(scan)
    invalid_last_count = 0

    # cache symbols metadata
    for c in scan:
        db.upsert_symbol(conn, c.symbol, c.con_id, c.primary_exchange)

    # snapshot metrics + initial filters
    prelim: List[Tuple[ibkr.IbContractInfo, Metrics]] = []
    for c in scan:
        last, prev_close, vol_today, bid, ask = ibkr.snapshot_metrics(ib, c.symbol)
        if last is None or last <= 0:
            invalid_last_count += 1
            continue
        if not (settings.filters.price_min <= last <= settings.filters.price_max):
            continue

        change_pct = None
        if prev_close not in (None, 0) and last is not None:
            change_pct = ((last - prev_close) / prev_close) * 100.0
        if change_pct is None or change_pct < settings.filters.change_min_pct:
            continue
        if vol_today is None or vol_today < settings.filters.volume_min:
            continue

        spread = None
        if bid is not None and ask is not None and bid > 0 and ask > 0:
            spread = ask - bid

        m = Metrics(
            symbol=c.symbol,
            last=last,
            prev_close=prev_close,
            change_pct=float(change_pct),
            volume_today=int(vol_today) if vol_today is not None else None,
            bid=bid,
            ask=ask,
            spread=float(spread) if spread is not None else None,
        )
        prelim.append((c, m))

    # float (DB cache + FMP for missing)
    float_map = db.load_float_snapshots(conn, asof_date_ny)
    provider = FmpFloatProvider(settings.fmp_api_key)
    missing = [m.symbol for _, m in prelim if m.symbol not in float_map]
    if missing:
        fetched = provider.prefetch(conn, missing, asof_date_ny, allow_stale_days=settings.float_allow_stale_days)
        float_map.update(fetched)

    # apply float filter
    filtered: List[Tuple[ibkr.IbContractInfo, Metrics]] = []
    for c, m in prelim:
        fs = float_map.get(m.symbol)
        m.float_shares = fs
        if fs is not None and fs > settings.filters.float_max:
            continue
        filtered.append((c, m))

    # RVOL: compute for top N by change_pct to avoid pacing
    filtered.sort(key=lambda t: (t[1].change_pct or 0.0), reverse=True)
    top_for_rvol = filtered[: settings.filters.max_rvol_symbols]

    duration_days = settings.rvol_lookback_days + 3
    since_utc = (now_ny - timedelta(days=duration_days)).astimezone(UTC)
    since_iso = _iso(since_utc)

    for c, m in top_for_rvol:
        # try cached bars
        cached = db.load_minute_volumes_since(conn, m.symbol, since_iso)
        bars: List[Tuple[datetime, int]] = []
        if cached:
            for ts_iso, vol in cached:
                bars.append((datetime.fromisoformat(ts_iso), int(vol)))

        # if not enough cache, fetch from IBKR
        if len(bars) < 500:
            hist = ibkr.historical_bars_1m(ib, m.symbol, duration_days=duration_days, use_rth=settings.use_rth)
            if hist:
                rows = []
                for b in hist:
                    dt = b.date
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=NY)
                    ts_utc = dt.astimezone(UTC).replace(microsecond=0).isoformat()
                    rows.append((ts_utc, float(b.open), float(b.high), float(b.low), float(b.close), int(b.volume or 0)))
                db.cache_minute_bars(conn, m.symbol, rows)
                bars = [(datetime.fromisoformat(ts), int(v)) for ts, v in db.load_minute_volumes_since(conn, m.symbol, since_iso)]

        m.rvol = rvol.compute_rvol_time_of_day(
            bars,
            anchor_ny=anchor,
            lookback_days=settings.rvol_lookback_days,
            now_ny=now_ny,
        )

    # final filters + grading
    final: List[dict] = []
    for c, m in filtered:
        if m.rvol is not None and m.rvol < settings.filters.rvol_min:
            continue
        if m.spread is not None and m.spread > (settings.filters.spread_max * 2.0):
            continue

        grade, score = grade_and_score(m, float_max=settings.filters.float_max, spread_max=settings.filters.spread_max)
        tv = output.tv_symbol(m.symbol, c.primary_exchange)

        final.append({
            "symbol": m.symbol,
            "primaryExchange": c.primary_exchange,
            "tvSymbol": tv,
            "last": m.last,
            "prevClose": m.prev_close,
            "changePct": m.change_pct,
            "volumeToday": m.volume_today,
            "bid": m.bid,
            "ask": m.ask,
            "spread": m.spread,
            "floatShares": m.float_shares,
            "rvol": m.rvol,
            "grade": grade,
            "score": score,
        })

    # order by grade then score desc
    order = {"A": 0, "B": 1, "C": 2, "D": 3}
    final.sort(key=lambda x: (order.get(x["grade"], 9), -float(x["score"])))

    payload = {
        "run_id": str(uuid.uuid4()),
        "generated_utc": _iso(now_utc),
        "generated_ny": _iso(now_ny),
        "scan": {
            "candidates": scan_candidates_count,
            "prelim": len(prelim),
            "filtered": len(filtered),
            "final": len(final),
            "invalid_last": invalid_last_count,
        },
        "filters": asdict(settings.filters),
        "rvol": {
            "anchor_time_ny": settings.rvol_anchor_ny,
            "lookback_days": settings.rvol_lookback_days,
            "use_rth": settings.use_rth,
        },
        "symbols": final,
        "tradingview": {
            "txt_symbols": [x["tvSymbol"] for x in final],
        },
    }

    ib.disconnect()
    conn.close()
    return payload
