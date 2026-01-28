from __future__ import annotations

import math
import os
import uuid
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from zoneinfo import ZoneInfo

from . import db, ibkr, rvol, output, news, sanity
from .float_provider import FmpFloatProvider
from .scoring import Metrics, grade_and_score
from .settings import RuntimeSettings

NY = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


def _iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat()


def build_watchlist(settings: RuntimeSettings) -> dict:
    now_ny = datetime.now(tz=NY)
    now_utc = datetime.now(tz=UTC)
    asof_date_ny = now_ny.date().isoformat()
    conn = db.connect(settings.db_path)
    db.init_schema(conn, schema_file=os.path.join("config", "schema.sql"))

    ib = ibkr.connect(settings.ib_host, settings.ib_port, settings.ib_client_id, settings.ib_timeout_s, settings.ib_market_data_type)
    if settings.debug:
        print(
            "IBKR market_data_type={code} ({label})".format(
                code=settings.ib_market_data_type,
                label=ibkr.market_data_type_label(settings.ib_market_data_type),
            )
        )
        print(
            "Scanner config instrument={inst} location={loc} scanCode={code} rows={rows}".format(
                inst=ibkr.SCAN_INSTRUMENT,
                loc=ibkr.SCAN_LOCATION_CODE,
                code=ibkr.SCAN_CODE,
                rows=settings.filters.max_candidates,
            )
        )

    scan = ibkr.scan_top_perc_gainers(
        ib,
        price_min=settings.filters.price_min,
        price_max=settings.filters.price_max,
        volume_min=settings.filters.volume_min,
        max_rows=settings.filters.max_candidates,
    )
    scan_raw_count = len(scan)
    excluded_otc_pink = 0
    if settings.exclude_otc_pink:
        before = len(scan)
        scan = [c for c in scan if not ibkr.is_otc_pink(c.primary_exchange)]
        excluded_otc_pink = before - len(scan)
        if settings.debug and excluded_otc_pink:
            print(f"Scanner excluded OTC/Pink: {excluded_otc_pink}")
    scan_candidates_count = len(scan)
    if settings.debug:
        print(f"scan candidates: {scan_candidates_count}")
        if scan_candidates_count == 0:
            print("scan candidates is 0; scanner tried fallback codes, check IBKR errors/entitlements")
    invalid_last_count = 0
    drop_reasons = {
        "invalid_last": 0,
        "price_out_of_range": 0,
        "missing_prev_close": 0,
        "change_below_min": 0,
        "missing_volume": 0,
        "volume_below_min": 0,
    }

    # cache symbols metadata
    for c in scan:
        db.upsert_symbol(conn, c.symbol, c.con_id, c.primary_exchange)

    # snapshot metrics + initial filters
    prelim: List[Tuple[ibkr.IbContractInfo, Metrics]] = []
    for c in scan:
        last, prev_close, vol_today, bid, ask = ibkr.snapshot_metrics(ib, c.symbol)
        if last is None or last <= 0:
            invalid_last_count += 1
            drop_reasons["invalid_last"] += 1
            continue
        if not (settings.filters.price_min <= last <= settings.filters.price_max):
            drop_reasons["price_out_of_range"] += 1
            continue

        if prev_close is None or prev_close <= 0:
            drop_reasons["missing_prev_close"] += 1
            continue
        change_pct = ((last - prev_close) / prev_close) * 100.0
        if change_pct < settings.filters.change_min_pct:
            drop_reasons["change_below_min"] += 1
            continue
        if vol_today is None:
            drop_reasons["missing_volume"] += 1
            continue
        if vol_today < settings.filters.volume_min:
            drop_reasons["volume_below_min"] += 1
            continue

        spread = None
        if bid is not None and ask is not None and bid > 0 and ask > 0:
            spread = ask - bid
        spread_pct = None
        if spread is not None and last is not None and last > 0:
            spread_pct = spread / last

        m = Metrics(
            symbol=c.symbol,
            last=last,
            prev_close=prev_close,
            change_pct=float(change_pct),
            volume_today=int(vol_today) if vol_today is not None else None,
            bid=bid,
            ask=ask,
            spread=float(spread) if spread is not None else None,
            spread_pct=float(spread_pct) if spread_pct is not None else None,
        )
        prelim.append((c, m))
    if settings.debug:
        print(f"prelim after snapshot+basic filters: {len(prelim)}")
        print(f"prelim drop reasons: {drop_reasons}")

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
    if settings.debug:
        print(f"after float filter: {len(filtered)}")

    # RVOL: compute for top N by change_pct to avoid pacing
    filtered.sort(key=lambda t: (t[1].change_pct or 0.0), reverse=True)
    top_for_rvol = filtered[: settings.filters.max_rvol_symbols]

    duration_days = settings.rvol_lookback_days + 7
    since_utc = (now_ny - timedelta(days=duration_days)).astimezone(UTC)
    since_iso = _iso(since_utc)

    for idx, (c, m) in enumerate(top_for_rvol):
        if settings.debug:
            print(f"RVOL {idx + 1}/{len(top_for_rvol)} {m.symbol} ...")
        # try cached bars
        cached = db.load_minute_volumes_since(conn, m.symbol, since_iso)
        bars: List[Tuple[datetime, int]] = []
        if cached:
            for ts_iso, vol in cached:
                dt = datetime.fromisoformat(ts_iso)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC)
                bars.append((dt, int(vol)))

        expected_bars_per_day = rvol.session_bar_count(settings.rvol_session, settings.rvol_bar_size)
        min_required_bars = expected_bars_per_day * max(1, settings.rvol_min_history_days)
        use_rth_for_hist = settings.use_rth
        if settings.rvol_session.strip().upper() == "RTH+PRE":
            use_rth_for_hist = False
        # if not enough cache, fetch from IBKR
        if len(bars) < min_required_bars:
            try:
                hist = ibkr.historical_bars_intraday(
                    ib,
                    m.symbol,
                    duration_days=duration_days,
                    use_rth=use_rth_for_hist,
                    bar_size=settings.rvol_bar_size,
                )
            except TimeoutError:
                if settings.debug:
                    print(f"RVOL timeout {m.symbol}")
                ib.sleep(settings.rvol_throttle_s)
                continue
            ib.sleep(settings.rvol_throttle_s)
            if hist:
                rows = []
                for b in hist:
                    dt = b.date
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=NY)
                    ts_utc = dt.astimezone(UTC).replace(microsecond=0).isoformat()
                    rows.append((ts_utc, float(b.open), float(b.high), float(b.low), float(b.close), int(b.volume or 0)))
                db.cache_minute_bars(conn, m.symbol, rows)
                bars = []
                for ts, v in db.load_minute_volumes_since(conn, m.symbol, since_iso):
                    dt = datetime.fromisoformat(ts)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=UTC)
                    bars.append((dt, int(v)))

        baseline_curve = rvol.get_or_build_baseline_curve(
            conn,
            symbol=m.symbol,
            bars_utc=bars,
            now_ny=now_ny,
            session=settings.rvol_session,
            bar_size=settings.rvol_bar_size,
            lookback_days=settings.rvol_lookback_days,
            method=settings.rvol_method,
            trim_pct=settings.rvol_trim_pct,
            min_history_days=settings.rvol_min_history_days,
            min_baseline=settings.rvol_min_baseline,
            calendar_name=settings.market_calendar,
        )
        rvol_result = rvol.compute_rvol_time_of_day(
            symbol=m.symbol,
            bars_utc=bars,
            baseline_curve=baseline_curve,
            now_ny=now_ny,
            session=settings.rvol_session,
            bar_size=settings.rvol_bar_size,
            min_history_days=settings.rvol_min_history_days,
            min_baseline=settings.rvol_min_baseline,
            cap=settings.rvol_cap,
        )
        if rvol_result:
            m.rvol = rvol_result.rvol
            m.rvol_raw = rvol_result.rvol_raw
            m.rvol_days_valid = rvol_result.history_days_used
            m.rvol_cap_applied = rvol_result.cap_applied
            m.rvol_cumvol_today = rvol_result.cumvol_today
            m.rvol_baseline = rvol_result.baseline_cumvol
            m.rvol_minute_index = rvol_result.minute_index
            m.rvol_baseline_low = rvol_result.baseline_low
            m.rvol_insufficient_history = rvol_result.insufficient_history
            m.rvol_session_mismatch = rvol_result.session_mismatch
            cap_for_score = settings.rvol_cap if settings.rvol_cap and settings.rvol_cap > 1 else 200.0
            if m.rvol_raw is not None:
                m.rvol_score = min(1.0, math.log10(max(m.rvol_raw, 1.0)) / math.log10(cap_for_score))
            else:
                m.rvol_score = None
        else:
            m.rvol = None
            m.rvol_raw = None
            m.rvol_days_valid = None
            m.rvol_cap_applied = None
            m.rvol_cumvol_today = None
            m.rvol_baseline = None
            m.rvol_minute_index = None
            m.rvol_baseline_low = None
            m.rvol_insufficient_history = None
            m.rvol_session_mismatch = None
            m.rvol_score = None

        if settings.debug:
            if rvol_result:
                print(
                    "RVOL detail {sym}: t={idx} cumvol={cum} baseline={base} days={days} "
                    "raw={raw} cap={cap} flags=baseline_low:{low} insufficient_history:{ih} session_mismatch:{sm}"
                    .format(
                        sym=m.symbol,
                        idx=rvol_result.minute_index,
                        cum=rvol_result.cumvol_today,
                        base=rvol_result.baseline_cumvol,
                        days=rvol_result.history_days_used,
                        raw=rvol_result.rvol_raw,
                        cap="yes" if rvol_result.cap_applied else "no",
                        low=rvol_result.baseline_low,
                        ih=rvol_result.insufficient_history,
                        sm=rvol_result.session_mismatch,
                    )
                )
            else:
                print(f"RVOL detail {m.symbol}: insufficient data or baseline below floor")

    # final filters + grading
    candidates: List[Tuple[ibkr.IbContractInfo, Metrics]] = []
    for c, m in filtered:
        rvol_for_filter = m.rvol_raw if m.rvol_raw is not None else m.rvol
        rvol_required = True
        if settings.rvol_permissive_if_not_live and int(settings.ib_market_data_type) != 1:
            rvol_required = False
        if rvol_for_filter is None:
            if rvol_required:
                continue
        elif rvol_for_filter < settings.filters.rvol_min:
            continue
        if settings.filters.spread_abs_max > 0:
            if m.spread is None or m.spread > settings.filters.spread_abs_max:
                continue
        if settings.filters.spread_pct_max > 0:
            if m.spread_pct is None or m.spread_pct > settings.filters.spread_pct_max:
                continue
        candidates.append((c, m))

    news_map: Dict[str, dict] = {}
    news_disabled_reason = None
    provider_name = (settings.news_provider or "fmp").strip().lower()
    if provider_name not in ("fmp", "none"):
        if settings.debug:
            print(f"WARN: NEWS_PROVIDER={provider_name!r} unsupported; disabling news.")
        provider_name = "none"

    news_enabled = provider_name == "fmp" and settings.news_lookback_hours > 0
    if provider_name == "none":
        news_enabled = False
        news_disabled_reason = "disabled"
        news_map = news.fetch_news([m.symbol for _, m in candidates], settings.news_lookback_hours, provider="none")
    elif settings.news_lookback_hours <= 0:
        news_enabled = False
        news_disabled_reason = "lookback_disabled"
    elif candidates:
        news_symbols = sorted(candidates, key=lambda t: (t[1].change_pct or 0.0), reverse=True)
        if settings.news_debug_top_n > 0:
            news_symbols = news_symbols[: settings.news_debug_top_n]
        fetch_list = [m.symbol for _, m in news_symbols]
        try:
            news_map = news.fetch_news(fetch_list, settings.news_lookback_hours, provider=provider_name, api_key=settings.fmp_api_key)
            news_enabled = True
        except news.NewsProviderRestricted:
            news_disabled_reason = "fmp_plan_restricted"
            news_enabled = False
            news_map = {m.symbol: news.restricted_result(provider="fmp") for _, m in candidates}
            if settings.debug:
                print("NEWS disabled: FMP endpoint restricted (402). Upgrade plan or switch provider.")

        if settings.debug and news_enabled and settings.news_debug_top_n > 0:
            for _, m in news_symbols:
                info = news_map.get(m.symbol, {})
                status = info.get("status")
                total_news = info.get("totalNews")
                recent_news = info.get("recentNews")
                first_date = info.get("publishedAt")
                first_headline = info.get("headline")
                print(
                    f"NEWS {m.symbol} status={status} total={total_news} recent={recent_news} "
                    f"first={first_date} headline={first_headline}"
                )

    final: List[dict] = []
    for c, m in candidates:
        news_info = news_map.get(m.symbol) or {}
        headline = news_info.get("headline")
        summary = news_info.get("summary")
        catalyst_text = summary or headline
        has_catalyst_val = news_info.get("hasCatalyst")
        catalyst_error = news_info.get("error")
        m.has_catalyst = bool(has_catalyst_val)

        flags = sanity.run_sanity_checks(
            m.last,
            m.prev_close,
            m.change_pct,
            m.spread,
            m.spread_pct,
            m.volume_today,
            prevclose_min=settings.sanity_prevclose_min,
            change_pct_max=settings.sanity_change_pct_max,
            spread_pct_max=settings.spread_pct_max,
            min_vol_for_high_change=settings.min_vol_for_high_change,
        )
        m.suspect_corporate_action = bool(flags.get("suspectCorporateAction"))
        m.suspect_data = bool(flags.get("suspectData"))

        if has_catalyst_val is True and not catalyst_text:
            catalyst_text = "news"
        if catalyst_error in ("restricted_endpoint_402", "disabled"):
            catalyst_text = "unavailable"
        elif has_catalyst_val is None:
            catalyst_text = "unknown"
        elif not has_catalyst_val and m.suspect_corporate_action and not catalyst_text:
            catalyst_text = "corporate_action_suspect"

        grade, score = grade_and_score(
            m,
            float_max=settings.filters.float_max,
            spread_abs_max=settings.filters.spread_abs_max,
            spread_pct_max=settings.filters.spread_pct_max,
        )
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
            "spreadPct": m.spread_pct,
            "floatShares": m.float_shares,
            "rvol": m.rvol,
            "rvolRaw": m.rvol_raw,
            "rvolCumVol": m.rvol_cumvol_today,
            "rvolBaseline": m.rvol_baseline,
            "rvolMinuteIndex": m.rvol_minute_index,
            "rvolScore": m.rvol_score,
            "rvolDaysValid": m.rvol_days_valid,
            "capApplied": m.rvol_cap_applied,
            "rvolFlags": {
                "baselineLow": m.rvol_baseline_low,
                "insufficientHistory": m.rvol_insufficient_history,
                "sessionMismatch": m.rvol_session_mismatch,
            },
            "hasCatalyst": has_catalyst_val,
            "catalyst": catalyst_text,
            "catalystProvider": news_info.get("provider"),
            "catalystHeadline": headline,
            "catalystSummary": summary,
            "catalystPublishedAt": news_info.get("publishedAt"),
            "catalystSource": news_info.get("source"),
            "catalystUrl": news_info.get("url"),
            "catalystError": news_info.get("error"),
            "suspectCorporateAction": m.suspect_corporate_action,
            "suspectData": m.suspect_data,
            "grade": grade,
            "score": score,
        })

    # order by grade then score desc
    order = {"A": 0, "B": 1, "C": 2, "D": 3}
    final.sort(key=lambda x: (order.get(x["grade"], 9), -float(x["score"])))
    if settings.debug:
        print(f"final after rvol/spread filters: {len(final)}")

    filters_payload = asdict(settings.filters)
    filters_payload["spread_max"] = filters_payload.get("spread_pct_max")
    filters_payload["spreadAbsMax"] = filters_payload.get("spread_abs_max")
    filters_payload["spreadPctMax"] = filters_payload.get("spread_pct_max")

    payload = {
        "run_id": str(uuid.uuid4()),
        "generated_utc": _iso(now_utc),
        "generated_ny": _iso(now_ny),
        "config": {
            "profile": settings.profile_used,
            "debug": settings.debug,
            "time_filters": {
                "enabled": settings.time_filters_enabled,
                "label": settings.time_bucket_label,
                "window": settings.time_bucket_window,
                "tz": settings.time_bucket_tz,
            },
            "ibkr": {
                "host": settings.ib_host,
                "port": settings.ib_port,
                "client_id": settings.ib_client_id,
                "timeout_s": settings.ib_timeout_s,
                "market_data_type": settings.ib_market_data_type,
                "market_data_type_label": ibkr.market_data_type_label(settings.ib_market_data_type),
            },
            "scanner": {
                "instrument": ibkr.SCAN_INSTRUMENT,
                "location_code": ibkr.SCAN_LOCATION_CODE,
                "scan_code": ibkr.SCAN_CODE,
                "max_rows": settings.filters.max_candidates,
            },
        },
        "scan": {
            "raw_candidates": scan_raw_count,
            "candidates": scan_candidates_count,
            "prelim": len(prelim),
            "filtered": len(filtered),
            "final": len(final),
            "invalid_last": invalid_last_count,
            "excluded_otc_pink": excluded_otc_pink,
            "prelim_drop_reasons": drop_reasons,
        },
        "filters": filters_payload,
        "rvol": {
            "anchor_time_ny": settings.rvol_anchor_ny,
            "lookback_days": settings.rvol_lookback_days,
            "use_rth": settings.use_rth,
            "bar_size": settings.rvol_bar_size,
            "session": settings.rvol_session,
            "method": settings.rvol_method,
            "trim_pct": settings.rvol_trim_pct,
            "min_days_valid": settings.rvol_min_history_days,
            "baseline_floor_vol": settings.rvol_min_baseline,
            "cap": settings.rvol_cap,
            "permissive_if_not_live": settings.rvol_permissive_if_not_live,
            "market_data_type": settings.ib_market_data_type,
        },
        "news": {
            "lookback_hours": settings.news_lookback_hours,
            "provider": provider_name,
            "enabled": news_enabled,
            "reason_if_disabled": news_disabled_reason,
        },
        "symbols": final,
        "tradingview": {
            "txt_symbols": [x["tvSymbol"] for x in final],
        },
    }

    ib.disconnect()
    conn.close()
    return payload
