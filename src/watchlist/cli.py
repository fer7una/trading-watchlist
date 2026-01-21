from __future__ import annotations

import json
import os
import sqlite3
import uuid
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from .builder import build_watchlist
from .market_phase import MarketPhase, get_market_phase, get_schedule_for_date
from .output import tv_symbol, write_json, write_tradingview_txt
from .profiles import resolve_effective_profile

from dotenv import load_dotenv
load_dotenv()

NY = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


def _serialize_schedule_times(schedule: dict[str, datetime | None]) -> dict[str, str | None]:
    return {k: (v.isoformat() if v else None) for k, v in schedule.items()}


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _load_last_watchlist(out_dir: str) -> tuple[dict | None, str | None]:
    path = os.path.join(out_dir, "watchlist.json")
    if not os.path.exists(path):
        return None, None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError:
        return None, path

    if isinstance(data, dict) and data.get("fallback_used") and isinstance(data.get("original"), dict):
        return data["original"], path
    return data if isinstance(data, dict) else None, path


def _load_last_watchlist_from_db(db_path: str) -> tuple[dict | None, datetime | None]:
    if not db_path or not os.path.exists(db_path):
        return None, None
    try:
        conn = sqlite3.connect(db_path)
    except sqlite3.Error:
        return None, None

    try:
        cur = conn.execute(
            """
            SELECT run_id, generated_utc, anchor_time_ny, lookback_days, filters_json
            FROM watchlist_runs
            ORDER BY generated_utc DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if not row:
            return None, None
        run_id, generated_utc, anchor_time_ny, lookback_days, filters_json = row

        cur = conn.execute(
            """
            SELECT wi.symbol, wi.grade, wi.score, wi.last, wi.change_pct, wi.volume_today,
                   wi.rvol, wi.float_shares, wi.spread, s.primary_exchange
            FROM watchlist_items wi
            LEFT JOIN symbols s ON s.symbol = wi.symbol
            WHERE wi.run_id = ?
            """,
            (run_id,),
        )
        rows = cur.fetchall()
        if not rows:
            return None, None

        items = []
        for (
            symbol,
            grade,
            score,
            last,
            change_pct,
            volume_today,
            rvol,
            float_shares,
            spread,
            primary_exchange,
        ) in rows:
            items.append(
                {
                    "symbol": symbol,
                    "primaryExchange": primary_exchange,
                    "tvSymbol": tv_symbol(symbol, primary_exchange),
                    "last": last,
                    "changePct": change_pct,
                    "volumeToday": volume_today,
                    "rvol": rvol,
                    "floatShares": float_shares,
                    "spread": spread,
                    "grade": grade,
                    "score": score,
                }
            )

        order = {"A": 0, "B": 1, "C": 2, "D": 3}
        items.sort(key=lambda x: (order.get(x.get("grade"), 9), -float(x.get("score") or 0.0)))

        payload = {
            "run_id": run_id,
            "generated_utc": generated_utc,
            "symbols": items,
            "tradingview": {"txt_symbols": [x["tvSymbol"] for x in items]},
        }
        if anchor_time_ny or lookback_days is not None:
            payload["rvol"] = {
                "anchor_time_ny": anchor_time_ny,
                "lookback_days": lookback_days,
            }
        if filters_json:
            try:
                payload["filters"] = json.loads(filters_json)
            except json.JSONDecodeError:
                pass

        return payload, _parse_iso(generated_utc)
    except sqlite3.Error:
        return None, None
    finally:
        conn.close()


def _is_stale(path: str, now_utc: datetime, max_hours: float) -> bool:
    mtime = datetime.fromtimestamp(os.path.getmtime(path), tz=UTC)
    return now_utc - mtime > timedelta(hours=max_hours)


def _extract_tv_symbols(payload: dict) -> list[str]:
    tv_symbols = payload.get("tradingview", {}).get("txt_symbols")
    if isinstance(tv_symbols, list):
        return tv_symbols

    symbols = payload.get("symbols", [])
    derived = []
    for item in symbols:
        if not isinstance(item, dict):
            continue
        if item.get("tvSymbol"):
            derived.append(item["tvSymbol"])
            continue
        symbol = item.get("symbol")
        if symbol:
            derived.append(tv_symbol(symbol, item.get("primaryExchange")))
    return derived


def _build_fallback_payload(
    reason: str,
    now_utc: datetime,
    now_ny: datetime,
    profile_used: str,
    phase: MarketPhase,
    schedule_times_ny: dict[str, str | None],
    last_payload: dict | None,
) -> dict:
    symbols = []
    tradingview = {"txt_symbols": []}
    if isinstance(last_payload, dict):
        symbols = list(last_payload.get("symbols", []))
        tv_symbols = last_payload.get("tradingview", {}).get("txt_symbols")
        tradingview = {"txt_symbols": tv_symbols if isinstance(tv_symbols, list) else _extract_tv_symbols(last_payload)}

    payload = {
        "run_id": str(uuid.uuid4()),
        "generated_utc": now_utc.replace(microsecond=0).isoformat(),
        "generated_ny": now_ny.replace(microsecond=0).isoformat(),
        "fallback_used": True,
        "fallback_reason": reason,
        "profile_used": profile_used,
        "phase": profile_used,
        "market_phase": phase.value,
        "schedule_times_ny": schedule_times_ny,
        "symbols": symbols,
        "tradingview": tradingview,
    }
    if isinstance(last_payload, dict):
        payload["original"] = last_payload
    return payload


def _apply_closed_fallback(
    fallback_mode: str,
    out_dir: str,
    db_path: str | None,
    reason: str,
    now_utc: datetime,
    now_ny: datetime,
    profile_used: str,
    phase: MarketPhase,
    schedule_times_ny: dict[str, str | None],
    stale_max_hours: float,
) -> dict:
    mode = (fallback_mode or "last_ok").strip().lower()
    if mode not in ("last_ok", "empty", "research"):
        mode = "last_ok"

    last_payload, last_path = _load_last_watchlist(out_dir)
    if isinstance(last_payload, dict) and not isinstance(last_payload.get("symbols"), list):
        last_payload = None

    if mode == "empty":
        return _build_fallback_payload(
            reason,
            now_utc,
            now_ny,
            profile_used,
            phase,
            schedule_times_ny,
            None,
        )

    if mode == "last_ok":
        if last_payload is not None and last_path and not _is_stale(last_path, now_utc, stale_max_hours):
            return _build_fallback_payload(
                reason,
                now_utc,
                now_ny,
                profile_used,
                phase,
                schedule_times_ny,
                last_payload,
            )

        db_payload, db_generated = _load_last_watchlist_from_db(db_path or "")
        if db_payload and db_generated and now_utc - db_generated <= timedelta(hours=stale_max_hours):
            return _build_fallback_payload(
                reason,
                now_utc,
                now_ny,
                profile_used,
                phase,
                schedule_times_ny,
                db_payload,
            )

        return _build_fallback_payload(
            f"{reason}_stale",
            now_utc,
            now_ny,
            profile_used,
            phase,
            schedule_times_ny,
            None,
        )

    if mode == "research":
        if last_payload is not None:
            return _build_fallback_payload(
                reason,
                now_utc,
                now_ny,
                profile_used,
                phase,
                schedule_times_ny,
                last_payload,
            )

        db_payload, _ = _load_last_watchlist_from_db(db_path or "")
        if db_payload:
            return _build_fallback_payload(
                reason,
                now_utc,
                now_ny,
                profile_used,
                phase,
                schedule_times_ny,
                db_payload,
            )

        return _build_fallback_payload(
            f"{reason}_no_last",
            now_utc,
            now_ny,
            profile_used,
            phase,
            schedule_times_ny,
            None,
        )

    return _build_fallback_payload(
        f"{reason}_no_last",
        now_utc,
        now_ny,
        profile_used,
        phase,
        schedule_times_ny,
        None,
    )


def main() -> int:
    now_utc = datetime.now(tz=UTC)
    profile_used, settings = resolve_effective_profile(None, now_utc)
    profile_mode = os.getenv("PROFILE", "auto").strip().lower()

    now_ny = now_utc.astimezone(NY)
    phase = get_market_phase(now_utc)
    schedule_times = get_schedule_for_date(now_ny.date())
    schedule_times_ny = _serialize_schedule_times(schedule_times)

    payload = build_watchlist(settings)
    payload["profile_used"] = profile_used
    payload["phase"] = profile_used
    payload["market_phase"] = phase.value
    payload["schedule_times_ny"] = schedule_times_ny

    scan_meta = payload.get("scan", {})
    no_candidates = scan_meta.get("candidates", 0) == 0
    final_empty = len(payload.get("symbols", [])) == 0
    invalid_last = scan_meta.get("invalid_last", 0)
    invalid_all = scan_meta.get("candidates", 0) > 0 and invalid_last >= scan_meta.get("candidates", 0)
    require_active = os.getenv("REQUIRE_ACTIVE_MARKETDATA", "0") == "1"
    no_active_data = require_active and invalid_all

    treat_as_closed = profile_mode == "closed" or phase == MarketPhase.CLOSED
    if treat_as_closed and (no_candidates or final_empty or no_active_data):
        if no_candidates:
            reason = "market_closed_no_candidates"
        elif no_active_data:
            reason = "market_closed_no_active_data"
        else:
            reason = "market_closed_filtered_empty"

        fallback_mode = os.getenv("CLOSED_FALLBACK", "last_ok")
        stale_max_hours = float(os.getenv("CLOSED_STALE_MAX_HOURS", "36"))
        print(f"WARN: Closed-market fallback engaged ({fallback_mode}) - {reason}.")
        payload = _apply_closed_fallback(
            fallback_mode,
            settings.out_dir,
            settings.db_path,
            reason,
            now_utc,
            now_ny,
            profile_used,
            phase,
            schedule_times_ny,
            stale_max_hours,
        )

    out_dir = settings.out_dir
    write_json(out_dir, payload)
    write_tradingview_txt(out_dir, payload["tradingview"]["txt_symbols"])

    print(f"OK: {len(payload['symbols'])} symbols")
    print(f"out_dir: {os.path.abspath(out_dir)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
