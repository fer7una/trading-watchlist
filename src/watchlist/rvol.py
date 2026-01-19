from __future__ import annotations

from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Tuple

NY = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


def minute_index(dt_ny: datetime, anchor: time) -> int:
    anchor_dt = dt_ny.replace(hour=anchor.hour, minute=anchor.minute, second=0, microsecond=0)
    return int((dt_ny - anchor_dt).total_seconds() // 60)


def compute_rvol_time_of_day(
    bars_utc: List[Tuple[datetime, int]],
    *,
    anchor_ny: time,
    lookback_days: int,
    now_ny: datetime,
) -> Optional[float]:
    """Compute time-of-day RVOL.

    RVOL(t) = cumVol_today(t) / avg(cumVol_pastDays(t))
    where t is the minute index from anchor_ny.
    """
    if not bars_utc:
        return None

    today = now_ny.date()
    idx_now = minute_index(now_ny, anchor_ny)
    if idx_now < 0:
        return None

    # bucket per NY-date and minute index
    vols: Dict[datetime.date, Dict[int, int]] = {}
    for ts_utc, vol in bars_utc:
        ts_ny = ts_utc.astimezone(NY)
        idx = minute_index(ts_ny, anchor_ny)
        if idx < 0:
            continue
        d = ts_ny.date()
        vols.setdefault(d, {})[idx] = vols.setdefault(d, {}).get(idx, 0) + int(vol)

    def cum_for_day(d: datetime.date) -> int:
        m = vols.get(d, {})
        return sum(m.get(i, 0) for i in range(0, idx_now + 1))

    today_cum = cum_for_day(today)
    if today_cum <= 0:
        return None

    past_days = sorted([d for d in vols.keys() if d != today], reverse=True)
    past_days = past_days[:lookback_days]
    if len(past_days) < max(3, lookback_days // 3):
        return None

    avg_cum = sum(cum_for_day(d) for d in past_days) / len(past_days)
    if avg_cum <= 0:
        return None

    return float(today_cum / avg_cum)
