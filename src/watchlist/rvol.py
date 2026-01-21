from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
from typing import Dict, Iterable, List, Optional, Tuple

NY = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")
RTH_OPEN = time(9, 30)
RTH_CLOSE = time(16, 0)


@dataclass(frozen=True)
class RvolResult:
    rvol: float
    rvol_raw: float
    days_valid: int
    baseline: float
    baseline_min: float
    baseline_median: float
    baseline_max: float
    today_volume: int
    method: str
    cap_applied: bool


def median(vals: List[float]) -> Optional[float]:
    if not vals:
        return None
    ordered = sorted(vals)
    mid = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return float(ordered[mid])
    return float((ordered[mid - 1] + ordered[mid]) / 2.0)


def trimmed_mean(vals: List[float], trim_pct: float) -> Optional[float]:
    if not vals:
        return None
    pct = max(0.0, min(float(trim_pct or 0.0), 0.49))
    ordered = sorted(vals)
    trim = int(len(ordered) * pct)
    if trim * 2 >= len(ordered):
        return float(sum(ordered) / len(ordered))
    trimmed = ordered[trim : len(ordered) - trim]
    if not trimmed:
        return None
    return float(sum(trimmed) / len(trimmed))


def select_method(method: str, vals: List[float], trim_pct: float) -> Optional[float]:
    method_clean = (method or "median").strip().lower()
    if method_clean == "mean":
        return float(sum(vals) / len(vals)) if vals else None
    if method_clean == "trimmed_mean":
        return trimmed_mean(vals, trim_pct)
    return median(vals)


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


def _parse_time_hhmm(value: str) -> time:
    hh, mm = value.split(":")
    return time(int(hh), int(mm))


def _parse_bar_minutes(bar_size: str) -> int:
    parts = bar_size.strip().split()
    if not parts:
        return 1
    try:
        mins = int(parts[0])
    except ValueError:
        return 1
    return max(1, mins)


def _cap_end_clock(now_ny: datetime, *, use_rth: bool) -> time:
    end_clock = now_ny.timetz().replace(tzinfo=None)
    if end_clock > RTH_CLOSE:
        end_clock = RTH_CLOSE
    if use_rth and end_clock < RTH_OPEN:
        end_clock = RTH_OPEN
    return end_clock


def compute_rvol_from_bars_details(
    bars_utc: Iterable[Tuple[datetime, int]],
    *,
    anchor_time_ny: str,
    lookback_days: int,
    use_rth: bool,
    now_ny: datetime,
    bar_size: str,
    method: str = "median",
    trim_pct: float = 0.15,
    min_days_valid: int = 5,
    baseline_floor_vol: int = 0,
    cap: Optional[float] = None,
) -> Optional[RvolResult]:
    if not bars_utc:
        return None

    anchor = _parse_time_hhmm(anchor_time_ny)
    start_clock = anchor
    if use_rth and start_clock < RTH_OPEN:
        start_clock = RTH_OPEN

    end_clock = _cap_end_clock(now_ny, use_rth=use_rth)
    if end_clock <= start_clock:
        return None

    window_minutes = int(
        (datetime.combine(now_ny.date(), end_clock) - datetime.combine(now_ny.date(), start_clock)).total_seconds()
        // 60
    )
    bar_minutes = _parse_bar_minutes(bar_size)
    expected_bars = max(1, int(window_minutes / bar_minutes))
    min_bars = max(1, int(expected_bars * 0.4))

    vols: Dict[datetime.date, int] = {}
    counts: Dict[datetime.date, int] = {}
    for ts_utc, vol in bars_utc:
        ts_ny = ts_utc.astimezone(NY)
        t = ts_ny.timetz().replace(tzinfo=None)
        if use_rth and (t < RTH_OPEN or t > RTH_CLOSE):
            continue
        if t < start_clock:
            continue
        if t > end_clock:
            continue
        d = ts_ny.date()
        vols[d] = vols.get(d, 0) + int(vol)
        counts[d] = counts.get(d, 0) + 1

    today = now_ny.date()
    today_vol = vols.get(today, 0)
    today_count = counts.get(today, 0)
    if today_vol <= 0 or today_count < min_bars:
        return None

    past_days = sorted([d for d in vols.keys() if d != today], reverse=True)
    valid_past: List[int] = []
    for d in past_days:
        if len(valid_past) >= lookback_days:
            break
        if counts.get(d, 0) < min_bars:
            continue
        vol = vols.get(d, 0)
        if vol <= 0:
            continue
        valid_past.append(vol)

    if len(valid_past) < max(1, min_days_valid) or not valid_past:
        return None

    baseline = select_method(method, valid_past, trim_pct)
    if baseline is None or baseline <= 0:
        return None
    if baseline_floor_vol and baseline < baseline_floor_vol:
        return None

    rvol_raw = float(today_vol / baseline)
    rvol = rvol_raw
    cap_applied = False
    if cap is not None and cap > 0 and rvol_raw > cap:
        rvol = float(cap)
        cap_applied = True

    baseline_min = float(min(valid_past))
    baseline_max = float(max(valid_past))
    baseline_median = float(median(valid_past) or baseline)

    return RvolResult(
        rvol=rvol,
        rvol_raw=rvol_raw,
        days_valid=len(valid_past),
        baseline=float(baseline),
        baseline_min=baseline_min,
        baseline_median=baseline_median,
        baseline_max=baseline_max,
        today_volume=int(today_vol),
        method=(method or "median").strip().lower(),
        cap_applied=cap_applied,
    )


def compute_rvol_from_bars(
    bars_utc: Iterable[Tuple[datetime, int]],
    *,
    anchor_time_ny: str,
    lookback_days: int,
    use_rth: bool,
    now_ny: datetime,
    bar_size: str,
    method: str = "median",
    trim_pct: float = 0.15,
    min_days_valid: int = 5,
    baseline_floor_vol: int = 0,
    cap: Optional[float] = None,
) -> Optional[float]:
    result = compute_rvol_from_bars_details(
        bars_utc,
        anchor_time_ny=anchor_time_ny,
        lookback_days=lookback_days,
        use_rth=use_rth,
        now_ny=now_ny,
        bar_size=bar_size,
        method=method,
        trim_pct=trim_pct,
        min_days_valid=min_days_valid,
        baseline_floor_vol=baseline_floor_vol,
        cap=cap,
    )
    return result.rvol if result else None


def compute_rvol(
    ib,
    symbol: str,
    *,
    anchor_time_ny: str,
    lookback_days: int,
    use_rth: bool,
    now_ny: datetime,
    bar_size: str,
    method: str = "median",
    trim_pct: float = 0.15,
    min_days_valid: int = 5,
    baseline_floor_vol: int = 0,
    cap: Optional[float] = None,
) -> Optional[RvolResult]:
    from . import ibkr

    duration_days = lookback_days + 3
    hist = ibkr.historical_bars_intraday(
        ib,
        symbol,
        duration_days=duration_days,
        use_rth=use_rth,
        bar_size=bar_size,
    )
    bars: List[Tuple[datetime, int]] = []
    for b in hist or []:
        dt = b.date
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=NY)
        bars.append((dt.astimezone(UTC), int(b.volume or 0)))

    return compute_rvol_from_bars_details(
        bars,
        anchor_time_ny=anchor_time_ny,
        lookback_days=lookback_days,
        use_rth=use_rth,
        now_ny=now_ny,
        bar_size=bar_size,
        method=method,
        trim_pct=trim_pct,
        min_days_valid=min_days_valid,
        baseline_floor_vol=baseline_floor_vol,
        cap=cap,
    )
