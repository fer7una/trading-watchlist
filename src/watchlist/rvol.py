from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, time
from zoneinfo import ZoneInfo
from typing import Dict, Iterable, List, Optional, Tuple

import pandas_market_calendars as mcal

NY = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")
RTH_OPEN = time(9, 30)
RTH_CLOSE = time(16, 0)
PRE_OPEN = time(4, 0)


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


@dataclass(frozen=True)
class BaselineCurve:
    symbol: str
    session: str
    bar_size: str
    lookback_days: int
    method: str
    trim_pct: float
    updated_at: datetime
    baseline_cumvol: List[float]
    history_days_used: int
    notes: Optional[str] = None


@dataclass(frozen=True)
class DayVolumeSeries:
    date: str
    vol_1m: List[int]
    cumvol_1m: List[int]
    missing_bars: int


@dataclass(frozen=True)
class RvolTimeOfDayResult:
    symbol: str
    minute_index: Optional[int]
    cumvol_today: Optional[int]
    baseline_cumvol: Optional[float]
    rvol_raw: Optional[float]
    rvol: Optional[float]
    history_days_used: int
    baseline_low: bool
    insufficient_history: bool
    session_mismatch: bool
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


def _session_times(session: str) -> Tuple[time, time]:
    session_clean = (session or "RTH").strip().upper()
    if session_clean == "RTH+PRE":
        return PRE_OPEN, RTH_CLOSE
    return RTH_OPEN, RTH_CLOSE


def _session_minutes(session: str) -> int:
    start, end = _session_times(session)
    return int((datetime.combine(date.today(), end) - datetime.combine(date.today(), start)).total_seconds() // 60)


def session_bar_count(session: str, bar_size: str) -> int:
    return max(1, int(_session_minutes(session) / _parse_bar_minutes(bar_size)))


def minute_index_in_session(now_ny: datetime, session: str) -> Optional[int]:
    start, end = _session_times(session)
    t = now_ny.timetz().replace(tzinfo=None)
    if t < start or t >= end:
        return None
    start_dt = datetime.combine(now_ny.date(), start)
    return int((datetime.combine(now_ny.date(), t) - start_dt).total_seconds() // 60)


def compute_rvol_time_of_day_legacy(
    bars_utc: List[Tuple[datetime, int]],
    *,
    anchor_ny: time,
    lookback_days: int,
    now_ny: datetime,
) -> Optional[float]:
    """Compute time-of-day RVOL (legacy).

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
    import re

    s = (bar_size or "").strip().lower()
    if not s:
        return 1
    parts = s.split()
    if len(parts) >= 2:
        try:
            mins = int(parts[0])
            return max(1, mins)
        except ValueError:
            return 1
    m = re.match(r"^(\d+)", s)
    if m:
        try:
            return max(1, int(m.group(1)))
        except ValueError:
            return 1
    return 1


def previous_trading_days(now_ny: datetime, lookback_days: int, calendar_name: str | None = None) -> List[date]:
    cal = mcal.get_calendar(calendar_name or "NYSE")
    start_date = now_ny.date() - timedelta(days=max(lookback_days * 3, 10))
    schedule = cal.schedule(start_date=start_date, end_date=now_ny.date())
    if schedule.empty:
        return []
    days = [d.date() for d in schedule.index]
    days = [d for d in days if d < now_ny.date()]
    return days[-lookback_days:]


def get_intraday_1m_volume(
    bars_utc: Iterable[Tuple[datetime, int]],
    *,
    date_ny: date,
    session: str,
    bar_size: str,
) -> Optional[DayVolumeSeries]:
    bar_minutes = _parse_bar_minutes(bar_size)
    if bar_minutes <= 0:
        return None
    start, end = _session_times(session)
    expected_bars = max(1, int(_session_minutes(session) / bar_minutes))
    vols = [0 for _ in range(expected_bars)]
    counts = [0 for _ in range(expected_bars)]
    found_any = False

    for ts_utc, vol in bars_utc:
        ts_ny = ts_utc.astimezone(NY)
        if ts_ny.date() != date_ny:
            continue
        t = ts_ny.timetz().replace(tzinfo=None)
        if t < start or t >= end:
            continue
        found_any = True
        mins_since = int((datetime.combine(date_ny, t) - datetime.combine(date_ny, start)).total_seconds() // 60)
        idx = mins_since // bar_minutes
        if 0 <= idx < expected_bars:
            vols[idx] += int(vol or 0)
            counts[idx] += 1

    if not found_any:
        return None

    missing_bars = sum(1 for c in counts if c == 0)
    cumvol: List[int] = []
    running = 0
    for v in vols:
        running += int(v)
        cumvol.append(running)

    return DayVolumeSeries(
        date=date_ny.isoformat(),
        vol_1m=vols,
        cumvol_1m=cumvol,
        missing_bars=missing_bars,
    )


def build_baseline_curve_from_bars(
    *,
    symbol: str,
    bars_utc: Iterable[Tuple[datetime, int]],
    now_ny: datetime,
    session: str,
    bar_size: str,
    lookback_days: int,
    method: str,
    trim_pct: float,
    min_history_days: int,
    min_baseline: int,
    calendar_name: str | None = None,
) -> BaselineCurve:
    bars_list = list(bars_utc)
    days = previous_trading_days(now_ny, lookback_days, calendar_name=calendar_name)
    series_list: List[DayVolumeSeries] = []
    missing_total = 0
    for d in days:
        series = get_intraday_1m_volume(bars_list, date_ny=d, session=session, bar_size=bar_size)
        if series is None:
            continue
        series_list.append(series)
        missing_total += series.missing_bars

    history_days_used = len(series_list)
    expected_bars = max(1, int(_session_minutes(session) / _parse_bar_minutes(bar_size)))
    baseline_cumvol: List[float] = []
    notes_parts: List[str] = []

    for t in range(expected_bars):
        samples = [float(s.cumvol_1m[t]) for s in series_list if t < len(s.cumvol_1m)]
        base = select_method(method, samples, trim_pct) or 0.0
        if base < float(min_baseline):
            base = float(min_baseline)
        baseline_cumvol.append(float(base))

    if history_days_used < max(1, min_history_days):
        notes_parts.append("insufficient_history")
    if missing_total > 0:
        notes_parts.append(f"missing_bars={missing_total}")
    if history_days_used == 0:
        notes_parts.append("no_history")

    return BaselineCurve(
        symbol=symbol,
        session=(session or "RTH").strip().upper(),
        bar_size=bar_size,
        lookback_days=lookback_days,
        method=(method or "median").strip().lower(),
        trim_pct=float(trim_pct),
        updated_at=datetime.now(tz=UTC),
        baseline_cumvol=baseline_cumvol,
        history_days_used=history_days_used,
        notes=";".join(notes_parts) if notes_parts else None,
    )


def compute_rvol_time_of_day(
    *,
    symbol: str,
    bars_utc: Iterable[Tuple[datetime, int]],
    baseline_curve: Optional[BaselineCurve],
    now_ny: datetime,
    session: str,
    bar_size: str,
    min_history_days: int,
    min_baseline: int,
    cap: Optional[float] = None,
) -> RvolTimeOfDayResult:
    bars_list = list(bars_utc)
    minute_idx = minute_index_in_session(now_ny, session)
    if minute_idx is None:
        return RvolTimeOfDayResult(
            symbol=symbol,
            minute_index=None,
            cumvol_today=None,
            baseline_cumvol=None,
            rvol_raw=None,
            rvol=None,
            history_days_used=baseline_curve.history_days_used if baseline_curve else 0,
            baseline_low=False,
            insufficient_history=True if baseline_curve is None else baseline_curve.history_days_used < min_history_days,
            session_mismatch=True,
            cap_applied=False,
        )

    today_series = get_intraday_1m_volume(
        bars_list,
        date_ny=now_ny.date(),
        session=session,
        bar_size=bar_size,
    )
    cumvol_today = None
    if today_series and minute_idx < len(today_series.cumvol_1m):
        cumvol_today = int(today_series.cumvol_1m[minute_idx])

    baseline_val = None
    history_days = baseline_curve.history_days_used if baseline_curve else 0
    if baseline_curve and minute_idx < len(baseline_curve.baseline_cumvol):
        baseline_val = float(baseline_curve.baseline_cumvol[minute_idx])

    rvol_raw = None
    rvol = None
    cap_applied = False
    if baseline_val and baseline_val > 0 and cumvol_today is not None:
        rvol_raw = float(cumvol_today / baseline_val)
        rvol = rvol_raw
        if cap is not None and cap > 0 and rvol_raw > cap:
            rvol = float(cap)
            cap_applied = True

    baseline_low = bool(baseline_val is not None and baseline_val <= float(min_baseline))
    insufficient_history = history_days < max(1, min_history_days)

    return RvolTimeOfDayResult(
        symbol=symbol,
        minute_index=minute_idx,
        cumvol_today=cumvol_today,
        baseline_cumvol=baseline_val,
        rvol_raw=rvol_raw,
        rvol=rvol,
        history_days_used=history_days,
        baseline_low=baseline_low,
        insufficient_history=insufficient_history,
        session_mismatch=False,
        cap_applied=cap_applied,
    )


def _baseline_curve_stale(curve: BaselineCurve, now_ny: datetime) -> bool:
    updated_ny = curve.updated_at.astimezone(NY)
    return updated_ny.date() != now_ny.date()


def load_baseline_curve(
    conn,
    *,
    symbol: str,
    session: str,
    bar_size: str,
    lookback_days: int,
    method: str,
    trim_pct: float,
) -> Optional[BaselineCurve]:
    from . import db

    row = db.load_baseline_curve(
        conn,
        symbol=symbol,
        session=session,
        bar_size=bar_size,
        lookback_days=lookback_days,
        method=method,
        trim_pct=trim_pct,
    )
    if not row:
        return None
    updated = datetime.fromisoformat(row["updated_utc"])
    if updated.tzinfo is None:
        updated = updated.replace(tzinfo=UTC)
    return BaselineCurve(
        symbol=row["symbol"],
        session=row["session"],
        bar_size=row["bar_size"],
        lookback_days=row["lookback_days"],
        method=row["method"],
        trim_pct=row["trim_pct"],
        updated_at=updated,
        baseline_cumvol=row["baseline_cumvol"],
        history_days_used=row["history_days_used"],
        notes=row.get("notes"),
    )


def upsert_baseline_curve(conn, curve: BaselineCurve) -> None:
    from . import db

    db.upsert_baseline_curve(
        conn,
        symbol=curve.symbol,
        session=curve.session,
        bar_size=curve.bar_size,
        lookback_days=curve.lookback_days,
        method=curve.method,
        trim_pct=curve.trim_pct,
        updated_utc=curve.updated_at.astimezone(UTC).replace(microsecond=0).isoformat(),
        history_days_used=curve.history_days_used,
        baseline_cumvol=curve.baseline_cumvol,
        notes=curve.notes,
    )


def get_or_build_baseline_curve(
    conn,
    *,
    symbol: str,
    bars_utc: Iterable[Tuple[datetime, int]],
    now_ny: datetime,
    session: str,
    bar_size: str,
    lookback_days: int,
    method: str,
    trim_pct: float,
    min_history_days: int,
    min_baseline: int,
    calendar_name: str | None = None,
) -> BaselineCurve:
    cached = load_baseline_curve(
        conn,
        symbol=symbol,
        session=session,
        bar_size=bar_size,
        lookback_days=lookback_days,
        method=method,
        trim_pct=trim_pct,
    )
    if cached and not _baseline_curve_stale(cached, now_ny):
        return cached

    curve = build_baseline_curve_from_bars(
        symbol=symbol,
        bars_utc=bars_utc,
        now_ny=now_ny,
        session=session,
        bar_size=bar_size,
        lookback_days=lookback_days,
        method=method,
        trim_pct=trim_pct,
        min_history_days=min_history_days,
        min_baseline=min_baseline,
        calendar_name=calendar_name,
    )
    upsert_baseline_curve(conn, curve)
    return curve


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
