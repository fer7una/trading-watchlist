from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from watchlist import rvol

NY = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


def _bar(date_ny: date, hour: int, minute: int, vol: int) -> tuple[datetime, int]:
    dt_ny = datetime(date_ny.year, date_ny.month, date_ny.day, hour, minute, tzinfo=NY)
    return dt_ny.astimezone(UTC), vol


def test_trimmed_mean_basic() -> None:
    vals = [1, 2, 100, 101, 102]
    assert rvol.trimmed_mean(vals, 0.2) == pytest.approx((2 + 100 + 101) / 3)


def test_build_baseline_curve_mean() -> None:
    now_ny = datetime(2024, 6, 5, 10, 0, tzinfo=NY)
    bars = []
    for d, vols in [
        (date(2024, 6, 4), [100, 100]),
        (date(2024, 6, 3), [200, 200]),
        (date(2024, 5, 31), [300, 300]),
    ]:
        for i, v in enumerate(vols):
            bars.append(_bar(d, 9, 30 + i, v))

    curve = rvol.build_baseline_curve_from_bars(
        symbol="TEST",
        bars_utc=bars,
        now_ny=now_ny,
        session="RTH",
        bar_size="1 min",
        lookback_days=3,
        method="mean",
        trim_pct=0.0,
        min_history_days=1,
        min_baseline=0,
        calendar_name="NYSE",
    )

    assert len(curve.baseline_cumvol) == 390
    assert curve.baseline_cumvol[0] == pytest.approx(200.0)
    assert curve.baseline_cumvol[1] == pytest.approx(400.0)


def test_rvol_time_of_day_ratio() -> None:
    now_ny = datetime(2024, 6, 5, 9, 31, tzinfo=NY)
    bars = [
        _bar(date(2024, 6, 5), 9, 30, 10_000),
        _bar(date(2024, 6, 5), 9, 31, 10_000),
    ]
    curve = rvol.BaselineCurve(
        symbol="TEST",
        session="RTH",
        bar_size="1 min",
        lookback_days=10,
        method="mean",
        trim_pct=0.0,
        updated_at=now_ny.astimezone(UTC),
        baseline_cumvol=[10_000.0] * 390,
        history_days_used=10,
        notes=None,
    )

    result = rvol.compute_rvol_time_of_day(
        symbol="TEST",
        bars_utc=bars,
        baseline_curve=curve,
        now_ny=now_ny,
        session="RTH",
        bar_size="1 min",
        min_history_days=10,
        min_baseline=1_000,
        cap=None,
    )

    assert result.minute_index == 1
    assert result.cumvol_today == 20_000
    assert result.baseline_cumvol == pytest.approx(10_000.0)
    assert result.rvol_raw == pytest.approx(2.0)


def test_rvol_baseline_low_flag() -> None:
    now_ny = datetime(2024, 6, 5, 9, 31, tzinfo=NY)
    bars = [
        _bar(date(2024, 6, 5), 9, 30, 500),
        _bar(date(2024, 6, 5), 9, 31, 500),
    ]
    curve = rvol.BaselineCurve(
        symbol="TEST",
        session="RTH",
        bar_size="1 min",
        lookback_days=5,
        method="median",
        trim_pct=0.1,
        updated_at=now_ny.astimezone(UTC),
        baseline_cumvol=[1_000.0] * 390,
        history_days_used=5,
        notes=None,
    )

    result = rvol.compute_rvol_time_of_day(
        symbol="TEST",
        bars_utc=bars,
        baseline_curve=curve,
        now_ny=now_ny,
        session="RTH",
        bar_size="1 min",
        min_history_days=5,
        min_baseline=1_000,
        cap=None,
    )

    assert result.baseline_low is True


def test_rvol_session_mismatch() -> None:
    now_ny = datetime(2024, 6, 5, 8, 0, tzinfo=NY)
    result = rvol.compute_rvol_time_of_day(
        symbol="TEST",
        bars_utc=[],
        baseline_curve=None,
        now_ny=now_ny,
        session="RTH",
        bar_size="1 min",
        min_history_days=10,
        min_baseline=1_000,
        cap=None,
    )
    assert result.session_mismatch is True
    assert result.rvol_raw is None


def test_rvol_time_of_day_integration() -> None:
    now_ny = datetime(2024, 6, 5, 9, 31, tzinfo=NY)
    days = rvol.previous_trading_days(now_ny, lookback_days=10, calendar_name="NYSE")
    bars = []
    for d in days:
        bars.append(_bar(d, 9, 30, 1_000))
        bars.append(_bar(d, 9, 31, 1_000))

    bars.append(_bar(date(2024, 6, 5), 9, 30, 3_000))
    bars.append(_bar(date(2024, 6, 5), 9, 31, 3_000))

    curve = rvol.build_baseline_curve_from_bars(
        symbol="TEST",
        bars_utc=bars,
        now_ny=now_ny,
        session="RTH",
        bar_size="1 min",
        lookback_days=10,
        method="mean",
        trim_pct=0.0,
        min_history_days=5,
        min_baseline=1_000,
        calendar_name="NYSE",
    )

    result = rvol.compute_rvol_time_of_day(
        symbol="TEST",
        bars_utc=bars,
        baseline_curve=curve,
        now_ny=now_ny,
        session="RTH",
        bar_size="1 min",
        min_history_days=5,
        min_baseline=1_000,
        cap=None,
    )

    assert result.rvol_raw is not None
    assert result.rvol_raw > 1.0
