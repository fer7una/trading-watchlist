from datetime import date, timedelta
from zoneinfo import ZoneInfo

from watchlist.market_phase import MarketPhase, get_market_phase, get_schedule_for_date

UTC = ZoneInfo("UTC")


def test_market_phase_regular_day() -> None:
    date_ny = date(2024, 6, 3)
    schedule = get_schedule_for_date(date_ny, calendar_name="NYSE")
    assert schedule

    pre = schedule["pre"]
    market_open = schedule["market_open"]
    market_close = schedule["market_close"]
    post = schedule["post"]

    assert pre is not None
    assert market_open is not None
    assert market_close is not None
    assert post is not None
    assert pre < market_open < market_close < post

    assert (
        get_market_phase((pre + timedelta(minutes=1)).astimezone(UTC), calendar_name="NYSE")
        == MarketPhase.PREMARKET
    )
    assert (
        get_market_phase((market_open + timedelta(minutes=1)).astimezone(UTC), calendar_name="NYSE")
        == MarketPhase.OPEN
    )
    assert (
        get_market_phase((market_close + timedelta(minutes=1)).astimezone(UTC), calendar_name="NYSE")
        == MarketPhase.POST
    )
    assert (
        get_market_phase((post + timedelta(minutes=1)).astimezone(UTC), calendar_name="NYSE")
        == MarketPhase.CLOSED
    )
