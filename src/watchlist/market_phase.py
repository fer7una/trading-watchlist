from __future__ import annotations

import os
from datetime import date, datetime
from enum import Enum
from zoneinfo import ZoneInfo

import pandas as pd
import pandas_market_calendars as mcal

NY = ZoneInfo("America/New_York")


class MarketPhase(str, Enum):
    PREMARKET = "PREMARKET"
    OPEN = "OPEN"
    POST = "POST"
    CLOSED = "CLOSED"


def _calendar_name(calendar_name: str | None) -> str:
    return calendar_name or os.getenv("MARKET_CALENDAR", "NYSE")


def get_schedule_for_date(date_ny: date, calendar_name: str | None = None) -> dict[str, datetime | None]:
    cal = mcal.get_calendar(_calendar_name(calendar_name))
    try:
        schedule = cal.schedule(
            start_date=date_ny,
            end_date=date_ny,
            market_times=["pre", "market_open", "market_close", "post"],
        )
    except TypeError:
        schedule = cal.schedule(start_date=date_ny, end_date=date_ny)

    if schedule.empty:
        return {}

    row = schedule.iloc[0]

    def _get(col: str) -> datetime | None:
        if col not in schedule.columns:
            return None
        ts = row[col]
        if ts is None or pd.isna(ts):
            return None
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return ts.tz_convert(NY)

    return {
        "pre": _get("pre"),
        "market_open": _get("market_open"),
        "market_close": _get("market_close"),
        "post": _get("post"),
    }


def get_market_phase(now_utc: datetime, calendar_name: str | None = None) -> MarketPhase:
    now_ny = now_utc.astimezone(NY)
    schedule = get_schedule_for_date(now_ny.date(), calendar_name=calendar_name)
    if not schedule:
        return MarketPhase.CLOSED

    pre = schedule.get("pre") or schedule.get("market_open")
    market_open = schedule.get("market_open")
    market_close = schedule.get("market_close")
    post = schedule.get("post") or schedule.get("market_close")

    if market_open is None or market_close is None:
        return MarketPhase.CLOSED

    if pre is not None and now_ny < pre:
        return MarketPhase.CLOSED
    if pre is not None and now_ny < market_open:
        return MarketPhase.PREMARKET
    if now_ny < market_close:
        return MarketPhase.OPEN
    if post is not None and now_ny < post:
        return MarketPhase.POST
    return MarketPhase.CLOSED
