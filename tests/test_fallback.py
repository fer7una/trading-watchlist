import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from watchlist.cli import _apply_closed_fallback
from watchlist.market_phase import MarketPhase

UTC = ZoneInfo("UTC")
NY = ZoneInfo("America/New_York")


def test_closed_fallback_last_ok(tmp_path) -> None:
    last_payload = {
        "symbols": [
            {"symbol": "ABC", "tvSymbol": "NYSE:ABC", "primaryExchange": "NYSE"},
        ],
        "tradingview": {"txt_symbols": ["NYSE:ABC"]},
    }
    out_dir = tmp_path
    path = out_dir / "watchlist.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(last_payload, f)

    now_utc = datetime(2024, 6, 3, 12, 0, tzinfo=UTC)
    one_hour_ago = now_utc.timestamp() - 3600
    os.utime(path, (one_hour_ago, one_hour_ago))

    payload = _apply_closed_fallback(
        "last_ok",
        str(out_dir),
        None,
        "market_closed_no_candidates",
        now_utc,
        now_utc.astimezone(NY),
        "PRE",
        MarketPhase.CLOSED,
        {},
        36,
    )

    assert payload["fallback_used"] is True
    assert payload["original"] == last_payload
    assert payload["symbols"] == last_payload["symbols"]
    assert payload["tradingview"]["txt_symbols"] == ["NYSE:ABC"]
