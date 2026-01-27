from __future__ import annotations

import json
import os
from dataclasses import asdict
from typing import List, Optional

from .scoring import Metrics


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def tv_symbol(symbol: str, primary_exchange: Optional[str]) -> str:
    if not primary_exchange:
        return f"NYSE:{symbol}"
    pe = primary_exchange.upper()
    if pe == "SMART":
        return symbol
    if "NASDAQ" in pe:
        return f"NASDAQ:{symbol}"
    if "NYSE" in pe:
        return f"NYSE:{symbol}"
    if "AMEX" in pe or "ARCA" in pe:
        return f"AMEX:{symbol}"
    return f"NYSE:{symbol}"


def write_json(out_dir: str, payload: dict) -> str:
    ensure_dir(out_dir)
    p = os.path.join(out_dir, "watchlist.json")
    with open(p, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    return p


def write_tradingview_txt(out_dir: str, tv_symbols: List[str]) -> str:
    ensure_dir(out_dir)
    p = os.path.join(out_dir, "tradingview_import.txt")
    with open(p, "w", encoding="utf-8") as f:
        f.write(",".join(tv_symbols))
    return p
