from __future__ import annotations

import os

from .builder import build_watchlist
from .output import write_json, write_tradingview_txt
from .settings import load_settings


def main() -> int:
    settings = load_settings()
    payload = build_watchlist(settings)

    out_dir = settings.out_dir
    write_json(out_dir, payload)
    write_tradingview_txt(out_dir, payload["tradingview"]["txt_symbols"])

    print(f"OK: {len(payload['symbols'])} symbols")
    print(f"out_dir: {os.path.abspath(out_dir)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
