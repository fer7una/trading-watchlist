#!/usr/bin/env python3
from __future__ import annotations

import os
from watchlist.settings import load_settings
from watchlist.builder import build_watchlist
from watchlist import output


def main() -> int:
    settings = load_settings()
    payload = build_watchlist(settings)

    out_dir = settings.out_dir
    output.write_json(out_dir, payload)
    output.write_tradingview_txt(out_dir, payload["tradingview"]["txt_symbols"])

    print(f"OK: {len(payload['symbols'])} symbols")
    print(f"out_dir: {os.path.abspath(out_dir)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
