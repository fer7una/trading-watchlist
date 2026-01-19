from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class Filters:
    price_min: float
    price_max: float
    change_min_pct: float
    volume_min: int
    rvol_min: float
    float_max: int
    spread_max: float
    max_candidates: int
    max_rvol_symbols: int


@dataclass(frozen=True)
class RuntimeSettings:
    # Paths
    db_path: str
    out_dir: str

    # IBKR
    ib_host: str
    ib_port: int
    ib_client_id: int
    ib_timeout_s: float

    # FMP
    fmp_api_key: str

    # RVOL
    rvol_anchor_ny: str  # HH:MM
    rvol_lookback_days: int
    use_rth: bool

    filters: Filters


def load_settings(project_root: str | None = None) -> RuntimeSettings:
    # Load .env if present
    root = Path(project_root or os.getcwd())
    env_path = root / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    else:
        load_dotenv()  # noop

    def _get(name: str, default: str | None = None) -> str:
        v = os.getenv(name, default)
        if v is None or v == "":
            raise RuntimeError(f"Missing env var: {name}")
        return v

    fmp_key = _get("FMP_API_KEY")

    filters = Filters(
        price_min=float(os.getenv("PRICE_MIN", "2")),
        price_max=float(os.getenv("PRICE_MAX", "20")),
        change_min_pct=float(os.getenv("CHANGE_MIN_PCT", "10")),
        volume_min=int(os.getenv("VOLUME_MIN", "200000")),
        rvol_min=float(os.getenv("RVOL_MIN", "3")),
        float_max=int(os.getenv("FLOAT_MAX", "10000000")),
        spread_max=float(os.getenv("SPREAD_MAX", "0.15")),
        max_candidates=int(os.getenv("MAX_CANDIDATES", "60")),
        max_rvol_symbols=int(os.getenv("MAX_RVOL_SYMBOLS", "30")),
    )

    return RuntimeSettings(
        db_path=os.getenv("WATCHLIST_DB", "./data/watchlist.db"),
        out_dir=os.getenv("OUT_DIR", "./out"),
        ib_host=os.getenv("IB_HOST", "127.0.0.1"),
        ib_port=int(os.getenv("IB_PORT", "7497")),
        ib_client_id=int(os.getenv("IB_CLIENT_ID", "7")),
        ib_timeout_s=float(os.getenv("IB_TIMEOUT_S", "10")),
        fmp_api_key=fmp_key,
        rvol_anchor_ny=os.getenv("RVOL_ANCHOR_NY", "04:00"),
        rvol_lookback_days=int(os.getenv("RVOL_LOOKBACK_DAYS", "20")),
        use_rth=os.getenv("USE_RTH", "0") == "1",
        filters=filters,
    )
