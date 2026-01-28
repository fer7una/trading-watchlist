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
    spread_abs_max: float
    spread_pct_max: float
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
    ib_market_data_type: int

    # FMP
    fmp_api_key: str
    float_allow_stale_days: int

    # Profiles
    profile_used: str

    # RVOL
    rvol_anchor_ny: str  # HH:MM
    rvol_lookback_days: int
    rvol_bar_size: str
    rvol_throttle_s: float
    use_rth: bool
    rvol_session: str
    rvol_method: str
    rvol_trim_pct: float
    rvol_min_history_days: int
    rvol_min_baseline: int
    rvol_cap: float
    rvol_permissive_if_not_live: bool

    # News
    news_provider: str
    news_lookback_hours: int
    news_debug_top_n: int

    # Sanity checks
    spread_pct_max: float
    sanity_prevclose_min: float
    sanity_change_pct_max: float
    min_vol_for_high_change: int

    # Market calendar
    market_calendar: str

    # Debug
    debug: bool
    # Intraday filters
    time_filters_enabled: bool
    time_bucket_label: str | None
    time_bucket_window: str | None
    time_bucket_tz: str | None
    # Scanner universe
    exclude_otc_pink: bool

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

    open_raw = os.getenv("OPEN", "").strip()
    open_profile = None
    if open_raw in ("0", "1"):
        open_profile = open_raw == "1"

    debug_enabled = os.getenv("DEBUG", "0") in ("1", "true", "True")
    spread_pct_env = os.getenv("SPREAD_PCT_MAX", "").strip()
    spread_abs_env = os.getenv("SPREAD_ABS_MAX", "").strip()
    legacy_spread_env = os.getenv("SPREAD_MAX", "").strip()
    spread_pct_max = None
    used_legacy_spread = False
    if spread_pct_env:
        spread_pct_max = float(spread_pct_env)
    elif legacy_spread_env:
        spread_pct_max = float(legacy_spread_env)
        used_legacy_spread = True
    else:
        spread_pct_max = 0.05
    spread_abs_max = float(spread_abs_env) if spread_abs_env else 0.0
    if used_legacy_spread and debug_enabled:
        print("DEBUG: SPREAD_MAX is deprecated; treating as SPREAD_PCT_MAX.")

    filters = Filters(
        price_min=float(os.getenv("PRICE_MIN", "2")),
        price_max=float(os.getenv("PRICE_MAX", "20")),
        change_min_pct=float(os.getenv("CHANGE_MIN_PCT", "10")),
        volume_min=int(os.getenv("VOLUME_MIN", "200000")),
        rvol_min=float(os.getenv("RVOL_MIN", "3")),
        float_max=int(os.getenv("FLOAT_MAX", "10000000")),
        spread_abs_max=spread_abs_max,
        spread_pct_max=spread_pct_max,
        max_candidates=int(os.getenv("MAX_CANDIDATES", "50")),
        max_rvol_symbols=int(os.getenv("MAX_RVOL_SYMBOLS", "15")),
    )

    rvol_anchor_env = os.getenv("RVOL_ANCHOR_NY")
    use_rth_env = os.getenv("USE_RTH")
    if open_profile is not None:
        rvol_anchor_ny = rvol_anchor_env or ("09:30" if open_profile else "04:00")
        use_rth = (use_rth_env == "1") if use_rth_env not in (None, "") else open_profile
    else:
        rvol_anchor_ny = os.getenv("RVOL_ANCHOR_NY", "04:00")
        use_rth = os.getenv("USE_RTH", "0") == "1"

    min_history_env = os.getenv("RVOL_MIN_HISTORY_DAYS", "") or os.getenv("MIN_RVOL_DAYS_VALID", "")
    min_history_days = int(min_history_env or "10")
    min_baseline_env = os.getenv("RVOL_MIN_BASELINE", "") or os.getenv("RVOL_BASELINE_FLOOR_VOL", "")
    min_baseline = int(min_baseline_env or "1000")
    rvol_method_env = os.getenv("RVOL_BASELINE_METHOD", "") or os.getenv("RVOL_METHOD", "")
    rvol_method = (rvol_method_env or "trimmed_mean").strip().lower()
    rvol_session = os.getenv("RVOL_SESSION", "RTH").strip().upper()

    return RuntimeSettings(
        db_path=os.getenv("WATCHLIST_DB", "./data/watchlist.db"),
        out_dir=os.getenv("OUT_DIR", "./out"),
        ib_host=os.getenv("IB_HOST", "127.0.0.1"),
        ib_port=int(os.getenv("IB_PORT", "7497")),
        ib_client_id=int(os.getenv("IB_CLIENT_ID", "7")),
        ib_timeout_s=float(os.getenv("IB_TIMEOUT_S", "10")),
        ib_market_data_type=int(os.getenv("IB_MARKET_DATA_TYPE", "3")),
        fmp_api_key=fmp_key,
        float_allow_stale_days=int(os.getenv("FLOAT_ALLOW_STALE_DAYS", "14")),
        profile_used="OPEN" if open_profile else "PRE",
        rvol_anchor_ny=rvol_anchor_ny,
        rvol_lookback_days=int(os.getenv("RVOL_LOOKBACK_DAYS", "30")),
        rvol_bar_size=os.getenv("RVOL_BAR_SIZE", "1m"),
        rvol_throttle_s=float(os.getenv("RVOL_THROTTLE_S", "0.25")),
        use_rth=use_rth,
        rvol_session=rvol_session,
        rvol_method=rvol_method,
        rvol_trim_pct=float(os.getenv("RVOL_TRIM_PCT", "0.10")),
        rvol_min_history_days=min_history_days,
        rvol_min_baseline=min_baseline,
        rvol_cap=float(os.getenv("RVOL_CAP", "200.0")),
        rvol_permissive_if_not_live=os.getenv("RVOL_PERMISSIVE_IF_NOT_LIVE", "1") in ("1", "true", "True"),
        news_provider=os.getenv("NEWS_PROVIDER", "fmp").strip().lower(),
        news_lookback_hours=int(os.getenv("NEWS_LOOKBACK_HOURS", "24")),
        news_debug_top_n=int(os.getenv("NEWS_DEBUG_TOP_N", "10")),
        spread_pct_max=spread_pct_max,
        sanity_prevclose_min=float(os.getenv("SANITY_PREVCLOSE_MIN", "1.0")),
        sanity_change_pct_max=float(os.getenv("SANITY_CHANGE_PCT_MAX", "150.0")),
        min_vol_for_high_change=int(os.getenv("MIN_VOL_FOR_HIGH_CHANGE", "50000")),
        market_calendar=os.getenv("MARKET_CALENDAR", "NYSE"),
        debug=debug_enabled,
        time_filters_enabled=False,
        time_bucket_label=None,
        time_bucket_window=None,
        time_bucket_tz=None,
        exclude_otc_pink=os.getenv("EXCLUDE_OTC_PINK", "1") in ("1", "true", "True"),
        filters=filters,
    )
