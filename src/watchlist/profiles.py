from __future__ import annotations

import os
from dataclasses import replace
from datetime import datetime
from typing import Mapping

from .market_phase import MarketPhase, get_market_phase
from .settings import Filters, RuntimeSettings, load_settings


_PROFILE_FIELDS = (
    "PRICE_MIN",
    "PRICE_MAX",
    "FLOAT_MAX",
    "CHANGE_MIN_PCT",
    "VOLUME_MIN",
    "RVOL_MIN",
    "RVOL_ANCHOR_NY",
    "USE_RTH",
    "SPREAD_PCT_MAX",
    "SPREAD_ABS_MAX",
    "SPREAD_MAX",
    "MAX_CANDIDATES",
    "MAX_RVOL_SYMBOLS",
)


def _env_key(prefix: str, name: str) -> str:
    prefix = prefix.strip().upper()
    if prefix.endswith("_"):
        prefix = prefix[:-1]
    return f"{prefix}_{name}" if prefix else name


def _has_profile_overrides(prefix: str, env: Mapping[str, str]) -> bool:
    for name in _PROFILE_FIELDS:
        if env.get(_env_key(prefix, name)):
            return True
    return False


def _parse_bool(value: str) -> bool:
    return value.strip() == "1"


def load_profile(
    prefix: str,
    env: Mapping[str, str] | None = None,
    base_settings: RuntimeSettings | None = None,
) -> RuntimeSettings:
    env = env or os.environ
    base_settings = base_settings or load_settings()

    def _get_value(name: str, default: str) -> str:
        v = env.get(_env_key(prefix, name))
        return v if v not in (None, "") else default

    def _get_optional(name: str) -> str | None:
        v = env.get(_env_key(prefix, name))
        return v if v not in (None, "") else None

    base_filters = base_settings.filters
    spread_pct_raw = _get_optional("SPREAD_PCT_MAX")
    spread_abs_raw = _get_optional("SPREAD_ABS_MAX")
    legacy_spread_raw = _get_optional("SPREAD_MAX")
    spread_pct_max = None
    used_legacy_spread = False
    if spread_pct_raw is not None:
        spread_pct_max = float(spread_pct_raw)
    elif legacy_spread_raw is not None:
        spread_pct_max = float(legacy_spread_raw)
        used_legacy_spread = True
    else:
        spread_pct_max = base_filters.spread_pct_max
    spread_abs_max = float(spread_abs_raw) if spread_abs_raw is not None else base_filters.spread_abs_max
    if used_legacy_spread and base_settings.debug:
        print(f"DEBUG: {_env_key(prefix, 'SPREAD_MAX')} is deprecated; treating as SPREAD_PCT_MAX.")

    filters = Filters(
        price_min=float(_get_value("PRICE_MIN", str(base_filters.price_min))),
        price_max=float(_get_value("PRICE_MAX", str(base_filters.price_max))),
        change_min_pct=float(_get_value("CHANGE_MIN_PCT", str(base_filters.change_min_pct))),
        volume_min=int(_get_value("VOLUME_MIN", str(base_filters.volume_min))),
        rvol_min=float(_get_value("RVOL_MIN", str(base_filters.rvol_min))),
        float_max=int(_get_value("FLOAT_MAX", str(base_filters.float_max))),
        spread_abs_max=spread_abs_max,
        spread_pct_max=spread_pct_max,
        max_candidates=int(_get_value("MAX_CANDIDATES", str(base_filters.max_candidates))),
        max_rvol_symbols=int(_get_value("MAX_RVOL_SYMBOLS", str(base_filters.max_rvol_symbols))),
    )

    rvol_anchor_ny = _get_value("RVOL_ANCHOR_NY", base_settings.rvol_anchor_ny)
    use_rth = _parse_bool(_get_value("USE_RTH", "1" if base_settings.use_rth else "0"))

    return replace(
        base_settings,
        rvol_anchor_ny=rvol_anchor_ny,
        use_rth=use_rth,
        filters=filters,
        profile_used=prefix.upper(),
    )


def resolve_effective_profile(profile_mode: str | None, now_utc: datetime) -> tuple[str, RuntimeSettings]:
    base_settings = load_settings()
    env = os.environ
    open_raw = env.get("OPEN", "").strip()
    if open_raw in ("0", "1"):
        prefix = "OPEN" if open_raw == "1" else "PRE"
        settings = load_profile(prefix, env=env, base_settings=base_settings)
        return prefix, settings

    mode = (profile_mode or env.get("PROFILE", "auto")).strip().lower()
    phase = get_market_phase(now_utc)
    force_profile = env.get("FORCE_PROFILE", "0") == "1"

    def _auto_prefix() -> str:
        if phase == MarketPhase.OPEN:
            return "OPEN"
        if phase == MarketPhase.POST and _has_profile_overrides("POST", env):
            return "POST"
        return "PRE"

    prefix = _auto_prefix()

    if mode in ("auto", ""):
        prefix = _auto_prefix()
    elif mode in ("premarket", "pre"):
        if phase == MarketPhase.OPEN:
            print("WARN: PROFILE=premarket while phase is OPEN; using PRE_ profile in session.")
        prefix = "PRE"
    elif mode == "open":
        if phase != MarketPhase.OPEN and not force_profile:
            print("WARN: PROFILE=open but phase is not OPEN; forcing PRE_ (set FORCE_PROFILE=1 to override).")
            prefix = "PRE"
        else:
            if phase != MarketPhase.OPEN:
                print("WARN: PROFILE=open forced outside OPEN; results may be empty.")
            prefix = "OPEN"
    elif mode == "closed":
        prefix = "PRE"
    else:
        print(f"WARN: Unknown PROFILE={mode!r}; defaulting to auto.")
        prefix = _auto_prefix()

    settings = load_profile(prefix, env=env, base_settings=base_settings)
    return prefix, settings
