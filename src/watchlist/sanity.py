from __future__ import annotations

import math
from typing import Optional


def _is_bad_number(value: Optional[float]) -> bool:
    if value is None:
        return True
    try:
        return math.isnan(value) or math.isinf(value)
    except TypeError:
        return True


def run_sanity_checks(
    last: Optional[float],
    prev_close: Optional[float],
    change_pct: Optional[float],
    spread: Optional[float],
    spread_pct: Optional[float],
    volume_today: Optional[int],
    *,
    prevclose_min: float,
    change_pct_max: float,
    spread_pct_max: float,
    min_vol_for_high_change: int,
) -> dict:
    suspect_corporate_action = False
    suspect_data = False

    if _is_bad_number(last) or _is_bad_number(prev_close):
        suspect_data = True
    if not _is_bad_number(prev_close) and not _is_bad_number(change_pct):
        if prev_close < prevclose_min and change_pct > change_pct_max:
            suspect_corporate_action = True

    if spread_pct is not None and spread_pct_max > 0 and spread_pct > spread_pct_max:
        suspect_data = True

    if min_vol_for_high_change > 0 and not _is_bad_number(change_pct) and volume_today is not None:
        if change_pct > 80 and volume_today < min_vol_for_high_change:
            suspect_data = True

    return {
        "suspectCorporateAction": suspect_corporate_action,
        "suspectData": suspect_data,
    }
