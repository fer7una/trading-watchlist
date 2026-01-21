from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass
class Metrics:
    symbol: str
    last: Optional[float] = None
    prev_close: Optional[float] = None
    change_pct: Optional[float] = None
    volume_today: Optional[int] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    spread: Optional[float] = None
    spread_pct: Optional[float] = None
    float_shares: Optional[int] = None
    rvol: Optional[float] = None
    rvol_raw: Optional[float] = None
    rvol_score: Optional[float] = None
    rvol_days_valid: Optional[int] = None
    rvol_cap_applied: Optional[bool] = None
    has_catalyst: bool = False
    suspect_corporate_action: bool = False
    suspect_data: bool = False


_GRADE_ORDER = {"A": 0, "B": 1, "C": 2, "D": 3}


def _cap_grade(grade: str, max_grade: str) -> str:
    if _GRADE_ORDER.get(grade, 9) < _GRADE_ORDER.get(max_grade, 9):
        return max_grade
    return grade


def grade_and_score(
    m: Metrics,
    *,
    float_max: int,
    spread_abs_max: float,
    spread_pct_max: float,
) -> Tuple[str, float]:
    """Simple quality score & grade.

    Score weights are heuristic. Adjust once you have data.
    """
    change = m.change_pct or 0.0
    rvol = m.rvol or 0.0
    vol = float(m.volume_today or 0)
    flt = float(m.float_shares or (float_max * 10))
    spread = float(m.spread or 999)

    s_change = min(max(change, 0.0), 50.0) / 50.0
    if m.rvol_score is not None:
        s_rvol = min(max(float(m.rvol_score), 0.0), 1.0)
    else:
        s_rvol = min(max(rvol, 0.0), 10.0) / 10.0
    s_vol = min(vol / 5_000_000.0, 1.0)
    s_float = min(float_max / flt, 1.0)
    effective_spread_max = float(spread_abs_max or 0.0)
    if effective_spread_max <= 0 and spread_pct_max > 0 and m.last:
        effective_spread_max = float(m.last) * float(spread_pct_max)
    s_spread = max(0.0, 1.0 - (spread / effective_spread_max)) if effective_spread_max > 0 else 0.0

    score = (0.30 * s_change) + (0.30 * s_rvol) + (0.20 * s_vol) + (0.15 * s_float) + (0.05 * s_spread)
    if m.has_catalyst:
        score += 0.05
    if m.suspect_corporate_action:
        score -= 0.25
    if m.suspect_data:
        score -= 0.15
    score = min(max(score, 0.0), 1.0)

    # Grade thresholds (Ross-ish)
    spread_ok = True if effective_spread_max <= 0 else spread <= effective_spread_max
    spread_ok_loose = True if effective_spread_max <= 0 else spread <= (effective_spread_max * 1.5)

    if (change >= 15 and rvol >= 5 and (m.volume_today or 0) >= 1_000_000 and (m.float_shares or 999999999) <= float_max and spread_ok):
        grade = "A"
    elif (change >= 10 and rvol >= 3 and (m.volume_today or 0) >= 500_000 and spread_ok_loose):
        grade = "B"
    elif (change >= 7 and rvol >= 2):
        grade = "C"
    else:
        grade = "D"

    if m.suspect_corporate_action or m.suspect_data:
        grade = _cap_grade(grade, "C")
    return grade, score
