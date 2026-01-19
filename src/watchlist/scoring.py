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
    float_shares: Optional[int] = None
    rvol: Optional[float] = None


def grade_and_score(m: Metrics, *, float_max: int, spread_max: float) -> Tuple[str, float]:
    """Simple quality score & grade.

    Score weights are heuristic. Adjust once you have data.
    """
    change = m.change_pct or 0.0
    rvol = m.rvol or 0.0
    vol = float(m.volume_today or 0)
    flt = float(m.float_shares or (float_max * 10))
    spread = float(m.spread or 999)

    s_change = min(max(change, 0.0), 50.0) / 50.0
    s_rvol = min(max(rvol, 0.0), 10.0) / 10.0
    s_vol = min(vol / 5_000_000.0, 1.0)
    s_float = min(float_max / flt, 1.0)
    s_spread = max(0.0, 1.0 - (spread / spread_max)) if spread_max > 0 else 0.0

    score = (0.30 * s_change) + (0.30 * s_rvol) + (0.20 * s_vol) + (0.15 * s_float) + (0.05 * s_spread)

    # Grade thresholds (Ross-ish)
    if (change >= 15 and rvol >= 5 and (m.volume_today or 0) >= 1_000_000 and (m.float_shares or 999999999) <= float_max and spread <= spread_max):
        return "A", score
    if (change >= 10 and rvol >= 3 and (m.volume_today or 0) >= 500_000 and spread <= (spread_max * 1.5)):
        return "B", score
    if (change >= 7 and rvol >= 2):
        return "C", score
    return "D", score
