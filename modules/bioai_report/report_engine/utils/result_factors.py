"""Select curated 'What could be affecting your results?' factor sets by score.

These ranges are intentionally independent of risk-band boundaries
(0–25 / 26–50 / 51–75 / 76–100).
"""

from __future__ import annotations

from typing import Literal

from modules.bioai_report.report_engine.utils.score_bands import clamp_score

ResultFactorSetKey = Literal["set_1", "set_2", "set_3", "set_4", "set_5"]

RESULT_FACTOR_TITLE = "What could be affecting your results?"

# Inclusive score ranges → KB result_factor_sets keys.
_RESULT_FACTOR_SETS: tuple[tuple[ResultFactorSetKey, int, int], ...] = (
    ("set_1", 0, 20),
    ("set_2", 21, 40),
    ("set_3", 41, 60),
    ("set_4", 61, 80),
    ("set_5", 81, 100),
)


def score_to_result_factor_set_key(score: float | int) -> ResultFactorSetKey:
    """Map a disease score to its result-factor set key (set_1 … set_5)."""
    value = clamp_score(score)
    for key, lo, hi in _RESULT_FACTOR_SETS:
        if lo <= value <= hi:
            return key
    return "set_5"
