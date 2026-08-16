"""Deterministic 5-point score slab and risk-band helpers."""

from __future__ import annotations

from modules.bioai_report.report_engine.models.knowledge_base import RiskBandName

# Canonical 5-point slabs used across all disease knowledge bases.
SCORE_SLABS: tuple[tuple[int, int], ...] = (
    (0, 5),
    (6, 10),
    (11, 15),
    (16, 20),
    (21, 25),
    (26, 30),
    (31, 35),
    (36, 40),
    (41, 45),
    (46, 50),
    (51, 55),
    (56, 60),
    (61, 65),
    (66, 70),
    (71, 75),
    (76, 80),
    (81, 85),
    (86, 90),
    (91, 95),
    (96, 100),
)

_RISK_BANDS: tuple[tuple[RiskBandName, int, int], ...] = (
    ("healthy", 0, 25),
    ("increased_risk", 26, 50),
    ("high_risk", 51, 75),
    ("very_high_risk", 76, 100),
)

_RISK_DISPLAY: dict[RiskBandName, str] = {
    "healthy": "Healthy",
    "increased_risk": "Increased",
    "high_risk": "High",
    "very_high_risk": "Very High",
}


def clamp_score(score: float | int) -> int:
    """Round and clamp a disease score to the integer 0–100 range."""
    value = int(round(float(score)))
    return max(0, min(100, value))


def score_to_slab(score: float | int) -> tuple[int, int]:
    """Map a score to its exclusive 5-point slab (lo, hi), e.g. 67 → (66, 70)."""
    value = clamp_score(score)
    for lo, hi in SCORE_SLABS:
        if lo <= value <= hi:
            return lo, hi
    return 96, 100


def slab_key(lo: int, hi: int) -> str:
    """KB score-band object key, e.g. (66, 70) → '66_70'."""
    return f"{lo}_{hi}"


def slab_label(lo: int, hi: int) -> str:
    """Human-readable band label, e.g. (66, 70) → '66-70'."""
    return f"{lo}-{hi}"


def score_to_risk_band(score: float | int) -> RiskBandName:
    """Map a score to its risk-band name (healthy / increased_risk / …)."""
    value = clamp_score(score)
    for name, lo, hi in _RISK_BANDS:
        if lo <= value <= hi:
            return name
    return "very_high_risk"


def risk_band_range(name: RiskBandName) -> str:
    """Return the inclusive range string for a risk band, e.g. '51-75'."""
    for band_name, lo, hi in _RISK_BANDS:
        if band_name == name:
            return f"{lo}-{hi}"
    return "76-100"


def risk_display_label(name: RiskBandName) -> str:
    """Frontend-friendly risk label derived from the risk band."""
    return _RISK_DISPLAY[name]
