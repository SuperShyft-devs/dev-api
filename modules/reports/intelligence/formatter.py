"""Format / serialize layer for chart footers, leadership cards, and JSON output.

Flat merge (no business-logic changes) of:
  - intelligence/format/format_chart_footer.py
  - intelligence/serialize.py

Imports presentation helpers from ``.generator`` (sanitize / compose_footer /
generate_leadership_cards). Generator lazy-imports this module to avoid cycles.
"""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any

from .generator import (
    compose_footer_text,
    generate_leadership_cards,
    sanitize_insight_text,
)
from .models import ChartNarrative, CompanyHealthProfile, LeadershipTakeawayCard, StructuredInsight


def format_chart_footer(insight: StructuredInsight) -> ChartNarrative:
    """Format structured insight as a single dashboard footer paragraph."""
    text = sanitize_insight_text(compose_footer_text(insight))
    return ChartNarrative(
        tone=insight.tone,
        text=text,
        structured=insight,
        confidence=insight.confidence,
    )


def format_leadership_cards(profile: CompanyHealthProfile) -> list[LeadershipTakeawayCard]:
    """Leadership cards — dedicated generator, not footer reuse."""
    return generate_leadership_cards(profile)


class ReportBlock(dict):
    """Future report block — exposes all three parts separately.

    Behaves like a plain mapping with attribute-style access so callers can use
    either ``block["observation"]`` or ``block.observation``.
    """

    def __getattr__(self, item: str):
        try:
            return self[item]
        except KeyError as exc:  # pragma: no cover - defensive
            raise AttributeError(item) from exc


def format_report_block(insight: StructuredInsight) -> ReportBlock:
    from .generator import ensure_structured_field_punctuation

    observation, explanation, recommendation = ensure_structured_field_punctuation(
        sanitize_insight_text(insight.observation),
        sanitize_insight_text(insight.explanation),
        sanitize_insight_text(insight.recommendation),
    )
    return ReportBlock(
        observation=observation,
        explanation=explanation,
        recommendation=recommendation,
        confidence=insight.confidence,
        tone=insight.tone,
    )


# ---------------------------------------------------------------------------
# serialize.py
# ---------------------------------------------------------------------------


def to_jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(v) for v in value]
    if is_dataclass(value) and not isinstance(value, type):
        return to_jsonable(asdict(value))
    if hasattr(value, "__dict__"):
        return to_jsonable(vars(value))
    return str(value)


def narrative_to_dict(narrative: Any) -> dict:
    return to_jsonable(narrative)


def leadership_to_dict(cards: list) -> list:
    return [to_jsonable(card) for card in cards]
