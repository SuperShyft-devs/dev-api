"""Public Nutrition Intelligence Engine integration layer.

Backend usage::

    from nutrition_engine import calculate_nutrition
    result = calculate_nutrition(payload)

This module does not change scoring, indicator weights, mappings, or the
output JSON contract. It only adapts the existing questionnaire payload
(including ``hight_unit``) and returns the existing engine JSON.
"""

from __future__ import annotations

from typing import Any, Mapping

from modules.reports.nutrition_intelligence.engine import (
    run_nutrition_intelligence_from_lookup,
    serialize_health_span_nutrition,
)

__all__ = ["calculate_nutrition"]


def calculate_nutrition(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    """Run the Nutrition Intelligence Engine on a questionnaire payload.

    Accepts the existing ``POST /calculate`` JSON body (stable option codes).
    Returns the existing Health Span nutrition JSON dict.
    """
    if payload is None:
        raise ValueError("payload is required")
    if not isinstance(payload, Mapping):
        raise TypeError("payload must be a JSON object")

    lookup = _to_questionnaire_lookup(payload)
    result = run_nutrition_intelligence_from_lookup(lookup)
    return serialize_health_span_nutrition(result)


def _to_questionnaire_lookup(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Copy the request body into the engine lookup without renaming fields.

    The public API keeps ``hight_unit``. Internally the normalizer expects
    height as ``{"value": ..., "unit": ...}`` (same shape ReportsService uses
    in the questionnaire lookup). Scalar height + unit is wrapped here only.
    """
    lookup = dict(payload)
    height = lookup.get("height")
    if height is not None and not isinstance(height, dict):
        unit = lookup.get("hight_unit")
        if unit is None:
            unit = lookup.get("height_unit")
        lookup["height"] = {"value": height, "unit": unit}
    return lookup
