"""Classify a specific-IgE result with a verified assay profile.

The allergen knowledge base is not a source of cutoffs. Without a verified
profile, the result stays indeterminate.
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import Any

DEFAULT_RULES = (
    Path(__file__).resolve().parent.parent
    / "knowledge_base"
    / "allergy"
    / "assay_interpretation_rules.json"
)


def load_assay_rules(path: Path | None = None) -> dict[str, Any]:
    payload = json.loads((path or DEFAULT_RULES).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("assay interpretation rules must be an object")
    return payload


def interpret_specific_ige(
    *,
    rules: dict[str, Any],
    laboratory_id: str | None,
    allergen_id: str | None,
    parse_status: str,
    comparator: str | None,
    number: Decimal | None,
    unit: str | None,
    lab_flag: str | None,
    lab_class: str | None,
) -> dict[str, Any]:
    if parse_status == "missing":
        return _outcome("missing", None, "not_applicable", None)
    if parse_status == "invalid" or number is None:
        return _outcome("invalid", None, "not_applicable", "value could not be parsed")

    profile, rule_id = _profile(rules, laboratory_id, allergen_id)
    if profile is None or profile.get("status") != "verified":
        return _outcome(
            "indeterminate",
            rule_id,
            "unverified",
            "no verified assay rule for this result",
        )
    profile_unit = _unit(profile.get("unit"))
    if profile_unit is None or _unit(unit) != profile_unit:
        return _outcome("indeterminate", rule_id, "unverified", "unit does not match the verified assay")

    cutoff = profile.get("positive_cutoff")
    interval_status = _interval_status(comparator, number, cutoff)
    flag_status = _flag_status(lab_flag) if profile.get("accept_laboratory_flag") else None
    if flag_status and interval_status and flag_status != interval_status:
        if {flag_status, interval_status} <= {"positive", "negative_per_lab"}:
            return _outcome(
                "indeterminate",
                rule_id,
                "unverified",
                "contradictory laboratory flag and value",
            )
    status = flag_status or interval_status or "indeterminate"
    derived_class = _class_for_interval(profile.get("classes") or [], comparator, number)
    if lab_class and derived_class and lab_class != derived_class:
        return _outcome(
            "indeterminate",
            rule_id,
            "unverified",
            "contradictory laboratory class and validated class table",
            lab_class=lab_class,
        )
    return _outcome(
        status if status != "indeterminate" or interval_status is None else "indeterminate",
        rule_id,
        "verified" if status in {"positive", "negative_per_lab"} else "unverified",
        None if status in {"positive", "negative_per_lab"} else "result interval is not inside one classification",
        lab_class=lab_class or derived_class,
    )


def _profile(
    rules: dict[str, Any],
    laboratory_id: str | None,
    allergen_id: str | None,
) -> tuple[dict[str, Any] | None, str | None]:
    laboratories = rules.get("laboratories") if isinstance(rules.get("laboratories"), list) else []
    laboratory = next(
        (
            item
            for item in laboratories
            if isinstance(item, dict) and item.get("laboratory_id") == laboratory_id
        ),
        None,
    )
    if laboratory is None:
        return None, None
    profiles = laboratory.get("profiles") if isinstance(laboratory.get("profiles"), dict) else {}
    analyte_rules = laboratory.get("analyte_rules") if isinstance(laboratory.get("analyte_rules"), dict) else {}
    selected = analyte_rules.get(allergen_id) if allergen_id else None
    if isinstance(selected, dict) and isinstance(selected.get("override"), dict):
        override = dict(selected["override"])
        override.setdefault("status", "verified")
        return override, f"{laboratory_id}:{allergen_id}:override"
    profile_id = "specific_ige_default"
    if isinstance(selected, dict) and selected.get("profile_id"):
        profile_id = str(selected["profile_id"])
    profile = profiles.get(profile_id)
    if not isinstance(profile, dict):
        return None, f"{laboratory_id}:{profile_id}"
    return profile, f"{laboratory_id}:{profile_id}"


def _interval_status(
    comparator: str | None,
    number: Decimal,
    cutoff: Any,
) -> str | None:
    if not isinstance(cutoff, dict) or cutoff.get("value") is None or not cutoff.get("comparator"):
        return None
    try:
        limit = Decimal(str(cutoff["value"]))
    except Exception:
        return None
    operator = str(cutoff["comparator"])
    low, high, low_inclusive, high_inclusive = _bounds(comparator, number)
    if _entirely(low, high, low_inclusive, high_inclusive, operator, limit, positive=True):
        return "positive"
    if _entirely(low, high, low_inclusive, high_inclusive, operator, limit, positive=False):
        return "negative_per_lab"
    return "indeterminate"


def _bounds(
    comparator: str | None,
    number: Decimal,
) -> tuple[Decimal | None, Decimal | None, bool, bool]:
    if comparator in {None, "=", "=="}:
        return number, number, True, True
    if comparator == "<":
        return None, number, False, False
    if comparator == "<=":
        return None, number, False, True
    if comparator == ">":
        return number, None, False, False
    if comparator == ">=":
        return number, None, True, False
    return number, number, True, True


def _entirely(
    low: Decimal | None,
    high: Decimal | None,
    low_inclusive: bool,
    high_inclusive: bool,
    operator: str,
    limit: Decimal,
    *,
    positive: bool,
) -> bool:
    """True when every value in the interval has the requested classification."""
    if positive:
        return _above_cutoff(low, low_inclusive, operator, limit, side="low") and _above_cutoff(
            high, high_inclusive, operator, limit, side="high"
        )
    return _below_cutoff(high, high_inclusive, operator, limit, side="high") and _below_cutoff(
        low, low_inclusive, operator, limit, side="low"
    )


def _above_cutoff(
    value: Decimal | None,
    inclusive: bool,
    operator: str,
    limit: Decimal,
    *,
    side: str,
) -> bool:
    if value is None:
        return side == "high"
    if value > limit:
        return True
    if value < limit:
        return False
    if operator == ">":
        return side == "low" and not inclusive
    if operator == ">=":
        if side == "low":
            return True
        return inclusive
    return False


def _below_cutoff(
    value: Decimal | None,
    inclusive: bool,
    operator: str,
    limit: Decimal,
    *,
    side: str,
) -> bool:
    if value is None:
        return side == "low"
    if value < limit:
        return True
    if value > limit:
        return False
    if operator == ">":
        if side == "high":
            return True
        return inclusive
    if operator == ">=":
        return side == "high" and not inclusive
    return False


def _class_for_interval(classes: list[Any], comparator: str | None, number: Decimal) -> str | None:
    matched: set[str] = set()
    for item in classes:
        if not isinstance(item, dict) or not item.get("class_id"):
            continue
        status = _interval_status(comparator, number, item.get("interval"))
        if status == "positive":
            matched.add(str(item["class_id"]))
        elif status == "indeterminate":
            return None
    if len(matched) == 1:
        return next(iter(matched))
    return None


def _flag_status(flag: str | None) -> str | None:
    if not flag:
        return None
    token = flag.strip().lower()
    if token in {"positive", "reactive", "detected", "above"}:
        return "positive"
    if token in {"negative", "non reactive", "non-reactive", "not detected", "below"}:
        return "negative_per_lab"
    if token in {"indeterminate", "equivocal", "borderline"}:
        return "indeterminate"
    return None


def _unit(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    return value.replace(" ", "").replace("µ", "u").replace("μ", "u").lower()


def _outcome(
    status: str,
    rule_id: str | None,
    confidence: str,
    warning: str | None,
    lab_class: str | None = None,
) -> dict[str, Any]:
    return {
        "result_status": status,
        "applied_rule_id": rule_id,
        "interpretation_confidence": confidence,
        "warning": warning,
        "lab_class": lab_class,
    }
