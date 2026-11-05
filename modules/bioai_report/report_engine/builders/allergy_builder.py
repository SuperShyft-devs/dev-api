"""Normalize raw allergy measurements and build the three report sections.

Measurements come from ``blood_report_raw.digital_data``. Grouped
``blood_parameters`` supply catalogue names only when ``provider_test_name``
matches the raw test name exactly. Numeric grouped values are never used.
"""

from __future__ import annotations

import json
import re
from decimal import Decimal, InvalidOperation
from typing import Any

from modules.bioai_report.report_engine.builders.assay_interpreter import (
    interpret_specific_ige,
    load_assay_rules,
)
from modules.bioai_report.report_engine.knowledge_base.allergy_loader import (
    AllergyKnowledgeBase,
    default_allergy_kb,
)
from modules.bioai_report.report_engine.models.allergy import (
    AllergyAssessment,
    AllergyAvailability,
    AllergyBreakdown,
    AllergyBreakdownGroup,
    AllergyDetailedInsights,
    AllergyInsight,
    AllergyOverview,
    InterpretiveNote,
    AllergyResult,
    AllergySections,
    RecommendationCard,
    TotalIgeResult,
)

_POSITIVE_FLAGS = {"positive", "reactive", "detected", "above"}
_NEGATIVE_FLAGS = {"negative", "non reactive", "non-reactive", "not detected", "below"}
_INDETERMINATE_FLAGS = {"indeterminate", "equivocal", "borderline"}
_CONTINUATION_LIMIT = 40

# Page-3 cards. Titles follow the Bio-AI allergy pages. One-line summaries come
# from the knowledge base except the folded food card, which has no single group.
_PAGE_CARDS: list[dict[str, Any]] = [
    {"id": "allergy_drugs", "title": "Medicines", "source_groups": ["allergy_drugs"]},
    {"id": "pets", "title": "Pets / Animals", "source_groups": ["pets"]},
    {"id": "allergy_insects", "title": "Insects", "source_groups": ["allergy_insects"]},
    {"id": "allergy_dust_mites", "title": "Dust & Mites", "source_groups": ["allergy_dust_mites"]},
    {"id": "allergy_non_veg", "title": "Food (Non-Veg)", "source_groups": ["allergy_non_veg"]},
    {
        "id": "food_veg_dairy",
        "title": "Food (Veg & Dairy)",
        "source_groups": ["allergy_veg", "allergy_dairy", "allergy_seeds_nuts_legumes"],
        "summary": "Milk, wheat, nuts, fruits and vegetables.",
    },
    {"id": "allergy_moulds_yeast", "title": "Moulds & Yeast", "source_groups": ["allergy_moulds_yeast"]},
    {"id": "allergy_grass", "title": "Grass", "source_groups": ["allergy_grass"]},
    {"id": "allergy_weed_tree", "title": None, "source_groups": ["allergy_weed_tree"]},
    {"id": "allergy_spices", "title": None, "source_groups": ["allergy_spices"]},
    {"id": "allergy_occupational", "title": None, "source_groups": ["allergy_occupational"]},
    {"id": "allergy_parasites", "title": None, "source_groups": ["allergy_parasites"]},
]

# Laboratory profile titles used on blood_parameters.group_name.
_GROUP_KEYS = {
    "allergy pets": "pets",
    "allergy pet profile": "pets",
    "allergy drugs": "allergy_drugs",
    "allergy drugs profile": "allergy_drugs",
    "allergy dust mites": "allergy_dust_mites",
    "allergy insects": "allergy_insects",
    "allergy grass": "allergy_grass",
    "allergy grass mix": "allergy_grass",
    "allergy moulds and yeast": "allergy_moulds_yeast",
    "allergy moulds yeast": "allergy_moulds_yeast",
    "allergy weed and tree": "allergy_weed_tree",
    "allergy veg": "allergy_veg",
    "allergy veg food and fruit": "allergy_veg",
    "allergy non veg": "allergy_non_veg",
    "allergy non veg food": "allergy_non_veg",
    "allergy dairy": "allergy_dairy",
    "allergy dairy products": "allergy_dairy",
    "allergy seeds nuts legumes": "allergy_seeds_nuts_legumes",
    "allergy spices": "allergy_spices",
    "allergy occupational": "allergy_occupational",
    "allergy parasites": "allergy_parasites",
    "allergy ige": "ige",
    "ige total": "ige",
}


class AllergyPrepared:
    def __init__(
        self,
        availability: AllergyAvailability | None,
        sections: AllergySections | None,
    ) -> None:
        self.availability = availability
        self.sections = sections


def _screening_flags_present(parameter_groups: list[Any]) -> bool:
    return any(
        isinstance(group, dict)
        and ("has_allergy_screening" in group or "has_cancer_screening" in group)
        for group in parameter_groups
    )


def _is_cancer_group_name(name: Any) -> bool:
    return "cancer" in _norm_group(str(name or ""))


def panels_booked(parameters: Any) -> tuple[bool, bool]:
    """Return whether blood_parameters lists cancer screening and allergy screening.

    ``has_cancer_screening`` / ``has_allergy_screening`` book a group when true.
    A stored false flag does not hide a group whose title is a known allergy
    panel or contains "cancer": normalized bookings always write the flag, and
    older snapshots left real panels at false. Generic containers such as
    "Allergy Profile" still require the allergy flag once flags are present.
    """
    parameter_groups = _as_parameter_groups(parameters)
    if _screening_flags_present(parameter_groups):
        cancer = any(
            isinstance(group, dict)
            and (
                group.get("has_cancer_screening") is True
                or _is_cancer_group_name(group.get("group_name"))
            )
            for group in parameter_groups
        )
        booked = _booked_tests(parameters, default_allergy_kb())
        return cancer, bool(booked["groups"])
    cancer = any(
        isinstance(group, dict) and _is_cancer_group_name(group.get("group_name"))
        for group in parameter_groups
    )
    booked = _booked_tests(parameters, default_allergy_kb())
    return cancer, bool(booked["groups"])


def cancer_measurements_from_booking(booking: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Laboratory rows for cancer panels on this blood booking.

    Values come from ``blood_report_raw.digital_data`` when the test name matches
    a booked cancer group. Grouped numeric values are used only when the raw row
    is absent.
    """
    if not booking:
        return []
    names: set[str] = set()
    grouped_rows: list[dict[str, Any]] = []
    for group in _as_parameter_groups(booking.get("blood_parameters")):
        if not isinstance(group, dict):
            continue
        if group.get("has_cancer_screening") is not True and not _is_cancer_group_name(
            group.get("group_name")
        ):
            continue
        for test in group.get("tests") or []:
            if not isinstance(test, dict):
                continue
            grouped_rows.append(test)
            for key in ("provider_test_name", "test_name"):
                label = test.get(key)
                if isinstance(label, str) and label.strip():
                    names.add(label.strip())
    raw_by_name = {
        str(row.get("test_name") or "").strip(): row
        for row in _digital_data(booking)
        if str(row.get("test_name") or "").strip()
    }
    markers: list[dict[str, Any]] = []
    seen: set[str] = set()
    for name in names:
        row = raw_by_name.get(name)
        if row is None:
            continue
        seen.add(name)
        markers.append(
            {
                "test_name": name,
                "value": row.get("value"),
                "unit": row.get("unit"),
                "reference_low": row.get("min_range"),
                "reference_high": row.get("max_range"),
            }
        )
    for test in grouped_rows:
        catalogue = str(test.get("test_name") or "").strip()
        provider = str(test.get("provider_test_name") or catalogue).strip()
        label = provider or catalogue
        if not label or label in seen or catalogue in seen:
            continue
        if test.get("value") is None:
            continue
        seen.add(label)
        markers.append(
            {
                "test_name": label,
                "value": test.get("value"),
                "unit": test.get("unit"),
                "reference_low": test.get("lower_range"),
                "reference_high": test.get("higher_range"),
            }
        )
    return markers


def prepare_allergy(
    booking: dict[str, Any] | None,
    *,
    assessment_instance_id: int | None,
    kb: AllergyKnowledgeBase | None = None,
    resolution_status: str = "resolved",
    resolution_reason: str | None = None,
    assay_rules: dict[str, Any] | None = None,
) -> AllergyPrepared | None:
    """Detect booked allergy profiles and build sections only when values exist."""
    sections = build_allergy_sections(
        booking,
        assessment_instance_id=assessment_instance_id,
        kb=kb,
        resolution_status=resolution_status,
        resolution_reason=resolution_reason,
        assay_rules=assay_rules,
    )
    if sections is None:
        return None
    availability = sections.assessment.availability
    if availability is None:
        return AllergyPrepared(None, sections)
    if not availability.allergy_tests_booked:
        return None
    if not availability.allergy_results_available:
        return AllergyPrepared(availability, None)
    return AllergyPrepared(availability, sections)


def build_allergy_sections(
    booking: dict[str, Any] | None,
    *,
    assessment_instance_id: int | None,
    kb: AllergyKnowledgeBase | None = None,
    resolution_status: str = "resolved",
    resolution_reason: str | None = None,
    assay_rules: dict[str, Any] | None = None,
) -> AllergySections | None:
    """Return allergy sections, or None when this booking has no allergy panel."""
    if resolution_status != "resolved":
        return AllergySections(
            assessment=AllergyAssessment(
                assessment_instance_id=assessment_instance_id,
                resolution_status=resolution_status,
                resolution_reason=resolution_reason,
                panel_present=False,
            ),
            overview=_empty_overview(resolution_status, resolution_reason),
            detailed_insights=AllergyDetailedInsights(
                screening_explanation=None,
                narrative_withheld=True,
                withheld_reason=resolution_reason or resolution_status,
            ),
            breakdown=AllergyBreakdown(),
        )

    store = kb or default_allergy_kb()
    if not booking:
        return None
    raw_rows = _digital_data(booking)
    alias_map = _provider_alias_map(booking.get("blood_parameters"))
    booked = _booked_tests(booking.get("blood_parameters"), store)
    if not booked["groups"] and resolution_status == "resolved":
        return None

    booked_names = set(booked["by_raw_name"])
    allergy_rows = [
        row
        for row in raw_rows
        if _is_allergy_measurement(
            row,
            store,
            booked_names,
            set(booked["groups"]),
        )
    ]
    results: list[AllergyResult] = []
    total: TotalIgeResult | None = None
    warnings: list[str] = []
    if booked.get("profile_only") and booked["groups"]:
        warnings.append(
            "Allergy Profile lists booked profiles, not individual analytes. "
            "Received laboratory rows are kept. Unlisted members are not marked negative."
        )
    seen_specific: dict[str, int] = {}
    rules = assay_rules if assay_rules is not None else load_assay_rules()
    laboratory_id = _as_str(booking.get("laboratory_id"))
    for row in allergy_rows:
        parsed = _parse_row(
            row,
            store,
            alias_map,
            warnings,
            rules=rules,
            laboratory_id=laboratory_id,
        )
        if parsed[0] == "total":
            if total is not None:
                warnings.append("duplicate Total IgE measurement")
            total = parsed[1]
        else:
            key = parsed[1].allergen_id or parsed[1].lab_test_name
            seen_specific[key] = seen_specific.get(key, 0) + 1
            results.append(parsed[1])
    for name, count in seen_specific.items():
        if count > 1:
            warnings.append(f"duplicate measurement for {name}")

    reported_ids = set()
    for item in results:
        reported_ids.add(item.lab_test_name)
        if item.allergen_id:
            reported_ids.add(item.allergen_id)
    for expected in booked["specific"]:
        if expected["raw_name"] in reported_ids or expected["catalogue_name"] in reported_ids:
            continue
        if expected["allergen_id"] and expected["allergen_id"] in reported_ids:
            continue
        results.append(_missing_booked(expected, store))

    expected_count = len(booked["specific"])
    missing_count = sum(1 for item in results if item.result_status == "missing" and item.provenance == "booked_without_raw_result")
    reported_count = expected_count - missing_count
    total_booked = booked["total"] is not None
    total_reported = total is not None and total.result_status not in {"missing", "invalid"}
    panel_complete = expected_count > 0 and missing_count == 0 and (not total_booked or total_reported)
    if expected_count == 0 and total_booked:
        panel_complete = total_reported
    received = [item for item in results if item.result_status != "missing"]
    results_available = bool(received) or total_reported
    if any("contradictory" in warning or warning.startswith("duplicate") for warning in warnings):
        status = "needs_review"
    elif not booked["groups"]:
        status = "not_booked"
    elif not results_available:
        status = "awaiting_results"
    elif missing_count > 0 or (total_booked and not total_reported):
        status = "partial"
    else:
        status = "ready"
    if any(item.result_status in {"missing", "invalid", "indeterminate"} for item in results):
        completeness = "partial" if results_available or missing_count else "absent"
    elif panel_complete:
        completeness = "complete"
    elif results_available:
        completeness = "partial"
    else:
        completeness = "absent"
    availability = AllergyAvailability(
        allergy_tests_booked=bool(booked["groups"]),
        allergy_results_available=results_available,
        total_ige_booked=total_booked,
        total_ige_reported=bool(total_reported),
        booked_source_groups=booked["groups"],
        expected_test_count=expected_count,
        reported_test_count=reported_count,
        missing_test_count=missing_count,
        panel_complete=panel_complete,
        processing_status=status,
        validation_warnings=warnings,
    )
    if not results_available and resolution_status == "resolved":
        assessment = AllergyAssessment(
            assessment_instance_id=assessment_instance_id,
            blood_booking_id=_as_int(booking.get("id")),
            booking_reference=_as_str(booking.get("booking_id")),
            collection_date=_as_str(booking.get("collection_date")),
            resolution_status="resolved",
            panel_present=False,
            data_completeness="absent",
            availability=availability,
            validation_warnings=warnings,
            kb_version=store.version,
        )
        return AllergySections(
            assessment=assessment,
            overview=_empty_overview("awaiting_results", "allergy profiles are booked and results have not arrived"),
            detailed_insights=AllergyDetailedInsights(
                screening_explanation="Allergy profiles are booked. Measured results are not available yet.",
                narrative_withheld=True,
                withheld_reason="awaiting_results",
            ),
            breakdown=AllergyBreakdown(),
        )
    assessment = AllergyAssessment(
        assessment_instance_id=assessment_instance_id,
        blood_booking_id=_as_int(booking.get("id")),
        booking_reference=_as_str(booking.get("booking_id")),
        collection_date=_as_str(booking.get("collection_date")),
        laboratory_source="participant_blood_bookings.blood_report_raw.digital_data",
        resolution_status="resolved",
        panel_present=True,
        data_completeness=completeness,
        availability=availability,
        total_ige=total,
        results=results,
        validation_warnings=warnings,
        clinical_review_status=store.rules_review_status,
        narratives_approved=store.content_released(),
        qualified_clinical_approval=store.qualified_clinical_approval,
        kb_version=store.version,
    )
    return AllergySections(
        assessment=assessment,
        overview=_overview(assessment, store),
        detailed_insights=_insights(assessment, store),
        breakdown=_breakdown(assessment, store),
    )


def parse_raw_value(value: Any) -> tuple[str | None, str | None, Decimal | None, str]:
    """Return raw text, comparator, numeric bound, and parse status.

    ``<0.35`` keeps the comparator. It is not an exact measurement of 0.35.
    """
    if value is None:
        return None, None, None, "missing"
    if isinstance(value, bool):
        return str(value), None, None, "invalid"
    if isinstance(value, (int, float, Decimal)):
        try:
            number = Decimal(str(value))
        except InvalidOperation:
            return str(value), None, None, "invalid"
        return format(number, "f"), "=", number, "exact"
    text = str(value).strip()
    if not text:
        return None, None, None, "missing"
    comparator = None
    body = text
    if text.startswith("<=") or text.startswith(">="):
        comparator = text[:2]
        body = text[2:].strip()
    elif text[0] in "<>":
        comparator = text[0]
        body = text[1:].strip()
    try:
        number = Decimal(body.replace(",", ""))
    except InvalidOperation:
        return text, None, None, "invalid"
    if comparator is None:
        return text, "=", number, "exact"
    return text, comparator, number, "bounded"


def _parse_row(
    row: dict[str, Any],
    kb: AllergyKnowledgeBase,
    alias_map: dict[str, dict[str, Any]],
    warnings: list[str],
    *,
    rules: dict[str, Any],
    laboratory_id: str | None,
) -> tuple[str, Any]:
    raw_name = str(row.get("test_name") or "").strip()
    raw_text, comparator, number, parse_status = parse_raw_value(row.get("value"))
    flag = _as_str(row.get("lab_flag") or row.get("flag") or row.get("result_flag"))
    lab_class = _as_str(row.get("lab_class") or row.get("class") or row.get("ige_class"))
    match_status, entry, group_name = _resolve_name(raw_name, kb, alias_map)
    if entry is None and _is_total_ige_name(raw_name, kb):
        entry = kb.by_id.get("ige_total")
        match_status = "exact_name" if entry is not None else "unresolved"
    interpreted = interpret_specific_ige(
        rules=rules,
        laboratory_id=laboratory_id,
        allergen_id=None if entry is None else entry.get("allergen_id"),
        parse_status=parse_status,
        comparator=comparator,
        number=number,
        unit=_as_str(row.get("unit")),
        lab_flag=flag,
        lab_class=lab_class,
    )
    status = interpreted["result_status"]
    warning = interpreted["warning"]
    if interpreted.get("lab_class"):
        lab_class = interpreted["lab_class"]
    # Total IgE uses the reference interval printed on the report, not a
    # specific-IgE assay cutoff, so a missing assay profile is not a warning.
    if warning and not (entry and entry.get("allergen_id") == "ige_total"):
        warnings.append(f"{raw_name}: {warning}")
    record = kb.record(entry.get("allergen_id")) if entry else None
    approved_record = bool(record and kb.narratives_approved(record))
    review = str((record or {}).get("clinical_review_status") or kb.rules_review_status)
    if entry and entry.get("allergen_id") == "ige_total":
        low, high = _range(row)
        band = _total_band(parse_status, number, low, high)
        return "total", TotalIgeResult(
            lab_test_name=raw_name,
            allergen_id="ige_total",
            raw_value=raw_text,
            comparator=None if comparator == "=" else comparator,
            numeric_value=number,
            unit=_as_str(row.get("unit")),
            reference_low=low,
            reference_high=high,
            reference_text=_reference_text(low, high),
            band=band,
            result_status=status if parse_status in {"missing", "invalid"} else band,
            narratives_approved=kb.narratives_approved(record or kb.total_ige),
        )
    return "specific", AllergyResult(
        lab_test_name=raw_name,
        allergen_id=entry.get("allergen_id") if entry else None,
        match_status=match_status,
        common_name=(record or {}).get("common_name") if approved_record else None,
        scientific_name=(record or {}).get("scientific_name") if approved_record else None,
        display_name=_display_name((record or {}).get("common_name") if approved_record else None, raw_name),
        source_group=entry.get("source_group") if entry else None,
        source_group_name=group_name,
        canonical_category=entry.get("canonical_category") if entry else None,
        raw_value=raw_text,
        comparator=None if comparator == "=" else comparator,
        numeric_value=number,
        unit=_as_str(row.get("unit")),
        lab_class=lab_class,
        lab_flag=flag,
        applied_rule_id=interpreted["applied_rule_id"],
        interpretation_confidence=interpreted["interpretation_confidence"],
        reference_low=_range(row)[0],
        reference_high=_range(row)[1],
        provider_parameter_id=_as_str(row.get("parameter_id")),
        result_status=status,
        review_status=review,
        provenance="blood_report_raw.digital_data",
    )


def _resolve_name(
    raw_name: str,
    kb: AllergyKnowledgeBase,
    alias_map: dict[str, dict[str, Any]],
) -> tuple[str, dict[str, Any] | None, str | None]:
    exact = kb.by_lab_name.get(raw_name)
    if exact is not None:
        grouped = alias_map.get(raw_name)
        return "exact_name", exact, (grouped or {}).get("group_name")
    grouped = alias_map.get(raw_name)
    if grouped is not None:
        catalogue = kb.by_lab_name.get(str(grouped.get("catalogue_name") or ""))
        if catalogue is not None:
            return "provider_alias", catalogue, grouped.get("group_name")
    for entry in kb.entries:
        aliases = entry.get("aliases") if isinstance(entry.get("aliases"), list) else []
        if raw_name in aliases and entry.get("clinical_review_status") == "approved":
            return "provider_alias", entry, None
    return "unresolved", None, (grouped or {}).get("group_name") if grouped else None


def _classify(
    parse_status: str,
    comparator: str | None,
    flag: str | None,
    lab_class: str | None,
) -> tuple[str, str | None]:
    flag_status = _flag_status(flag)
    value_status = _value_status(parse_status, comparator)
    if flag_status and value_status and flag_status != value_status:
        if {flag_status, value_status} <= {"positive", "negative_per_lab"}:
            return "indeterminate", "contradictory laboratory flag and value"
    if flag_status:
        return flag_status, None
    if lab_class and _flag_status(lab_class):
        return _flag_status(lab_class), None
    if value_status:
        return value_status, None
    return "indeterminate", None


def _flag_status(flag: str | None) -> str | None:
    if not flag:
        return None
    token = flag.strip().lower()
    if token in _POSITIVE_FLAGS:
        return "positive"
    if token in _NEGATIVE_FLAGS:
        return "negative_per_lab"
    if token in _INDETERMINATE_FLAGS:
        return "indeterminate"
    if token in {"invalid", "error"}:
        return "invalid"
    return None


def _value_status(parse_status: str, comparator: str | None) -> str | None:
    if parse_status == "missing":
        return "missing"
    if parse_status == "invalid":
        return "invalid"
    if comparator == "<":
        return "negative_per_lab"
    if comparator == ">":
        return "positive"
    return None


def _overview(assessment: AllergyAssessment, kb: AllergyKnowledgeBase) -> AllergyOverview:
    detected = [item for item in assessment.results if item.result_status == "positive"]
    non_detected = [item for item in assessment.results if item.result_status == "negative_per_lab"]
    withheld = not assessment.narratives_approved
    total = assessment.total_ige
    interpretation = None
    status_label = _ige_status_label(kb, total.band) if total is not None and not withheld else None
    if total is not None and status_label:
        total.status_label = status_label
    if total is not None and not withheld and total.narratives_approved:
        content = (kb.total_ige.get("status_content") or {}).get(total.band or "")
        if isinstance(content, dict):
            interpretation = content.get("interpretation_insight")
    elif total is not None and total.band == "within_reference":
        interpretation = "Within the laboratory reference interval printed on the report."
    elif total is not None and total.band == "above_reference":
        interpretation = (
            "Above the laboratory reference interval printed on the report. "
            "This is not a diagnosis of allergy."
        )
    elif total is not None and total.band == "below_reference":
        interpretation = "Below the laboratory reference interval printed on the report."
    indeterminate_count = sum(1 for item in assessment.results if item.result_status == "indeterminate")
    missing_count = sum(1 for item in assessment.results if item.result_status == "missing")
    return AllergyOverview(
        what_is_ige=None if withheld else _approved_ige_definition(kb),
        narrative_withheld=withheld,
        withheld_reason="clinical_review_pending" if withheld else None,
        total_ige=total,
        total_ige_interpretation=interpretation,
        total_ige_status_label=status_label,
        specific_tested=len(assessment.results),
        detected_count=len(detected),
        non_detected_count=len(non_detected),
        indeterminate_count=indeterminate_count,
        missing_count=missing_count,
        negative_count_unverified=not non_detected and (indeterminate_count > 0 or missing_count > 0),
        detected_allergens=detected,
        class_explanation=None if withheld else _caution_text(kb, "C_CLASS"),
        sensitization_note=(
            _caution_text(kb, "C_SENS")
            if not withheld
            else "Laboratory values are shown. Patient-facing interpretation is withheld pending clinical review."
        ),
    )


def _insights(assessment: AllergyAssessment, kb: AllergyKnowledgeBase) -> AllergyDetailedInsights:
    detected = [item for item in assessment.results if item.result_status == "positive"]
    insights: list[AllergyInsight] = []
    for item in detected:
        record = kb.record(item.allergen_id)
        approved = kb.narratives_approved(record)
        definition = record.get("definition") if approved and record else None
        interpretation = record.get("interpretation_insight") if approved and record else None
        summary_parts = [str(part) for part in (definition, interpretation) if part]
        insights.append(
            AllergyInsight(
                lab_test_name=item.lab_test_name,
                allergen_id=item.allergen_id,
                match_status=item.match_status,
                raw_value=item.raw_value,
                unit=item.unit,
                result_status=item.result_status,
                common_name=record.get("common_name") if approved and record else None,
                display_name=item.display_name or item.lab_test_name,
                scientific_name=item.scientific_name,
                lab_class=item.lab_class,
                definition=str(definition) if definition else None,
                card_summary=str(record.get("card_summary")) if approved and record and record.get("card_summary") else None,
                summary=" ".join(summary_parts) or None,
                exposure_sources=list(record.get("exposure_sources") or []) if approved and record else [],
                interpretation_insight=str(interpretation) if interpretation else None,
                caution=record.get("caution") if approved and record else None,
                possible_symptoms=list(record.get("typical_symptoms") or []) if approved and record else [],
                narrative_withheld=not approved,
            )
        )
    cards: list[RecommendationCard] = []
    if assessment.narratives_approved and detected:
        cards = _select_recommendations(detected, kb)
    explanation = None
    if not detected:
        missing = sum(1 for item in assessment.results if item.result_status == "missing")
        indeterminate = sum(1 for item in assessment.results if item.result_status == "indeterminate")
        if missing:
            explanation = (
                "No received specific-IgE result was reported as detected. "
                "Booked tests without a laboratory value are missing, not negative."
            )
        elif indeterminate and not any(
            item.result_status == "negative_per_lab" for item in assessment.results
        ):
            explanation = (
                "Specific-IgE values are shown with the laboratory comparator. "
                "They are not classified as sensitization because no verified assay rule applies."
            )
        else:
            explanation = (
                "No specific-IgE result in this panel was classified as sensitization. "
                "A below-threshold result is not a statement that a food or medicine is safe."
            )
    return AllergyDetailedInsights(
        insights=insights,
        recommendations=cards,
        interpretive_notes=[] if not assessment.narratives_approved else _interpretive_notes(kb),
        screening_explanation=explanation,
        narrative_withheld=not assessment.narratives_approved,
        withheld_reason=None if assessment.narratives_approved else "clinical_review_pending",
    )


def _select_recommendations(detected: list[AllergyResult], kb: AllergyKnowledgeBase) -> list[RecommendationCard]:
    categories = {item.canonical_category for item in detected if item.canonical_category}
    allergen_ids = {item.allergen_id for item in detected if item.allergen_id}
    chosen: list[RecommendationCard] = []
    seen_themes: set[str] = set()
    general_used = False
    ranked = sorted(
        kb.recommendations.values(),
        key=lambda item: (
            {"safety_critical": 0, "important": 1, "routine": 2}.get(str(item.get("clinical_priority")), 9),
            0 if item.get("applicable_allergen_ids") else 1,
            int(item.get("display_priority") or 0),
            str(item.get("recommendation_id")),
        ),
    )
    for item in ranked:
        if not kb.recommendation_released(item):
            continue
        cats = set(item.get("applicable_categories") or [])
        ids = set(item.get("applicable_allergen_ids") or [])
        if not (cats & categories or ids & allergen_ids):
            continue
        required = item.get("required_context") or []
        if isinstance(required, list) and required:
            continue
        if isinstance(required, dict) and any(required.values()):
            continue
        theme = str(item.get("theme") or item.get("recommendation_id"))
        if theme in seen_themes:
            continue
        rec_id = str(item.get("recommendation_id"))
        if rec_id.startswith("gen_"):
            if general_used:
                continue
            general_used = True
        seen_themes.add(theme)
        chosen.append(
            RecommendationCard(
                recommendation_id=rec_id,
                title=str(item.get("title") or ""),
                description=str(item.get("description") or ""),
            )
        )
        if len(chosen) == 4:
            break
    return chosen


def _breakdown(assessment: AllergyAssessment, kb: AllergyKnowledgeBase) -> AllergyBreakdown:
    catalogue = {
        str(group.get("source_group")): group
        for group in kb.source_groups
        if isinstance(group, dict) and group.get("source_group")
    }
    buckets: dict[str, list[AllergyResult]] = {}
    for item in assessment.results:
        key = item.source_group or "unlisted_laboratory_results"
        buckets.setdefault(key, []).append(item)
    groups: list[AllergyBreakdownGroup] = []
    used: set[str] = set()
    for spec in _PAGE_CARDS:
        members = [str(key) for key in spec["source_groups"]]
        items: list[AllergyResult] = []
        present: list[str] = []
        for key in members:
            if key not in buckets:
                continue
            items.extend(buckets[key])
            present.append(key)
            used.add(key)
        if not items:
            continue
        groups.append(_breakdown_group(spec, present, items, catalogue))
    for key, items in buckets.items():
        if key in used:
            continue
        groups.append(
            _breakdown_group(
                {"id": key, "title": None, "source_groups": [key]},
                [key],
                items,
                catalogue,
            )
        )
    note = _caution_text(kb, "C_NEG")
    return AllergyBreakdown(
        groups=groups,
        no_positives_note_title='What does "No positives reported" mean?',
        no_positives_note=note,
    )


def _breakdown_group(
    spec: dict[str, Any],
    present: list[str],
    items: list[AllergyResult],
    catalogue: dict[str, dict[str, Any]],
) -> AllergyBreakdownGroup:
    detected_items = [item for item in items if item.result_status == "positive"]
    detected = len(detected_items)
    non_detected = sum(1 for item in items if item.result_status == "negative_per_lab")
    indeterminate = sum(1 for item in items if item.result_status == "indeterminate")
    missing = sum(1 for item in items if item.result_status in {"missing", "invalid"})
    if detected:
        status = "detected"
    elif indeterminate or missing:
        status = "incomplete"
    elif non_detected == len(items) and items:
        status = "not_detected"
    else:
        status = "incomplete"
    primary = catalogue.get(present[0], {})
    title = spec.get("title") or primary.get("patient_name") or "Additional laboratory results"
    summary = spec.get("summary")
    if not summary and len(present) == 1:
        summary = primary.get("explanation")
    elif not summary:
        parts = [catalogue.get(key, {}).get("explanation") for key in present]
        summary = " ".join(str(part) for part in parts if part) or None
    names = []
    for item in items:
        if isinstance(item.source_group_name, str) and item.source_group_name not in names:
            names.append(item.source_group_name)
    if detected == 1:
        status_line = "1 finding to review"
    elif detected:
        status_line = f"{detected} findings to review"
    else:
        status_line = "No positives reported"
    return AllergyBreakdownGroup(
        source_group=str(spec["id"]),
        source_groups=present,
        title=str(title),
        summary=str(summary) if summary else None,
        badge="Detected" if detected else "No positives",
        status_line=status_line,
        findings_line=_findings_line(detected_items) if detected_items else None,
        laboratory_group_name=", ".join(names) if names else None,
        total_tests=len(items),
        detected_count=detected,
        non_detected_count=non_detected,
        indeterminate_count=indeterminate,
        missing_count=missing,
        category_status=status,
        results=items,
        highlighted=detected_items,
        continuation_required=len(items) >= _CONTINUATION_LIMIT,
    )


def _findings_line(items: list[AllergyResult]) -> str:
    labels: list[str] = []
    for item in items:
        name = item.display_name or item.common_name or item.lab_test_name
        if item.lab_class:
            labels.append(f"{name} (Class {item.lab_class})")
        else:
            labels.append(name)
    if len(labels) == 1:
        sentence = labels[0]
    elif len(labels) == 2:
        sentence = f"{labels[0]} and {labels[1]}"
    else:
        sentence = f"{', '.join(labels[:-1])} and {labels[-1]}"
    return f"{sentence}."


def _caution_text(kb: AllergyKnowledgeBase, caution_id: str) -> str | None:
    entry = kb.caution_library.get(caution_id)
    if isinstance(entry, dict) and entry.get("patient_text"):
        return str(entry["patient_text"])
    return None


def _ige_status_label(kb: AllergyKnowledgeBase, band: str | None) -> str | None:
    content = (kb.total_ige.get("status_content") or {}).get(band or "")
    if isinstance(content, dict) and content.get("label"):
        return str(content["label"])
    return None


def _interpretive_notes(kb: AllergyKnowledgeBase) -> list[InterpretiveNote]:
    notes: list[InterpretiveNote] = []
    sensitization = _caution_text(kb, "C_SENS")
    ige_class = _caution_text(kb, "C_CLASS")
    review = kb.recommendations.get("gen_see_allergist") or {}
    review_text = review.get("description") if kb.recommendation_released(review) else None
    if sensitization:
        notes.append(InterpretiveNote(label="Positive test vs allergy", text=sensitization))
    if ige_class:
        notes.append(InterpretiveNote(label="Allergy class", text=ige_class))
    if review_text:
        notes.append(InterpretiveNote(label="Clinical review", text=str(review_text)))
    return notes


def _display_name(common_name: Any, lab_test_name: str) -> str:
    text = str(common_name).strip() if common_name else lab_test_name
    if "(" in text:
        text = text.split("(", 1)[0].strip()
    return text or lab_test_name


def _norm_group(name: str) -> str:
    text = name.lower().replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _as_parameter_groups(parameters: Any) -> list[Any]:
    """Read ``participant_blood_bookings.blood_parameters`` as a group list."""
    if isinstance(parameters, str):
        text = parameters.strip()
        if not text:
            return []
        try:
            parameters = json.loads(text)
        except json.JSONDecodeError:
            return []
    if isinstance(parameters, dict) and isinstance(parameters.get("groups"), list):
        parameters = parameters["groups"]
    return parameters if isinstance(parameters, list) else []


def _booked_tests(parameters: Any, kb: AllergyKnowledgeBase) -> dict[str, Any]:
    groups: list[str] = []
    specific: list[dict[str, Any]] = []
    by_raw_name: dict[str, dict[str, Any]] = {}
    total: dict[str, Any] | None = None
    profile_only = False
    parameter_groups = _as_parameter_groups(parameters)
    if not parameter_groups:
        return {
            "groups": groups,
            "specific": specific,
            "by_raw_name": by_raw_name,
            "total": total,
            "profile_only": False,
        }
    parameters = parameter_groups
    flags_present = _screening_flags_present(parameters)
    for group in parameters:
        if not isinstance(group, dict):
            continue
        group_name = str(group.get("group_name") or "")
        source_group = _GROUP_KEYS.get(_norm_group(group_name))
        flag_on = group.get("has_allergy_screening") is True
        if source_group is None and flags_present and not flag_on:
            continue
        if source_group is None:
            if group.get("has_allergy_screening") is True or _norm_group(group_name) in {
                "allergy profile",
                "allergy",
                "allergies",
            }:
                _book_profile_container(group, kb, groups, by_raw_name)
                found_total = _profile_total(group, kb)
                if found_total is not None:
                    total = found_total
                    if "ige" not in groups:
                        groups.append("ige")
                profile_only = True
            continue
        if source_group not in groups:
            groups.append(source_group)
        for test in group.get("tests") or []:
            if not isinstance(test, dict):
                continue
            catalogue = str(test.get("test_name") or "").strip()
            provider = str(test.get("provider_test_name") or catalogue).strip()
            entry = kb.by_lab_name.get(catalogue)
            slot = {
                "raw_name": provider or catalogue,
                "catalogue_name": catalogue,
                "allergen_id": None if entry is None else entry.get("allergen_id"),
                "source_group": source_group,
                "group_name": group.get("group_name"),
            }
            if source_group == "ige" or (entry and entry.get("allergen_id") == "ige_total"):
                total = slot
            else:
                specific.append(slot)
            if slot["raw_name"]:
                by_raw_name[slot["raw_name"]] = slot
            if catalogue:
                by_raw_name.setdefault(catalogue, slot)
    return {
        "groups": groups,
        "specific": specific,
        "by_raw_name": by_raw_name,
        "total": total,
        "profile_only": profile_only and not specific,
    }


def _book_profile_container(
    group: dict[str, Any],
    kb: AllergyKnowledgeBase,
    groups: list[str],
    by_raw_name: dict[str, dict[str, Any]],
) -> None:
    for test in group.get("tests") or []:
        if not isinstance(test, dict):
            continue
        catalogue = str(test.get("test_name") or "").strip()
        provider = str(test.get("provider_test_name") or catalogue).strip()
        source_group = _GROUP_KEYS.get(_norm_group(catalogue)) or _GROUP_KEYS.get(_norm_group(provider))
        if source_group is None or source_group == "ige":
            continue
        if source_group not in groups:
            groups.append(source_group)
        by_raw_name.setdefault(catalogue, {"catalogue_name": catalogue, "source_group": source_group})


def _profile_total(group: dict[str, Any], kb: AllergyKnowledgeBase) -> dict[str, Any] | None:
    for test in group.get("tests") or []:
        if not isinstance(test, dict):
            continue
        catalogue = str(test.get("test_name") or "").strip()
        provider = str(test.get("provider_test_name") or catalogue).strip()
        source_group = _GROUP_KEYS.get(_norm_group(catalogue)) or _GROUP_KEYS.get(_norm_group(provider))
        if source_group != "ige":
            continue
        entry = kb.by_id.get("ige_total") or {}
        return {
            "raw_name": provider or catalogue,
            "catalogue_name": entry.get("lab_test_name") or catalogue,
            "allergen_id": "ige_total",
            "source_group": "ige",
            "group_name": catalogue,
        }
    return None


def _is_total_ige_name(name: str, kb: AllergyKnowledgeBase) -> bool:
    if _norm_group(name) in {"ige total", "total ige", "immunoglobulin e ige total", "immunoglobulin e total"}:
        return True
    record = kb.total_ige if isinstance(kb.total_ige, dict) else {}
    aliases = record.get("aliases") if isinstance(record.get("aliases"), list) else []
    return name == record.get("lab_test_name") or name in aliases


def _is_allergy_measurement(
    row: dict[str, Any],
    kb: AllergyKnowledgeBase,
    booked_names: set[str],
    booked_groups: set[str],
) -> bool:
    name = str(row.get("test_name") or "")
    if name in booked_names:
        return True
    if "ige" in booked_groups and _is_total_ige_name(name, kb):
        return True
    entry = kb.by_lab_name.get(name)
    if entry is not None:
        return entry.get("source_group") in booked_groups
    specific_groups = booked_groups - {"ige"}
    if not specific_groups or not _is_specific_ige_unit(row.get("unit")):
        return False
    _raw, comparator, _number, status = parse_raw_value(row.get("value"))
    if comparator in {"<", ">", "<=", ">="}:
        return True
    high = _as_str(row.get("max_range"))
    return high in {"0.35", "<0.35"} and status == "exact"


def _missing_booked(expected: dict[str, Any], kb: AllergyKnowledgeBase) -> AllergyResult:
    entry = kb.by_lab_name.get(expected.get("catalogue_name") or "")
    record = kb.record(entry.get("allergen_id")) if entry else None
    return AllergyResult(
        lab_test_name=str(expected.get("catalogue_name") or expected.get("raw_name")),
        allergen_id=None if entry is None else entry.get("allergen_id"),
        match_status="exact_name" if entry is not None else "unresolved",
        source_group=expected.get("source_group"),
        source_group_name=expected.get("group_name"),
        canonical_category=None if entry is None else entry.get("canonical_category"),
        result_status="missing",
        review_status=str((record or {}).get("clinical_review_status") or kb.rules_review_status),
        provenance="booked_without_raw_result",
    )


def _provider_alias_map(parameters: Any) -> dict[str, dict[str, Any]]:
    mapping: dict[str, dict[str, Any]] = {}
    parameters = _as_parameter_groups(parameters)
    if not parameters:
        return mapping
    for group in parameters:
        if not isinstance(group, dict):
            continue
        group_name = str(group.get("group_name") or "")
        if not group_name.lower().startswith("allergy"):
            continue
        for test in group.get("tests") or []:
            if not isinstance(test, dict):
                continue
            provider = test.get("provider_test_name")
            catalogue = test.get("test_name")
            if isinstance(provider, str) and provider.strip() and isinstance(catalogue, str):
                mapping[provider] = {
                    "catalogue_name": catalogue,
                    "group_name": group_name,
                    "test_id": test.get("test_id"),
                }
    return mapping


def _digital_data(booking: dict[str, Any]) -> list[dict[str, Any]]:
    raw = booking.get("blood_report_raw")
    if isinstance(raw, dict) and isinstance(raw.get("digital_data"), list):
        return [row for row in raw["digital_data"] if isinstance(row, dict)]
    return []


def _is_allergy_row(
    row: dict[str, Any],
    kb: AllergyKnowledgeBase,
    alias_map: dict[str, dict[str, Any]],
) -> bool:
    name = str(row.get("test_name") or "")
    if name in kb.by_lab_name or name in alias_map:
        return True
    if not _is_specific_ige_unit(row.get("unit")):
        return False
    raw, comparator, _number, status = parse_raw_value(row.get("value"))
    if comparator in {"<", ">"}:
        return True
    high = _as_str(row.get("max_range"))
    return high in {"0.35", "<0.35"} and status == "exact" and raw is not None


def _is_specific_ige_unit(unit: Any) -> bool:
    text = str(unit or "").replace(" ", "").replace("µ", "u").replace("μ", "u").lower()
    if text.startswith("m") or text.startswith("u"):
        return False
    return text == "iu/ml"


def _range(row: dict[str, Any]) -> tuple[Decimal | None, Decimal | None]:
    return _decimal(row.get("min_range")), _decimal(row.get("max_range"))


def _decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip()
    if not text or text[0] in "<>":
        return None
    try:
        return Decimal(text.replace(",", ""))
    except InvalidOperation:
        return None


def _reference_text(low: Decimal | None, high: Decimal | None) -> str | None:
    if low is None and high is None:
        return None
    return f"{low if low is not None else ''}–{high if high is not None else ''}"


def _total_band(
    parse_status: str,
    number: Decimal | None,
    low: Decimal | None,
    high: Decimal | None,
) -> str:
    if parse_status == "missing":
        return "missing"
    if parse_status == "invalid" or number is None:
        return "invalid"
    if low is None or high is None:
        return "uninterpretable"
    if number < low:
        return "below_reference"
    if number > high:
        return "above_reference"
    return "within_reference"


def _approved_ige_definition(kb: AllergyKnowledgeBase) -> str | None:
    if not kb.narratives_approved(kb.total_ige):
        return None
    definition = kb.total_ige.get("definition")
    return str(definition) if definition else None


def _empty_overview(status: str, reason: str | None) -> AllergyOverview:
    return AllergyOverview(
        narrative_withheld=True,
        withheld_reason=reason or status,
        sensitization_note="Allergy results were not attached because the blood booking could not be resolved.",
    )


def _as_str(value: Any) -> str | None:
    if value is None or isinstance(value, (dict, list)):
        return None
    text = str(value).strip()
    return text or None


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
