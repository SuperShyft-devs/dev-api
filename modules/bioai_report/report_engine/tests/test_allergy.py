"""Allergy panel parsing, resolution, and BioReport integration."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

from modules.bioai_report.report_engine.builders.allergy_builder import (
    build_allergy_sections,
    parse_raw_value,
)
from modules.bioai_report.report_engine.builders.report_builder import build_bioreport
from modules.bioai_report.report_engine.services.allergy_booking import resolve_bookings

FIXTURE = Path(__file__).parent / "fixtures" / "allergy_panel_anonymized.json"


def _panel() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _verified_rules() -> dict:
    return {
        "laboratories": [
            {
                "laboratory_id": "fixture_lab",
                "assay_id": "synthetic",
                "method": "synthetic fixture, not a production assay",
                "profiles": {
                    "specific_ige_default": {
                        "status": "verified",
                        "unit": "IU/mL",
                        "positive_cutoff": {"comparator": ">=", "value": "0.35"},
                        "classes": [],
                        "accept_laboratory_flag": True,
                    },
                    "high_cutoff": {
                        "status": "verified",
                        "unit": "IU/mL",
                        "positive_cutoff": {"comparator": ">", "value": "0.70"},
                        "classes": [],
                        "accept_laboratory_flag": False,
                    },
                },
                "analyte_rules": {},
            }
        ]
    }


def _sections(booking: dict, rules=None):
    sections = build_allergy_sections(
        booking,
        assessment_instance_id=7,
        assay_rules=rules,
    )
    assert sections is not None
    return sections


def test_uploaded_panel_preserves_specific_ige_and_total():
    sections = _sections(_panel())
    assessment = sections.assessment
    assert len(assessment.results) == 215
    assert sum(1 for item in assessment.results if item.match_status == "unresolved") == 4
    unresolved = {item.lab_test_name for item in assessment.results if item.match_status == "unresolved"}
    assert unresolved == {"Chick Pea", "Lemon", "Millet", "Pear"}
    assert assessment.total_ige is not None
    assert assessment.total_ige.raw_value == "30.9"
    assert assessment.total_ige.numeric_value == Decimal("30.9")
    assert assessment.total_ige.unit == "IU/ml"
    assert assessment.total_ige.band == "within_reference"
    assert all(item.comparator == "<" for item in assessment.results)
    assert all(item.result_status == "indeterminate" for item in assessment.results)
    assert assessment.qualified_clinical_approval == "pending"
    assert assessment.narratives_approved is True
    assert sections.overview.what_is_ige
    assert sections.overview.detected_count == 0
    assert sections.detailed_insights.insights == []
    assert sections.overview.detected_count == sum(group.detected_count for group in sections.breakdown.groups)
    assert len(sections.breakdown.groups) == 13
    veg = next(group for group in sections.breakdown.groups if group.source_group == "food_veg_dairy")
    assert veg.title == "Food (Veg & Dairy)"
    assert "allergy_veg" in veg.source_groups
    assert veg.continuation_required is True
    assert veg.badge == "No positives"
    assert veg.status_line == "No positives reported"
    pets = next(group for group in sections.breakdown.groups if group.source_group == "pets")
    assert pets.title == "Pets / Animals"
    assert pets.summary
    medicines = next(group for group in sections.breakdown.groups if group.source_group == "allergy_drugs")
    assert medicines.title == "Medicines"
    assert sections.overview.total_ige_status_label == "Within the reference range"
    assert sections.overview.negative_count_unverified is True
    assert sections.overview.class_explanation
    assert sections.detailed_insights.interpretive_notes
    assert sections.breakdown.no_positives_note
    assert sections.assessment.availability.processing_status == "ready"
    assert sections.detailed_insights.insights == []
    assert sections.detailed_insights.recommendations == []
    assert sections.overview.narrative_withheld is False
    pigeon = next(item for item in assessment.results if item.lab_test_name == "Pigeon Droppings")
    assert pigeon.comparator == "<"
    assert pigeon.numeric_value == Decimal("0.35")
    assert pigeon.result_status == "indeterminate"
    assert pigeon.allergen_id == "pigeon_droppings"


def test_value_parsing_bounds_are_not_exact_measurements():
    raw, comparator, number, status = parse_raw_value("<0.35")
    assert (raw, comparator, number, status) == ("<0.35", "<", Decimal("0.35"), "bounded")
    raw, comparator, number, status = parse_raw_value("0.35")
    assert comparator == "="
    assert number == Decimal("0.35")
    raw, comparator, number, status = parse_raw_value(">100")
    assert comparator == ">"
    assert number == Decimal("100")
    assert parse_raw_value("")[3] == "missing"
    assert parse_raw_value("see note")[3] == "invalid"


def _row(name: str, value: str, **extra):
    row = {
        "test_name": name,
        "value": value,
        "unit": "IU/mL",
        "min_range": "0",
        "max_range": "0.35",
        "parameter_id": "1",
    }
    row.update(extra)
    return row


def _booking(rows, groups=None):
    if groups is None:
        specific = [row for row in rows if row["test_name"] != "IGE total"]
        groups = [{
            "group_name": "Allergy Pets",
            "tests": [
                {"test_name": row["test_name"], "provider_test_name": row["test_name"]}
                for row in specific
                if row["test_name"] not in {"Lemon", "Millet", "Chick Pea", "Pear"}
            ],
        }]
        if any(row["test_name"] == "IGE total" for row in rows):
            groups.append({
                "group_name": "Allergy IgE",
                "tests": [{"test_name": "IGE total", "provider_test_name": "IGE total"}],
            })
    return {
        "id": 9,
        "user_id": 3,
        "engagement_id": 4,
        "booking_id": "b",
        "collection_date": "2026-02-02",
        "blood_parameters": groups,
        "blood_report_raw": {"digital_data": rows},
    }


def test_positive_flag_is_shared_by_all_sections():
    booking = _booking([
        _row("Tyrophagus Putrescentiae", "0.78", lab_flag="positive"),
        _row("Cat Dander", "1.25", lab_flag="positive"),
        _row("Peanut", "0.52", lab_flag="positive"),
        _row("Penicillin V", "2.5", lab_flag="positive"),
        _row("IGE total", "200", unit="IU/ml", min_range="0", max_range="158"),
    ])
    booking["laboratory_id"] = "fixture_lab"
    sections = _sections(booking, _verified_rules())
    assert sections.overview.detected_count == 4
    assert len(sections.detailed_insights.insights) == 4
    assert all(item.definition for item in sections.detailed_insights.insights)
    assert sections.detailed_insights.recommendations
    assert len(sections.detailed_insights.recommendations) <= 4
    joined = " ".join(card.description.lower() for card in sections.detailed_insights.recommendations)
    assert "stop eating" not in joined
    assert "stop your medicine" not in joined
    detected_names = {item.lab_test_name for item in sections.overview.detected_allergens}
    breakdown_detected = {
        item.lab_test_name
        for group in sections.breakdown.groups
        for item in group.highlighted
    }
    assert detected_names == breakdown_detected
    assert sections.assessment.total_ige.band == "above_reference"
    assert sections.overview.total_ige_status_label == "Above the reference range"
    assert "cannot confirm allergy" in (sections.overview.total_ige_interpretation or "")
    dust = next(group for group in sections.breakdown.groups if group.source_group == "allergy_dust_mites")
    assert dust.badge == "Detected"
    assert dust.status_line == "1 finding to review"
    assert dust.findings_line
    mite = next(item for item in sections.detailed_insights.insights if item.allergen_id == "tyrophagus_putrescentiae")
    assert mite.display_name == "Storage Mite"
    assert mite.scientific_name == "Tyrophagus putrescentiae"
    assert mite.definition
    assert mite.summary


def test_exact_number_without_flag_is_indeterminate():
    sections = _sections(_booking([_row("Cat Dander", "0.35")]))
    result = sections.assessment.results[0]
    assert result.comparator is None
    assert result.result_status == "indeterminate"
    assert sections.overview.detected_count == 0
    assert sections.breakdown.groups[0].category_status == "incomplete"


def test_above_bound_is_positive_without_inventing_a_class():
    booking = _booking([_row("Cat Dander", ">100")])
    booking["laboratory_id"] = "fixture_lab"
    sections = _sections(booking, _verified_rules())
    result = sections.assessment.results[0]
    assert result.result_status == "positive"
    assert result.lab_class is None
    assert result.comparator == ">"


def test_contradictory_flag_and_value():
    booking = _booking([_row("Cat Dander", ">100", lab_flag="negative")])
    booking["laboratory_id"] = "fixture_lab"
    sections = _sections(booking, _verified_rules())
    result = sections.assessment.results[0]
    assert result.result_status == "indeterminate"
    assert sections.assessment.validation_warnings


def test_unknown_analyte_is_kept():
    sections = _sections(_booking([_row("Lemon", "<0.35")]))
    result = sections.assessment.results[0]
    assert result.match_status == "unresolved"
    assert result.allergen_id is None
    assert sections.detailed_insights.insights == []


def test_missing_unit_and_cutoff_do_not_invent_status():
    sections = _sections(_booking([
        {"test_name": "Cat Dander", "value": "0.78", "unit": None, "parameter_id": "9"},
    ]))
    result = sections.assessment.results[0]
    assert result.unit is None
    assert result.result_status == "indeterminate"
    assert result.reference_high is None


def test_incomplete_panel_keeps_missing_tests():
    booking = _booking(
        [_row("Cat Dander", "<0.35")],
        groups=[{
            "group_name": "Allergy Pets",
            "tests": [
                {"test_name": "Cat Dander", "provider_test_name": "Cat Dander"},
                {"test_name": "Dog Dander", "provider_test_name": "Dog Dander"},
            ],
        }],
    )
    sections = _sections(booking)
    availability = sections.assessment.availability
    assert availability.processing_status == "partial"
    assert availability.expected_test_count == 2
    assert availability.reported_test_count == 1
    assert availability.missing_test_count == 1
    assert availability.panel_complete is False
    missing = next(item for item in sections.assessment.results if item.lab_test_name == "Dog Dander")
    assert missing.result_status == "missing"
    assert sections.overview.non_detected_count == 0


def test_normal_total_ige_with_specific_sensitization():
    booking = _booking([
        _row("Cat Dander", "1.1", lab_flag="positive"),
        _row("IGE total", "30.9", unit="IU/ml", min_range="0", max_range="158"),
    ])
    booking["laboratory_id"] = "fixture_lab"
    sections = _sections(booking, _verified_rules())
    assert sections.assessment.total_ige.band == "within_reference"
    assert sections.overview.detected_count == 1


def test_no_allergy_results_omit_sections():
    booking = _booking(
        [{"test_name": "Haemoglobin (HB)", "value": "10.5", "unit": "g/dL", "min_range": "12", "max_range": "15"}],
        groups=[{"group_name": "Complete Hemogram", "tests": [{"test_name": "Haemoglobin (HB)", "provider_test_name": "Haemoglobin (HB)"}]}],
    )
    assert build_allergy_sections(booking, assessment_instance_id=1) is None
    report = build_bioreport(
        {"data": {"record_id": "x", "diseases": [], "gender": "female", "age": 30}},
        record_id="x",
    )
    payload = report.to_dict()
    assert "allergy_overview" not in payload
    assert "allergy_detailed_insights" not in payload
    assert "allergy_breakdown" not in payload
    assert "allergy_availability" not in payload


def test_multiple_engagements_are_not_collapsed_to_latest():
    bookings = [
        {"id": 1, "user_id": 5, "engagement_id": 1, "collection_date": "2026-08-01"},
        {"id": 2, "user_id": 5, "engagement_id": 2, "collection_date": "2026-01-01"},
    ]
    status, booking, _reason = resolve_bookings(bookings, user_id=5, engagement_id=2)
    assert status == "resolved"
    assert booking["id"] == 2
    status, booking, reason = resolve_bookings(
        bookings + [{"id": 3, "user_id": 5, "engagement_id": 2}],
        user_id=5,
        engagement_id=2,
    )
    assert status == "unresolved"
    assert booking is None
    assert reason


def test_bioreport_includes_allergy_without_changing_disease_keys():
    report = build_bioreport(
        {"data": {"record_id": "x", "diseases": [], "gender": "female", "age": 30}},
        record_id="x",
        allergy_booking=_panel(),
        assessment_instance_id=7,
    )
    payload = report.to_dict()
    assert payload["allergy_overview"]["specific_tested"] == 215
    assert payload["allergy_overview"]["detected_count"] == 0
    assert payload["allergy_detailed_insights"]["insights"] == []
    assert payload["allergy_availability"]["processing_status"] == "ready"
    assert payload["allergy_availability"]["expected_test_count"] == 211
    assert payload["allergy_availability"]["panel_complete"] is True
    assert "disease_sections" in payload
    assert "detailed_cancer_screening" in payload


def test_booked_without_results_does_not_invent_negatives():
    booking = _booking([], groups=[{
        "group_name": "Allergy Dust Mites",
        "tests": [{"test_name": "Euroglyphus Maynei", "provider_test_name": "Euroglyphus Maynei"}],
    }])
    report = build_bioreport(
        {"data": {"record_id": "x", "diseases": [], "gender": "female", "age": 30}},
        record_id="x",
        allergy_booking=booking,
    )
    payload = report.to_dict()
    assert payload["allergy_availability"]["processing_status"] == "awaiting_results"
    assert "allergy_overview" not in payload


def test_one_profile_and_total_ige_only_and_specific_without_total():
    one = _sections(_booking([_row("Cat Dander", "<0.35")]))
    assert one.assessment.availability.booked_source_groups == ["pets"]
    assert one.overview.specific_tested == 1

    total_only = _sections(_booking([
        _row("IGE total", "30.9", unit="IU/ml", min_range="0", max_range="158"),
    ]))
    assert total_only.assessment.availability.total_ige_booked is True
    assert total_only.overview.specific_tested == 0
    assert total_only.assessment.total_ige.raw_value == "30.9"

    specific = _booking([_row("Peanut", "<0.35")])
    specific["blood_parameters"] = [{
        "group_name": "Allergy Seeds/Nuts/Legumes",
        "tests": [{"test_name": "Peanut", "provider_test_name": "Peanut"}],
    }]
    sections = _sections(specific)
    assert sections.assessment.total_ige is None
    assert sections.assessment.availability.total_ige_booked is False
    assert sections.overview.specific_tested == 1


def test_duplicate_measurement_needs_review():
    booking = _booking([_row("Cat Dander", "<0.35"), _row("Cat Dander", "1.2", lab_flag="positive")])
    sections = _sections(booking)
    assert sections.assessment.availability.processing_status == "needs_review"
    assert sections.overview.detected_count == 0


def test_blood_parameters_gate_cancer_and_allergy():
    from modules.bioai_report.report_engine.builders.allergy_builder import panels_booked

    cancer, allergy = panels_booked([{
        "group_name": "Allergy Profile",
        "tests": [{"test_name": "Allergy Pet profile"}, {"test_name": "IGE total"}],
    }, {
        "group_name": "Cancer Male C-suite",
        "tests": [{"test_name": "PSA"}],
    }, {
        "group_name": "Lipid Profile",
        "tests": [{"test_name": "Cholesterol"}],
    }])
    assert cancer is True
    assert allergy is True
    no_cancer, no_allergy = panels_booked([{"group_name": "Lipid Profile", "tests": []}])
    assert no_cancer is False
    assert no_allergy is False
    flagged_cancer, flagged_allergy = panels_booked([{
        "group_name": "Allergy Profile",
        "has_allergy_screening": True,
        "has_cancer_screening": False,
        "tests": [{"test_name": "Allergy Pet profile"}],
    }, {
        "group_name": "Cancer Male C-suite",
        "has_allergy_screening": False,
        "has_cancer_screening": False,
        "tests": [{"test_name": "PSA"}],
    }, {
        "group_name": "Screening",
        "has_cancer_screening": True,
        "has_allergy_screening": False,
        "tests": [],
    }])
    assert flagged_cancer is True
    assert flagged_allergy is True
    off_cancer, off_allergy = panels_booked([{
        "group_name": "Cancer Male C-suite",
        "has_cancer_screening": False,
        "has_allergy_screening": False,
        "tests": [{"test_name": "PSA"}],
    }, {
        "group_name": "Allergy Profile",
        "has_cancer_screening": False,
        "has_allergy_screening": False,
        "tests": [{"test_name": "IGE total"}],
    }])
    assert off_cancer is True
    assert off_allergy is False

    report = build_bioreport(
        {"data": {"record_id": "gate", "gender": "male", "diseases": []}},
        record_id="gate",
        include_cancer=False,
        cancer_results_available=False,
        allergy_results_available=False,
    )
    payload = report.to_dict()
    assert payload["cancer_results_available"] is False
    assert payload["allergy_results_available"] is False
    assert "individual_cancer_information" not in payload
    assert "detailed_cancer_screening" not in payload
    assert "allergy_overview" not in payload


def test_screening_flags_gate_allergy_profiles():
    booked = {
        "id": 1,
        "blood_parameters": [{
            "group_name": "Panels",
            "has_allergy_screening": True,
            "has_cancer_screening": False,
            "tests": [
                {"test_name": "Allergy Pet profile", "provider_test_name": "Allergy Pet profile"},
                {"test_name": "IGE total", "provider_test_name": "IGE total"},
            ],
        }],
        "blood_report_raw": {"digital_data": [
            {"test_name": "Cat Dander", "value": "<0.35", "unit": "IU/mL", "min_range": "0", "max_range": "0.35", "parameter_id": "1"},
        ]},
    }
    sections = _sections(booked)
    assert sections.assessment.availability.allergy_tests_booked is True
    assert "pets" in sections.assessment.availability.booked_source_groups
    skipped = {
        "id": 1,
        "blood_parameters": [{
            "group_name": "Allergy Profile",
            "has_allergy_screening": False,
            "has_cancer_screening": False,
            "tests": [{"test_name": "Allergy Pet profile"}],
        }],
        "blood_report_raw": {"digital_data": [
            {"test_name": "Cat Dander", "value": "<0.35", "unit": "IU/mL", "min_range": "0", "max_range": "0.35", "parameter_id": "1"},
        ]},
    }
    assert build_allergy_sections(skipped, assessment_instance_id=7) is None


def test_allergy_profile_group_books_nested_profile_names():
    booking = {
        "id": 1,
        "user_id": 2,
        "engagement_id": 14,
        "booking_id": "fixture",
        "collection_date": "2026-05-24",
        "blood_parameters": [{
            "group_name": "Allergy Profile",
            "test_count": 2,
            "tests": [
                {"test_id": 152, "test_name": "Allergy Pet profile", "provider_test_name": "Allergy Pet profile", "value": None},
                {"test_id": 154, "test_name": "IGE total", "provider_test_name": "IGE total", "value": None},
            ],
        }, {
            "group_name": "Complete Hemogram",
            "tests": [{"test_name": "Hemoglobin Hb", "provider_test_name": "Hemoglobin Hb"}],
        }],
        "blood_report_raw": {"digital_data": [
            {"test_name": "Cat Dander", "value": "<0.35", "unit": "IU/mL", "min_range": "0", "max_range": "0.35", "parameter_id": "1"},
            {"test_name": "IMMUNOGLOBULIN E (IgE) TOTAL", "value": "30.9", "unit": "IU/ml", "min_range": "0", "max_range": "158", "parameter_id": "2"},
            {"test_name": "Hemoglobin Hb", "value": "10.5", "unit": "g/dL", "min_range": "12", "max_range": "15", "parameter_id": "3"},
            {"test_name": "Peanut", "value": "<0.35", "unit": "IU/mL", "min_range": "0", "max_range": "0.35", "parameter_id": "4"},
        ]},
    }
    sections = _sections(booking)
    availability = sections.assessment.availability
    assert availability.allergy_tests_booked is True
    assert set(availability.booked_source_groups) == {"pets", "ige"}
    assert availability.total_ige_booked is True
    names = {item.lab_test_name for item in sections.assessment.results}
    assert "Cat Dander" in names
    assert "Peanut" not in names
    assert "Hemoglobin Hb" not in names
    assert sections.assessment.total_ige is not None
    assert sections.assessment.total_ige.raw_value == "30.9"
    assert all(item.result_status != "missing" for item in sections.assessment.results)


def test_project_owner_release_does_not_claim_clinical_signoff():
    from modules.bioai_report.report_engine.knowledge_base.allergy_loader import AllergyKnowledgeBase

    kb = AllergyKnowledgeBase()
    record = kb.record("cat_dander")
    assert record["clinical_review_status"] == "pending"
    assert kb.qualified_clinical_approval == "pending"
    assert kb.content_released() is True
    assert kb.narratives_approved(record) is True
    kb.manifest = {
        "content_release": {"status": "pending", "authority": "project_owner", "covers": []}
    }
    assert kb.narratives_approved(record) is False


def test_assay_boundaries_overrides_and_units():
    from decimal import Decimal

    from modules.bioai_report.report_engine.builders.assay_interpreter import interpret_specific_ige

    rules = _verified_rules()
    rules["laboratories"][0]["analyte_rules"] = {
        "peanut": {"profile_id": "high_cutoff"},
        "cat_dander": {
            "override": {
                "status": "verified",
                "unit": "IU/mL",
                "positive_cutoff": {"comparator": ">=", "value": "3.5"},
                "accept_laboratory_flag": False,
            }
        },
    }

    def run(allergen, comparator, number, unit="IU/mL"):
        return interpret_specific_ige(
            rules=rules,
            laboratory_id="fixture_lab",
            allergen_id=allergen,
            parse_status="bounded" if comparator not in {None, "="} else "exact",
            comparator=None if comparator == "=" else comparator,
            number=Decimal(number),
            unit=unit,
            lab_flag=None,
            lab_class=None,
        )["result_status"]

    assert run("tyrophagus_putrescentiae", "<", "0.35") == "negative_per_lab"
    assert run("cat_dander", "<=", "3.5") == "indeterminate"
    assert run("cat_dander", "=", "3.5") == "positive"
    assert run("cat_dander", ">", "3.5") == "positive"
    assert run("peanut", "=", "0.52") == "negative_per_lab"
    assert run("tyrophagus_putrescentiae", "=", "0.35") == "positive"
    assert run("cat_dander", "=", "4", unit="kU/L") == "indeterminate"
    blocked = interpret_specific_ige(
        rules={"laboratories": []},
        laboratory_id=None,
        allergen_id="cat_dander",
        parse_status="bounded",
        comparator="<",
        number=Decimal("0.35"),
        unit="IU/mL",
        lab_flag=None,
        lab_class=None,
    )
    assert blocked["result_status"] == "indeterminate"


def test_named_allergy_group_stays_booked_when_screening_flag_is_false():
    booking = {
        "id": 1,
        "blood_parameters": [{
            "group_name": "Allergy Pets",
            "has_allergy_screening": False,
            "has_cancer_screening": False,
            "tests": [{"test_name": "Cat Dander", "provider_test_name": "Cat Dander"}],
        }],
        "blood_report_raw": {"digital_data": [
            {"test_name": "Cat Dander", "value": "<0.35", "unit": "IU/mL", "min_range": "0", "max_range": "0.35"},
        ]},
    }
    sections = _sections(booking)
    assert "pets" in sections.assessment.availability.booked_source_groups
    assert sections.overview.specific_tested == 1
    assert sections.assessment.results[0].lab_test_name == "Cat Dander"


def test_total_ige_does_not_warn_about_missing_specific_assay_rule():
    booking = {
        "id": 1,
        "blood_parameters": [{
            "group_name": "Allergy IgE",
            "has_allergy_screening": True,
            "has_cancer_screening": False,
            "tests": [{"test_name": "IGE total", "provider_test_name": "IMMUNOGLOBULIN E (IgE) TOTAL"}],
        }],
        "blood_report_raw": {"digital_data": [
            {
                "test_name": "IMMUNOGLOBULIN E (IgE) TOTAL",
                "value": "30.9",
                "unit": "IU/ml",
                "min_range": "0",
                "max_range": "158",
            },
        ]},
    }
    sections = _sections(booking)
    assert sections.assessment.total_ige is not None
    assert sections.assessment.total_ige.band == "within_reference"
    warnings = sections.assessment.availability.validation_warnings
    assert not any("no verified assay rule" in warning for warning in warnings)


def test_cancer_panel_markers_are_read_when_screening_flag_is_false():
    report = build_bioreport(
        {"data": {"record_id": "gate", "gender": "male", "diseases": []}},
        record_id="gate",
        include_cancer=True,
        cancer_results_available=True,
        allergy_booking={
            "blood_parameters": [{
                "group_name": "Cancer Male C-suite",
                "has_cancer_screening": False,
                "has_allergy_screening": False,
                "tests": [
                    {"test_name": "PSA", "provider_test_name": "PROSTATIC SPECIFIC ANTIGEN (PSA TOTAL)"},
                    {"test_name": "CA 19.9", "provider_test_name": "CA-19.9"},
                ],
            }],
            "blood_report_raw": {"digital_data": [
                {
                    "test_name": "PROSTATIC SPECIFIC ANTIGEN (PSA TOTAL)",
                    "value": "0.49",
                    "unit": "ng/mL",
                    "min_range": "0.01",
                    "max_range": "4",
                },
                {
                    "test_name": "CA-19.9",
                    "value": "< 1.2",
                    "unit": "U/mL",
                    "min_range": "1.2",
                    "max_range": "37",
                },
                {
                    "test_name": "CARCINO EMBRYONIC ANTIGEN (CEA)",
                    "value": "5.73",
                    "unit": "ng/mL",
                    "min_range": "0",
                    "max_range": "5",
                },
            ]},
        },
    )
    cards = {
        card["cancer_code"]: card
        for card in report.to_dict()["detailed_cancer_screening"]["cancer_results"]
    }
    assert cards["prostate_cancer"]["marker_name"] == "PROSTATIC SPECIFIC ANTIGEN (PSA TOTAL)"
    assert cards["prostate_cancer"]["value"] == 0.49
    assert cards["prostate_cancer"]["range_flag"] == "in_range"
    assert cards["pancreatic_cancer"]["marker_name"] == "CA-19.9"
    assert "colorectal_cancer" not in cards
