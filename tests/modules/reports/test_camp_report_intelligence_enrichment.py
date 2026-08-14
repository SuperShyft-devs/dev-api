"""Tests for camp-report intelligence enrichment assembly."""

from __future__ import annotations

import copy

from modules.reports.camp_report_intelligence import (
    INTELLIGENCE_CAMP_SECTIONS,
    enrich_camp_report_with_intelligence,
    generate_report_insights,
)


def _gender_distribution(*, elevated_key: str = "rarely_or_never") -> dict:
    return {
        "male": {
            "group": ["less_than_30mins", "30_60_mins", "more_than_60_mins", elevated_key],
            "percent": [20.0, 30.0, 10.0, 40.0],
            "count": [10, 15, 5, 20],
        },
        "female": {
            "group": ["less_than_30mins", "30_60_mins", "more_than_60_mins", elevated_key],
            "percent": [25.0, 35.0, 15.0, 25.0],
            "count": [12, 18, 8, 12],
        },
    }


def _sleep_distribution() -> dict:
    return {
        "male": {
            "group": ["less_than_5hrs", "between_5_7_hrs", "between_7_9_hrs", "more_than_9hrs"],
            "percent": [15.0, 40.0, 35.0, 10.0],
            "count": [8, 20, 17, 5],
        },
        "female": {
            "group": ["less_than_5hrs", "between_5_7_hrs", "between_7_9_hrs", "more_than_9hrs"],
            "percent": [10.0, 45.0, 40.0, 5.0],
            "count": [5, 22, 20, 3],
        },
    }


def _disease_item(code: str) -> dict:
    groups = ["healthy", "increased", "high", "very_high"]
    return {
        "code": code,
        "male": {
            "group": groups,
            "percent": [40.0, 20.0, 25.0, 15.0],
            "count": [20, 10, 12, 8],
            "elevated_percent": 40.0,
        },
        "female": {
            "group": groups,
            "percent": [50.0, 20.0, 20.0, 10.0],
            "count": [25, 10, 10, 5],
            "elevated_percent": 30.0,
        },
    }


def sample_camp_report() -> dict:
    """Minimal camp report covering mapped + unmapped sections."""
    return {
        "meta": {"summary_available": True, "refreshed_at": "2026-08-10T00:00:00Z"},
        "kpis": {
            "data": {"male_enrolled": 50, "female_enrolled": 50, "total_enrolled": 100},
            "name": "KPIs",
            "description": "kpi-desc",
        },
        "participation_by_age": {
            "data": {
                "age_group": ["18–25", "26–35", "36–45"],
                "enrolled": [20, 40, 40],
                "percent": [20.0, 40.0, 40.0],
            },
            "name": "Participation by Age",
            "description": "participation-desc",
        },
        "overall_risk_score": {
            "data": {
                "group": ["optimal", "low_risk", "increased_risk", "high_risk"],
                "percent": [20.0, 30.0, 35.0, 15.0],
                "count": [20, 30, 35, 15],
            },
            "name": "Overall Risk Score",
            "description": None,
        },
        "distribution_by_physical_activity_frequency": {
            "data": _gender_distribution(),
            "name": "Physical Activity",
            "description": "pa-desc",
        },
        "distribution_by_sleeping_hours": {
            "data": _sleep_distribution(),
            "name": "Sleep",
            "description": "sleep-desc",
        },
        "distribution_by_oxidative_stress": {
            "data": {
                "group": ["low", "moderate", "high", "very_high"],
                "percent": [25.0, 35.0, 30.0, 10.0],
                "total_employees": 100,
            },
            "name": "Oxidative Stress",
            "description": None,
        },
        "distribution_by_gender_by_metabolic_syndrome": {
            "data": {
                "diseases": [
                    _disease_item("metabolic_syndrome"),
                    _disease_item("diabetes"),
                ]
            },
            "name": "Metabolic Syndrome",
            "description": "metabolic-desc",
        },
        "positive_wins": {
            "data": {
                "low_risk": [
                    {"code": "hypertension", "name": "Hypertension", "risk_status": "low"}
                ],
                "healthy_habits": [{"habit_label": "Regular exercise"}],
                "healthy_profiles": ["Balanced lifestyle"],
            },
            "name": "Positive Wins",
            "description": "wins-desc",
        },
        "blood_and_lab_intelligence": {
            "data": {"lipid_profile": {"ldl": {"in_range_percent": 70}}},
            "name": "Blood & Lab",
            "description": "blood-desc",
        },
        "company_average_scores": {
            "data": {
                "nutrition": {"score": 62},
                "fitness": {"score": 55},
                "lifestyle": {"score": 58},
            },
            "name": "Company Average Scores",
            "description": None,
        },
        "ranking": {
            "data": {"departments": []},
            "name": "Ranking",
            "description": "ranking-desc",
        },
    }


def test_enrich_preserves_top_level_keys_and_section_fields():
    original = sample_camp_report()
    snapshot = copy.deepcopy(original)

    enriched = enrich_camp_report_with_intelligence(original)

    assert list(enriched.keys()) == list(snapshot.keys())
    assert set(enriched.keys()) == set(snapshot.keys())

    for key, section in snapshot.items():
        if key == "meta":
            assert enriched[key] == section
            continue
        assert enriched[key]["data"] == section["data"]
        assert enriched[key]["name"] == section["name"]
        assert enriched[key]["description"] == section["description"]

    # Input must not be mutated.
    assert original == snapshot
    assert "intelligence" not in original["overall_risk_score"]


def test_enrich_attaches_intelligence_to_mapped_sections_only():
    enriched = enrich_camp_report_with_intelligence(sample_camp_report())

    for section_key in INTELLIGENCE_CAMP_SECTIONS:
        assert "intelligence" in enriched[section_key], section_key

    for section_key in (
        "kpis",
        "blood_and_lab_intelligence",
        "company_average_scores",
        "ranking",
    ):
        assert enriched[section_key] == sample_camp_report()[section_key]
        assert "intelligence" not in enriched[section_key]

    assert "intelligence" not in enriched["meta"]


def test_enrich_does_not_include_profile_or_leadership_cards():
    enriched = enrich_camp_report_with_intelligence(sample_camp_report())

    assert "profile" not in enriched
    assert "leadership_cards" not in enriched
    assert "concerns" not in enriched
    assert "positives" not in enriched

    raw = generate_report_insights(sample_camp_report())
    assert "profile" in raw
    assert "leadership_cards" in raw


def test_enrich_metabolic_and_positives_structure():
    enriched = enrich_camp_report_with_intelligence(sample_camp_report())

    metabolic = enriched["distribution_by_gender_by_metabolic_syndrome"]["intelligence"]
    assert isinstance(metabolic, dict)
    assert "disease_risks" in metabolic
    assert "disease_deep_dive" in metabolic
    assert isinstance(metabolic["disease_deep_dive"], dict)

    positives = enriched["positive_wins"]["intelligence"]
    assert isinstance(positives, dict)
    assert "positive_highlights" in positives
    assert "positives" in positives


def test_enrich_lifestyle_sections_have_gender_views():
    enriched = enrich_camp_report_with_intelligence(sample_camp_report())

    for section_key in (
        "distribution_by_physical_activity_frequency",
        "distribution_by_sleeping_hours",
    ):
        intel = enriched[section_key]["intelligence"]
        assert set(intel.keys()) >= {"both", "male", "female"}


_FORBIDDEN_PUBLIC_INTEL_KEYS = frozenset(
    {
        "structured",
        "severity_band",
        "confidence",
        "effect_ids",
        "lever_ids",
        "related_metrics",
        "notes",
        "section_id",
        "mode",
        "text",
    }
)


def _walk_intelligence(node: object):
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _walk_intelligence(value)
    elif isinstance(node, list):
        for item in node:
            yield from _walk_intelligence(item)


def test_enrich_public_intelligence_is_frontend_contract_only():
    original = sample_camp_report()
    raw = generate_report_insights(original)
    overall_raw = (raw.get("concerns") or {}).get("overall_risk") or {}
    assert "structured" in overall_raw
    assert "confidence" in overall_raw

    enriched = enrich_camp_report_with_intelligence(original)
    overall = enriched["overall_risk_score"]["intelligence"]
    assert set(overall.keys()) == {"tone", "observation", "explanation", "recommendation"}
    assert overall["tone"]
    assert overall["observation"]
    assert overall["explanation"]
    assert overall["recommendation"]

    structured = (overall_raw.get("structured") or {})
    assert overall["observation"] == structured.get("observation")
    assert overall["explanation"] == structured.get("explanation")
    assert overall["recommendation"] == structured.get("recommendation")
    assert overall["tone"] == overall_raw.get("tone") or structured.get("tone")

    for section_key in INTELLIGENCE_CAMP_SECTIONS:
        for node in _walk_intelligence(enriched[section_key]["intelligence"]):
            leaked = _FORBIDDEN_PUBLIC_INTEL_KEYS.intersection(node)
            assert not leaked, f"{section_key} leaked {sorted(leaked)} in {list(node)}"


def test_enrich_skips_missing_sections_without_creating_them():
    report = {
        "meta": {},
        "overall_risk_score": {
            "data": {
                "group": ["optimal", "low_risk", "increased_risk", "high_risk"],
                "percent": [40.0, 30.0, 20.0, 10.0],
                "count": [40, 30, 20, 10],
            },
            "name": "Overall Risk Score",
            "description": None,
        },
        "kpis": {
            "data": {"male_enrolled": 10, "female_enrolled": 10},
            "name": "KPIs",
            "description": None,
        },
    }
    enriched = enrich_camp_report_with_intelligence(report)

    assert set(enriched.keys()) == set(report.keys())
    assert "intelligence" in enriched["overall_risk_score"]
    assert "intelligence" not in enriched["kpis"]
    assert "participation_by_age" not in enriched
    assert "positive_wins" not in enriched
