"""Tests for Bio-AI result-factor set selection and KB coverage."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from modules.bioai_report.report_engine.builders.disease_builder import build_disease_section
from modules.bioai_report.report_engine.knowledge_base.loader import KnowledgeBaseStore
from modules.bioai_report.report_engine.models.assessment import AssessmentDisease
from modules.bioai_report.report_engine.models.knowledge_base import DiseaseKnowledgeBase
from modules.bioai_report.report_engine.models.report import DiseaseSection
from modules.bioai_report.report_engine.utils.result_factors import (
    RESULT_FACTOR_TITLE,
    score_to_result_factor_set_key,
)
from modules.bioai_report.report_engine.utils.score_bands import score_to_risk_band

KB_DIR = (
    Path(__file__).resolve().parents[3]
    / "modules"
    / "bioai_report"
    / "report_engine"
    / "knowledge_base"
)
EXCEL_EXPECTED_PATH = Path(__file__).parent / "excel_result_factors_expected.json"

EXPECTED_DISEASE_IDS = [
    "cardiac_health",
    "dyslipidemia",
    "hypertension",
    "metabolic_syndrome",
    "nafld",
    "obesity",
    "oxidative_stress",
    "thyroid_health",
    "type2_diabetes",
]

FACTOR_SET_BOUNDARIES = [
    (0, "set_1"),
    (20, "set_1"),
    (21, "set_2"),
    (40, "set_2"),
    (41, "set_3"),
    (60, "set_3"),
    (61, "set_4"),
    (80, "set_4"),
    (81, "set_5"),
    (100, "set_5"),
]

# Risk bands must remain unchanged (independent of factor-set ranges).
RISK_BAND_CASES = [
    (0, "healthy"),
    (23, "healthy"),
    (25, "healthy"),
    (26, "increased_risk"),
    (50, "increased_risk"),
    (51, "high_risk"),
    (67, "high_risk"),
    (75, "high_risk"),
    (76, "very_high_risk"),
    (84, "very_high_risk"),
    (100, "very_high_risk"),
]

LEGACY_DISEASE_SECTION_FIELDS = {
    "disease_id",
    "title",
    "overview",
    "current_status",
    "lifestyle",
    "nutrition",
    "monitoring",
    "positive_takeaway",
}


@pytest.fixture(scope="module")
def kb_store() -> KnowledgeBaseStore:
    return KnowledgeBaseStore(KB_DIR)


@pytest.fixture(scope="module")
def excel_expected() -> dict:
    return json.loads(EXCEL_EXPECTED_PATH.read_text(encoding="utf-8"))


@pytest.mark.parametrize("score,expected_key", FACTOR_SET_BOUNDARIES)
def test_score_to_result_factor_set_key_boundaries(score: int, expected_key: str) -> None:
    assert score_to_result_factor_set_key(score) == expected_key


@pytest.mark.parametrize("score,expected_band", RISK_BAND_CASES)
def test_risk_band_calculation_unchanged(score: int, expected_band: str) -> None:
    assert score_to_risk_band(score) == expected_band


def test_factor_set_ranges_differ_from_risk_bands() -> None:
    """Documented divergence: score 23 is Healthy but uses set_2."""
    assert score_to_risk_band(23) == "healthy"
    assert score_to_result_factor_set_key(23) == "set_2"
    assert score_to_risk_band(67) == "high_risk"
    assert score_to_result_factor_set_key(67) == "set_4"
    assert score_to_risk_band(84) == "very_high_risk"
    assert score_to_result_factor_set_key(84) == "set_5"


def test_all_disease_kb_files_validate(kb_store: KnowledgeBaseStore) -> None:
    disease_ids = kb_store.list_disease_ids()
    assert sorted(disease_ids) == EXPECTED_DISEASE_IDS
    for disease_id in disease_ids:
        kb = kb_store.get(disease_id)
        assert isinstance(kb, DiseaseKnowledgeBase)
        assert kb.disease_id == disease_id


def test_no_disease_missing_factor_sets(kb_store: KnowledgeBaseStore) -> None:
    for disease_id in kb_store.list_disease_ids():
        sets = kb_store.get(disease_id).result_factor_sets
        for key in ("set_1", "set_2", "set_3", "set_4", "set_5"):
            factor_set = sets.get(key)
            assert factor_set is not None, f"{disease_id} missing {key}"
            assert factor_set.score_range
            assert factor_set.factors, f"{disease_id} {key} has no factors"


def test_factor_text_exactly_matches_excel(
    kb_store: KnowledgeBaseStore, excel_expected: dict
) -> None:
    assert set(excel_expected) == set(EXPECTED_DISEASE_IDS)
    for disease_id, expected_sets in excel_expected.items():
        kb = kb_store.get(disease_id)
        for set_key, expected in expected_sets.items():
            actual = kb.result_factor_sets.get(set_key)
            assert actual is not None
            assert actual.score_range == expected["score_range"]
            assert actual.factors == expected["factors"]


@pytest.mark.parametrize("score,expected_key", FACTOR_SET_BOUNDARIES)
def test_disease_section_selects_correct_factor_set(
    kb_store: KnowledgeBaseStore, score: int, expected_key: str
) -> None:
    kb = kb_store.get("metabolic_syndrome")
    disease = AssessmentDisease(
        code="metabolic_syndrome",
        name="Metabolic Syndrome",
        risk_score_scaled=score,
    )
    section = build_disease_section(disease, kb_store=kb_store, kb=kb)
    assert section is not None
    expected_factors = kb.result_factor_sets.get(expected_key).factors
    assert section.result_factors.title == RESULT_FACTOR_TITLE
    assert section.result_factors.factors == expected_factors


def test_result_factors_appears_for_every_disease(kb_store: KnowledgeBaseStore) -> None:
    for disease_id in kb_store.list_disease_ids():
        disease = AssessmentDisease(
            code=disease_id,
            name=disease_id,
            risk_score_scaled=67,
        )
        section = build_disease_section(disease, kb_store=kb_store)
        assert section is not None
        assert section.result_factors.title == RESULT_FACTOR_TITLE
        assert section.result_factors.factors == kb_store.get(
            disease_id
        ).result_factor_sets.set_4.factors


def test_existing_output_fields_remain_unchanged(kb_store: KnowledgeBaseStore) -> None:
    disease = AssessmentDisease(
        code="metabolic_syndrome",
        name="Metabolic Syndrome",
        risk_score_scaled=67,
        risk_status="High",
    )
    section = build_disease_section(disease, kb_store=kb_store)
    assert section is not None
    payload = section.model_dump(mode="json")
    assert LEGACY_DISEASE_SECTION_FIELDS.issubset(payload.keys())
    assert "result_factors" in payload
    assert payload["current_status"]["score"] == 67
    assert payload["current_status"]["risk"] == "High"
    assert payload["current_status"]["band"] == "66-70"
    assert isinstance(payload["lifestyle"]["tips"], list)
    assert isinstance(payload["nutrition"]["recommendations"], list)
    assert isinstance(payload["monitoring"]["recommendations"], list)
    assert isinstance(payload["positive_takeaway"], str)
    assert isinstance(section, DiseaseSection)
    assert section.score == 67
    assert section.risk == "High"
    assert section.band == "66-70"
