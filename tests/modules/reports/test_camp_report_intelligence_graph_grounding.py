"""Intelligence stays generic: facts come from whatever Camp JSON is supplied.

Graph extras may improve analyzer accuracy. Observation/mode/recommendation
still come from the existing engine (elevated/healthy shares, knowledge, levers).
"""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path

from modules.reports.camp_report_intelligence import enrich_camp_report_with_intelligence
from modules.reports.camp_report_intelligence.intelligence_src.analyzer import (
    dashboard_input_from_camp_report,
    finding_from_disease,
    finding_from_overall_risk,
    finding_from_physical_activity,
    finding_from_positive_wins,
    finding_from_sleep,
    _map_diseases,
)
from modules.reports.camp_report_section_builders import (
    DISEASE_RISK_BANDS,
    _build_gender_risk_distribution,
)
from modules.reports.camp_report_intelligence.intelligence_src.knowledge import MEDICAL_FRAMES, medical_frame
from modules.reports.camp_report_intelligence.intelligence_src.models import round1

ROOT = Path(__file__).resolve().parents[3]
FIXTURE = ROOT / "camp_report_json"
_PCT = re.compile(r"(\d+(?:\.\d+)?)%")

_NARRATOR_MARKERS = (
    "largest band",
    "largest slice",
    "largest plotted",
    "count-weighted",
    "not a plotted slice",
    "excludes the Increased band",
    "matching the chart's elevated total",
)


def _fmt_share(value: float) -> str:
    rounded = round1(float(value))
    if rounded == int(rounded):
        return str(int(rounded))
    return str(rounded)


def _pcts(text: str) -> list[float]:
    return [float(match) for match in _PCT.findall(text or "")]


def _load_fixture() -> dict:
    return json.loads(FIXTURE.read_text())


def _intel_block(section: dict, *path: str) -> dict:
    node = section.get("intelligence") or {}
    for key in path:
        node = node.get(key) or {}
    return node


def _leaf(section: dict, *path: str) -> dict:
    """Frontend intelligence leaf: tone / observation / explanation / recommendation."""
    block = _intel_block(section, *path)
    if "both" in (section.get("intelligence") or {}) and not path:
        block = _intel_block(section, "both")
    structured = block.get("structured")
    if isinstance(structured, dict):
        return structured
    return block


def _obs(section: dict, *path: str) -> str:
    return str(_leaf(section, *path).get("observation") or "")


def _why(section: dict, *path: str) -> str:
    return str(_leaf(section, *path).get("explanation") or "")


def _rec(section: dict, *path: str) -> str:
    return str(_leaf(section, *path).get("recommendation") or "")


def _tone(section: dict, *path: str) -> str:
    block = _intel_block(section, *path)
    if "both" in (section.get("intelligence") or {}) and not path:
        block = _intel_block(section, "both")
    return str(block.get("tone") or "")


def _dist(counts: dict[str, int]) -> dict:
    return _build_gender_risk_distribution(counts, DISEASE_RISK_BANDS)


def _assert_not_graph_narrator(text: str) -> None:
    lowered = (text or "").lower()
    for marker in _NARRATOR_MARKERS:
        assert marker not in lowered, f"observation narrates the graph ({marker!r}): {text}"


def test_fixture_preserves_camp_report_data_and_structure():
    original = _load_fixture()
    snapshot = copy.deepcopy(original)
    enriched = enrich_camp_report_with_intelligence(original)

    assert list(enriched.keys()) == list(snapshot.keys())
    assert "profile" not in enriched
    assert "leadership_cards" not in enriched
    for key, section in snapshot.items():
        if not isinstance(section, dict):
            continue
        assert enriched[key].get("data") == section.get("data")
        assert enriched[key].get("name") == section.get("name")
        assert enriched[key].get("description") == section.get("description")
    assert original == snapshot


def test_observations_use_analyzer_shares_not_category_census():
    report = _load_fixture()
    snapshot = copy.deepcopy(report)
    enriched = enrich_camp_report_with_intelligence(report)
    data = dashboard_input_from_camp_report(report)
    kpis = ((report.get("kpis") or {}).get("data") or {})
    weights = {
        "male": float(kpis.get("male_enrolled") or 1),
        "female": float(kpis.get("female_enrolled") or 1),
    }

    overall = finding_from_overall_risk(data.overall_risk_score)
    overall_text = _obs(enriched["overall_risk_score"])
    assert _fmt_share(overall.elevated_share) in overall_text
    _assert_not_graph_narrator(overall_text)
    if overall.elevated_share >= 30:
        assert _tone(enriched["overall_risk_score"]) == "concern"
    else:
        assert _tone(enriched["overall_risk_score"]) == "positive"

    pa_section = enriched["distribution_by_physical_activity_frequency"]
    for view in ("both", "male", "female"):
        finding = finding_from_physical_activity(data.physical_activity, view, weights)
        text = _obs(pa_section, view)
        assert _fmt_share(finding.elevated_share) in text
        assert "physical activity" in text.lower()
        _assert_not_graph_narrator(text)

    sleep_section = enriched["distribution_by_sleeping_hours"]
    for view in ("both", "male", "female"):
        finding = finding_from_sleep(data.sleep, view, weights)
        text = _obs(sleep_section, view)
        assert _fmt_share(finding.elevated_share) in text
        _assert_not_graph_narrator(text)

    participation = _obs(enriched["participation_by_age"])
    top = dashboard_input_from_camp_report(report).participation_by_age
    assert top
    ranked = sorted(top, key=lambda row: (row.percent, row.enrolled), reverse=True)
    assert ranked[0].age_group in participation
    assert _fmt_share(ranked[0].percent) in participation
    _assert_not_graph_narrator(participation)

    positives = _obs(enriched["positive_wins"], "positive_highlights")
    low_risk = ((report.get("positive_wins") or {}).get("data") or {}).get("low_risk") or []
    profiles = ((report.get("positive_wins") or {}).get("data") or {}).get("healthy_profiles") or []
    for item in low_risk:
        name = str((item or {}).get("name") or "").strip()
        if name:
            assert name in positives
    for name in profiles:
        if str(name).strip():
            assert str(name) in positives
    assert _pcts(positives) == []

    assert report == snapshot


def test_overall_risk_observation_changes_when_distribution_changes():
    """Synthetic variation 1: same schema, different overall-risk mix."""
    healthy_report = _load_fixture()
    elevated_report = copy.deepcopy(healthy_report)

    healthy_report["overall_risk_score"]["data"]["percent"] = [55.0, 35.0, 6.0, 4.0]
    healthy_report["overall_risk_score"]["data"]["count"] = [55, 35, 6, 4]
    elevated_report["overall_risk_score"]["data"]["percent"] = [5.0, 10.0, 40.0, 45.0]
    elevated_report["overall_risk_score"]["data"]["count"] = [5, 10, 40, 45]

    healthy_in = copy.deepcopy(healthy_report)
    elevated_in = copy.deepcopy(elevated_report)
    healthy_out = enrich_camp_report_with_intelligence(healthy_report)
    elevated_out = enrich_camp_report_with_intelligence(elevated_report)

    healthy_finding = finding_from_overall_risk(
        dashboard_input_from_camp_report(healthy_report).overall_risk_score
    )
    elevated_finding = finding_from_overall_risk(
        dashboard_input_from_camp_report(elevated_report).overall_risk_score
    )
    healthy_text = _obs(healthy_out["overall_risk_score"])
    elevated_text = _obs(elevated_out["overall_risk_score"])

    assert healthy_finding.elevated_share < 30
    assert elevated_finding.elevated_share >= 30
    assert _fmt_share(healthy_finding.elevated_share) in healthy_text
    assert _fmt_share(elevated_finding.elevated_share) in elevated_text
    assert _tone(healthy_out["overall_risk_score"]) == "positive"
    assert _tone(elevated_out["overall_risk_score"]) == "concern"
    assert healthy_text != elevated_text
    _assert_not_graph_narrator(healthy_text)
    _assert_not_graph_narrator(elevated_text)
    assert healthy_report == healthy_in
    assert elevated_report == elevated_in


def test_sleep_observation_changes_when_distribution_changes():
    """Synthetic variation 2: same schema, inverted sleep mix."""
    recovered = _load_fixture()
    strained = copy.deepcopy(recovered)

    def _set_sleep(report: dict, percents: list[float], counts: list[int]) -> None:
        for gender in ("male", "female"):
            side = report["distribution_by_sleeping_hours"]["data"][gender]
            side["percent"] = list(percents)
            side["count"] = list(counts)

    _set_sleep(recovered, [2.0, 8.0, 85.0, 5.0], [2, 8, 85, 5])
    _set_sleep(strained, [20.0, 65.0, 10.0, 5.0], [20, 65, 10, 5])

    recovered_in = copy.deepcopy(recovered)
    strained_in = copy.deepcopy(strained)
    recovered_out = enrich_camp_report_with_intelligence(recovered)
    strained_out = enrich_camp_report_with_intelligence(strained)

    rec_finding = finding_from_sleep(
        dashboard_input_from_camp_report(recovered).sleep, "both"
    )
    strain_finding = finding_from_sleep(
        dashboard_input_from_camp_report(strained).sleep, "both"
    )
    rec_text = _obs(recovered_out["distribution_by_sleeping_hours"], "both")
    strain_text = _obs(strained_out["distribution_by_sleeping_hours"], "both")

    assert rec_finding.elevated_share < 30
    assert strain_finding.elevated_share >= 30
    assert _fmt_share(rec_finding.healthy_share) in rec_text or _fmt_share(rec_finding.elevated_share) in rec_text
    assert _fmt_share(strain_finding.elevated_share) in strain_text
    assert "recommended" in rec_text.lower() or "healthy sleep" in rec_text.lower()
    assert "outside the recommended sleep range" in strain_text.lower()
    assert rec_text != strain_text
    _assert_not_graph_narrator(rec_text)
    _assert_not_graph_narrator(strain_text)
    assert recovered == recovered_in
    assert strained == strained_in


def test_disease_lead_follows_whichever_condition_is_highest_in_input():
    report = _load_fixture()
    data = dashboard_input_from_camp_report(report)
    top = (data.top_high_risk_diseases or [None])[0]
    if top is None:
        return
    enriched = enrich_camp_report_with_intelligence(report)
    lead = _obs(
        enriched["distribution_by_gender_by_metabolic_syndrome"],
        "disease_risks",
    )
    assert _fmt_share(top.high_risk_percent) in lead
    assert "leads with" in lead.lower()
    _assert_not_graph_narrator(lead)


def test_missing_optional_sections_still_enrich_what_is_present():
    report = {
        "meta": {},
        "overall_risk_score": {
            "data": {
                "group": ["optimal", "low_risk", "increased_risk", "high_risk"],
                "percent": [70.0, 20.0, 6.0, 4.0],
                "count": [70, 20, 6, 4],
            },
            "name": "Overall Risk Score",
            "description": None,
        },
        "kpis": {"data": {"male_enrolled": 10, "female_enrolled": 10}, "name": "KPIs", "description": None},
    }
    snapshot = copy.deepcopy(report)
    enriched = enrich_camp_report_with_intelligence(report)

    assert set(enriched.keys()) == set(snapshot.keys())
    assert "intelligence" in enriched["overall_risk_score"]
    assert "participation_by_age" not in enriched
    assert "positive_wins" not in enriched
    assert "distribution_by_sleeping_hours" not in enriched
    finding = finding_from_overall_risk(
        dashboard_input_from_camp_report(report).overall_risk_score
    )
    text = _obs(enriched["overall_risk_score"])
    assert _fmt_share(finding.elevated_share) in text
    assert report == snapshot


def test_disease_gender_gap_is_high_plus_very_high_not_an_average():
    item = {
        "code": "type_2_diabetes",
        "male": _dist({"healthy": 20, "increased": 55, "high": 18, "very_high": 7}),
        "female": _dist({"healthy": 35, "increased": 45, "high": 15, "very_high": 5}),
    }
    _, diseases = _map_diseases({"diseases": [item]})
    finding = finding_from_disease(diseases[0])
    assert finding.gender_gap is not None
    assert finding.gender_gap.male_elevated == 25.0
    assert finding.gender_gap.female_elevated == 20.0
    assert finding.extras["increased_percent"] == 50.0
    assert finding.extras["high_plus_very_high"] == 22.5
    assert finding.dominant.label == "Increased"


def test_lifestyle_both_uses_responder_counts_not_kpi_blend():
    report = {
        "kpis": {"data": {"male_enrolled": 90, "female_enrolled": 10}},
        "distribution_by_physical_activity_frequency": {
            "data": {
                "male": {
                    "group": ["less_than_30mins", "30_60_mins", "more_than_60_mins", "rarely_or_never"],
                    "percent": [10.0, 10.0, 10.0, 70.0],
                    "count": [1, 1, 1, 7],
                },
                "female": {
                    "group": ["less_than_30mins", "30_60_mins", "more_than_60_mins", "rarely_or_never"],
                    "percent": [80.0, 10.0, 10.0, 0.0],
                    "count": [8, 1, 1, 0],
                },
            }
        },
    }
    data = dashboard_input_from_camp_report(report)
    both = finding_from_physical_activity(data.physical_activity, "both", {"male": 90, "female": 10})
    rarely = next(c for c in both.categories if c.label == "Rarely or Never")
    lt30 = next(c for c in both.categories if c.label == "Less than 30mins")
    assert rarely.percent == 35.0
    assert lt30.percent == 45.0
    assert both.elevated_share == 80.0


def test_positive_wins_finding_has_no_fabricated_residual_percent():
    from modules.reports.camp_report_intelligence.intelligence_src.models import HealthyHabit, PositiveWinDisease, PositiveWins

    finding = finding_from_positive_wins(
        PositiveWins(
            low_risk=[PositiveWinDisease(code="x", name="Thyroid Health", risk_status="low")],
            healthy_habits=[HealthyHabit(habit_label="Better Hydration")],
            healthy_profiles=["Lipid Profile"],
        )
    )
    labels = {c.label for c in finding.categories}
    assert "Opportunity residual" not in labels
    assert finding.healthy_share == 100.0


def _pcos_item(*, female_elevated: float = 72.7) -> dict:
    """Female-only PCOS matrix. Male counts/percents are zero."""
    female_healthy = round(100.0 - female_elevated, 1)
    high = round(female_elevated * 0.7, 1)
    very_high = round(female_elevated - high, 1)
    female_n = 11
    high_n = int(round(female_n * high / 100.0))
    vh_n = int(round(female_n * very_high / 100.0))
    healthy_n = female_n - high_n - vh_n
    return {
        "code": "pcos_pcod",
        "male": {
            "group": ["healthy", "increased", "high", "very_high"],
            "percent": [0.0, 0.0, 0.0, 0.0],
            "count": [0, 0, 0, 0],
            "elevated_percent": 0.0,
        },
        "female": {
            "group": ["healthy", "increased", "high", "very_high"],
            "percent": [female_healthy, 0.0, high, very_high],
            "count": [healthy_n, 0, high_n, vh_n],
            "elevated_percent": female_elevated,
        },
    }


def test_female_only_metric_does_not_claim_all_employees():
    """Subgroup denominators must be preserved in generated wording."""
    report = _load_fixture()
    report["distribution_by_gender_by_metabolic_syndrome"]["data"]["diseases"] = [
        _pcos_item(female_elevated=72.7)
    ]
    snapshot = copy.deepcopy(report)
    _, diseases = _map_diseases({"diseases": report["distribution_by_gender_by_metabolic_syndrome"]["data"]["diseases"]})
    finding = finding_from_disease(diseases[0])
    enriched = enrich_camp_report_with_intelligence(report)

    pcos = _obs(
        enriched["distribution_by_gender_by_metabolic_syndrome"],
        "disease_deep_dive",
        "pcos_pcod",
    )
    lowered = pcos.lower()
    assert _fmt_share(finding.elevated_share) in pcos
    assert finding.extras.get("male_has_data") is False
    assert finding.extras.get("female_has_data") is True
    assert "female" in lowered
    assert "of employees in elevated" not in lowered
    assert "of employees" not in lowered
    _assert_not_graph_narrator(pcos)
    assert report == snapshot


def test_oxidative_wording_does_not_treat_low_as_dominant_when_moderate_leads():
    report = _load_fixture()
    ox = report["distribution_by_oxidative_stress"]["data"]
    ox["percent"] = [32.7, 41.5, 15.7, 10.0]
    ox["count"] = [33, 41, 16, 10]
    snapshot = copy.deepcopy(report)
    enriched = enrich_camp_report_with_intelligence(report)

    text = _obs(enriched["distribution_by_oxidative_stress"])
    lowered = text.lower()
    assert _fmt_share(25.7) in text
    assert "moderate" in lowered
    assert "most employees" not in lowered
    assert "well controlled for most" not in lowered
    assert "predominantly healthy" not in lowered
    assert "elevation carries" not in lowered
    _assert_not_graph_narrator(text)
    assert report == snapshot


def test_knowledge_medical_wording_is_used_for_sleep_and_diseases():
    report = _load_fixture()
    enriched = enrich_camp_report_with_intelligence(report)

    sleep_why = " ".join(
        _why(enriched["distribution_by_sleeping_hours"], view)
        for view in ("both", "male", "female")
    ).lower()
    assert "elevation carries" not in sleep_why
    sleep_frame = medical_frame("sleep").lower()
    assert "recovery" in sleep_why
    assert "cognitive" in sleep_why or "hormone" in sleep_why or "metabolic" in sleep_why
    assert "recovery" in sleep_frame

    deep = (
        enriched["distribution_by_gender_by_metabolic_syndrome"]["intelligence"] or {}
    ).get("disease_deep_dive") or {}
    used_knowledge = False
    markers = (
        "blood sugar",
        "cardiovascular",
        "hormonal",
        "fatty-liver",
        "cholesterol",
        "thyroid",
        "insulin",
        "metabolic",
    )
    for metric_id, narrative in deep.items():
        why = str((narrative or {}).get("explanation") or "").lower()
        frame = (MEDICAL_FRAMES.get(metric_id) or "").lower()
        if not why or not frame:
            continue
        if any(marker in why and marker in frame for marker in markers):
            used_knowledge = True
            break
    assert used_knowledge or deep == {}


def test_recommendations_remain_present_and_distinct_from_explanations():
    report = _load_fixture()
    enriched = enrich_camp_report_with_intelligence(report)

    recs = [
        _rec(enriched["overall_risk_score"]),
        _rec(enriched["distribution_by_sleeping_hours"], "both"),
        _rec(enriched["distribution_by_oxidative_stress"]),
        _rec(enriched["positive_wins"], "positive_highlights"),
    ]
    whys = [
        _why(enriched["overall_risk_score"]),
        _why(enriched["distribution_by_sleeping_hours"], "both"),
        _why(enriched["distribution_by_oxidative_stress"]),
        _why(enriched["positive_wins"], "positive_highlights"),
    ]
    for rec, why in zip(recs, whys):
        assert rec.strip()
        assert why.strip()
        assert rec.strip().lower() != why.strip().lower()
        assert "elevation carries" not in rec.lower()
        assert "elevation carries" not in why.lower()
