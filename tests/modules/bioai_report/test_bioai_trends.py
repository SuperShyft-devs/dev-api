"""Tests for Bio-AI historical health-trends API."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from core.exceptions import AppError
from modules.bioai_report.report_engine.models.assessment import AssessmentDisease, AssessmentPayload
from modules.bioai_report.report_engine.models.trends import TREND_DISEASE_TITLES
from modules.bioai_report.report_engine.services.trend_service import BioAITrendService, TREND_DISEASE_IDS


class _FakeUsers:
    def __init__(self, exists: bool = True, gender: str | None = None) -> None:
        self.exists = exists
        self.gender = gender

    async def get_user_by_id(self, db, user_id: int):
        if not self.exists:
            return None
        return SimpleNamespace(user_id=user_id, gender=self.gender, sex=self.gender)


class _FakeAssessments:
    def __init__(self, rows: list) -> None:
        self.rows = rows

    async def list_completed_instances_for_user(self, db, *, user_id: int):
        return self.rows

    async def get_instance_by_id(self, db, assessment_instance_id: int):
        for instance, _package in self.rows:
            if int(instance.assessment_instance_id) == int(assessment_instance_id):
                if not hasattr(instance, "user_id"):
                    instance.user_id = 1
                return instance
        return None


class _FakeFetch:
    def __init__(self, by_record: dict[str, AssessmentPayload]) -> None:
        self.by_record = by_record

    async def fetch(self, *, record_id: str, assessment_type_code: str | None = None):
        return self.by_record[record_id]


def _instance(*, instance_id: int, record_id: str, completed: str) -> SimpleNamespace:
    return SimpleNamespace(
        assessment_instance_id=instance_id,
        metsights_record_id=record_id,
        completed_at=datetime.fromisoformat(completed).replace(tzinfo=timezone.utc),
        assigned_at=None,
        package_id=1,
        user_id=1,
    )


def _package(*, type_code: str = "1") -> SimpleNamespace:
    return SimpleNamespace(assessment_type_code=type_code, package_id=1)


def _payload(
    *,
    date: str,
    diseases: list[tuple[str, int | None]],
    metabolic_score: float | int | None = None,
    metabolic_health_status: str | None = None,
    sex: str | None = None,
    gender: str | None = None,
) -> AssessmentPayload:
    return AssessmentPayload(
        assessment_date=date,
        metabolic_score=metabolic_score,
        metabolic_health_status=metabolic_health_status,
        sex=sex,
        gender=gender or sex,
        diseases=[
            AssessmentDisease(
                code=code,
                risk_score_scaled=score,
                risk_status=None,
            )
            for code, score in diseases
        ],
    )


def _service(rows, payloads, *, user_exists: bool = True, gender: str | None = None) -> BioAITrendService:
    return BioAITrendService(
        assessment_service=_FakeFetch(payloads),
        assessments_repository=_FakeAssessments(rows),
        users_repository=_FakeUsers(exists=user_exists, gender=gender),
    )


@pytest.mark.asyncio
async def test_single_assessment_trend_not_available():
    rows = [(_instance(instance_id=1001, record_id="r1", completed="2024-06-06"), _package())]
    payloads = {
        "r1": _payload(
            date="2024-06-06",
            diseases=[("metabolic_syndrome", 16), ("pcos", None)],
        )
    }
    result = await _service(rows, payloads).get_trends_for_user(None, user_id=123)
    assert result.assessment_count == 1
    assert result.trend_available is False
    assert result.assessments[0].assessment_instance_id == 1001
    assert result.assessments[0].assessment_date == "2024-06-06"
    mets = result.trends.metabolic_syndrome[0]
    assert mets.score == 16
    assert mets.risk_status == "Healthy"
    assert mets.risk_band == "0-25"


@pytest.mark.asyncio
async def test_multiple_assessments_chronological_and_trend_available():
    rows = [
        (_instance(instance_id=1003, record_id="r3", completed="2025-04-18"), _package()),
        (_instance(instance_id=1001, record_id="r1", completed="2024-06-06"), _package()),
        (_instance(instance_id=1002, record_id="r2", completed="2024-12-26"), _package()),
    ]
    payloads = {
        "r1": _payload(date="2024-06-06", diseases=[("obesity", 20)]),
        "r2": _payload(date="2024-12-26", diseases=[("obesity", 40)]),
        "r3": _payload(date="2025-04-18", diseases=[("obesity", 70)]),
    }
    result = await _service(rows, payloads).get_trends_for_user(None, user_id=123)
    ids = [a.assessment_instance_id for a in result.assessments]
    assert ids == [1001, 1002, 1003]
    assert result.assessment_count == 3
    assert result.trend_available is True
    assert [p.score for p in result.trends.obesity] == [20, 40, 70]
    assert len(set(ids)) == 3


@pytest.mark.asyncio
async def test_missing_disease_is_null_not_zero():
    rows = [(_instance(instance_id=1, record_id="r1", completed="2025-04-18"), _package())]
    payloads = {"r1": _payload(date="2025-04-18", diseases=[("obesity", 10)])}
    result = await _service(rows, payloads).get_trends_for_user(None, user_id=1)
    pcos = result.trends.pcos[0]
    assert pcos.score is None
    assert pcos.risk_status is None
    assert pcos.date == "2025-04-18"


@pytest.mark.asyncio
async def test_genuine_zero_score_is_preserved():
    rows = [(_instance(instance_id=1, record_id="r1", completed="2024-06-06"), _package())]
    payloads = {"r1": _payload(date="2024-06-06", diseases=[("nafld", 0)])}
    result = await _service(rows, payloads).get_trends_for_user(None, user_id=1)
    point = result.trends.nafld[0]
    assert point.score == 0
    assert point.risk_status == "Healthy"
    assert point.risk_band == "0-25"


@pytest.mark.asyncio
async def test_user_with_no_assessments_returns_empty_series():
    result = await _service([], {}).get_trends_for_user(None, user_id=123)
    assert result.assessment_count == 0
    assert result.trend_available is False
    assert result.assessments == []
    for key in TREND_DISEASE_IDS:
        assert getattr(result.trends, key) == []


@pytest.mark.asyncio
async def test_unknown_user_raises_not_found():
    with pytest.raises(AppError) as exc:
        await _service([], {}, user_exists=False).get_trends_for_user(None, user_id=999)
    assert exc.value.status_code == 404
    assert exc.value.error_code == "USER_NOT_FOUND"


@pytest.mark.asyncio
async def test_diabetes_alias_maps_to_type2_diabetes():
    rows = [(_instance(instance_id=1, record_id="r1", completed="2024-01-01"), _package())]
    payloads = {"r1": _payload(date="2024-01-01", diseases=[("diabetes", 55)])}
    result = await _service(rows, payloads).get_trends_for_user(None, user_id=1)
    assert result.trends.type2_diabetes[0].score == 55
    assert result.trends.type2_diabetes[0].risk_band == "51-75"


@pytest.mark.asyncio
async def test_fitprint_and_unknown_packages_are_excluded():
    rows = [
        (_instance(instance_id=7311, record_id="fitprint", completed="2025-01-01"), _package(type_code="7")),
        (_instance(instance_id=1, record_id="basic", completed="2024-06-06"), _package(type_code="1")),
        (_instance(instance_id=7310, record_id="pro", completed="2025-04-18"), _package(type_code="2")),
        (_instance(instance_id=99, record_id="none", completed="2024-01-01"), None),
    ]
    payloads = {
        "fitprint": _payload(date="2025-01-01", diseases=[("obesity", 99)]),
        "basic": _payload(date="2024-06-06", diseases=[("obesity", 20)]),
        "pro": _payload(date="2025-04-18", diseases=[("obesity", 40)]),
        "none": _payload(date="2024-01-01", diseases=[("obesity", 1)]),
    }
    result = await _service(rows, payloads).get_trends_for_user(None, user_id=1)
    assert [a.assessment_instance_id for a in result.assessments] == [1, 7310]
    assert result.assessment_count == 2
    assert result.trend_available is True
    assert [p.score for p in result.trends.obesity] == [20, 40]


@pytest.mark.asyncio
async def test_metabolic_score_fills_metabolic_syndrome_when_disease_missing():
    rows = [(_instance(instance_id=1, record_id="r1", completed="2024-06-06"), _package())]
    payloads = {
        "r1": _payload(
            date="2024-06-06",
            diseases=[("obesity", 20)],
            metabolic_score=21,
            metabolic_health_status="Healthy",
        )
    }
    result = await _service(rows, payloads).get_trends_for_user(None, user_id=1)
    mets = result.trends.metabolic_syndrome[0]
    assert mets.score == 21
    assert mets.risk_status == "Healthy"


@pytest.mark.asyncio
async def test_metabolic_syndrome_disease_row_wins_over_metabolic_score():
    rows = [(_instance(instance_id=1, record_id="r1", completed="2024-06-06"), _package())]
    payloads = {
        "r1": _payload(
            date="2024-06-06",
            diseases=[("metabolic_syndrome", 16)],
            metabolic_score=99,
        )
    }
    result = await _service(rows, payloads).get_trends_for_user(None, user_id=1)
    assert result.trends.metabolic_syndrome[0].score == 16


@pytest.mark.asyncio
async def test_cutoff_excludes_assessments_after_requested_date():
    rows = [
        (_instance(instance_id=2023, record_id="y2023", completed="2023-01-01"), _package()),
        (_instance(instance_id=2024, record_id="y2024", completed="2024-01-01"), _package()),
        (_instance(instance_id=2025, record_id="y2025", completed="2025-01-01"), _package()),
        (_instance(instance_id=2026, record_id="y2026", completed="2026-01-01"), _package()),
    ]
    payloads = {
        "y2023": _payload(date="2023-06-01T10:00:00+05:30", diseases=[("obesity", 10)]),
        "y2024": _payload(date="2024-06-15T14:30:00+05:30", diseases=[("obesity", 20)]),
        "y2025": _payload(date="2025-06-01T10:00:00+05:30", diseases=[("obesity", 30)]),
        "y2026": _payload(date="2026-06-01T10:00:00+05:30", diseases=[("obesity", 40)]),
    }
    svc = _service(rows, payloads)

    latest = await svc.get_trends_for_user(
        None, user_id=1, through_date="2026-06-01", through_instance_id=2026
    )
    assert [a.assessment_instance_id for a in latest.assessments] == [2023, 2024, 2025, 2026]
    assert latest.assessment_count == 4
    assert latest.trend_available is True

    mid = await svc.get_trends_for_user(
        None, user_id=1, through_date="2025-06-01", through_instance_id=2025
    )
    assert [a.assessment_instance_id for a in mid.assessments] == [2023, 2024, 2025]
    assert 2026 not in [a.assessment_instance_id for a in mid.assessments]

    older = await svc.get_trends_for_user(
        None, user_id=1, through_date="2024-06-15", through_instance_id=2024
    )
    assert [a.assessment_instance_id for a in older.assessments] == [2023, 2024]
    assert [p.score for p in older.trends.obesity] == [10, 20]


@pytest.mark.asyncio
async def test_same_day_later_instance_is_excluded_by_instance_id():
    rows = [
        (_instance(instance_id=10, record_id="a", completed="2024-06-15"), _package()),
        (_instance(instance_id=11, record_id="b", completed="2024-06-15"), _package()),
    ]
    payloads = {
        "a": _payload(date="2024-06-15T08:00:00+05:30", diseases=[("obesity", 20)]),
        "b": _payload(date="2024-06-15T18:00:00+05:30", diseases=[("obesity", 40)]),
    }
    result = await _service(rows, payloads).get_trends_for_user(
        None, user_id=1, through_date="2024-06-15", through_instance_id=10
    )
    assert [a.assessment_instance_id for a in result.assessments] == [10]
    assert result.trends.obesity[0].score == 20


def _series_by_id(health_trends: dict) -> dict:
    return {item["disease_id"]: item for item in health_trends["series"]}


def _assert_graph_points(points: list[dict]) -> None:
    for point in points:
        assert set(point) == {"date", "score"}
        assert point["score"] is not None
        assert point["score"] != "diabetes"


def test_health_trends_false_when_not_available():
    from modules.bioai_report.report_engine.models.trends import (
        BioAITrendAssessment,
        BioAITrendPoint,
        BioAITrendResponse,
        BioAITrendsByDisease,
    )

    embedded = BioAITrendResponse(
        user_id=3,
        assessment_count=1,
        trend_available=False,
        assessments=[BioAITrendAssessment(assessment_instance_id=7312, assessment_date="2026-02-03")],
        trends=BioAITrendsByDisease(
            obesity=[
                BioAITrendPoint(
                    date="2026-02-03",
                    score=14,
                    risk_status="Healthy",
                    risk_band="0-25",
                    assessment_instance_id=7312,
                )
            ]
        ),
    ).to_report_field()
    assert embedded is False


def test_health_trends_series_omits_nulls_and_extra_fields():
    from modules.bioai_report.report_engine.models.trends import (
        BioAITrendPoint,
        BioAITrendResponse,
        BioAITrendsByDisease,
    )

    embedded = BioAITrendResponse(
        user_id=1,
        assessment_count=2,
        trend_available=True,
        trends=BioAITrendsByDisease(
            obesity=[
                BioAITrendPoint(
                    date="2024-01-01",
                    score=20,
                    risk_status="Healthy",
                    risk_band="0-25",
                    assessment_instance_id=1,
                ),
                BioAITrendPoint(
                    date="2025-01-01",
                    score=None,
                    risk_status=None,
                    risk_band=None,
                    assessment_instance_id=2,
                ),
            ],
            pcos=[
                BioAITrendPoint(
                    date="2024-01-01",
                    score=None,
                    assessment_instance_id=1,
                )
            ],
            nafld=[
                BioAITrendPoint(
                    date="2024-01-01",
                    score=0,
                    risk_status="Healthy",
                    risk_band="0-25",
                    assessment_instance_id=1,
                )
            ],
        ),
    ).to_report_field()
    assert embedded is not False
    by_id = _series_by_id(embedded)
    assert "pcos" not in by_id
    assert by_id["obesity"]["title"] == TREND_DISEASE_TITLES["obesity"]
    assert by_id["obesity"]["points"] == [{"date": "2024-01-01", "score": 20}]
    assert by_id["nafld"]["points"] == [{"date": "2024-01-01", "score": 0}]
    _assert_graph_points(by_id["obesity"]["points"])
    assert "user_id" not in embedded
    assert "assessment_count" not in embedded
    assert "trend_available" not in embedded
    assert "diseases" not in embedded
    assert "diabetes" not in [item["disease_id"] for item in embedded["series"]]


@pytest.mark.asyncio
async def test_embed_for_report_applies_historical_cutoff():
    rows = [
        (_instance(instance_id=2023, record_id="y2023", completed="2023-01-01"), _package()),
        (_instance(instance_id=2024, record_id="y2024", completed="2024-01-01"), _package()),
        (_instance(instance_id=2025, record_id="y2025", completed="2025-01-01"), _package()),
        (_instance(instance_id=2026, record_id="y2026", completed="2026-01-01"), _package()),
    ]
    payloads = {
        "y2023": _payload(date="2023-06-01T10:00:00+05:30", diseases=[("obesity", 10)]),
        "y2024": _payload(date="2024-06-15T14:30:00+05:30", diseases=[("obesity", 20)]),
        "y2025": _payload(date="2025-06-01T10:00:00+05:30", diseases=[("obesity", 30)]),
        "y2026": _payload(date="2026-06-01T10:00:00+05:30", diseases=[("obesity", 40)]),
    }
    svc = _service(rows, payloads)

    latest = await svc.embed_for_assessment_instance(
        None,
        assessment_instance_id=2026,
        report_payload={"patient": {"assessment_date": "2026-06-01T10:00:00+05:30"}},
    )
    assert latest is not False
    obesity = _series_by_id(latest)["obesity"]["points"]
    assert [p["score"] for p in obesity] == [10, 20, 30, 40]
    assert [p["date"] for p in obesity] == [
        "2023-06-01",
        "2024-06-15",
        "2025-06-01",
        "2026-06-01",
    ]
    assert "pcos" not in _series_by_id(latest)

    mid = await svc.embed_for_assessment_instance(
        None,
        assessment_instance_id=2025,
        report_payload={"patient": {"assessment_date": "2025-06-01T10:00:00+05:30"}},
    )
    assert [p["score"] for p in _series_by_id(mid)["obesity"]["points"]] == [10, 20, 30]

    older = await svc.embed_for_assessment_instance(
        None,
        assessment_instance_id=2024,
        report_payload={"patient": {"assessment_date": "2024-06-15T14:30:00+05:30"}},
    )
    assert [p["score"] for p in _series_by_id(older)["obesity"]["points"]] == [10, 20]
    assert 0 not in [p["score"] for p in _series_by_id(older)["obesity"]["points"] if p["score"] is None]

    single = await svc.embed_for_assessment_instance(
        None,
        assessment_instance_id=2023,
        report_payload={"patient": {"assessment_date": "2023-06-01T10:00:00+05:30"}},
    )
    assert single is False


@pytest.mark.asyncio
async def test_same_day_cutoff_in_health_trends():
    rows = [
        (_instance(instance_id=10, record_id="a", completed="2024-06-15"), _package()),
        (_instance(instance_id=11, record_id="b", completed="2024-06-15"), _package()),
    ]
    payloads = {
        "a": _payload(date="2024-06-15T08:00:00+05:30", diseases=[("obesity", 20)]),
        "b": _payload(date="2024-06-15T18:00:00+05:30", diseases=[("obesity", 40)]),
    }
    only_first = await _service(rows, payloads).embed_for_assessment_instance(
        None,
        assessment_instance_id=10,
        report_payload={"patient": {"assessment_date": "2024-06-15T08:00:00+05:30"}},
    )
    assert only_first is False

    both = await _service(rows, payloads).embed_for_assessment_instance(
        None,
        assessment_instance_id=11,
        report_payload={"patient": {"assessment_date": "2024-06-15T18:00:00+05:30"}},
    )
    assert [p["score"] for p in _series_by_id(both)["obesity"]["points"]] == [20, 40]


@pytest.mark.asyncio
async def test_female_pcos_appears_in_health_trends():
    rows = [
        (_instance(instance_id=1, record_id="r1", completed="2024-01-01"), _package()),
        (_instance(instance_id=2, record_id="r2", completed="2025-01-01"), _package()),
    ]
    payloads = {
        "r1": _payload(date="2024-01-01", diseases=[("pcos", 30), ("obesity", 20)], sex="female"),
        "r2": _payload(date="2025-01-01", diseases=[("pcos", 40), ("obesity", 25)], sex="female"),
    }
    result = await _service(rows, payloads, gender="female").get_trends_for_user(None, user_id=1)
    assert result.trend_available is True
    assert [p.score for p in result.trends.pcos] == [30, 40]

    embedded = await _service(rows, payloads, gender="female").embed_for_assessment_instance(
        None,
        assessment_instance_id=2,
        report_payload={"patient": {"assessment_date": "2025-01-01", "sex": "female"}},
    )
    pcos = _series_by_id(embedded)["pcos"]
    assert pcos["title"] == "PCOS"
    assert [p["score"] for p in pcos["points"]] == [30, 40]


@pytest.mark.asyncio
async def test_male_pcos_is_absent_from_health_trends():
    rows = [
        (_instance(instance_id=1, record_id="r1", completed="2024-01-01"), _package()),
        (_instance(instance_id=2, record_id="r2", completed="2025-01-01"), _package()),
    ]
    payloads = {
        "r1": _payload(date="2024-01-01", diseases=[("pcos", 88), ("obesity", 20)], sex="male"),
        "r2": _payload(date="2025-01-01", diseases=[("pcos", 90), ("obesity", 25)], sex="male"),
    }
    embedded = await _service(rows, payloads, gender="male").embed_for_assessment_instance(
        None,
        assessment_instance_id=2,
        report_payload={"patient": {"assessment_date": "2025-01-01", "sex": "male", "gender": "male"}},
    )
    by_id = _series_by_id(embedded)
    assert "pcos" not in by_id
    assert [p["score"] for p in by_id["obesity"]["points"]] == [20, 25]


@pytest.mark.asyncio
async def test_diabetes_alias_is_type2_diabetes_in_health_trends():
    rows = [
        (_instance(instance_id=1, record_id="r1", completed="2024-01-01"), _package()),
        (_instance(instance_id=2, record_id="r2", completed="2025-01-01"), _package()),
    ]
    payloads = {
        "r1": _payload(date="2024-01-01", diseases=[("diabetes", 55)]),
        "r2": _payload(date="2025-01-01", diseases=[("diabetes", 60)]),
    }
    embedded = await _service(rows, payloads).embed_for_assessment_instance(
        None,
        assessment_instance_id=2,
        report_payload={"patient": {"assessment_date": "2025-01-01"}},
    )
    ids = [item["disease_id"] for item in embedded["series"]]
    assert "diabetes" not in ids
    assert "type2_diabetes" in ids
    assert _series_by_id(embedded)["type2_diabetes"]["title"] == "Type 2 Diabetes"
    assert [p["score"] for p in _series_by_id(embedded)["type2_diabetes"]["points"]] == [55, 60]


@pytest.mark.asyncio
async def test_health_trends_false_does_not_strip_existing_report_fields():
    report = {
        "patient": {
            "name": "Rishi Nagar",
            "sex": "male",
            "assessment_date": "2023-11-28T14:55:32",
        },
        "executive_summary": {
            "metabolic_score": 20,
            "overall_status": "Healthy",
        },
        "disease_sections": [
            {
                "disease_id": "oxidative_stress",
                "title": "Oxidative Stress",
                "current_status": {"score": 33, "risk": "Increased", "band": "31-35"},
            }
        ],
        "report_metadata": {
            "record_id": "D445236F209D",
            "disease_count": 1,
            "source": "metsights",
        },
    }
    original = json.dumps(report, sort_keys=True)
    rows = [(_instance(instance_id=1, record_id="r1", completed="2023-11-28"), _package())]
    payloads = {
        "r1": _payload(
            date="2023-11-28",
            diseases=[("oxidative_stress", 33), ("nafld", 21)],
        )
    }
    health_trends = await _service(rows, payloads).embed_for_assessment_instance(
        None,
        assessment_instance_id=1,
        report_payload=report,
    )
    merged = {**report, "health_trends": health_trends}
    without_trends = {k: v for k, v in merged.items() if k != "health_trends"}
    assert json.dumps(without_trends, sort_keys=True) == original
    assert "trends" not in merged
    assert merged["health_trends"] is False
    assert merged["disease_sections"][0]["current_status"]["score"] == 33
    assert merged["executive_summary"]["metabolic_score"] == 20


@pytest.mark.asyncio
async def test_merged_report_includes_health_trends_series():
    report = {
        "patient": {"assessment_date": "2025-06-01", "sex": "female"},
        "executive_summary": {"metabolic_score": 20},
        "disease_sections": [{"disease_id": "hypertension", "current_status": {"score": 32}}],
        "report_metadata": {"disease_count": 1},
    }
    original = json.dumps(report, sort_keys=True)
    rows = [
        (_instance(instance_id=1, record_id="r1", completed="2025-01-01"), _package()),
        (_instance(instance_id=2, record_id="r2", completed="2025-06-01"), _package()),
    ]
    payloads = {
        "r1": _payload(date="2025-01-01", diseases=[("hypertension", 40), ("oxidative_stress", 35)]),
        "r2": _payload(date="2025-06-01", diseases=[("hypertension", 32), ("oxidative_stress", 28)]),
    }
    health_trends = await _service(rows, payloads, gender="female").embed_for_assessment_instance(
        None,
        assessment_instance_id=2,
        report_payload=report,
    )
    merged = {**report, "health_trends": health_trends}
    assert json.dumps({k: v for k, v in merged.items() if k != "health_trends"}, sort_keys=True) == original
    assert "trends" not in merged
    by_id = _series_by_id(merged["health_trends"])
    assert by_id["hypertension"]["title"] == "Hypertension"
    assert by_id["oxidative_stress"]["title"] == "Oxidative Stress"
    _assert_graph_points(by_id["hypertension"]["points"])
    assert [p["score"] for p in by_id["hypertension"]["points"]] == [40, 32]


def test_openapi_registers_report_route_without_standalone_trends():
    from main import app

    paths = app.openapi()["paths"]
    assert "/bioai-report/{user_id}/trends" not in paths
    assert "/bioai-report/{assessment_instance_id}" in paths
    assert "/bioai-report/content/{assessment_instance_id}" not in paths
