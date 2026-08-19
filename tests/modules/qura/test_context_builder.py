"""Real ContextBuilder tests using only fake existing-service boundaries."""

from __future__ import annotations

import inspect
from types import SimpleNamespace

import pytest

from core.exceptions import AppError
from modules.qura.context_builder import ContextBuilder
from modules.qura.explainability import EvidenceBuilder
from modules.qura.schemas import QueryPlan


class FakeAssessmentsService:
    def __init__(self, rows=None):
        self.rows = rows if rows is not None else [(SimpleNamespace(assessment_instance_id=77), None)]
        self.calls = []

    async def list_my_assessments(self, db, *, user_id, page, limit):
        self.calls.append({"user_id": user_id, "page": page, "limit": limit})
        return self.rows, len(self.rows)


class FakeReportsService:
    def __init__(self, *, groups=None, risks=None, error=None):
        self.groups = groups if groups is not None else []
        self.risks = risks if risks is not None else []
        self.error = error
        self.blood_calls = []
        self.risk_calls = []

    async def get_blood_parameters_for_user(self, db, **kwargs):
        self.blood_calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self.groups

    async def get_risk_analysis_for_user(self, db, **kwargs):
        self.risk_calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return SimpleNamespace(diseases=self.risks)


def make_builder(*, reports=None, assessments=None):
    return ContextBuilder(
        db=object(),
        reports_service=reports or FakeReportsService(),
        assessments_service=assessments or FakeAssessmentsService(),
    )


def blood_groups():
    return [
        SimpleNamespace(
            group_name="Lipids",
            tests=[
                SimpleNamespace(
                    parameter_key="ldl",
                    test_name="LDL Cholesterol",
                    value=162.0,
                    unit="mg/dL",
                    lower_range=0.0,
                    higher_range=100.0,
                    name="Source Patient",  # Must be ignored.
                    email="patient@example.com",  # Must be ignored.
                    phone="9999999999",  # Must be ignored.
                    date_of_birth="1990-01-01",  # Must be ignored.
                    user_id=99,  # Must be ignored.
                ),
                SimpleNamespace(
                    parameter_key="hdl",
                    test_name="HDL Cholesterol",
                    value=50.0,
                    unit="mg/dL",
                    lower_range=40.0,
                    higher_range=None,
                ),
            ],
        )
    ]


@pytest.mark.asyncio
async def test_marker_plan_uses_reports_service_and_keeps_only_requested_marker():
    reports = FakeReportsService(groups=blood_groups())
    context = await make_builder(reports=reports).build(
        user_id=99,
        plan=QueryPlan(intent="explain_marker", required_markers=["ldl"]),
        user_gender="female",
    )

    assert len(reports.blood_calls) == 1
    assert reports.blood_calls[0]["assessment_id"] == 77
    assert reports.blood_calls[0]["endpoint"] == "/qura/chat"
    assert [marker.code for marker in context.selected_markers] == ["ldl"]
    assert context.selected_markers[0].flag == "unavailable"
    assert context.metadata.requested_data == ["marker:ldl"]
    assert context.metadata.found_data == ["marker:ldl"]


@pytest.mark.asyncio
async def test_report_summary_maps_current_markers_and_bio_ai_risks():
    reports = FakeReportsService(
        groups=blood_groups(),
        risks=[SimpleNamespace(code="dyslipidemia", name="Dyslipidemia", risk_score_scaled=42)],
    )
    context = await make_builder(reports=reports).build(
        user_id=99,
        plan=QueryPlan(intent="report_summary", include_current_report_markers=True, include_bio_ai_risks=True),
    )

    assert [marker.code for marker in context.selected_markers] == ["ldl", "hdl"]
    assert [(risk.code, risk.score) for risk in context.risk_scores] == [("dyslipidemia", 42)]
    assert len(reports.blood_calls) == 1
    assert len(reports.risk_calls) == 1
    assert context.metadata.data_available is True


@pytest.mark.asyncio
async def test_no_blood_report_is_explicit_and_never_fabricates_values():
    context = await make_builder(reports=FakeReportsService(groups=[])).build(
        user_id=99,
        plan=QueryPlan(intent="explain_marker", required_markers=["ldl"]),
    )

    assert context.selected_markers == []
    assert "marker:ldl" in context.metadata.missing_data
    assert context.metadata.data_available is False


@pytest.mark.asyncio
async def test_source_phi_and_internal_ids_are_excluded_from_health_context():
    context = await make_builder(reports=FakeReportsService(groups=blood_groups())).build(
        user_id=99,
        plan=QueryPlan(intent="explain_marker", required_markers=["ldl"]),
        user_first_name="Source",
        user_last_name="Patient",
    )
    payload = context.model_dump()
    serialized = str(payload).lower()
    for forbidden in ("source patient", "patient@example.com", "9999999999", "1990-01-01", "user_id", "assessment_id"):
        assert forbidden not in serialized


@pytest.mark.asyncio
async def test_reports_service_error_becomes_controlled_missing_data():
    context = await make_builder(
        reports=FakeReportsService(error=AppError(status_code=503, error_code="UPSTREAM", message="private details"))
    ).build(
        user_id=99,
        plan=QueryPlan(intent="explain_marker", required_markers=["ldl"]),
    )

    assert context.metadata.data_available is False
    assert "health_data_unavailable" in context.metadata.missing_data
    assert "private details" not in str(context.model_dump())


@pytest.mark.asyncio
async def test_evidence_uses_only_actual_context_values():
    context = await make_builder(reports=FakeReportsService(groups=blood_groups())).build(
        user_id=99,
        plan=QueryPlan(intent="explain_marker", required_markers=["ldl"]),
    )
    evidence = EvidenceBuilder().build(context)
    assert [(marker.code, marker.value, marker.unit) for marker in evidence.markers] == [("ldl", 162.0, "mg/dL")]


def test_qura_context_builder_has_no_direct_provider_integration():
    source = inspect.getsource(ContextBuilder).lower()
    for forbidden in ("healthians", "metsights", "diagnostics", "httpx", "client."):
        assert forbidden not in source
