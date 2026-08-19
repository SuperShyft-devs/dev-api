"""Tests for the intentionally offline Qura Phase-1 scaffold."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from core.dependencies import get_current_user
from core.exceptions import add_exception_handlers
from modules.qura.context_builder import MockContextBuilder
from modules.qura.explainability import EvidenceBuilder
from modules.qura.intent import IntentClassifier
from modules.qura.llm_client import LLMClient
from modules.qura.prompt_builder import PromptBuilder
from modules.qura.query_planner import QueryPlanner
from modules.qura.router import get_qura_service, router
from modules.qura.safety import SafetyLayer
from modules.qura.schemas import ChatRequest, ContextMeta, HealthContext, Marker, Profile, QueryPlan
from modules.qura.service import QuraService


def make_service(context_builder=None) -> QuraService:
    safety = SafetyLayer()
    classifier = IntentClassifier(safety)
    return QuraService(
        safety_layer=safety,
        intent_classifier=classifier,
        query_planner=QueryPlanner(classifier),
        context_builder=context_builder or MockContextBuilder(),
        prompt_builder=PromptBuilder(),
        llm_client=LLMClient(primary=StaticProvider()),
        evidence_builder=EvidenceBuilder(),
    )


class StaticProvider:
    async def complete(self, *, prompt, temperature):
        return {
            "answer": "The requested report data is unavailable.",
            "grounding": [],
            "confidence": "low",
            "safety_flags": [],
            "escalation": "none",
            "recommendations": [],
        }


@pytest.mark.parametrize(
    ("message", "intent"),
    [
        ("Hello", "greeting_small_talk"),
        ("Explain my LDL", "explain_marker"),
        ("Explain my Bio-AI risk", "explain_bio_ai_risk"),
        ("Explain my latest blood report", "report_summary"),
        ("Can you diagnose me?", "unsupported_medical"),
        ("I have chest pain and trouble breathing", "emergency"),
    ],
)
def test_phase1_intent_detection(message: str, intent: str):
    assert IntentClassifier().classify(message).primary == intent


def test_query_plan_minimizes_marker_request():
    classifier = IntentClassifier()
    intent = classifier.classify("Explain my LDL")
    plan = QueryPlanner(classifier).build(intent, "Explain my LDL")
    assert plan.required_markers == ["ldl"]
    assert plan.include_current_report_markers is False
    assert plan.include_bio_ai_risks is False


def test_query_plan_for_report_summary_requests_current_data_only():
    classifier = IntentClassifier()
    plan = QueryPlanner(classifier).build(
        classifier.classify("Explain my latest blood report"),
        "Explain my latest blood report",
    )
    assert plan.include_current_report_markers is True
    assert plan.include_bio_ai_risks is True
    assert plan.include_marker_deltas is False


@pytest.mark.asyncio
async def test_mock_context_builder_explicitly_marks_missing_data_and_has_no_phi():
    context = await MockContextBuilder().build(
        user_id=999,
        plan=QueryPlan(intent="explain_marker", required_markers=["ldl"]),
    )
    payload = context.model_dump()
    assert context.metadata.source == "mock"
    assert context.metadata.data_available is False
    assert "requested_markers_unavailable" in context.metadata.missing_data
    assert "user_id" not in payload
    assert "name" not in payload
    assert "email" not in payload


def test_health_context_rejects_identity_fields():
    with pytest.raises(Exception):
        HealthContext(
            metadata=ContextMeta(source="test"),
            profile=Profile.model_validate({"name": "not allowed"}),
        )


def test_marker_flag_defaults_to_unavailable_without_range_calculation():
    marker = Marker(code="ldl", name="LDL", value=162, unit="mg/dL", ref_low=0, ref_high=100)
    assert marker.flag == "unavailable"


def test_safety_precheck_blocks_medication_diagnosis_and_emergency():
    safety = SafetyLayer()
    assert safety.pre_check("What dosage should I take?").categories == ["medication_or_dosage"]
    assert safety.pre_check("Diagnose me").categories == ["diagnosis_request"]
    assert safety.pre_check("I have chest pain").categories == ["emergency"]


@pytest.mark.asyncio
async def test_service_returns_safe_fallback_without_llm_or_health_retrieval():
    response = await make_service().chat(
        user_id=1,
        request=ChatRequest(message="Explain my latest blood report"),
    )
    assert "unavailable" in response.answer
    assert response.evidence.markers == []
    assert response.safety.blocked is False


@pytest.mark.asyncio
async def test_service_refuses_dosage_request_before_context_building():
    class FailingBuilder:
        async def build(self, *, user_id: int, plan: QueryPlan) -> HealthContext:
            raise AssertionError("Blocked request must not build health context")

    response = await make_service(FailingBuilder()).chat(
        user_id=1,
        request=ChatRequest(message="What dose of medicine should I take?"),
    )
    assert response.safety.blocked is True
    assert response.safety.categories == ["medication_or_dosage"]
    assert response.escalation == "clinician"


@pytest.mark.asyncio
async def test_router_requires_authentication():
    app = FastAPI()
    add_exception_handlers(app)
    app.include_router(router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/qura/chat", json={"message": "hello"})
    assert response.status_code in {401, 403}


@pytest.mark.asyncio
async def test_router_uses_authenticated_user_and_preserves_response_contract():
    app = FastAPI()
    add_exception_handlers(app)
    app.include_router(router)

    async def current_user():
        return SimpleNamespace(user_id=42)

    app.dependency_overrides[get_current_user] = current_user
    app.dependency_overrides[get_qura_service] = make_service
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/qura/chat", json={"message": "Explain my LDL"})

    assert response.status_code == 200
    data = response.json()["data"]
    assert set(data) == {"answer", "evidence", "safety", "escalation", "recommendations", "conversation_id"}
    assert data["evidence"]["markers"] == []
