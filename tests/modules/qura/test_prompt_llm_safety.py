"""Prompt, structured-completion, and deterministic post-check tests."""

from __future__ import annotations

import pytest

from modules.qura.context_builder import HealthContextBuilder
from modules.qura.explainability import EvidenceBuilder
from modules.qura.intent import IntentClassifier
from modules.qura.llm_client import LLMClient, LLMClientError
from modules.qura.prompt_builder import PromptBuilder
from modules.qura.query_planner import QueryPlanner
from modules.qura.safety import SafetyLayer
from modules.qura.schemas import (
    ChatRequest,
    ContextMeta,
    HealthContext,
    IntentResult,
    LLMStructuredOutput,
    Marker,
    QueryPlan,
)
from modules.qura.service import QuraService


def ldl_context(*, missing=None) -> HealthContext:
    return HealthContext(
        selected_markers=[
            Marker(code="ldl", name="LDL Cholesterol", value=162, unit="mg/dL", ref_low=0, ref_high=100)
        ] if not missing else [],
        metadata=ContextMeta(source="latest_health_report", missing_data=missing or []),
    )


def build_prompt(context: HealthContext):
    return PromptBuilder().build(
        message="Explain my LDL",
        intent=IntentResult(primary="explain_marker"),
        plan=QueryPlan(intent="explain_marker", required_markers=["ldl"]),
        context=context,
        language="english",
    )


def valid_output(**overrides):
    data = {
        "answer": "Your LDL is 162 mg/dL.",
        "grounding": ["ldl"],
        "confidence": "high",
        "safety_flags": [],
        "escalation": "none",
        "recommendations": [],
    }
    data.update(overrides)
    return data


def test_prompt_is_versioned_structured_and_contains_only_health_context_data():
    prompt = build_prompt(ldl_context())
    assert prompt.version == "qura_phase1_v1"
    assert prompt.user_question == "Explain my LDL"
    assert '"value":162.0' in prompt.health_context_json
    assert "Source Patient" not in prompt.health_context_json
    assert "email" not in prompt.health_context_json
    assert "Do not diagnose" in prompt.safety_rules
    assert '"grounding"' in prompt.output_format


def test_prompt_explicitly_represents_missing_data():
    prompt = build_prompt(ldl_context(missing=["marker:ldl"]))
    assert "marker:ldl" in prompt.health_context_json
    assert "marker:ldl" in prompt.task


class FakeProvider:
    def __init__(self, result=None, error=None):
        self.result = result
        self.error = error
        self.calls = 0

    async def complete(self, *, prompt, temperature):
        self.calls += 1
        if self.error:
            raise self.error
        return self.result


@pytest.mark.asyncio
async def test_llm_client_validates_structured_response():
    output = await LLMClient(primary=FakeProvider(result=valid_output())).complete(build_prompt(ldl_context()))
    assert output.grounding == ["ldl"]


@pytest.mark.asyncio
async def test_llm_client_rejects_invalid_or_malformed_response():
    with pytest.raises(LLMClientError):
        await LLMClient(primary=FakeProvider(result="not-json")).complete(build_prompt(ldl_context()))
    with pytest.raises(LLMClientError):
        await LLMClient(primary=FakeProvider(result=valid_output(confidence="certain"))).complete(build_prompt(ldl_context()))


@pytest.mark.asyncio
async def test_llm_client_controls_provider_failure_and_uses_fallback_boundary():
    primary = FakeProvider(error=TimeoutError("private provider failure"))
    fallback = FakeProvider(result=valid_output())
    output = await LLMClient(primary=primary, fallback=fallback).complete(build_prompt(ldl_context()))
    assert output.answer == "Your LDL is 162 mg/dL."
    assert primary.calls == fallback.calls == 1


def test_post_check_accepts_supported_ldl_and_rejects_invented_numbers():
    safety = SafetyLayer()
    assert safety.post_check(LLMStructuredOutput.model_validate(valid_output()), ldl_context()).blocked is False
    invented = LLMStructuredOutput.model_validate(valid_output(answer="Your LDL is 200 mg/dL."))
    assert "unsupported_numerical_claim" in safety.post_check(invented, ldl_context()).categories


def test_post_check_rejects_diagnosis_dosage_missing_data_and_invalid_grounding():
    safety = SafetyLayer()
    assert "diagnosis_claim" in safety.post_check(
        LLMStructuredOutput.model_validate(valid_output(answer="You have dyslipidemia.")), ldl_context()
    ).categories
    assert "medication_or_dosage" in safety.post_check(
        LLMStructuredOutput.model_validate(valid_output(answer="Take 10 mg daily.")), ldl_context()
    ).categories
    missing = ldl_context(missing=["marker:ldl"])
    hallucinated = LLMStructuredOutput.model_validate(valid_output(answer="Your LDL is 162 mg/dL."))
    assert "missing_data_contradiction" in safety.post_check(hallucinated, missing).categories
    bad_grounding = LLMStructuredOutput.model_validate(valid_output(grounding=["hdl"]))
    assert "invalid_grounding" in safety.post_check(bad_grounding, ldl_context()).categories


class StaticContextBuilder:
    async def build(self, **kwargs) -> HealthContext:
        return ldl_context()


def make_service(provider) -> QuraService:
    safety = SafetyLayer()
    classifier = IntentClassifier(safety)
    return QuraService(
        safety_layer=safety,
        intent_classifier=classifier,
        query_planner=QueryPlanner(classifier),
        context_builder=StaticContextBuilder(),
        prompt_builder=PromptBuilder(),
        llm_client=LLMClient(primary=provider),
        evidence_builder=EvidenceBuilder(),
    )


@pytest.mark.asyncio
async def test_end_to_end_ldl_response_is_llm_backed_and_grounded():
    response = await make_service(FakeProvider(result=valid_output())).chat(
        user_id=1,
        request=ChatRequest(message="Explain my LDL"),
    )
    assert response.answer == "Your LDL is 162 mg/dL."
    assert [(marker.code, marker.value) for marker in response.evidence.markers] == [("ldl", 162.0)]


@pytest.mark.asyncio
async def test_end_to_end_missing_marker_does_not_expose_or_invent_value():
    class MissingContextBuilder:
        async def build(self, **kwargs) -> HealthContext:
            return ldl_context(missing=["marker:ldl"])

    safety = SafetyLayer()
    classifier = IntentClassifier(safety)
    service = QuraService(
        safety_layer=safety,
        intent_classifier=classifier,
        query_planner=QueryPlanner(classifier),
        context_builder=MissingContextBuilder(),
        prompt_builder=PromptBuilder(),
        llm_client=LLMClient(primary=FakeProvider(result=valid_output(answer="LDL data is unavailable.", grounding=[]))),
        evidence_builder=EvidenceBuilder(),
    )
    response = await service.chat(user_id=1, request=ChatRequest(message="What is my LDL?"))
    assert response.safety.blocked is False
    assert "unavailable" in response.answer
