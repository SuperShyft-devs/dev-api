"""Explicit Qura pipeline orchestration with deterministic safety boundaries."""

from __future__ import annotations

from modules.qura.context_builder import HealthContextBuilder
from modules.qura.explainability import EvidenceBuilder
from modules.qura.intent import IntentClassifier
from modules.qura.llm_client import LLMClient, LLMClientError
from modules.qura.prompt_builder import PromptBuilder
from modules.qura.query_planner import QueryPlanner
from modules.qura.safety import SafetyLayer
from modules.qura.schemas import ChatRequest, ChatResponse, HealthContext, SafetyFlags


class QuraService:
    def __init__(
        self,
        *,
        safety_layer: SafetyLayer,
        intent_classifier: IntentClassifier,
        query_planner: QueryPlanner,
        context_builder: HealthContextBuilder,
        prompt_builder: PromptBuilder,
        llm_client: LLMClient,
        evidence_builder: EvidenceBuilder,
    ) -> None:
        self._safety_layer = safety_layer
        self._intent_classifier = intent_classifier
        self._query_planner = query_planner
        self._context_builder = context_builder
        self._prompt_builder = prompt_builder
        self._llm_client = llm_client
        self._evidence_builder = evidence_builder

    async def chat(
        self,
        *,
        user_id: int,
        request: ChatRequest,
        user_gender: str | None = None,
        user_first_name: str = "",
        user_last_name: str = "",
        ip_address: str = "unknown",
        user_agent: str = "unknown",
    ) -> ChatResponse:
        safety = self._safety_layer.pre_check(request.message)
        intent = self._intent_classifier.classify(request.message)
        if safety.blocked:
            return ChatResponse(
                answer=self._blocked_answer(safety.categories),
                safety=safety,
                escalation="emergency" if "emergency" in safety.categories else "clinician",
                conversation_id=request.conversation_id,
            )

        plan = self._query_planner.build(intent, request.message)
        context = await self._context_builder.build(
            user_id=user_id,
            plan=plan,
            user_gender=user_gender,
            user_first_name=user_first_name,
            user_last_name=user_last_name,
            ip_address=ip_address,
            user_agent=user_agent,
        )
        prompt = self._prompt_builder.build(
            message=request.message,
            intent=intent,
            plan=plan,
            context=context,
            language=request.language_preference,
        )
        try:
            output = await self._llm_client.complete(prompt)
        except LLMClientError:
            return self._safe_llm_fallback(request=request, context=context)

        post_safety = self._safety_layer.post_check(output, context)
        if post_safety.blocked:
            return ChatResponse(
                answer="I can only provide an explanation grounded in your verified report data. Please review the report with a qualified clinician.",
                evidence=self._evidence_builder.build(context),
                safety=post_safety,
                escalation="clinician",
                conversation_id=request.conversation_id,
            )
        return ChatResponse(
            answer=output.answer,
            evidence=self._evidence_builder.build(context),
            safety=post_safety,
            escalation=output.escalation,
            recommendations=[],
            conversation_id=request.conversation_id,
        )

    def _safe_llm_fallback(self, *, request: ChatRequest, context: HealthContext) -> ChatResponse:
        return ChatResponse(
            answer="I’m unable to generate a report explanation right now. Please try again later or review the report with a qualified clinician.",
            evidence=self._evidence_builder.build(context),
            safety=SafetyFlags(categories=["llm_unavailable"], reasons=["Language service unavailable."]),
            escalation="none",
            conversation_id=request.conversation_id,
        )

    @staticmethod
    def _blocked_answer(categories: list[str]) -> str:
        if "emergency" in categories:
            return "If this may be an emergency, contact local emergency services or seek urgent in-person care now."
        if "medication_or_dosage" in categories:
            return "I cannot provide medication or dosage instructions. Please speak with a qualified clinician or pharmacist."
        return "I cannot diagnose a condition. A qualified clinician can assess your symptoms and health information."
