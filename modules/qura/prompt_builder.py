"""Deterministic, provider-neutral prompt construction."""

from __future__ import annotations

import json

from modules.qura.schemas import BuiltPrompt, HealthContext, IntentResult, QueryPlan


class PromptBuilder:
    PROMPT_VERSION = "qura_phase1_v1"

    _SYSTEM = (
        "You are Qura, a health-report explanation assistant. Explain only information supplied "
        "by the application in HEALTH CONTEXT, using simple understandable language. The application "
        "is the source of truth. Acknowledge unavailable information and suggest professional medical "
        "advice when appropriate."
    )
    _SAFETY = (
        "Do not diagnose diseases, prescribe medication, or provide medication dosage instructions. "
        "Do not invent laboratory values, reference ranges, risk scores, historical values, percentages, "
        "clinical deltas, BMI, nutrition quantities, or disease probabilities. Do not calculate clinical "
        "values. Do not claim a condition is definitely present from this report. State clinical numbers "
        "only when they appear in HEALTH CONTEXT. Missing data is unavailable and must never be guessed."
    )

    def build(
        self,
        *,
        message: str,
        intent: IntentResult,
        plan: QueryPlan,
        context: HealthContext,
        language: str | None,
        safety_constraints: str | None = None,
    ) -> BuiltPrompt:
        context_json = json.dumps(context.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        task = json.dumps(
            {
                "intent": intent.primary,
                "requested_markers": plan.required_markers,
                "missing_data": context.metadata.missing_data,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        output_format = json.dumps(
            {
                "answer": "string",
                "grounding": ["health-context identifier"],
                "confidence": "low|medium|high",
                "safety_flags": ["string"],
                "escalation": "none|clinician|emergency",
                "recommendations": [],
            },
            separators=(",", ":"),
        )
        return BuiltPrompt(
            version=self.PROMPT_VERSION,
            system_instructions=self._SYSTEM,
            task=task,
            health_context_json=context_json,
            user_question=message,
            safety_rules=" ".join(part for part in (self._SAFETY, safety_constraints) if part),
            output_format=output_format,
            language=language or "english",
        )
