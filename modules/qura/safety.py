"""Deterministic safety checks; no LLM is involved in this layer."""

from __future__ import annotations

import re

from modules.qura.schemas import HealthContext, LLMStructuredOutput, SafetyFlags


class SafetyLayer:
    _EMERGENCY = re.compile(
        r"\b(chest pain|trouble breathing|difficulty breathing|can't breathe|cannot breathe|"
        r"suicid(?:e|al)|self[- ]harm|overdose|stroke symptoms|unconscious)\b",
        re.IGNORECASE,
    )
    _MEDICATION = re.compile(r"\b(dosage|dose|how much .*?(?:medicine|medication|tablet|pill)|prescribe)\b", re.IGNORECASE)
    _DIAGNOSIS = re.compile(r"\b(diagnose|diagnosis|do i have|tell me if i have)\b", re.IGNORECASE)
    _DEFINITE_DIAGNOSIS = re.compile(r"\b(you have|you are diagnosed with|this confirms|you definitely have)\b", re.IGNORECASE)
    _DOSAGE_IN_ANSWER = re.compile(r"\b(?:take|start|stop)\s+\d+(?:\.\d+)?\s*(?:mg|mcg|ml|tablet|pill)\b", re.IGNORECASE)
    _NUMBER = re.compile(r"(?<![A-Za-z])\d+(?:\.\d+)?")

    def pre_check(self, message: str) -> SafetyFlags:
        if self._EMERGENCY.search(message):
            return SafetyFlags(
                blocked=True,
                categories=["emergency"],
                reasons=["Emergency language requires urgent human care guidance."],
            )
        if self._MEDICATION.search(message):
            return SafetyFlags(
                blocked=True,
                categories=["medication_or_dosage"],
                reasons=["Qura does not provide medication or dosage instructions."],
            )
        if self._DIAGNOSIS.search(message):
            return SafetyFlags(
                blocked=True,
                categories=["diagnosis_request"],
                reasons=["Qura does not diagnose medical conditions."],
            )
        return SafetyFlags()

    def post_check(self, output: LLMStructuredOutput, context: HealthContext) -> SafetyFlags:
        categories: list[str] = []
        available_ids = {
            *[marker.code for marker in context.selected_markers],
            *[risk.code for risk in context.risk_scores],
            *[delta.code for delta in context.deltas],
        }
        if any(identifier not in available_ids for identifier in output.grounding):
            categories.append("invalid_grounding")
        if self._DEFINITE_DIAGNOSIS.search(output.answer):
            categories.append("diagnosis_claim")
        if self._DOSAGE_IN_ANSWER.search(output.answer):
            categories.append("medication_or_dosage")
        if output.recommendations:
            categories.append("recommendations_not_enabled")
        allowed_numbers = self._allowed_numbers(context)
        answer_numbers = set(self._NUMBER.findall(output.answer))
        if any(number not in allowed_numbers for number in answer_numbers):
            categories.append("unsupported_numerical_claim")
        if context.metadata.missing_data and any(
            identifier in output.answer.lower() for identifier in self._missing_identifiers(context)
        ) and answer_numbers:
            categories.append("missing_data_contradiction")
        return SafetyFlags(blocked=bool(categories), categories=list(dict.fromkeys(categories)))

    @staticmethod
    def _allowed_numbers(context: HealthContext) -> set[str]:
        values: list[float | int] = []
        for marker in context.selected_markers:
            values.extend(value for value in (marker.value, marker.ref_low, marker.ref_high) if value is not None)
        for risk in context.risk_scores:
            if risk.score is not None:
                values.append(risk.score)
        for delta in context.deltas:
            values.extend(value for value in (delta.current_value, delta.previous_value) if value is not None)
        allowed: set[str] = set()
        for value in values:
            allowed.add(str(value))
            allowed.add(str(float(value)).rstrip("0").rstrip("."))
        return allowed

    @staticmethod
    def _missing_identifiers(context: HealthContext) -> set[str]:
        return {
            item.split(":", 1)[1].lower()
            for item in context.metadata.missing_data
            if item.startswith("marker:")
        }
