"""Deterministic Phase-1 intent detection."""

from __future__ import annotations

import re

from modules.qura.safety import SafetyLayer
from modules.qura.schemas import IntentCandidate, IntentResult


class IntentClassifier:
    _GREETING = re.compile(r"^\s*(hi|hello|hey|namaste|good (morning|afternoon|evening))\s*[!.?]*\s*$", re.IGNORECASE)
    _REPORT_SUMMARY = re.compile(r"\b(latest |my )?(blood )?report\b", re.IGNORECASE)
    _BIO_AI = re.compile(r"\b(bio[ -]?ai|metabolic score|health risk|risk analysis)\b", re.IGNORECASE)
    _MARKER = re.compile(
        r"\b(ldl|hdl|cholesterol|triglycerides?|haemoglobin|hemoglobin|hba1c|glucose|"
        r"tsh|t3|t4|vitamin d|vitamin b12|creatinine|uric acid|ferritin)\b",
        re.IGNORECASE,
    )

    def __init__(self, safety_layer: SafetyLayer | None = None) -> None:
        self._safety_layer = safety_layer or SafetyLayer()

    def classify(self, message: str) -> IntentResult:
        safety = self._safety_layer.pre_check(message)
        if "emergency" in safety.categories:
            return self._result("emergency", 1.0)
        if safety.blocked:
            return self._result("unsupported_medical", 1.0)
        if self._GREETING.match(message):
            return self._result("greeting_small_talk", 1.0)
        if self._BIO_AI.search(message):
            return self._result("explain_bio_ai_risk", 0.95)
        if self._REPORT_SUMMARY.search(message):
            return self._result("report_summary", 0.95)
        if self._MARKER.search(message):
            return self._result("explain_marker", 0.9)
        return IntentResult(
            primary="unsupported_medical",
            intents=[IntentCandidate(name="unsupported_medical", confidence=0.6)],
            needs_clarification=True,
        )

    @staticmethod
    def _result(name: str, confidence: float) -> IntentResult:
        return IntentResult(primary=name, intents=[IntentCandidate(name=name, confidence=confidence)])

    def extract_marker_codes(self, message: str) -> list[str]:
        """Return only explicit marker tokens; this is not a medical inference."""
        aliases = {"haemoglobin": "hemoglobin", "triglyceride": "triglycerides"}
        return list(dict.fromkeys(aliases.get(match.group(0).lower(), match.group(0).lower()) for match in self._MARKER.finditer(message)))
