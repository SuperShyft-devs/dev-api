"""Deterministic, minimum-data query planning for Qura."""

from __future__ import annotations

from modules.qura.intent import IntentClassifier
from modules.qura.schemas import IntentResult, QueryPlan


class QueryPlanner:
    def __init__(self, intent_classifier: IntentClassifier | None = None) -> None:
        self._intent_classifier = intent_classifier or IntentClassifier()

    def build(self, intent: IntentResult, message: str) -> QueryPlan:
        if intent.primary == "explain_marker":
            markers = self._intent_classifier.extract_marker_codes(message)
            return QueryPlan(
                intent=intent.primary,
                required_markers=markers,
                missing_data=[] if markers else ["marker_name"],
            )
        if intent.primary == "report_summary":
            return QueryPlan(intent=intent.primary, include_current_report_markers=True, include_bio_ai_risks=True)
        if intent.primary == "explain_bio_ai_risk":
            return QueryPlan(intent=intent.primary, include_bio_ai_risks=True)
        return QueryPlan(intent=intent.primary)
