"""Fetch assessment JSON for a MetSights record_id.

Upstream source (production):

    GET {METSIGHTS_BASE_URL}/reports/{record_id}/

(FitPrint type ``7`` uses ``/reports/fitness-reports/{record_id}/``.)

This is step 1 of the Bio-AI pipeline. The engine never accepts assessment
JSON from the frontend.
"""

from __future__ import annotations

from typing import Any

from modules.bioai_report.report_engine.models.assessment import AssessmentPayload
from modules.bioai_report.report_engine.utils.assessment_normalizer import normalize_assessment
from modules.metsights.service import MetsightsService


class AssessmentFetchService:
    """Retrieve assessment data via the existing Metsights reports API."""

    def __init__(self, metsights_service: MetsightsService) -> None:
        self._metsights = metsights_service

    async def fetch_raw(
        self,
        *,
        record_id: str,
        assessment_type_code: str | None = None,
    ) -> dict[str, Any]:
        """Fetch the raw MetSights report payload for ``record_id``."""
        data = await self._metsights.get_report(
            record_id=record_id,
            assessment_type_code=assessment_type_code,
        )
        if not isinstance(data, dict):
            return {"data": data}
        return data

    async def fetch(
        self,
        *,
        record_id: str,
        assessment_type_code: str | None = None,
    ) -> AssessmentPayload:
        """Fetch and normalize assessment JSON for report assembly."""
        raw = await self.fetch_raw(
            record_id=record_id,
            assessment_type_code=assessment_type_code,
        )
        return normalize_assessment(raw, record_id=record_id)
