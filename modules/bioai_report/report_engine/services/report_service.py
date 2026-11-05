"""Orchestrate assessment fetch → patient enrichment → BioReport assembly.

``GET /bioai-report/{assessment_instance_id}`` uses stored
``individual_health_report.reports`` plus SuperShyft user demographics.
``generate_for_record(record_id)`` remains available for MetSights-backed
offline/record flows.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.assessments.models import AssessmentInstance
from modules.assessments.repository import AssessmentsRepository
from modules.bioai_report.report_engine.builders.report_builder import build_bioreport
from modules.bioai_report.report_engine.knowledge_base.loader import KnowledgeBaseStore
from modules.bioai_report.report_engine.models.report import BioReport
from modules.bioai_report.report_engine.services.assessment_service import AssessmentFetchService
from modules.bioai_report.report_engine.services.patient_service import PatientProfileService
from modules.bioai_report.report_engine.utils.patient_enrichment import (
    merge_patient_into_assessment,
    missing_demographic_fields,
)
from modules.reports.models import IndividualHealthReport
from modules.reports.repository import ReportsRepository

logger = logging.getLogger(__name__)

_AWARE_MIN = datetime.min.replace(tzinfo=timezone.utc)


def _as_aware(value: datetime | None) -> datetime:
    if value is None:
        return _AWARE_MIN
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _completed_sort_key(instance: AssessmentInstance) -> tuple[datetime, datetime, int]:
    return (
        _as_aware(getattr(instance, "completed_at", None)),
        _as_aware(getattr(instance, "assigned_at", None)),
        int(instance.assessment_instance_id),
    )


def _latest_completed_source(
    rows: list[tuple[AssessmentInstance, Any, IndividualHealthReport]],
) -> tuple[AssessmentInstance, Any, IndividualHealthReport]:
    return max(rows, key=lambda row: _completed_sort_key(row[0]))


def _has_stored_reports(ihr: IndividualHealthReport | None) -> bool:
    if ihr is None:
        return False
    reports = getattr(ihr, "reports", None)
    if reports is None:
        return False
    if isinstance(reports, str) and not reports.strip():
        return False
    if isinstance(reports, dict) and not reports:
        return False
    return True


def _stored_reports_dict(reports: Any, *, assessment_instance_id: int) -> dict[str, Any]:
    if reports is None:
        raise ValueError("IHR not available")
    if isinstance(reports, str):
        text = reports.strip()
        if not text:
            raise ValueError("IHR not available")
        try:
            reports = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("IHR not available") from exc
    if not isinstance(reports, dict) or not reports:
        raise ValueError("IHR not available")
    return reports


class BioReportService:
    """Application service that produces a BioReport from stored IHR or a record id."""

    def __init__(
        self,
        *,
        assessment_service: AssessmentFetchService,
        patient_service: PatientProfileService | None = None,
        kb_store: KnowledgeBaseStore | None = None,
        assessments_repository: AssessmentsRepository | None = None,
        reports_repository: ReportsRepository | None = None,
    ) -> None:
        self._assessment_service = assessment_service
        self._patient_service = patient_service
        self._kb_store = kb_store or KnowledgeBaseStore()
        self._assessments = assessments_repository or AssessmentsRepository()
        self._reports = reports_repository or ReportsRepository()

    async def generate_for_record(
        self,
        *,
        record_id: str,
        assessment_type_code: str | None = None,
        db: AsyncSession | None = None,
    ) -> BioReport:
        """Record-id pipeline: ``record_id`` → assessment → enrich → BioReport."""
        rid = (record_id or "").strip()
        if not rid:
            raise ValueError("record_id is required")

        assessment = await self._assessment_service.fetch_raw(
            record_id=rid,
            assessment_type_code=assessment_type_code,
        )
        return await self.generate_from_assessment_with_enrichment(
            assessment,
            record_id=rid,
            db=db,
        )

    async def generate_for_assessment_instance(
        self,
        *,
        assessment_instance_id: int,
        db: AsyncSession,
    ) -> BioReport:
        """``assessment_instance_id`` → that instance's stored IHR ``reports`` → BioReport.

        Uses only ``individual_health_report.reports`` for the requested assessment
        instance (never another camp's completed assessment). Does not call MetSights.
        """
        if assessment_instance_id is None:
            raise ValueError("assessment_instance_id is required")
        instance_id = int(assessment_instance_id)
        if instance_id <= 0:
            raise ValueError("assessment_instance_id must be positive")

        instance = await self._assessments.get_instance_by_id(
            db,
            assessment_instance_id=instance_id,
        )
        if instance is None:
            raise AppError(
                status_code=404,
                error_code="ASSESSMENT_NOT_FOUND",
                message="Assessment does not exist",
            )

        user_id = int(instance.user_id)
        instance_ihr = await self._reports.get_individual_report_by_assessment(
            db,
            assessment_instance_id=instance_id,
        )
        if not _has_stored_reports(instance_ihr):
            diagnostic = getattr(self._reports, "get_bioai_link_diagnostics_for_user", None)
            if callable(diagnostic):
                assessment_ids, ihr_assessment_ids = await diagnostic(db, user_id=user_id)
                logger.info(
                    "BioAI request: assessment_instance_id=%s user_id=%s "
                    "no IHR for instance assessment_ids=%s ihr_assessment_ids=%s",
                    instance_id,
                    user_id,
                    assessment_ids,
                    ihr_assessment_ids,
                )
            raise ValueError("IHR not available")

        ihr = instance_ihr
        source_record_id = getattr(instance, "metsights_record_id", None)
        logger.info(
            "BioAI request: assessment_instance_id=%s user_id=%s source=assessment_instance_ihr",
            instance_id,
            user_id,
        )
        assessment = _stored_reports_dict(
            getattr(ihr, "reports", None),
            assessment_instance_id=instance_id,
        )
        assessment = merge_patient_into_assessment(
            assessment,
            {"user_id": user_id},
            overwrite=True,
        )
        if self._patient_service is not None:
            assessment = await self._patient_service.enrich_assessment_from_local_user(
                assessment,
                user_id=user_id,
                assessment_instance_id=instance_id,
                db=db,
            )
            if db is not None:
                try:
                    await db.rollback()
                except Exception:
                    logger.exception(
                        "Bio-AI report could not reset DB session after patient enrichment "
                        "for assessment_instance_id=%s",
                        instance_id,
                    )
        record_id = str(
            source_record_id
            or assessment.get("record_id")
            or assessment.get("id")
            or instance_id
        )
        allergy_booking = None
        allergy_status = "resolved"
        allergy_reason = None
        cancer_booked = False
        allergy_booked = False
        if db is not None:
            from modules.bioai_report.report_engine.builders.allergy_builder import panels_booked
            from modules.bioai_report.report_engine.services.allergy_booking import (
                load_booking_for_assessment,
            )

            allergy_status, allergy_booking, allergy_reason = await load_booking_for_assessment(
                db,
                assessment_instance_id=instance_id,
            )
            if allergy_status == "not_found":
                allergy_booking = None
                allergy_status = "resolved"
                allergy_reason = None
            elif allergy_booking is not None:
                cancer_booked, allergy_booked = panels_booked(
                    allergy_booking.get("blood_parameters")
                )
        return await asyncio.to_thread(
            self.generate_from_assessment,
            assessment,
            record_id=record_id,
            allergy_booking=allergy_booking if allergy_booked or allergy_status != "resolved" else None,
            assessment_instance_id=instance_id,
            allergy_resolution_status=allergy_status,
            allergy_resolution_reason=allergy_reason,
            include_cancer=cancer_booked if db is not None else None,
            cancer_results_available=cancer_booked if db is not None else None,
            allergy_results_available=allergy_booked if db is not None else None,
        )

    async def generate_from_assessment_with_enrichment(
        self,
        assessment: dict[str, Any],
        *,
        record_id: str | None = None,
        db: AsyncSession | None = None,
    ) -> BioReport:
        """Enrich a pre-loaded assessment dict, then assemble BioReport."""
        resolved_record_id = (record_id or "").strip()
        if not resolved_record_id:
            body = assessment.get("data") if isinstance(assessment.get("data"), dict) else assessment
            resolved_record_id = str(
                (body or {}).get("record")
                or (body or {}).get("record_id")
                or (body or {}).get("id")
                or ""
            ).strip()

        merged = await self._enrich_assessment(
            assessment,
            record_id=resolved_record_id,
            db=db,
        )

        still_missing = missing_demographic_fields(merged)
        if still_missing:
            logger.info(
                "BioReport patient enrichment incomplete for record_id=%s missing=%s",
                resolved_record_id or "<unknown>",
                still_missing,
            )

        return await asyncio.to_thread(
            self.generate_from_assessment,
            merged,
            record_id=resolved_record_id or None,
        )

    async def _enrich_assessment(
        self,
        assessment: dict[str, Any],
        *,
        record_id: str,
        db: AsyncSession | None = None,
    ) -> dict[str, Any]:
        if self._patient_service is None:
            return assessment
        return await self._patient_service.enrich_assessment_if_needed(
            assessment,
            record_id=record_id,
            db=db,
        )

    def generate_from_assessment(
        self,
        assessment: dict[str, Any],
        *,
        record_id: str | None = None,
        allergy_booking: dict[str, Any] | None = None,
        assessment_instance_id: int | None = None,
        allergy_resolution_status: str = "resolved",
        allergy_resolution_reason: str | None = None,
        include_cancer: bool | None = None,
        cancer_results_available: bool | None = None,
        allergy_results_available: bool | None = None,
    ) -> BioReport:
        """Assemble a BioReport from an already-merged assessment dict (sync / unit tests)."""
        return build_bioreport(
            assessment,
            record_id=record_id,
            kb_store=self._kb_store,
            allergy_booking=allergy_booking,
            assessment_instance_id=assessment_instance_id,
            allergy_resolution_status=allergy_resolution_status,
            allergy_resolution_reason=allergy_resolution_reason,
            include_cancer=include_cancer,
            cancer_results_available=cancer_results_available,
            allergy_results_available=allergy_results_available,
        )
