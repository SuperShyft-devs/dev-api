"""Orchestrate assessment fetch → patient enrichment → BioReport assembly.

Production entry point is ``generate_for_record(record_id)``. The frontend
supplies only a MetSights ``record_id``; this service:

1. Fetches assessment JSON (MetSights ``GET /reports/{record_id}/``)
2. Enriches patient demographics for the same ``record_id``
3. Merges demographics into the assessment
4. Assembles the final BioReport via builders + knowledge base

Offline runners may inject a local assessment dict via
``generate_from_assessment_with_enrichment`` — enrichment and assembly stay
identical; only the assessment source differs.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from modules.assessments.repository import AssessmentsRepository
from modules.bioai_report.report_engine.builders.report_builder import build_bioreport
from modules.bioai_report.report_engine.knowledge_base.loader import KnowledgeBaseStore
from modules.bioai_report.report_engine.models.report import BioReport
from modules.bioai_report.report_engine.services.assessment_service import AssessmentFetchService
from modules.bioai_report.report_engine.services.patient_service import PatientProfileService
from modules.bioai_report.report_engine.utils.patient_enrichment import missing_demographic_fields

logger = logging.getLogger(__name__)


class BioReportService:
    """Application service that produces a BioReport from a MetSights ``record_id``."""

    def __init__(
        self,
        *,
        assessment_service: AssessmentFetchService,
        patient_service: PatientProfileService | None = None,
        kb_store: KnowledgeBaseStore | None = None,
    ) -> None:
        self._assessment_service = assessment_service
        self._patient_service = patient_service
        self._kb_store = kb_store or KnowledgeBaseStore()

    async def generate_for_record(
        self,
        *,
        record_id: str,
        assessment_type_code: str | None = None,
        db: AsyncSession | None = None,
    ) -> BioReport:
        """Production pipeline: ``record_id`` → assessment → enrich → BioReport.

        The frontend must not send patient details or disease payloads — only
        ``record_id``. Enrichment failures never abort report generation.
        """
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
        """Production pipeline: ``assessment_instance_id`` → assessment → enrich → BioReport.

        This resolves the underlying MetSights ``record_id`` using existing DB
        repository relationships, then reuses the unchanged enrichment + assembly
        pipeline.
        """
        if assessment_instance_id is None:
            raise ValueError("assessment_instance_id is required")
        instance_id = int(assessment_instance_id)
        if instance_id <= 0:
            raise ValueError("assessment_instance_id must be positive")

        assessments_repo = AssessmentsRepository()
        instance = await assessments_repo.get_instance_by_id(
            db,
            assessment_instance_id=instance_id,
        )
        if instance is None:
            raise ValueError(f"assessment_instance_id not found: {instance_id}")

        record_id = (getattr(instance, "metsights_record_id", None) or "").strip()
        if not record_id:
            raise ValueError(f"metsights_record_id missing for assessment_instance_id={instance_id}")

        # Derive the optional MetSights assessment type code from the linked package.
        assessment_type_code: str | None = None
        package_id = getattr(instance, "package_id", None)
        if package_id is not None:
            package = await assessments_repo.get_package_by_id(db, package_id=int(package_id))
            assessment_type_code = getattr(package, "assessment_type_code", None) if package else None

        assessment = await self._assessment_service.fetch_raw(
            record_id=record_id,
            assessment_type_code=assessment_type_code,
        )

        # Keep existing enrichment logic unchanged; it runs on the resolved MetSights record_id.
        return await self.generate_from_assessment_with_enrichment(
            assessment,
            record_id=record_id,
            db=db,
        )

    async def generate_from_assessment_with_enrichment(
        self,
        assessment: dict[str, Any],
        *,
        record_id: str | None = None,
        db: AsyncSession | None = None,
    ) -> BioReport:
        """Enrich a pre-loaded assessment dict, then assemble BioReport.

        Used by ``generate_for_record`` after the live MetSights fetch, and by
        offline runners that load assessment JSON from disk. Steps after the
        assessment source are identical.
        """
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

        return self.generate_from_assessment(merged, record_id=resolved_record_id or None)

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
    ) -> BioReport:
        """Assemble a BioReport from an already-merged assessment dict (sync / unit tests)."""
        return build_bioreport(
            assessment,
            record_id=record_id,
            kb_store=self._kb_store,
        )
