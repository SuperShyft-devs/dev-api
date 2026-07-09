"""Reports business service."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine
from datetime import date, datetime, timezone
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker

from core.config import settings
from core.exceptions import AppError
from db.session import AsyncSessionLocal
from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.engagements.models import Engagement
from modules.assessments.repository import AssessmentsRepository
from modules.audit.models import IntegrationSyncLog
from modules.audit.repository import AuditRepository
from modules.audit.service import AuditService
from modules.diagnostics.service import DiagnosticsService
from modules.metsights.service import MetsightsService
from modules.metsights.sync_service import MetsightsSyncService
from modules.reports.blood_parameters_normalizer import build_grouped_from_healthians
from modules.reports.blood_parameters_read_service import BloodParametersReadService
from modules.reports.blood_parameters_questionnaire_reader import BloodParametersQuestionnaireReader
from modules.reports.blood_parameters_schemas import has_usable_provider_blood_parameters
from modules.reports.healthians_booking_resolver import (
    HealthiansBookingSource,
    resolve_healthians_booking_id,
)
from db.seed.blood_parameters_registry import (
    ADVANCED_BLOOD_PARAMETER_CATEGORY_KEY,
    BLOOD_PARAMETER_CATEGORY_KEY,
)
from modules.reports.camp_report_section_builders import (
    extract_diseases,
    extract_metabolic_age,
    extract_metabolic_score,
)
from modules.reports.models import IndividualHealthReport, ReportsUserSyncState
from modules.reports.repository import ReportsRepository
from modules.users.models import User
from modules.questionnaire.healthy_habits_service import HealthyHabitsService
from modules.questionnaire.repository import QuestionnaireRepository
from modules.reports.schemas import (
    BioAiPdfResponse,
    BloodParameterGroupInReportResponse,
    DiagnosticPdfResponse,
    DiseaseDetailResponse,
    DiseaseListItem,
    DiseaseOverview,
    FitPrintParameter,
    FitPrintParameterRange,
    HealthSpanFitnessDetail,
    HealthSpanIndexResponse,
    HealthSpanLifestyleDetail,
    HealthSpanNutritionDetail,
    HealthyHabitItem,
    IdealRangeDetail,
    NutrientDetail,
    OverviewReportResponse,
    PositiveWins,
    RiskAnalysisItem,
    RiskAnalysisListResponse,
    WaterDetail,
)

logger = logging.getLogger(__name__)


_SYNC_IDLE = "idle"
_SYNC_IN_PROGRESS = "in_progress"
_SYNC_FAILED = "failed"

# Blood fetch failures that should yield empty profiles, not abort camp/overview aggregation.
BLOOD_DATA_UNAVAILABLE_ERROR_CODES = frozenset({
    "BLOOD_PARAMETERS_NOT_FOUND",
    "BLOOD_SAMPLE_NOT_COLLECTED",
    "INVALID_STATE",
    "EXTERNAL_SERVICE_UNAVAILABLE",
})

_OVERVIEW_METABOLIC_AGE_OVERRIDES: dict[int, float] = {
    1169: 52.0,
}

_BIO_AI_METSIGHTS_REPORT_URL_OVERRIDES: dict[str, str] = {
    "https://storages.metsights.com/reports/D6E1178CCA4F488C_Deepa_Gupta_MHR.pdf": (
        "https://api.supershyft.com/media/bio-ai/141a9b846e254200995dcbdcc1596ea5.pdf"
    ),
    "https://storages.metsights.com/reports/4334065C1F6F4027_Ms_Manali_Bhojwani_MHR.pdf": (
        "https://api.supershyft.com/media/bio-ai/f2d73e35010d4eadbf38c2ca5c4e34c9.pdf"
    ),
    "https://storages.metsights.com/reports/5890D12E4C024F2D_Apoorv_Jain_MHR.pdf": (
        "https://api.supershyft.com/media/bio-ai/91fd557d121f4768b8ad03d2e57ef0d3.pdf"
    ),
    "https://storages.metsights.com/reports/46D0D007925941B7_Kuldeep_Chobey_MHR.pdf": (
        "https://api.supershyft.com/media/bio-ai/c7e12326b7ca4dbfa5b58dce6af5b3cd.pdf"
    ),
    "https://storages.metsights.com/reports/A45996FC284642C5_Akash_Gupta_MHR.pdf": (
        "https://api.supershyft.com/media/bio-ai/9e529ee782984068813c511f7b944e26.pdf"
    ),
}


class ReportsService:
    """Business logic for report retrieval and caching."""

    def __init__(
        self,
        repository: ReportsRepository,
        assessments_repository: AssessmentsRepository,
        metsights_service: MetsightsService,
        diagnostics_service: DiagnosticsService,
        audit_service: AuditService | None = None,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
        healthy_habits_service: HealthyHabitsService | None = None,
        questionnaire_repository: QuestionnaireRepository | None = None,
        healthians_get_access_token: Callable[[], Coroutine[Any, Any, str]] | None = None,
        healthians_get_booking_digital_value: Callable[[str, str], Coroutine[Any, Any, dict]] | None = None,
        healthians_get_booking_report: Callable[[str, str], Coroutine[Any, Any, dict]] | None = None,
        metsights_sync_service: MetsightsSyncService | None = None,
    ):
        self._repository = repository
        self._assessments_repository = assessments_repository
        self._metsights_service = metsights_service
        self._diagnostics_service = diagnostics_service
        self._audit_service = audit_service
        self._session_factory = session_factory or AsyncSessionLocal
        self._healthy_habits_service = healthy_habits_service
        self._questionnaire_repository = questionnaire_repository
        self._healthians_get_access_token = healthians_get_access_token
        self._healthians_get_booking_digital_value = healthians_get_booking_digital_value
        self._healthians_get_booking_report = healthians_get_booking_report
        self._metsights_sync_service = metsights_sync_service
        self._blood_read_service = BloodParametersReadService(
            diagnostics_service=diagnostics_service,
            questionnaire_reader=BloodParametersQuestionnaireReader(questionnaire_repository),
        )

    def _require_audit_service(self) -> AuditService:
        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        return self._audit_service

    async def _get_individual_report(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        engagement_id: int,
        assessment_instance_id: int | None = None,
    ) -> IndividualHealthReport | None:
        """Load assessment-scoped IHR when assessment_instance_id is given; else engagement blood row."""
        if assessment_instance_id is not None:
            return await self._repository.get_individual_report_by_assessment(
                db,
                assessment_instance_id=assessment_instance_id,
            )
        return await self._repository.get_individual_report_by_engagement(
            db,
            user_id=user_id,
            engagement_id=engagement_id,
        )

    async def _get_blood_individual_report(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        engagement_id: int,
        assessment_instance_id: int | None = None,
    ) -> IndividualHealthReport | None:
        """Prefer assessment row with blood; fall back to any engagement row with blood."""
        if assessment_instance_id is not None:
            report = await self._repository.get_individual_report_by_assessment(
                db,
                assessment_instance_id=assessment_instance_id,
            )
            if report is not None and has_usable_provider_blood_parameters(report.blood_parameters):
                return report
        return await self._repository.get_individual_report_by_engagement(
            db,
            user_id=user_id,
            engagement_id=engagement_id,
        )

    @staticmethod
    def _reports_match_assessment_type(
        report_dict: dict[str, Any],
        assessment_type_code: str | None,
    ) -> bool:
        """Reject cached reports that belong to a different assessment type (Pro vs FitPrint)."""
        code = (assessment_type_code or "").strip()
        has_fitprint_shape = (
            "fitness_specification" in report_dict or "activity_specification" in report_dict
        )
        if code == "7":
            if has_fitprint_shape:
                return True
            # Pro/Basic-shaped payload on a FitPrint assessment.
            if "metabolic_age" in report_dict or "diseases" in report_dict:
                return False
            return True
        # Pro / Basic (and other non-FitPrint): reject FitPrint-shaped cache.
        if has_fitprint_shape:
            return False
        return True

    async def _persist_provider_blood_data(
        self,
        db: AsyncSession,
        *,
        individual_report: IndividualHealthReport | None,
        assessment_instance: AssessmentInstance,
        diagnostic_package_id: int,
        raw_customer: dict[str, Any],
    ) -> IndividualHealthReport:
        package_tests = await self._diagnostics_service.get_package_tests(
            db=db,
            package_id=diagnostic_package_id,
        )
        grouped, raw = build_grouped_from_healthians(
            raw_customer,
            package_groups=package_tests.groups,
        )
        if not has_usable_provider_blood_parameters(grouped):
            logger.warning(
                "Healthians normalize produced empty parameters for engagement_id=%s",
                assessment_instance.engagement_id,
            )

        engagement_row = await self._repository.get_individual_report_by_engagement(
            db,
            user_id=int(assessment_instance.user_id),
            engagement_id=int(assessment_instance.engagement_id),
        )
        # Prefer an existing blood-bearing engagement row; else assessment row; else any engagement row.
        if engagement_row is not None and (
            engagement_row.blood_parameters is not None or engagement_row.blood_report_raw is not None
        ):
            target = engagement_row
        elif individual_report is not None:
            target = individual_report
        elif engagement_row is not None:
            target = engagement_row
        else:
            target = None

        if target is None:
            target = IndividualHealthReport(
                user_id=assessment_instance.user_id,
                engagement_id=assessment_instance.engagement_id,
                assessment_instance_id=assessment_instance.assessment_instance_id,
                reports=None,
                blood_parameters=grouped,
                blood_report_raw=raw,
            )
            await self._repository.create_individual_report(db, target)
        else:
            target.blood_parameters = grouped
            target.blood_report_raw = raw
            # Do not re-point another assessment's row.
            if target.assessment_instance_id is None:
                target.assessment_instance_id = assessment_instance.assessment_instance_id
            await self._repository.update_individual_report(db, target)
        return target

    async def _import_metsights_blood_categories_if_requested(
        self,
        db: AsyncSession,
        *,
        assessment_instance: AssessmentInstance,
        user_id: int,
        reload: int,
    ) -> None:
        if reload != 1 or self._metsights_sync_service is None:
            return
        for category_key in (BLOOD_PARAMETER_CATEGORY_KEY, ADVANCED_BLOOD_PARAMETER_CATEGORY_KEY):
            try:
                await self._metsights_sync_service.import_category_from_metsights(
                    db,
                    assessment_instance_id=int(assessment_instance.assessment_instance_id),
                    user_id=user_id,
                    category_key=category_key,
                    category_of="metsights",
                    reload=1,
                )
            except AppError as exc:
                logger.debug(
                    "Metsights blood import skipped for assessment %s category %s: %s",
                    assessment_instance.assessment_instance_id,
                    category_key,
                    exc.error_code,
                )

    @staticmethod
    def _match_customer_by_name(
        data_list: list[Any],
        first_name: str,
        last_name: str,
    ) -> dict[str, Any] | None:
        """Find the customer entry whose name matches the user (case-insensitive, tokenised)."""
        target_full = f"{first_name} {last_name}".strip().lower()
        target_tokens = set(target_full.split())

        best: dict[str, Any] | None = None
        best_score = 0

        for entry in data_list:
            if not isinstance(entry, dict):
                continue
            customer_name = str(entry.get("customer_name") or "").strip().lower()
            if not customer_name:
                continue
            if customer_name == target_full:
                return entry
            entry_tokens = set(customer_name.split())
            overlap = len(target_tokens & entry_tokens)
            if overlap > best_score:
                best_score = overlap
                best = entry

        if best is not None and best_score >= 1:
            return best
        return data_list[0] if data_list else None

    async def _fetch_blood_parameters_from_provider(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        engagement_id: int,
        record_id: str,
        user_first_name: str,
        user_last_name: str,
    ) -> dict[str, Any]:
        """Fetch blood report from Healthians using participant or Metsights booking id."""
        if self._healthians_get_access_token is None or self._healthians_get_booking_digital_value is None:
            raise AppError(
                status_code=503,
                error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                message="Healthians integration is not configured",
            )

        resolved = await resolve_healthians_booking_id(
            db,
            user_id=user_id,
            engagement_id=engagement_id,
            record_id=record_id,
            metsights_service=self._metsights_service,
        )
        booking_id = resolved.booking_id

        try:
            access_token = await self._healthians_get_access_token()
        except Exception as exc:
            logger.error("Healthians auth failed: %s", exc)
            raise AppError(
                status_code=503,
                error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                message="Healthians authentication failed",
            ) from exc

        try:
            result = await self._healthians_get_booking_digital_value(access_token, booking_id)
        except Exception as exc:
            logger.error("Healthians getBookingDigitalValue failed: %s", exc)
            raise AppError(
                status_code=503,
                error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                message="Healthians booking digital value request failed",
            ) from exc

        data_list = result.get("data")
        if not isinstance(data_list, list) or not data_list:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Healthians returned no blood report data",
            )

        matched = self._match_customer_by_name(data_list, user_first_name, user_last_name)
        if matched is None:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Could not match the user's blood report from Healthians",
            )
        return matched

    async def get_blood_parameters_for_user(
        self,
        db: AsyncSession,
        *,
        assessment_id: int,
        user_id: int,
        user_gender: str | None,
        user_first_name: str = "",
        user_last_name: str = "",
        load_from: str = "provider",
        reload: int = 0,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> Any:
        assessment_row = await self._assessments_repository.get_instance_for_user_with_engagement(
            db,
            assessment_instance_id=assessment_id,
            user_id=user_id,
        )
        if assessment_row is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_NOT_FOUND", message="Assessment does not exist")

        assessment_instance, _package, engagement = assessment_row
        if engagement is None:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Assessment is missing engagement context",
            )

        if engagement.diagnostic_package_id is None:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Engagement has no diagnostic package",
            )
        diagnostic_package_id = int(engagement.diagnostic_package_id)
        normalized_gender = (user_gender or "").strip().lower() or None
        if normalized_gender not in {"male", "female"}:
            normalized_gender = None

        record_id = (assessment_instance.metsights_record_id or "").strip()

        # --- load_from=metsights: questionnaire responses only, never cache to blood_parameters ---
        if load_from == "metsights":
            await self._import_metsights_blood_categories_if_requested(
                db,
                assessment_instance=assessment_instance,
                user_id=user_id,
                reload=reload,
            )
            await self._require_audit_service().log_event(
                db,
                action="USER_FETCH_BLOOD_PARAMETERS_REPORT",
                endpoint=endpoint,
                ip_address=ip_address,
                user_agent=user_agent,
                user_id=user_id,
                session_id=None,
            )
            return await self._blood_read_service.build_from_questionnaire_responses(
                db=db,
                assessment_instance_id=assessment_instance.assessment_instance_id,
                diagnostic_package_id=diagnostic_package_id,
                user_gender=normalized_gender,
            )

        # --- load_from=provider (default) ---
        assessment_report = await self._get_individual_report(
            db,
            user_id=user_id,
            engagement_id=int(assessment_instance.engagement_id),
            assessment_instance_id=assessment_id,
        )
        existing_report = await self._get_blood_individual_report(
            db,
            user_id=user_id,
            engagement_id=int(assessment_instance.engagement_id),
            assessment_instance_id=assessment_id,
        )

        use_cache = (
            reload != 1
            and existing_report is not None
            and has_usable_provider_blood_parameters(existing_report.blood_parameters)
        )
        if use_cache:
            return await self._blood_read_service.build_from_canonical_or_legacy_provider(
                db=db,
                blood_parameters=existing_report.blood_parameters,
                diagnostic_package_id=diagnostic_package_id,
                user_gender=normalized_gender,
            )

        if record_id:
            try:
                raw_customer = await self._fetch_blood_parameters_from_provider(
                    db,
                    user_id=user_id,
                    engagement_id=int(assessment_instance.engagement_id),
                    record_id=record_id,
                    user_first_name=user_first_name,
                    user_last_name=user_last_name,
                )
                await self._persist_provider_blood_data(
                    db,
                    individual_report=assessment_report,
                    assessment_instance=assessment_instance,
                    diagnostic_package_id=diagnostic_package_id,
                    raw_customer=raw_customer,
                )
                await self._require_audit_service().log_event(
                    db,
                    action="USER_FETCH_BLOOD_PARAMETERS_REPORT",
                    endpoint=endpoint,
                    ip_address=ip_address,
                    user_agent=user_agent,
                    user_id=user_id,
                    session_id=None,
                )
                refreshed = await self._get_blood_individual_report(
                    db,
                    user_id=user_id,
                    engagement_id=int(assessment_instance.engagement_id),
                    assessment_instance_id=assessment_id,
                )
                cached = refreshed.blood_parameters if refreshed is not None else None
                if has_usable_provider_blood_parameters(cached):
                    return await self._blood_read_service.build_from_canonical_or_legacy_provider(
                        db=db,
                        blood_parameters=cached,
                        diagnostic_package_id=diagnostic_package_id,
                        user_gender=normalized_gender,
                    )
            except AppError as exc:
                if exc.error_code not in BLOOD_DATA_UNAVAILABLE_ERROR_CODES:
                    raise
                logger.debug(
                    "Provider blood parameters unavailable for assessment %s: %s",
                    assessment_id,
                    exc.error_code,
                )

        await self._require_audit_service().log_event(
            db,
            action="USER_FETCH_BLOOD_PARAMETERS_REPORT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user_id,
            session_id=None,
        )
        return await self._blood_read_service.build_from_questionnaire_responses(
            db=db,
            assessment_instance_id=assessment_instance.assessment_instance_id,
            diagnostic_package_id=diagnostic_package_id,
            user_gender=normalized_gender,
        )

    async def get_blood_parameters_for_user_by_engagement(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        user_id: int,
        user_gender: str | None,
        user_first_name: str = "",
        user_last_name: str = "",
        load_from: str = "provider",
        reload: int = 0,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> Any:
        """Resolve blood parameters for the current user via engagement_id."""
        instances = await self._assessments_repository.list_instances_for_user_engagement(
            db,
            user_id=user_id,
            engagement_id=engagement_id,
        )
        if not instances:
            # Serve engagement-cached blood when no assessment instance exists.
            existing_report = await self._repository.get_individual_report_by_engagement(
                db,
                user_id=user_id,
                engagement_id=engagement_id,
            )
            if (
                existing_report is not None
                and has_usable_provider_blood_parameters(existing_report.blood_parameters)
            ):
                stored = self._blood_read_service.groups_from_stored(existing_report.blood_parameters)
                if stored is not None:
                    await self._require_audit_service().log_event(
                        db,
                        action="USER_FETCH_BLOOD_PARAMETERS_REPORT",
                        endpoint=endpoint,
                        ip_address=ip_address,
                        user_agent=user_agent,
                        user_id=user_id,
                        session_id=None,
                    )
                    return stored
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="No assessment or blood report found for this engagement",
            )

        # Prefer instances with a Metsights record id (provider booking link).
        assessment_instance = instances[0]
        for inst in instances:
            record_id = (inst.metsights_record_id or "").strip()
            if record_id:
                assessment_instance = inst
                break

        return await self.get_blood_parameters_for_user(
            db,
            assessment_id=int(assessment_instance.assessment_instance_id),
            user_id=user_id,
            user_gender=user_gender,
            user_first_name=user_first_name,
            user_last_name=user_last_name,
            load_from=load_from,
            reload=reload,
            ip_address=ip_address,
            user_agent=user_agent,
            endpoint=endpoint,
        )

    async def get_diagnostic_pdf_for_user(
        self,
        db: AsyncSession,
        *,
        assessment_id: int,
        user_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> DiagnosticPdfResponse:
        row = await self._assessments_repository.get_instance_for_user_with_engagement(
            db,
            assessment_instance_id=assessment_id,
            user_id=user_id,
        )
        if row is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_NOT_FOUND", message="Assessment does not exist")

        assessment_instance, _package, _engagement = row

        assessment_report = await self._get_individual_report(
            db,
            user_id=user_id,
            engagement_id=int(assessment_instance.engagement_id),
            assessment_instance_id=assessment_id,
        )
        engagement_report = await self._repository.get_individual_report_by_engagement(
            db,
            user_id=user_id,
            engagement_id=int(assessment_instance.engagement_id),
        )
        existing_report = assessment_report
        for candidate in (assessment_report, engagement_report):
            if candidate is not None and (candidate.diagnostic_report_url or "").strip():
                existing_report = candidate
                break

        cached = (existing_report.diagnostic_report_url if existing_report is not None else None) or ""
        if cached.strip():
            await self._require_audit_service().log_event(
                db,
                action="USER_FETCH_DIAGNOSTIC_PDF_REPORT",
                endpoint=endpoint,
                ip_address=ip_address,
                user_agent=user_agent,
                user_id=user_id,
                session_id=None,
            )
            return DiagnosticPdfResponse(assessment_id=assessment_id, report_url=cached.strip())

        record_id = (assessment_instance.metsights_record_id or "").strip()
        if not record_id:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Metsights record id is missing for this assessment",
            )

        resolved = await resolve_healthians_booking_id(
            db,
            user_id=user_id,
            engagement_id=int(assessment_instance.engagement_id),
            record_id=record_id,
            metsights_service=self._metsights_service,
        )

        if resolved.source == HealthiansBookingSource.PARTICIPANT:
            if self._healthians_get_access_token is None or self._healthians_get_booking_report is None:
                raise AppError(
                    status_code=503,
                    error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                    message="Healthians integration is not configured",
                )
            try:
                access_token = await self._healthians_get_access_token()
                report_data = await self._healthians_get_booking_report(
                    access_token, resolved.booking_id
                )
            except Exception as exc:
                logger.error("Healthians getBookingReport failed: %s", exc)
                raise AppError(
                    status_code=503,
                    error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                    message="Healthians booking report request failed",
                ) from exc

            user_row = await db.execute(
                select(User.first_name, User.last_name).where(User.user_id == user_id)
            )
            user_names = user_row.one_or_none()
            first_name = (user_names[0] if user_names else "") or ""
            last_name = (user_names[1] if user_names else "") or ""

            report_list = report_data.get("data")
            if not isinstance(report_list, list) or not report_list:
                raise AppError(
                    status_code=422,
                    error_code="INVALID_STATE",
                    message="Diagnostic report PDF is not available for this record",
                )
            matched_report = self._match_customer_by_name(report_list, first_name, last_name)
            if matched_report is None:
                raise AppError(
                    status_code=422,
                    error_code="INVALID_STATE",
                    message="Diagnostic report PDF is not available for this record",
                )
            report_url = matched_report.get("report_url") or matched_report.get("url")
            if not isinstance(report_url, str) or not report_url.strip():
                raise AppError(
                    status_code=422,
                    error_code="INVALID_STATE",
                    message="Diagnostic report PDF is not available for this record",
                )
            report_url = report_url.strip()
        else:
            collection_data = resolved.collection_data or {}
            file_url = collection_data.get("file")
            if not isinstance(file_url, str) or not file_url.strip():
                raise AppError(
                    status_code=422,
                    error_code="INVALID_STATE",
                    message="Diagnostic report PDF is not available for this record",
                )
            report_url = file_url.strip()

        # Prefer engagement blood/diag row; else assessment row; else create assessment row.
        target = engagement_report if engagement_report is not None else assessment_report
        if target is None:
            target = IndividualHealthReport(
                user_id=assessment_instance.user_id,
                engagement_id=assessment_instance.engagement_id,
                assessment_instance_id=assessment_instance.assessment_instance_id,
                reports=None,
                blood_parameters=None,
                diagnostic_report_url=report_url,
            )
            await self._repository.create_individual_report(db, target)
        else:
            target.diagnostic_report_url = report_url
            await self._repository.update_individual_report(db, target)

        await self._require_audit_service().log_event(
            db,
            action="USER_FETCH_DIAGNOSTIC_PDF_REPORT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user_id,
            session_id=None,
        )

        return DiagnosticPdfResponse(assessment_id=assessment_id, report_url=report_url)

    async def get_bio_ai_pdf_for_user(
        self,
        db: AsyncSession,
        *,
        assessment_id: int,
        user_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> BioAiPdfResponse:
        row = await self._assessments_repository.get_instance_for_user_with_engagement(
            db,
            assessment_instance_id=assessment_id,
            user_id=user_id,
        )
        if row is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_NOT_FOUND", message="Assessment does not exist")

        assessment_instance, package, _engagement = row
        if package is None:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Assessment package is missing",
            )

        assessment_type_code = (package.assessment_type_code or "").strip()
        if assessment_type_code == "7":
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="FitPrint assessments do not have a BioAI report; use the FitPrint fitness report instead",
            )
        if assessment_type_code not in ("1", "2"):
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="BioAI reports are only available for MetSights Basic or MetSights Pro assessments",
            )

        existing_report = await self._get_individual_report(
            db,
            user_id=user_id,
            engagement_id=int(assessment_instance.engagement_id),
            assessment_instance_id=assessment_id,
        )

        cached = (existing_report.report_url if existing_report is not None else None) or ""
        if cached.strip():
            await self._require_audit_service().log_event(
                db,
                action="USER_FETCH_BIO_AI_PDF_REPORT",
                endpoint=endpoint,
                ip_address=ip_address,
                user_agent=user_agent,
                user_id=user_id,
                session_id=None,
            )
            report_url = _BIO_AI_METSIGHTS_REPORT_URL_OVERRIDES.get(cached.strip(), cached.strip())
            return BioAiPdfResponse(assessment_id=assessment_id, report_url=report_url)

        record_id = (assessment_instance.metsights_record_id or "").strip()
        if not record_id:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Metsights record id is missing for this assessment",
            )

        pdf_payload = await self._metsights_service.get_report_pdf(
            record_id=record_id,
            assessment_type_code=package.assessment_type_code,
        )
        pdf_dict = pdf_payload if isinstance(pdf_payload, dict) else {}
        file_url = pdf_dict.get("file")
        if not isinstance(file_url, str) or not file_url.strip():
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Metsights did not return a PDF URL for this record",
            )
        report_url = _BIO_AI_METSIGHTS_REPORT_URL_OVERRIDES.get(file_url.strip(), file_url.strip())

        if existing_report is None:
            report = IndividualHealthReport(
                user_id=assessment_instance.user_id,
                engagement_id=assessment_instance.engagement_id,
                assessment_instance_id=assessment_instance.assessment_instance_id,
                reports=None,
                blood_parameters=None,
                report_url=report_url,
            )
            await self._repository.create_individual_report(db, report)
        else:
            existing_report.report_url = report_url
            await self._repository.update_individual_report(db, existing_report)

        await self._require_audit_service().log_event(
            db,
            action="USER_FETCH_BIO_AI_PDF_REPORT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user_id,
            session_id=None,
        )

        return BioAiPdfResponse(assessment_id=assessment_id, report_url=report_url)

    @staticmethod
    def _top_healthy_profile_group_names(
        groups: list[BloodParameterGroupInReportResponse],
        *,
        limit: int = 3,
    ) -> list[str]:
        """Names of test groups with the most in-range parameters, up to ``limit``."""
        scored: list[tuple[int, str]] = []
        for group in groups:
            optimal = 0
            for test in group.tests:
                if test.value is None or test.lower_range is None or test.higher_range is None:
                    continue
                if test.lower_range <= test.value <= test.higher_range:
                    optimal += 1
            if optimal > 0:
                scored.append((optimal, group.group_name))
        scored.sort(key=lambda x: (-x[0], x[1]))
        return [name for _, name in scored[:limit]]

    @staticmethod
    def _top_low_risk_from_report_dict(
        report_dict: dict[str, Any],
        *,
        limit: int = 3,
    ) -> list[DiseaseOverview]:
        """Healthy diseases from a Metsights report, lowest risk_score_scaled first."""
        diseases_raw = extract_diseases(report_dict)
        positive_wins_list: list[DiseaseOverview] = []
        for d in diseases_raw:
            if not isinstance(d, dict):
                continue
            code = str(d.get("code") or "")
            name = str(d.get("name") or "")
            rs = d.get("risk_status")
            risk_status = str(rs) if rs is not None else ""
            rsc = d.get("risk_score_scaled")
            try:
                risk_score_scaled = int(rsc) if rsc is not None else 0
            except (TypeError, ValueError):
                risk_score_scaled = 0
            if risk_status == "Healthy":
                positive_wins_list.append(
                    DiseaseOverview(
                        code=code,
                        name=name,
                        risk_status=risk_status,
                        risk_score_scaled=risk_score_scaled,
                    )
                )
        positive_wins_list.sort(key=lambda x: (x.risk_score_scaled, x.code))
        return positive_wins_list[:limit]

    async def _resolve_report_dict_for_instance(
        self,
        db: AsyncSession,
        *,
        assessment_instance: AssessmentInstance,
        package: AssessmentPackage,
        individual_report: IndividualHealthReport | None,
        cache_on_fetch: bool = True,
    ) -> dict[str, Any]:
        """Return Metsights report JSON for an assessment, optionally persisting a fetch."""
        if individual_report is not None and individual_report.reports is not None:
            report_data = individual_report.reports
            # Non-empty dict only — empty {} must not block a Metsights refresh.
            # Reject wrong-type cache (e.g. FitPrint JSON on a Pro assessment row).
            if (
                isinstance(report_data, dict)
                and report_data
                and self._reports_match_assessment_type(report_data, package.assessment_type_code)
            ):
                return report_data

        record_id = (assessment_instance.metsights_record_id or "").strip()
        if not record_id:
            return {}

        report_data = await self._metsights_service.get_report(
            record_id=record_id,
            assessment_type_code=package.assessment_type_code,
        )
        report_dict = report_data if isinstance(report_data, dict) else {}

        if cache_on_fetch:
            if individual_report is None:
                individual_report = IndividualHealthReport(
                    user_id=assessment_instance.user_id,
                    engagement_id=assessment_instance.engagement_id,
                    assessment_instance_id=assessment_instance.assessment_instance_id,
                    reports=report_data,
                    blood_parameters=None,
                )
                await self._repository.create_individual_report(db, individual_report)
            else:
                individual_report.reports = report_data
                # Ensure row is pinned to this assessment (corrupt shared-row recovery).
                individual_report.assessment_instance_id = (
                    assessment_instance.assessment_instance_id
                )
                await self._repository.update_individual_report(db, individual_report)

        return report_dict

    async def compute_low_risk_for_instance(
        self,
        db: AsyncSession,
        *,
        assessment_instance: AssessmentInstance,
        package: AssessmentPackage | None,
        individual_report: IndividualHealthReport | None,
    ) -> list[DiseaseOverview]:
        """Overview-style low_risk diseases for one assessment; empty when unavailable."""
        if package is None:
            return []
        if (package.assessment_type_code or "").strip() == "7":
            return []
        try:
            report_dict = await self._resolve_report_dict_for_instance(
                db,
                assessment_instance=assessment_instance,
                package=package,
                individual_report=individual_report,
                cache_on_fetch=True,
            )
        except AppError as exc:
            logger.debug(
                "Skipping low_risk for assessment %s: %s",
                assessment_instance.assessment_instance_id,
                exc.error_code,
            )
            return []
        return self._top_low_risk_from_report_dict(report_dict)

    async def _resolve_blood_parameter_groups_for_overview(
        self,
        db: AsyncSession,
        *,
        assessment_instance: AssessmentInstance,
        individual_report: IndividualHealthReport,
        user_first_name: str = "",
        user_last_name: str = "",
        user_gender: str | None = None,
        diagnostic_package_id: int,
    ) -> list[BloodParameterGroupInReportResponse]:
        if has_usable_provider_blood_parameters(individual_report.blood_parameters):
            return await self._blood_read_service.build_from_canonical_or_legacy_provider(
                db=db,
                blood_parameters=individual_report.blood_parameters,
                diagnostic_package_id=diagnostic_package_id,
                user_gender=user_gender,
            )

        record_id = (assessment_instance.metsights_record_id or "").strip()
        if record_id and (
            self._healthians_get_access_token is not None
            and self._healthians_get_booking_digital_value is not None
        ):
            try:
                raw_customer = await self._fetch_blood_parameters_from_provider(
                    db,
                    user_id=int(assessment_instance.user_id),
                    engagement_id=int(assessment_instance.engagement_id),
                    record_id=record_id,
                    user_first_name=user_first_name,
                    user_last_name=user_last_name,
                )
                await self._persist_provider_blood_data(
                    db,
                    individual_report=individual_report,
                    assessment_instance=assessment_instance,
                    diagnostic_package_id=diagnostic_package_id,
                    raw_customer=raw_customer,
                )
                refreshed = await self._get_blood_individual_report(
                    db,
                    user_id=int(assessment_instance.user_id),
                    engagement_id=int(assessment_instance.engagement_id),
                    assessment_instance_id=int(assessment_instance.assessment_instance_id),
                )
                if refreshed is not None and has_usable_provider_blood_parameters(
                    refreshed.blood_parameters
                ):
                    return await self._blood_read_service.build_from_canonical_or_legacy_provider(
                        db=db,
                        blood_parameters=refreshed.blood_parameters,
                        diagnostic_package_id=diagnostic_package_id,
                        user_gender=user_gender,
                    )
            except AppError as exc:
                if exc.error_code not in BLOOD_DATA_UNAVAILABLE_ERROR_CODES:
                    raise
                logger.debug(
                    "Provider blood parameters unavailable for record %s: %s",
                    record_id,
                    exc.error_code,
                )

        return await self._blood_read_service.build_from_questionnaire_responses(
            db=db,
            assessment_instance_id=assessment_instance.assessment_instance_id,
            diagnostic_package_id=diagnostic_package_id,
            user_gender=user_gender,
        )

    async def compute_healthy_habits_and_profiles_for_instance(
        self,
        db: AsyncSession,
        *,
        assessment_instance: AssessmentInstance,
        package: AssessmentPackage | None,
        engagement: Engagement | None,
        individual_report: IndividualHealthReport | None,
        user_gender: str | None,
    ) -> tuple[list[HealthyHabitItem], list[str]]:
        """Healthy habits and profiles for one assessment (overview positive_wins subset)."""
        healthy_profiles: list[str] = []
        if (
            engagement is not None
            and engagement.diagnostic_package_id is not None
            and individual_report is not None
        ):
            try:
                normalized_gender = (user_gender or "").strip().lower() or None
                if normalized_gender not in {"male", "female"}:
                    normalized_gender = None
                user_first_name = ""
                user_last_name = ""
                if not has_usable_provider_blood_parameters(individual_report.blood_parameters):
                    user_row = await db.get(User, assessment_instance.user_id)
                    if user_row is not None:
                        user_first_name = user_row.first_name or ""
                        user_last_name = user_row.last_name or ""
                groups = await self._resolve_blood_parameter_groups_for_overview(
                    db,
                    assessment_instance=assessment_instance,
                    individual_report=individual_report,
                    user_first_name=user_first_name,
                    user_last_name=user_last_name,
                    user_gender=normalized_gender,
                    diagnostic_package_id=int(engagement.diagnostic_package_id),
                )
                healthy_profiles = self._top_healthy_profile_group_names(groups)
            except AppError as exc:
                if exc.error_code not in BLOOD_DATA_UNAVAILABLE_ERROR_CODES:
                    raise
                logger.debug(
                    "Skipping healthy profiles for assessment %s: %s",
                    assessment_instance.assessment_instance_id,
                    exc.error_code,
                )

        healthy_habits: list[HealthyHabitItem] = []
        if self._healthy_habits_service is not None and package is not None:
            computed = await self._healthy_habits_service.top_habits_for_assessment(
                db,
                assessment_instance_id=int(assessment_instance.assessment_instance_id),
                package_id=int(assessment_instance.package_id),
                limit=3,
            )
            healthy_habits = [
                HealthyHabitItem(habit_key=h.habit_key, habit_label=h.habit_label) for h in computed
            ]

        return healthy_habits, healthy_profiles

    async def get_overview_for_user(
        self,
        db: AsyncSession,
        *,
        assessment_id: int,
        user_id: int,
        user_gender: str | None,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> OverviewReportResponse:
        row = await self._assessments_repository.get_instance_for_user_with_engagement(
            db,
            assessment_instance_id=assessment_id,
            user_id=user_id,
        )
        if row is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_NOT_FOUND", message="Assessment does not exist")

        assessment_instance, package, engagement = row
        if package is None:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Assessment package is missing",
            )

        assessment_type_code = (package.assessment_type_code or "").strip()
        if assessment_type_code == "7":
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="FitPrint report overview is not allowed",
            )

        individual_report = await self._get_individual_report(
            db,
            user_id=user_id,
            engagement_id=int(assessment_instance.engagement_id),
            assessment_instance_id=assessment_id,
        )

        record_id = (assessment_instance.metsights_record_id or "").strip()
        cached_reports = (
            individual_report.reports
            if individual_report is not None and isinstance(individual_report.reports, dict)
            else None
        )
        reports_usable = bool(
            cached_reports
            and self._reports_match_assessment_type(cached_reports, assessment_type_code)
        )
        if not reports_usable and not record_id:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Metsights record id is missing for this assessment",
            )

        report_dict = await self._resolve_report_dict_for_instance(
            db,
            assessment_instance=assessment_instance,
            package=package,
            individual_report=individual_report,
            cache_on_fetch=True,
        )
        if individual_report is None:
            individual_report = await self._get_individual_report(
                db,
                user_id=user_id,
                engagement_id=int(assessment_instance.engagement_id),
                assessment_instance_id=assessment_id,
            )

        blood_report = await self._get_blood_individual_report(
            db,
            user_id=user_id,
            engagement_id=int(assessment_instance.engagement_id),
            assessment_instance_id=assessment_id,
        )
        profile_report = blood_report if blood_report is not None else individual_report

        metabolic_age = extract_metabolic_age(report_dict)
        if assessment_id in _OVERVIEW_METABOLIC_AGE_OVERRIDES:
            metabolic_age = _OVERVIEW_METABOLIC_AGE_OVERRIDES[assessment_id]
        diseases_raw = extract_diseases(report_dict)

        positive_wins_list = self._top_low_risk_from_report_dict(report_dict)
        risk_analysis_list: list[RiskAnalysisItem] = []
        for d in diseases_raw:
            if not isinstance(d, dict):
                continue
            code = str(d.get("code") or "")
            name = str(d.get("name") or "")
            rs = d.get("risk_status")
            risk_status = str(rs) if rs is not None else ""
            rsc = d.get("risk_score_scaled")
            try:
                risk_score_scaled = int(rsc) if rsc is not None else 0
            except (TypeError, ValueError):
                risk_score_scaled = 0
            hp = d.get("healthy_percentile")
            try:
                healthy_percentile = int(hp) if hp is not None else 0
            except (TypeError, ValueError):
                healthy_percentile = 0
            risk_analysis_list.append(
                RiskAnalysisItem(
                    code=code,
                    name=name,
                    risk_status=risk_status,
                    risk_score_scaled=risk_score_scaled,
                    healthy_percentile=healthy_percentile,
                )
            )

        risk_analysis_list.sort(key=lambda x: (-x.risk_score_scaled, x.code))
        risk_analysis_list = risk_analysis_list[:3]

        healthy_habits, healthy_profiles = await self.compute_healthy_habits_and_profiles_for_instance(
            db,
            assessment_instance=assessment_instance,
            package=package,
            engagement=engagement,
            individual_report=profile_report,
            user_gender=user_gender,
        )

        await self._require_audit_service().log_event(
            db,
            action="USER_FETCH_OVERVIEW_REPORT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user_id,
            session_id=None,
        )

        return OverviewReportResponse(
            assessment_id=assessment_id,
            metabolic_age=metabolic_age,
            positive_wins=PositiveWins(
                low_risk=positive_wins_list,
                healthy_habits=healthy_habits,
                healthy_profiles=healthy_profiles,
            ),
            risk_analysis=risk_analysis_list,
        )

    async def _get_or_fetch_report(
        self,
        db: AsyncSession,
        *,
        assessment_id: int,
        user_id: int,
    ) -> Any:
        row = await self._assessments_repository.get_instance_for_user(
            db,
            assessment_instance_id=assessment_id,
            user_id=user_id,
        )
        if row is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_NOT_FOUND", message="Assessment does not exist")

        assessment_instance, package = row
        if package is None:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Assessment package is missing",
            )

        existing_report = await self._get_individual_report(
            db,
            user_id=user_id,
            engagement_id=int(assessment_instance.engagement_id),
            assessment_instance_id=assessment_id,
        )

        if (
            existing_report is not None
            and isinstance(existing_report.reports, dict)
            and existing_report.reports
            and self._reports_match_assessment_type(
                existing_report.reports, package.assessment_type_code
            )
        ):
            return existing_report.reports

        record_id = (assessment_instance.metsights_record_id or "").strip()
        if not record_id:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Metsights record id is missing for this assessment",
            )

        report_data = await self._metsights_service.get_report(
            record_id=record_id,
            assessment_type_code=package.assessment_type_code,
        )

        if existing_report is None:
            report = IndividualHealthReport(
                user_id=assessment_instance.user_id,
                engagement_id=assessment_instance.engagement_id,
                assessment_instance_id=assessment_instance.assessment_instance_id,
                reports=report_data,
                blood_parameters=None,
            )
            await self._repository.create_individual_report(db, report)
        else:
            existing_report.reports = report_data
            existing_report.assessment_instance_id = assessment_instance.assessment_instance_id
            await self._repository.update_individual_report(db, existing_report)

        return report_data

    async def _get_or_fetch_report_optional(
        self,
        db: AsyncSession,
        *,
        assessment_id: int,
        user_id: int,
    ) -> Any | None:
        """Return cached/fetched Metsights report, or None if unavailable (trends skip)."""
        try:
            return await self._get_or_fetch_report(
                db,
                assessment_id=assessment_id,
                user_id=user_id,
            )
        except AppError as exc:
            if exc.error_code in {"REPORT_NOT_FOUND", "INVALID_STATE"}:
                return None
            raise

    async def get_risk_analysis_for_user(
        self,
        db: AsyncSession,
        *,
        assessment_id: int,
        user_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> RiskAnalysisListResponse:
        report_data = await self._get_or_fetch_report(
            db,
            assessment_id=assessment_id,
            user_id=user_id,
        )

        report_dict = report_data if isinstance(report_data, dict) else {}
        metabolic_score = extract_metabolic_score(report_dict)

        diseases_raw = extract_diseases(report_dict)
        diseases: list[DiseaseListItem] = []
        for d in diseases_raw:
            if not isinstance(d, dict):
                continue
            code = str(d.get("code") or "")
            name = str(d.get("name") or "")
            rsc = d.get("risk_score_scaled")
            try:
                risk_score_scaled = int(rsc) if rsc is not None else 0
            except (TypeError, ValueError):
                risk_score_scaled = 0
            diseases.append(
                DiseaseListItem(
                    code=code,
                    name=name,
                    risk_score_scaled=risk_score_scaled,
                )
            )

        await self._require_audit_service().log_event(
            db,
            action="USER_FETCH_RISK_ANALYSIS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user_id,
            session_id=None,
        )

        return RiskAnalysisListResponse(
            assessment_id=assessment_id,
            metabolic_score=metabolic_score,
            diseases=diseases,
        )

    async def get_disease_detail_for_user(
        self,
        db: AsyncSession,
        *,
        assessment_id: int,
        user_id: int,
        disease_code: str,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> DiseaseDetailResponse:
        report_data = await self._get_or_fetch_report(
            db,
            assessment_id=assessment_id,
            user_id=user_id,
        )

        report_dict = report_data if isinstance(report_data, dict) else {}
        diseases_raw = extract_diseases(report_dict)

        matched: dict[str, Any] | None = None
        for d in diseases_raw:
            if isinstance(d, dict) and str(d.get("code") or "") == disease_code:
                matched = d
                break

        if matched is None:
            raise AppError(
                status_code=404,
                error_code="DISEASE_NOT_FOUND",
                message=f"Disease '{disease_code}' not found in this report",
            )

        rsc = matched.get("risk_score_scaled")
        try:
            risk_score_scaled = int(rsc) if rsc is not None else 0
        except (TypeError, ValueError):
            risk_score_scaled = 0

        lc = matched.get("lifestyle_contribution")
        if lc is None:
            lifestyle_contribution: int | None = None
        else:
            try:
                lifestyle_contribution = int(lc)
            except (TypeError, ValueError):
                lifestyle_contribution = None

        dp = matched.get("disease_percentile")
        if dp is None:
            disease_percentile: int | None = None
        else:
            try:
                disease_percentile = int(dp)
            except (TypeError, ValueError):
                disease_percentile = None

        await self._require_audit_service().log_event(
            db,
            action="USER_FETCH_DISEASE_DETAIL",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user_id,
            session_id=None,
        )

        hp = await self._diagnostics_service.get_health_parameter_by_parameter_key(
            db, parameter_key=disease_code
        )

        return DiseaseDetailResponse(
            code=str(matched.get("code") or ""),
            name=str(matched.get("name") or ""),
            meaning=hp.meaning if hp is not None else None,
            unit=hp.unit if hp is not None else None,
            risk_score_scaled=risk_score_scaled,
            lifestyle_contribution=lifestyle_contribution,
            disease_percentile=disease_percentile,
            low_risk_lower_range_male=hp.low_risk_lower_range_male if hp is not None else None,
            low_risk_higher_range_male=hp.low_risk_higher_range_male if hp is not None else None,
            moderate_risk_lower_range_male=hp.moderate_risk_lower_range_male if hp is not None else None,
            moderate_risk_higher_range_male=hp.moderate_risk_higher_range_male if hp is not None else None,
            high_risk_lower_range_male=hp.high_risk_lower_range_male if hp is not None else None,
            high_risk_higher_range_male=hp.high_risk_higher_range_male if hp is not None else None,
            low_risk_lower_range_female=hp.low_risk_lower_range_female if hp is not None else None,
            low_risk_higher_range_female=hp.low_risk_higher_range_female if hp is not None else None,
            moderate_risk_lower_range_female=hp.moderate_risk_lower_range_female if hp is not None else None,
            moderate_risk_higher_range_female=hp.moderate_risk_higher_range_female if hp is not None else None,
            high_risk_lower_range_female=hp.high_risk_lower_range_female if hp is not None else None,
            high_risk_higher_range_female=hp.high_risk_higher_range_female if hp is not None else None,
            causes_when_high=hp.causes_when_high if hp is not None else None,
            causes_when_low=hp.causes_when_low if hp is not None else None,
            effects_when_high=hp.effects_when_high if hp is not None else None,
            effects_when_low=hp.effects_when_low if hp is not None else None,
            what_to_do_when_low=hp.what_to_do_when_low if hp is not None else None,
            what_to_do_when_high=hp.what_to_do_when_high if hp is not None else None,
        )

    # ------------------------------------------------------------------
    # Health Span Index
    # ------------------------------------------------------------------

    _NUTRITION_API_QUESTION_KEYS = [
        "exercise_frequency_week",
        "exercise_level",
        "healthy_breakfast_frequency",
        "diet_preference",
        "food_groups",
        "fresh_fruit_frequency",
        "fresh_vegetable_frequency",
        "baked_goods_frequency",
        "red_meat_frequency",
        "butter_dish_frequency",
        "dessert_frequency",
        "caffeine_frequency",
        "water_intake_frequency",
        "tobacco_frequency",
        "alcohol_frequency",
        "sickness_frequency",
    ]

    async def _call_nutrition_api(
        self,
        db: AsyncSession,
        payload: dict[str, Any],
        *,
        user_id: int | None = None,
        engagement_id: int | None = None,
    ) -> dict[str, Any]:
        audit_repo = AuditRepository()
        sync_log = await audit_repo.create_sync_log(
            db,
            IntegrationSyncLog(
                engagement_id=engagement_id,
                user_id=user_id,
                provider="nutrition_api",
                api_endpoint_url=settings.NUTRITION_API_URL,
                request_payload=payload,
                status="pending",
            ),
        )

        try:
            async with httpx.AsyncClient(timeout=settings.NUTRITION_API_TIMEOUT_SECONDS) as client:
                response = await client.post(
                    settings.NUTRITION_API_URL,
                    json=payload,
                    headers={"X-API-Key": settings.NUTRITION_API_KEY},
                )
                response.raise_for_status()
                data = response.json()
                response_payload = data if isinstance(data, dict) else {}
                await audit_repo.update_sync_log_status(
                    db,
                    sync_log_id=sync_log.sync_log_id,
                    status="success",
                    response_payload=response_payload,
                )
                return response_payload
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            error_message = f"HTTP {status}"
            if 400 <= status < 500:
                detail: str | None = None
                try:
                    body = exc.response.json()
                    if isinstance(body, dict):
                        raw_detail = body.get("detail")
                        if isinstance(raw_detail, str):
                            detail = raw_detail
                        elif isinstance(raw_detail, list):
                            # FastAPI-style validation error array from nutrition API.
                            detail = str(raw_detail)[:500]
                except Exception:
                    detail = None
                error_message = detail or "Nutrition API rejected request payload"
                await audit_repo.update_sync_log_status(
                    db,
                    sync_log_id=sync_log.sync_log_id,
                    status="failed",
                    error_message=error_message,
                )
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message=error_message,
                ) from exc
            await audit_repo.update_sync_log_status(
                db,
                sync_log_id=sync_log.sync_log_id,
                status="failed",
                error_message=error_message,
            )
            raise AppError(
                status_code=503,
                error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                message="Nutrition API request failed",
            ) from exc
        except httpx.HTTPError as exc:
            await audit_repo.update_sync_log_status(
                db,
                sync_log_id=sync_log.sync_log_id,
                status="failed",
                error_message=str(exc),
            )
            raise AppError(
                status_code=503,
                error_code="EXTERNAL_SERVICE_UNAVAILABLE",
                message="Nutrition API request failed",
            ) from exc

    async def _build_questionnaire_lookup(
        self,
        db: AsyncSession,
        *,
        source_assessment_instance_ids: list[int],
    ) -> tuple[dict[str, Any], dict[str, int]]:
        """Build a question_key -> answer dict from questionnaire responses across multiple instances.

        Returns (lookup, key_to_question_id) where key_to_question_id maps question_key -> question_id
        for option label resolution. Later instance ids override earlier ones for the same question_key.
        """
        if self._questionnaire_repository is None:
            raise RuntimeError("QuestionnaireRepository is required for health span index")

        responses = await self._questionnaire_repository.list_responses_for_instances(
            db,
            assessment_instance_ids=source_assessment_instance_ids,
        )
        if not responses:
            return {}, {}

        question_ids = list({int(r.question_id) for r in responses})
        definitions = await self._questionnaire_repository.get_definitions_by_ids(
            db, question_ids=question_ids,
        )

        lookup: dict[str, Any] = {}
        key_to_question_id: dict[str, int] = {}
        for r in responses:
            defn = definitions.get(int(r.question_id))
            if defn is None or not defn.question_key:
                continue
            lookup[defn.question_key] = r.answer
            key_to_question_id[defn.question_key] = int(defn.question_id)
        return lookup, key_to_question_id

    @staticmethod
    def _normalize_gender(value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip().lower()
        if normalized in {"m", "male"}:
            return "male"
        if normalized in {"f", "female"}:
            return "female"
        return normalized or None

    @staticmethod
    def _extract_scale_answer(answer: Any) -> tuple[float | int | None, str | None]:
        """Extract numeric scale value and unit from questionnaire answer."""
        if not isinstance(answer, dict):
            return None, None
        raw_value = answer.get("value")
        raw_unit = answer.get("unit")
        if raw_value is None:
            return None, str(raw_unit).strip() if raw_unit is not None and str(raw_unit).strip() else None
        if isinstance(raw_value, bool):
            return None, None
        parsed_value: float | int | None = None
        try:
            parsed_numeric = float(raw_value)
            parsed_value = int(parsed_numeric) if parsed_numeric.is_integer() else parsed_numeric
        except (TypeError, ValueError):
            parsed_value = None
        parsed_unit = str(raw_unit).strip() if raw_unit is not None and str(raw_unit).strip() else None
        return parsed_value, parsed_unit

    @staticmethod
    def _normalize_height_unit(value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().lower()
        if normalized in {"0", "cm"}:
            return "cm"
        if normalized in {"2", "ft/in", "ft", "feet"}:
            return "ft/in"
        return value.strip() or None

    async def _build_option_reverse_map(
        self,
        db: AsyncSession,
        key_to_question_id: dict[str, int],
    ) -> dict[str, dict[str, str]]:
        """Build {question_key: {display_name: option_value}} for answer resolution."""
        if self._questionnaire_repository is None or not key_to_question_id:
            return {}

        qid_to_key: dict[int, str] = {qid: qkey for qkey, qid in key_to_question_id.items()}
        all_options = await self._questionnaire_repository.list_options_for_question_ids(
            db, question_ids=list(qid_to_key.keys()),
        )

        reverse_map: dict[str, dict[str, str]] = {}
        for opt in all_options:
            qkey = qid_to_key.get(int(opt.question_id))
            if qkey is None:
                continue
            if qkey not in reverse_map:
                reverse_map[qkey] = {}
            reverse_map[qkey][opt.display_name] = opt.option_value
        return reverse_map

    def _build_nutrition_api_payload(self, lookup: dict[str, Any], *, user_gender: str | None = None, option_reverse_map: dict[str, dict[str, str]] | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        for key in self._NUTRITION_API_QUESTION_KEYS:
            val = lookup.get(key)
            if val is None:
                continue
            key_map = (option_reverse_map or {}).get(key, {})
            if isinstance(val, list):
                payload[key] = [key_map.get(str(v), str(v)) for v in val]
            else:
                payload[key] = key_map.get(str(val), str(val))

        # New nutrition API contract requires these identity/anthropometry fields.
        resolved_gender = self._normalize_gender(lookup.get("gender")) or self._normalize_gender(user_gender)
        if resolved_gender is not None:
            payload["gender"] = resolved_gender

        height_value, height_unit = self._extract_scale_answer(lookup.get("height"))
        if height_value is not None:
            payload["height"] = height_value
        normalized_height_unit = self._normalize_height_unit(height_unit)
        if normalized_height_unit is not None:
            payload["height_unit"] = normalized_height_unit

        return payload

    @staticmethod
    def _extract_questionnaire_value(lookup: dict[str, Any], key: str) -> str | None:
        val = lookup.get(key)
        if val is None:
            return None
        if isinstance(val, dict):
            raw = val.get("value")
            return str(raw) if raw is not None else None
        if isinstance(val, list):
            return ", ".join(str(v) for v in val)
        return str(val)

    async def _resolve_option_label(
        self,
        db: AsyncSession,
        *,
        lookup: dict[str, Any],
        key_to_question_id: dict[str, int],
        question_key: str,
    ) -> str | None:
        """Resolve a single_choice answer's option_value to its display_name."""
        val = lookup.get(question_key)
        if val is None:
            return None
        qid = key_to_question_id.get(question_key)
        if qid is None or self._questionnaire_repository is None:
            return str(val)
        options = await self._questionnaire_repository.list_options_for_question(db, question_id=qid)
        option_map = {opt.option_value: opt.display_name for opt in options}
        if isinstance(val, list):
            labels = [option_map.get(str(v), str(v)) for v in val]
            return ", ".join(labels)
        return option_map.get(str(val), str(val))

    @staticmethod
    def _extract_fitprint_parameter(
        params_list: Any,
        parameter_name: str | tuple[str, ...],
    ) -> FitPrintParameter | None:
        """Extract a full parameter object from a FitPrint report parameters list."""
        if not isinstance(params_list, list):
            return None
        names = (parameter_name,) if isinstance(parameter_name, str) else parameter_name
        for name in names:
            for p in params_list:
                if not isinstance(p, dict):
                    continue
                if p.get("parameter") != name:
                    continue
                raw_val = p.get("value")
                value = float(raw_val) if isinstance(raw_val, (int, float)) else None
                healthy_range_str = p.get("healthy_range")
                healthy_range: FitPrintParameterRange | None = None
                if isinstance(healthy_range_str, str) and "–" in healthy_range_str:
                    parts = healthy_range_str.split("–")
                    try:
                        healthy_range = FitPrintParameterRange(
                            min=float(parts[0].strip()),
                            max=float(parts[1].strip()),
                        )
                    except (ValueError, IndexError):
                        pass
                return FitPrintParameter(
                    parameter=p.get("parameter"),
                    code=p.get("code"),
                    value=value,
                    unit=p.get("unit"),
                    healthy_range=healthy_range,
                    status=p.get("status"),
                )
        return None

    async def get_health_span_index(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
        user_id: int,
        user_gender: str | None,
        source_assessment_instance_ids: list[int],
        include_details: bool,
    ) -> HealthSpanIndexResponse:
        # Step 1: Validate that the assessment is FitPrint (type_code "7")
        row = await self._assessments_repository.get_instance_for_user(
            db,
            assessment_instance_id=assessment_instance_id,
            user_id=user_id,
        )
        if row is None:
            raise AppError(
                status_code=404,
                error_code="ASSESSMENT_NOT_FOUND",
                message="Assessment does not exist",
            )

        assessment_instance, package = row
        if package is None:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Assessment package is missing",
            )

        assessment_type_code = (package.assessment_type_code or "").strip()
        if assessment_type_code != "7":
            raise AppError(
                status_code=400,
                error_code="INVALID_ASSESSMENT_TYPE",
                message="Health Span Index is only available for FitPrint assessments",
            )

        # Step 2: Get or fetch the FitPrint report (cache in individual_health_report)
        existing_report = await self._get_individual_report(
            db,
            user_id=user_id,
            engagement_id=int(assessment_instance.engagement_id),
            assessment_instance_id=assessment_instance_id,
        )

        cached = existing_report.reports if existing_report is not None else None
        if (
            isinstance(cached, dict)
            and cached
            and self._reports_match_assessment_type(cached, "7")
        ):
            report_data: Any = cached
        else:
            record_id = (assessment_instance.metsights_record_id or "").strip()
            if not record_id:
                raise AppError(
                    status_code=422,
                    error_code="INVALID_STATE",
                    message="Metsights record id is missing for this assessment",
                )

            report_data = await self._metsights_service.get_report(
                record_id=record_id,
                assessment_type_code="7",
            )

            if existing_report is None:
                existing_report = IndividualHealthReport(
                    user_id=assessment_instance.user_id,
                    engagement_id=assessment_instance.engagement_id,
                    assessment_instance_id=assessment_instance.assessment_instance_id,
                    reports=report_data,
                    blood_parameters=None,
                )
                await self._repository.create_individual_report(db, existing_report)
            else:
                existing_report.reports = report_data
                existing_report.assessment_instance_id = (
                    assessment_instance.assessment_instance_id
                )
                await self._repository.update_individual_report(db, existing_report)

        report_dict = report_data if isinstance(report_data, dict) else {}

        # Step 3: Extract scores from the FitPrint report
        fitness_spec = report_dict.get("fitness_specification") or {}
        activity_spec = report_dict.get("activity_specification") or {}

        raw_lifestyle = fitness_spec.get("score") if isinstance(fitness_spec, dict) else None
        lifestyle_score = float(raw_lifestyle) if isinstance(raw_lifestyle, (int, float)) else None

        raw_fitness = activity_spec.get("score") if isinstance(activity_spec, dict) else None
        fitness_score = float(raw_fitness) if isinstance(raw_fitness, (int, float)) else None

        # Step 4: Load questionnaire responses
        lookup, key_to_question_id = await self._build_questionnaire_lookup(
            db,
            source_assessment_instance_ids=source_assessment_instance_ids,
        )

        # Step 5: Call the nutrition API
        option_reverse_map = await self._build_option_reverse_map(db, key_to_question_id)
        nutrition_payload = self._build_nutrition_api_payload(lookup, user_gender=user_gender, option_reverse_map=option_reverse_map)
        nutrition_response = await self._call_nutrition_api(
            db,
            nutrition_payload,
            user_id=user_id,
            engagement_id=assessment_instance.engagement_id,
        )
        nutrition_score_raw = nutrition_response.get("nutrition_score")
        nutrition_score = float(nutrition_score_raw) if isinstance(nutrition_score_raw, (int, float)) else None

        # Step 6: Build the response
        if not include_details:
            return HealthSpanIndexResponse(
                lifestyle_score=lifestyle_score,
                nutrition_score=nutrition_score,
                fitness_score=fitness_score,
            )

        # Fitness details
        systolic_blood_pressure = self._extract_questionnaire_value(lookup, "systolic_blood_pressure")
        diastolic_blood_pressure = self._extract_questionnaire_value(lookup, "diastolic_blood_pressure")
        waist = self._extract_questionnaire_value(lookup, "waist_circumference")

        activity_params = activity_spec.get("parameters") if isinstance(activity_spec, dict) else None
        bmr_param = self._extract_fitprint_parameter(activity_params, "Basal Metabolic Rate")

        fitness_params = fitness_spec.get("parameters") if isinstance(fitness_spec, dict) else None
        body_fat_param = self._extract_fitprint_parameter(
            fitness_params, ("Estimated Body Fat", "Body Fat")
        )

        def _ideal_range(key: str) -> IdealRangeDetail | None:
            raw = nutrition_response.get(key)
            if not isinstance(raw, dict):
                return None
            return IdealRangeDetail(
                low=raw.get("low"),
                high=raw.get("high"),
                unit=raw.get("unit"),
            )

        fitness_detail = HealthSpanFitnessDetail(
            systolic_blood_pressure=systolic_blood_pressure,
            diastolic_blood_pressure=diastolic_blood_pressure,
            basal_metabolic_rate=bmr_param,
            waist=waist,
            estimated_body_fat=body_fat_param,
            ideal_waist=_ideal_range("ideal_waist"),
            ideal_bmr=_ideal_range("ideal_bmr"),
            ideal_body_fat=_ideal_range("ideal_body_fat"),
        )

        # Nutrition details
        def _nutrient(key: str) -> NutrientDetail | None:
            raw = nutrition_response.get(key)
            if not isinstance(raw, dict):
                return None
            return NutrientDetail(
                estimated_low=raw.get("estimated_low"),
                estimated_high=raw.get("estimated_high"),
                ideal_low=raw.get("ideal_low"),
                ideal_high=raw.get("ideal_high"),
                status=raw.get("status"),
            )

        water_raw = nutrition_response.get("water")
        water_detail: WaterDetail | None = None
        if isinstance(water_raw, dict):
            water_detail = WaterDetail(
                estimated_litres=water_raw.get("estimated_litres"),
                ideal_low_litres=water_raw.get("ideal_low_litres"),
                ideal_high_litres=water_raw.get("ideal_high_litres"),
                status=water_raw.get("status"),
            )

        nutrition_detail = HealthSpanNutritionDetail(
            carbs=_nutrient("carbs"),
            fats=_nutrient("fats"),
            protein=_nutrient("protein"),
            fibre=_nutrient("fibre"),
            water=water_detail,
        )

        # Lifestyle details (resolve option_value -> display_name)
        resolve = self._resolve_option_label
        lifestyle_detail = HealthSpanLifestyleDetail(
            physical_activity=await resolve(db, lookup=lookup, key_to_question_id=key_to_question_id, question_key="physical_activity_frequency"),
            smoke=await resolve(db, lookup=lookup, key_to_question_id=key_to_question_id, question_key="tobacco_frequency"),
            alcohol=await resolve(db, lookup=lookup, key_to_question_id=key_to_question_id, question_key="alcohol_frequency"),
            sleep=await resolve(db, lookup=lookup, key_to_question_id=key_to_question_id, question_key="sleeping_hours"),
            family_history=await resolve(db, lookup=lookup, key_to_question_id=key_to_question_id, question_key="family_health_history"),
        )

        return HealthSpanIndexResponse(
            lifestyle_score=lifestyle_score,
            nutrition_score=nutrition_score,
            fitness_score=fitness_score,
            fitness=fitness_detail,
            nutrition=nutrition_detail,
            lifestyle=lifestyle_detail,
        )

    async def _get_or_create_sync_state(self, db: AsyncSession, *, user_id: int) -> ReportsUserSyncState:
        state = await self._repository.get_user_sync_state(db, user_id=user_id)
        if state is not None:
            return state
        return await self._repository.create_user_sync_state(db, user_id=user_id)

    async def _build_trend_payload(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        parameter_key: str,
    ) -> dict[str, Any]:
        rows = await self._repository.list_individual_reports_for_user_with_assessment(
            db,
            user_id=user_id,
        )

        hp = await self._diagnostics_service.get_health_parameter_by_parameter_key(
            db, parameter_key=parameter_key,
        )
        external_pid: int | None = hp.external_parameter_id if hp is not None else None

        from modules.reports.blood_parameters_schemas import (
            is_grouped_blood_parameters,
            is_legacy_metsights_flat_format,
        )

        unit_key = f"{parameter_key}_unit"
        data_points: list[dict[str, Any]] = []
        unit: str | None = None
        seen_assessment_ids: set[int] = set()

        async def _append_point(
            *,
            assessment: AssessmentInstance,
            numeric_value: float,
            entry_unit: str | None,
        ) -> None:
            nonlocal unit
            if unit is None and entry_unit is not None:
                unit = entry_unit
            point_date = assessment.completed_at or assessment.assigned_at
            if point_date is None:
                return
            if isinstance(point_date, date):
                date_value = point_date.isoformat()[:10]
            else:
                date_value = str(point_date)[:10]
            data_points.append(
                {
                    "date": date_value,
                    "value": numeric_value,
                    "engagement_id": int(assessment.engagement_id),
                }
            )

        for report, assessment in rows:
            seen_assessment_ids.add(int(assessment.assessment_instance_id))
            blood_parameters = report.blood_parameters
            numeric_value: float | None = None
            entry_unit: str | None = None

            if is_grouped_blood_parameters(blood_parameters) or isinstance(blood_parameters, dict):
                numeric_value, entry_unit = await self._blood_read_service.extract_provider_parameter(
                    blood_parameters,
                    parameter_key=parameter_key,
                    external_parameter_id=external_pid,
                )
                if (
                    numeric_value is None
                    and isinstance(blood_parameters, dict)
                    and is_legacy_metsights_flat_format(blood_parameters)
                ):
                    raw_value = blood_parameters.get(parameter_key)
                    if raw_value is not None:
                        try:
                            numeric_value = float(raw_value)
                        except (TypeError, ValueError):
                            numeric_value = None
                    raw_unit = blood_parameters.get(unit_key)
                    if isinstance(raw_unit, str):
                        entry_unit = raw_unit

            if numeric_value is None:
                numeric_value, entry_unit = (
                    await self._blood_read_service._questionnaire_reader.extract_parameter_for_assessment(
                        db,
                        assessment_instance_id=int(assessment.assessment_instance_id),
                        parameter_key=parameter_key,
                    )
                )

            if numeric_value is None:
                continue
            await _append_point(
                assessment=assessment,
                numeric_value=numeric_value,
                entry_unit=entry_unit,
            )

        extra_assessments = await self._repository.list_metsights_pro_basic_assessments_for_user(
            db,
            user_id=user_id,
        )
        for assessment, _package in extra_assessments:
            aid = int(assessment.assessment_instance_id)
            if aid in seen_assessment_ids:
                continue
            numeric_value, entry_unit = (
                await self._blood_read_service._questionnaire_reader.extract_parameter_for_assessment(
                    db,
                    assessment_instance_id=aid,
                    parameter_key=parameter_key,
                )
            )
            if numeric_value is None:
                continue
            await _append_point(
                assessment=assessment,
                numeric_value=numeric_value,
                entry_unit=entry_unit,
            )

        return {
            "parameter": parameter_key,
            "unit": unit,
            "data_points": data_points,
        }

    async def _refresh_user_blood_parameters(self, *, user_id: int) -> None:
        """Import Metsights blood questionnaire categories for unsynced assessments."""
        async with self._session_factory() as db:
            state = await self._get_or_create_sync_state(db, user_id=user_id)
            try:
                if (state.sync_status or _SYNC_IDLE) != _SYNC_IN_PROGRESS:
                    state.sync_status = _SYNC_IN_PROGRESS
                    await self._repository.update_user_sync_state(db, state)
                    await db.commit()
                after_id = int(state.last_synced_assessment_instance_id or 0)
                rows = await self._repository.list_unsynced_assessments_with_record_id(
                    db,
                    user_id=user_id,
                    after_assessment_instance_id=after_id,
                )

                latest_synced_id = after_id
                for assessment in rows:
                    record_id = (assessment.metsights_record_id or "").strip()
                    if not record_id:
                        continue
                    if self._metsights_sync_service is None:
                        break

                    any_ok = False
                    for category_key in (
                        BLOOD_PARAMETER_CATEGORY_KEY,
                        ADVANCED_BLOOD_PARAMETER_CATEGORY_KEY,
                    ):
                        try:
                            await self._metsights_sync_service.import_category_from_metsights(
                                db,
                                assessment_instance_id=int(assessment.assessment_instance_id),
                                user_id=user_id,
                                category_key=category_key,
                                category_of="metsights",
                                reload=1,
                            )
                            any_ok = True
                        except AppError as exc:
                            logger.debug(
                                "Questionnaire blood import failed for assessment %s: %s",
                                assessment.assessment_instance_id,
                                exc.error_code,
                            )

                    if not any_ok:
                        # Do not advance past a failed assessment so the next run retries it.
                        break
                    latest_synced_id = max(latest_synced_id, int(assessment.assessment_instance_id))

                state.last_synced_assessment_instance_id = latest_synced_id if latest_synced_id > 0 else None
                state.sync_status = _SYNC_IDLE
                state.last_sync_error = None
                state.last_synced_at = datetime.now(timezone.utc)
                await self._repository.update_user_sync_state(db, state)
                await db.commit()
            except Exception as exc:
                await db.rollback()
                failed_state = await self._get_or_create_sync_state(db, user_id=user_id)
                failed_state.sync_status = _SYNC_FAILED
                failed_state.last_sync_error = str(exc)[:1000]
                await self._repository.update_user_sync_state(db, failed_state)
                await db.commit()

    def trigger_user_blood_parameters_refresh(self, *, user_id: int) -> None:
        asyncio.create_task(self._refresh_user_blood_parameters(user_id=user_id))

    async def get_blood_parameter_trends_for_user(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        blood_parameter: str,
    ) -> tuple[dict[str, Any], dict[str, Any], bool]:
        parameter_key = (blood_parameter or "").strip().lower()
        if not parameter_key:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        payload = await self._build_trend_payload(
            db,
            user_id=user_id,
            parameter_key=parameter_key,
        )
        state = await self._get_or_create_sync_state(db, user_id=user_id)
        latest = await self._repository.get_latest_assessment_with_record_id(db, user_id=user_id)
        latest_assessment_id = int(latest.assessment_instance_id) if latest is not None else None
        cursor = int(state.last_synced_assessment_instance_id or 0)
        stale = latest_assessment_id is not None and cursor < latest_assessment_id
        should_trigger = False
        if stale and (state.sync_status or _SYNC_IDLE) != _SYNC_IN_PROGRESS:
            state.sync_status = _SYNC_IN_PROGRESS
            state.last_sync_error = None
            await self._repository.update_user_sync_state(db, state)
            should_trigger = True

        meta = {
            "is_stale": stale,
            "sync_status": state.sync_status,
            "last_synced_at": state.last_synced_at,
            "last_synced_assessment_instance_id": state.last_synced_assessment_instance_id,
            "latest_assessment_instance_id": latest_assessment_id,
        }
        return payload, meta, should_trigger

    @staticmethod
    def _matches_disease_code(*, requested: str, report_code: str) -> bool:
        req = (requested or "").strip().lower()
        code = (report_code or "").strip().lower()
        if not req or not code:
            return False
        if code == req:
            return True
        if code.startswith(f"{req}/") or req.startswith(f"{code}/"):
            return True
        return False

    @staticmethod
    def _find_matching_disease_entry(diseases_raw: list[Any], *, disease_key: str) -> dict[str, Any] | None:
        for entry in diseases_raw:
            if not isinstance(entry, dict):
                continue
            code = str(entry.get("code") or "")
            if ReportsService._matches_disease_code(requested=disease_key, report_code=code):
                return entry
        return None

    @staticmethod
    def _serialize_disease_trend_field(value: Any) -> Any:
        if value is None or isinstance(value, (str, bool)):
            return value
        if isinstance(value, (int, float)):
            return float(value) if isinstance(value, float) else int(value)
        return value

    @staticmethod
    def _disease_entry_to_data_point(
        entry: dict[str, Any],
        *,
        date_value: str,
        engagement_id: int,
    ) -> dict[str, Any]:
        return {
            "date": date_value,
            "risk_score_scaled": ReportsService._serialize_disease_trend_field(entry.get("risk_score_scaled")),
        }

    async def _build_disease_trend_payload(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        disease_key: str,
    ) -> dict[str, Any]:
        rows = await self._repository.list_metsights_pro_basic_assessments_for_user(
            db,
            user_id=user_id,
        )

        hp = await self._diagnostics_service.get_health_parameter_by_parameter_key(
            db, parameter_key=disease_key,
        )
        unit: str | None = hp.unit if hp is not None else None

        data_points: list[dict[str, Any]] = []
        for assessment, _package in rows:
            report_data = await self._get_or_fetch_report_optional(
                db,
                assessment_id=int(assessment.assessment_instance_id),
                user_id=user_id,
            )
            if report_data is None:
                continue
            report_dict = report_data if isinstance(report_data, dict) else {}
            diseases_raw = extract_diseases(report_dict)

            matched = self._find_matching_disease_entry(diseases_raw, disease_key=disease_key)
            if matched is None:
                continue

            entry_unit = matched.get("unit")
            if unit is None and isinstance(entry_unit, str) and entry_unit.strip():
                unit = entry_unit.strip()

            point_date = assessment.completed_at or assessment.assigned_at
            if point_date is None:
                continue
            if isinstance(point_date, date):
                date_value = point_date.isoformat()[:10]
            else:
                date_value = str(point_date)[:10]

            data_points.append(
                self._disease_entry_to_data_point(
                    matched,
                    date_value=date_value,
                    engagement_id=int(assessment.engagement_id),
                )
            )

        return {
            "parameter": disease_key,
            "unit": unit,
            "data_points": data_points,
        }

    async def get_disease_trends_for_user(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        disease: str,
    ) -> dict[str, Any]:
        disease_key = (disease or "").strip().lower()
        if not disease_key:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        return await self._build_disease_trend_payload(
            db,
            user_id=user_id,
            disease_key=disease_key,
        )
