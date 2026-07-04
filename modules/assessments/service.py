"""Assessments service.

Business rules live here.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from db.seed.blood_parameters_registry import (
    ADVANCED_BLOOD_PARAMETER_CATEGORY_KEY,
    BLOOD_PARAMETER_CATEGORY_KEY,
    UNITLESS_BLOOD_PARAMETER_KEYS,
)
from modules.audit.service import AuditService
from modules.assessments.models import AssessmentCategoryProgress, AssessmentInstance
from modules.assessments.repository import AssessmentsRepository
from modules.assessments.schemas import MetsightsRecordIdUpdate
from modules.employee.service import EmployeeContext
from modules.questionnaire.models import QuestionnaireOption, QuestionnaireResponse
from modules.questionnaire.repository import QuestionnaireRepository
from modules.reports.blood_parameters_read_service import build_parameter_value_map
from modules.reports.blood_parameters_schemas import has_usable_provider_blood_parameters
from modules.reports.repository import ReportsRepository


_ALLOWED_USER_ASSESSMENT_STATUSES = {"active", "completed"}

_METSIGHTS_BLOOD_PACKAGE_CODES = frozenset({"METSIGHTS_BASIC", "METSIGHTS_PRO"})

_PACKAGE_BLOOD_CATEGORY_KEYS: dict[str, tuple[str, ...]] = {
    "METSIGHTS_BASIC": (BLOOD_PARAMETER_CATEGORY_KEY,),
    "METSIGHTS_PRO": (BLOOD_PARAMETER_CATEGORY_KEY, ADVANCED_BLOOD_PARAMETER_CATEGORY_KEY),
}


def _normalize_status(value: str | None) -> str:
    return (value or "").strip().lower()


def _normalize_label(value: str | None) -> str:
    return (value or "").strip().lower()


def _map_unit_to_option_value(unit: str | None, options: list[QuestionnaireOption]) -> str | None:
    """Map an IHR unit (display name or option code) to questionnaire option_value."""
    if unit is None:
        return None
    candidate = unit.strip()
    if not candidate:
        return None
    normalized = _normalize_label(candidate)
    for option in options:
        option_value = str(option.option_value or "").strip()
        display_name = str(option.display_name or "").strip()
        if _normalize_label(option_value) == normalized or _normalize_label(display_name) == normalized:
            return option_value
    return None


class AssessmentsService:
    def __init__(
        self,
        repository: AssessmentsRepository,
        *,
        questionnaire_repository: QuestionnaireRepository | None = None,
        reports_repository: ReportsRepository | None = None,
        audit_service: AuditService | None = None,
    ):
        self._repository = repository
        self._questionnaire = questionnaire_repository
        self._reports = reports_repository
        self._audit_service = audit_service

    async def list_instances_for_engagement(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
    ) -> list[AssessmentInstance]:
        return await self._repository.list_all_instances_for_engagement(db, engagement_id=engagement_id)

    async def ensure_instance_assigned(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        engagement_id: int,
        package_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
        metsights_record_id: str | None = None,
    ) -> AssessmentInstance:
        """Ensure an assessment instance exists for this (user, engagement, package).

        This is idempotent.
        """

        package = await self._repository.get_package_by_id(db, package_id=package_id)
        if package is None or (package.status or "").lower() != "active":
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Assessment package is not active")

        existing = await self._repository.get_instance_by_user_engagement_package(
            db,
            user_id=user_id,
            engagement_id=engagement_id,
            package_id=package_id,
        )
        if existing is not None:
            if metsights_record_id is not None:
                mid = (metsights_record_id or "").strip()
                ex_mid = (existing.metsights_record_id or "").strip()
                if mid:
                    if ex_mid and ex_mid != mid:
                        raise AppError(
                            status_code=409,
                            error_code="CONFLICT",
                            message="Metsights record id does not match existing assessment instance",
                        )
                    if not ex_mid:
                        await self._repository.set_metsights_record_id(
                            db,
                            assessment_instance_id=existing.assessment_instance_id,
                            metsights_record_id=mid,
                        )
                        existing.metsights_record_id = mid
            return existing

        mid_new = (metsights_record_id or "").strip() or None
        instance = AssessmentInstance(
            user_id=user_id,
            engagement_id=engagement_id,
            package_id=package_id,
            status="active",
            metsights_record_id=mid_new,
            assigned_at=datetime.now(timezone.utc),
            completed_at=None,
        )
        instance = await self._repository.create_instance(db, instance)
        package_categories = await self._repository.list_package_categories(db, package_id=package_id)
        for link in package_categories:
            existing_progress = await self._repository.get_category_progress(
                db,
                assessment_instance_id=instance.assessment_instance_id,
                category_id=link.category_id,
            )
            if existing_progress is not None:
                continue
            await self._repository.create_category_progress(
                db,
                AssessmentCategoryProgress(
                    assessment_instance_id=instance.assessment_instance_id,
                    category_id=link.category_id,
                    status="incomplete",
                    completed_at=None,
                ),
            )

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="SYSTEM_ASSIGN_ASSESSMENT_INSTANCE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user_id,
            session_id=None,
        )

        return instance

    async def create_instance_for_metsights_record(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        engagement_id: int,
        package_id: int,
        metsights_record_id: str,
        metsights_is_complete: bool,
        ip_address: str,
        user_agent: str,
        endpoint: str,
        assigned_at: datetime | None = None,
    ) -> AssessmentInstance:
        """Create an assessment instance keyed by Metsights record id.

        Idempotent on ``metsights_record_id`` only. Allows multiple instances per
        (user_id, engagement_id, package_id) when Metsights record ids differ.
        """

        mid = (metsights_record_id or "").strip()
        if not mid:
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Metsights record id is missing")

        package = await self._repository.get_package_by_id(db, package_id=package_id)
        if package is None or (package.status or "").lower() != "active":
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Assessment package is not active")

        existing = await self._repository.get_instance_by_metsights_record_id(db, metsights_record_id=mid)
        if existing is not None:
            if int(existing.user_id) != int(user_id):
                raise AppError(
                    status_code=409,
                    error_code="CONFLICT",
                    message="Metsights record id is already linked to another user",
                )
            return existing

        status = "completed" if metsights_is_complete else "active"
        completed_at = datetime.now(timezone.utc) if metsights_is_complete else None
        resolved_assigned_at = assigned_at if assigned_at is not None else datetime.now(timezone.utc)
        instance = AssessmentInstance(
            user_id=user_id,
            engagement_id=engagement_id,
            package_id=package_id,
            status=status,
            metsights_record_id=mid,
            assigned_at=resolved_assigned_at,
            completed_at=completed_at,
        )
        instance = await self._repository.create_instance(db, instance)
        package_categories = await self._repository.list_package_categories(db, package_id=package_id)
        for link in package_categories:
            existing_progress = await self._repository.get_category_progress(
                db,
                assessment_instance_id=instance.assessment_instance_id,
                category_id=link.category_id,
            )
            if existing_progress is not None:
                continue
            progress_status = "complete" if metsights_is_complete else "incomplete"
            progress_completed = datetime.now(timezone.utc) if metsights_is_complete else None
            await self._repository.create_category_progress(
                db,
                AssessmentCategoryProgress(
                    assessment_instance_id=instance.assessment_instance_id,
                    category_id=link.category_id,
                    status=progress_status,
                    completed_at=progress_completed,
                ),
            )

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="SYSTEM_ASSIGN_ASSESSMENT_INSTANCE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user_id,
            session_id=None,
        )

        return instance

    def _require_audit_service(self) -> AuditService:
        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        return self._audit_service

    async def get_instance_by_metsights_record_id(self, db: AsyncSession, metsights_record_id: str) -> AssessmentInstance | None:
        return await self._repository.get_instance_by_metsights_record_id(
            db, metsights_record_id=metsights_record_id
        )

    async def get_instances_by_metsights_record_ids(
        self, db: AsyncSession, metsights_record_ids: list[str]
    ) -> dict[str, AssessmentInstance]:
        return await self._repository.get_instances_by_metsights_record_ids(
            db, metsights_record_ids=metsights_record_ids
        )

    async def get_instance_by_id(self, db: AsyncSession, *, assessment_instance_id: int) -> AssessmentInstance | None:
        return await self._repository.get_instance_by_id(db, assessment_instance_id=assessment_instance_id)

    async def get_package_by_id(self, db: AsyncSession, package_id: int):
        return await self._repository.get_package_by_id(db, package_id=package_id)

    async def get_first_category_id_for_question_in_package(
        self,
        db: AsyncSession,
        *,
        package_id: int,
        question_id: int,
    ) -> int | None:
        return await self._repository.get_first_category_id_for_question_in_package(
            db,
            package_id=package_id,
            question_id=question_id,
        )

    async def get_package_by_assessment_type_code(self, db: AsyncSession, *, assessment_type_code: str):
        return await self._repository.get_package_by_assessment_type_code(
            db, assessment_type_code=assessment_type_code
        )

    async def list_my_assessments(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        page: int,
        limit: int,
    ) -> tuple[list[tuple], int]:
        instances = await self._repository.list_instances_for_user(db, user_id=user_id, page=page, limit=limit)
        total = await self._repository.count_instances_for_user(db, user_id=user_id)
        return instances, total

    async def get_assessment_details_for_user(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
        user_id: int,
    ):
        row = await self._repository.get_instance_for_user(
            db,
            assessment_instance_id=assessment_instance_id,
            user_id=user_id,
        )
        if row is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_NOT_FOUND", message="Assessment does not exist")
        return row

    async def change_assessment_status_for_user(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
        user_id: int,
        status: str,
        employee_ok: bool = False,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> AssessmentInstance:
        if employee_ok:
            instance = await self._repository.get_instance_by_id(db, assessment_instance_id)
            if instance is None:
                raise AppError(status_code=404, error_code="ASSESSMENT_NOT_FOUND", message="Assessment does not exist")
        else:
            row = await self._repository.get_instance_for_user(
                db,
                assessment_instance_id=assessment_instance_id,
                user_id=user_id,
            )
            if row is None:
                raise AppError(status_code=404, error_code="ASSESSMENT_NOT_FOUND", message="Assessment does not exist")
            instance, _package = row

        normalized = _normalize_status(status)
        if normalized not in _ALLOWED_USER_ASSESSMENT_STATUSES:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        current = _normalize_status(instance.status)

        if current == normalized:
            return instance

        if employee_ok:
            if current not in {"active", "completed"} or normalized not in {"active", "completed"}:
                raise AppError(
                    status_code=422,
                    error_code="INVALID_STATE",
                    message="Assessment status change is not allowed",
                )
        else:
            if current == "completed":
                raise AppError(status_code=422, error_code="INVALID_STATE", message="Assessment is already completed")

            if current != "active":
                raise AppError(status_code=422, error_code="INVALID_STATE", message="Assessment is not active")

            if normalized != "completed":
                raise AppError(status_code=422, error_code="INVALID_STATE", message="Assessment status change is not allowed")

        instance.status = normalized
        if normalized == "completed":
            instance.completed_at = datetime.now(timezone.utc)
        else:
            instance.completed_at = None
        instance = await self._repository.update_instance(db, instance)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_UPDATE_ASSESSMENT_STATUS" if employee_ok else "USER_UPDATE_ASSESSMENT_STATUS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user_id,
            session_id=None,
        )

        return instance

    async def set_metsights_record_id(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
        data: MetsightsRecordIdUpdate,
        current_employee: EmployeeContext,
    ) -> AssessmentInstance:
        instance = await self._repository.get_instance_by_id(db, assessment_instance_id=assessment_instance_id)
        if instance is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_NOT_FOUND", message="Assessment does not exist")

        await self._repository.set_metsights_record_id(
            db,
            assessment_instance_id=assessment_instance_id,
            metsights_record_id=data.metsights_record_id.strip(),
        )

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="EMPLOYEE_SET_METSIGHTS_RECORD_ID",
            endpoint="/assessments/{assessment_id}/metsights-record-id",
            ip_address="0.0.0.0",
            user_agent="employee-api",
            user_id=current_employee.user_id,
            session_id=None,
        )

        updated = await self._repository.get_instance_by_id(db, assessment_instance_id=assessment_instance_id)
        if updated is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_NOT_FOUND", message="Assessment does not exist")
        return updated

    async def submit_assessment_for_user(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        assessment_instance_id: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
        metsights_sync: object | None = None,
        source_assessment_instance_ids: list[int] | None = None,
    ) -> dict[str, Any]:
        """Mark questionnaire responses submitted, category progress and instance completed; push answers to Metsights."""

        if self._questionnaire is None:
            raise RuntimeError("QuestionnaireRepository is required for submit_assessment_for_user")

        instance = await self._repository.get_instance_by_id(db, assessment_instance_id=assessment_instance_id)
        if instance is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_NOT_FOUND", message="Assessment does not exist")

        if int(instance.user_id) != int(user_id):
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )

        current_status = (instance.status or "").lower()
        if current_status == "completed":
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Assessment is already completed")
        if current_status != "active":
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Assessment is not active")

        if metsights_sync is not None:
            await metsights_sync.push_questionnaire_to_metsights_for_submit(
                db,
                assessment_instance_id=assessment_instance_id,
                current_user_id=user_id,
                source_assessment_instance_ids=source_assessment_instance_ids,
            )

        now = datetime.now(timezone.utc)
        responses = await self._questionnaire.list_responses_for_instance(
            db,
            assessment_instance_id=assessment_instance_id,
        )
        for response in responses:
            response.submitted_at = now
            await self._questionnaire.update_response(db, response)

        package_categories = await self._repository.list_package_categories(db, package_id=instance.package_id)
        for link in package_categories:
            progress = await self._repository.get_category_progress(
                db,
                assessment_instance_id=assessment_instance_id,
                category_id=link.category_id,
            )
            if progress is None:
                progress = AssessmentCategoryProgress(
                    assessment_instance_id=assessment_instance_id,
                    category_id=link.category_id,
                    status="complete",
                    completed_at=now,
                )
                await self._repository.create_category_progress(db, progress)
            else:
                progress.status = "complete"
                progress.completed_at = now
                await self._repository.update_category_progress(db, progress)

        instance.status = "completed"
        instance.completed_at = now
        await self._repository.update_instance(db, instance)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="USER_SUBMIT_ASSESSMENT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=user_id,
            session_id=None,
        )

        return {"message": "Assessment submitted successfully"}

    async def draft_blood_parameters_from_report(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        assessment_instance_id: int,
    ) -> dict[str, Any]:
        """Draft blood-parameter questionnaire answers from individual_health_report.blood_parameters."""

        if self._questionnaire is None:
            raise RuntimeError("QuestionnaireRepository is required for draft_blood_parameters_from_report")
        if self._reports is None:
            raise RuntimeError("ReportsRepository is required for draft_blood_parameters_from_report")

        row = await self._repository.get_instance_for_user(
            db,
            assessment_instance_id=assessment_instance_id,
            user_id=user_id,
        )
        if row is None:
            raise AppError(status_code=404, error_code="ASSESSMENT_NOT_FOUND", message="Assessment does not exist")

        instance, package = row
        package_code = (getattr(package, "package_code", None) or "").strip() if package is not None else ""
        if package_code not in _METSIGHTS_BLOOD_PACKAGE_CODES:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Assessment package is not eligible for blood parameter import",
            )

        required_category_keys = _PACKAGE_BLOOD_CATEGORY_KEYS[package_code]
        package_links = await self._repository.list_package_categories(db, package_id=int(instance.package_id))
        linked_category_keys: set[str] = set()
        for link in package_links:
            category = await self._questionnaire.get_category_by_id(db, link.category_id)
            if category is None:
                continue
            if (category.category_of or "").strip().lower() != "metsights":
                continue
            key = (category.category_key or "").strip()
            if key:
                linked_category_keys.add(key)

        missing = [key for key in required_category_keys if key not in linked_category_keys]
        if missing:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message=(
                    "Assessment package is not linked to required blood parameter categories: "
                    + ", ".join(missing)
                ),
            )

        mrid = (instance.metsights_record_id or "").strip()
        if not mrid:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Assessment has no Metsights record id",
            )

        current_status = (instance.status or "").lower()
        if current_status == "completed":
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Assessment is already completed")
        if current_status != "active":
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Assessment is not active")

        report = await self._reports.get_individual_report_by_assessment(
            db,
            assessment_instance_id=int(instance.assessment_instance_id),
        )
        if report is None or not has_usable_provider_blood_parameters(report.blood_parameters):
            report = await self._reports.get_individual_report_by_engagement(
                db,
                user_id=int(instance.user_id),
                engagement_id=int(instance.engagement_id),
            )

        if report is None or report.blood_parameters is None:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Blood parameters report is not available",
            )
        if not has_usable_provider_blood_parameters(report.blood_parameters):
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Blood parameters report is not available",
            )

        values_by_key = build_parameter_value_map(report.blood_parameters)
        category_results: list[dict[str, Any]] = []
        total_drafted = 0

        for category_key in required_category_keys:
            category = await self._questionnaire.get_category_by_key_and_category_of(
                db,
                category_key=category_key,
                category_of="metsights",
            )
            if category is None:
                raise AppError(
                    status_code=422,
                    error_code="INVALID_STATE",
                    message=f"Category '{category_key}' is not configured",
                )

            questions = await self._questionnaire.list_questions_by_category(
                db,
                category_id=int(category.category_id),
            )
            drafted = 0
            skipped: list[str] = []

            for question in questions:
                question_key = (question.question_key or "").strip()
                if not question_key:
                    continue
                if (question.status or "").strip().lower() != "active":
                    skipped.append(f"{question_key}:inactive")
                    continue

                entry = values_by_key.get(question_key)
                if entry is None:
                    skipped.append(f"{question_key}:no_value")
                    continue

                value, unit = entry
                if question_key in UNITLESS_BLOOD_PARAMETER_KEYS:
                    answer: dict[str, Any] = {"value": value, "unit": "0"}
                else:
                    options = await self._questionnaire.list_options_for_question(
                        db,
                        question_id=int(question.question_id),
                    )
                    option_value = _map_unit_to_option_value(unit, options)
                    if option_value is None:
                        skipped.append(f"{question_key}:unmappable_unit")
                        continue
                    answer = {"value": value, "unit": option_value}

                existing = await self._questionnaire.get_response_by_instance_and_question_id(
                    db,
                    assessment_instance_id=int(instance.assessment_instance_id),
                    question_id=int(question.question_id),
                )
                if existing is not None:
                    existing.answer = answer
                    existing.category_id = int(category.category_id)
                    existing.submitted_at = None
                    await self._questionnaire.update_response(db, existing)
                else:
                    await self._questionnaire.create_response(
                        db,
                        QuestionnaireResponse(
                            assessment_instance_id=int(instance.assessment_instance_id),
                            question_id=int(question.question_id),
                            category_id=int(category.category_id),
                            answer=answer,
                            submitted_at=None,
                        ),
                    )
                drafted += 1

            total_drafted += drafted
            category_results.append(
                {
                    "category": category_key,
                    "responses_drafted": drafted,
                    "skipped": skipped,
                }
            )

        await self._sync_blood_category_progress(
            db,
            instance=instance,
            category_keys=required_category_keys,
        )

        return {
            "assessment_instance_id": int(instance.assessment_instance_id),
            "package_code": package_code,
            "responses_drafted": total_drafted,
            "categories": category_results,
        }

    async def _sync_blood_category_progress(
        self,
        db: AsyncSession,
        *,
        instance: AssessmentInstance,
        category_keys: tuple[str, ...],
    ) -> None:
        """Update category progress for drafted blood categories."""
        from modules.questionnaire.service import QuestionnaireService
        from modules.users.repository import UsersRepository

        if self._questionnaire is None:
            return

        q_service = QuestionnaireService(
            repository=self._questionnaire,
            users_repository=UsersRepository(),
        )
        now = datetime.now(timezone.utc)

        for category_key in category_keys:
            category = await self._questionnaire.get_category_by_key_and_category_of(
                db,
                category_key=category_key,
                category_of="metsights",
            )
            if category is None:
                continue

            all_required_answered = await q_service.is_category_complete(
                db,
                assessment_instance_id=int(instance.assessment_instance_id),
                category_id=int(category.category_id),
                user_id=int(instance.user_id),
            )

            progress = await self._repository.get_category_progress(
                db,
                assessment_instance_id=int(instance.assessment_instance_id),
                category_id=int(category.category_id),
            )

            if all_required_answered:
                if progress is None:
                    await self._repository.create_category_progress(
                        db,
                        AssessmentCategoryProgress(
                            assessment_instance_id=int(instance.assessment_instance_id),
                            category_id=int(category.category_id),
                            status="complete",
                            completed_at=now,
                        ),
                    )
                elif (progress.status or "").strip().lower() != "complete":
                    progress.status = "complete"
                    progress.completed_at = now
                    await self._repository.update_category_progress(db, progress)
            elif progress is not None and (progress.status or "").strip().lower() == "complete":
                progress.status = "incomplete"
                progress.completed_at = None
                await self._repository.update_category_progress(db, progress)
