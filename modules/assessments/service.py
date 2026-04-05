"""Assessments service.

Business rules live here.
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.audit.service import AuditService
from modules.assessments.models import AssessmentCategoryProgress, AssessmentInstance
from modules.assessments.repository import AssessmentsRepository
from modules.assessments.schemas import MetsightsRecordIdUpdate
from modules.employee.service import EmployeeContext


_ALLOWED_USER_ASSESSMENT_STATUSES = {"active", "completed"}


def _normalize_status(value: str | None) -> str:
    return (value or "").strip().lower()


class AssessmentsService:
    def __init__(self, repository: AssessmentsRepository, audit_service: AuditService | None = None):
        self._repository = repository
        self._audit_service = audit_service

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
        instance = AssessmentInstance(
            user_id=user_id,
            engagement_id=engagement_id,
            package_id=package_id,
            status=status,
            metsights_record_id=mid,
            assigned_at=datetime.now(timezone.utc),
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

    async def get_instance_by_id(self, db: AsyncSession, *, assessment_instance_id: int) -> AssessmentInstance | None:
        return await self._repository.get_instance_by_id(db, assessment_instance_id=assessment_instance_id)

    async def get_package_by_id(self, db: AsyncSession, package_id: int):
        return await self._repository.get_package_by_id(db, package_id=package_id)

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
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> AssessmentInstance:
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

        if current == "completed":
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Assessment is already completed")

        if current != "active":
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Assessment is not active")

        if normalized != "completed":
            raise AppError(status_code=422, error_code="INVALID_STATE", message="Assessment status change is not allowed")

        instance.status = normalized
        instance.completed_at = datetime.now(timezone.utc)
        instance = await self._repository.update_instance(db, instance)

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="USER_UPDATE_ASSESSMENT_STATUS",
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
