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
            return existing

        instance = AssessmentInstance(
            user_id=user_id,
            engagement_id=engagement_id,
            package_id=package_id,
            status="active",
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

    def _require_audit_service(self) -> AuditService:
        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        return self._audit_service

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
