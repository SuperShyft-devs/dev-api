"""Engagement-level assessment packages service.

Business rules:
- Listing packages for an engagement returns the unique packages backing the
  engagement's ``assessment_instances`` (not the engagement's primary
  ``assessment_package_id`` column).
- Adding a package is idempotent: for each distinct participant of the
  engagement, create an ``AssessmentInstance`` and a Metsights record if one
  does not yet exist. Best-effort on Metsights (instance is still created
  locally even when the remote call fails, matching existing booking flows).
- Removing a package is employee-only and blocks removal of the engagement's
  primary/default package. Local cascade deletes reports, questionnaire
  responses, category progress, and the assessment instance itself. The
  Metsights Records API has no DELETE endpoint, so the remote record is
  intentionally orphaned.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.assessments.models import AssessmentPackage
from modules.assessments.repository import AssessmentsRepository
from modules.assessments.service import AssessmentsService
from modules.audit.service import AuditService
from modules.employee.service import EmployeeContext
from modules.engagements.repository import EngagementsRepository
from modules.metsights.service import MetsightsService
from modules.questionnaire.repository import QuestionnaireRepository
from modules.reports.repository import ReportsRepository
from modules.users.repository import UsersRepository


logger = logging.getLogger(__name__)


def _package_to_dict(package: AssessmentPackage) -> dict[str, Any]:
    return {
        "package_id": package.package_id,
        "package_code": package.package_code,
        "display_name": package.display_name,
        "assessment_type_code": package.assessment_type_code,
        "status": package.status,
    }


class EngagementAssessmentPackagesService:
    def __init__(
        self,
        *,
        engagements_repository: EngagementsRepository,
        assessments_repository: AssessmentsRepository,
        reports_repository: ReportsRepository,
        questionnaire_repository: QuestionnaireRepository,
        users_repository: UsersRepository,
        assessments_service: AssessmentsService,
        metsights_service: MetsightsService,
        audit_service: AuditService,
    ):
        self._engagements = engagements_repository
        self._assessments_repo = assessments_repository
        self._reports = reports_repository
        self._questionnaire = questionnaire_repository
        self._users = users_repository
        self._assessments = assessments_service
        self._metsights = metsights_service
        self._audit = audit_service

    async def _ensure_participant_or_employee(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        user_id: int,
        employee: EmployeeContext | None,
    ) -> None:
        if employee is not None:
            return
        is_participant = await self._engagements.has_participant_for_user_engagement(
            db,
            user_id=user_id,
            engagement_id=engagement_id,
        )
        if not is_participant:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )

    async def list_packages_for_engagement(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        current_user_id: int,
        employee: EmployeeContext | None,
    ) -> list[dict[str, Any]]:
        engagement = await self._engagements.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )

        await self._ensure_participant_or_employee(
            db,
            engagement_id=engagement_id,
            user_id=current_user_id,
            employee=employee,
        )

        packages = await self._assessments_repo.list_distinct_packages_for_engagement(
            db,
            engagement_id=engagement_id,
        )
        return [_package_to_dict(p) for p in packages]

    async def add_package_to_engagement(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        package_code: str,
        current_user_id: int,
        employee: EmployeeContext | None,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict[str, Any]:
        code = (package_code or "").strip()
        if not code:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Invalid request",
            )

        engagement = await self._engagements.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )
        if (engagement.status or "").lower() != "active":
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Engagement is no longer active",
            )

        package = await self._assessments_repo.get_package_by_code(db, package_code=code)
        if package is None:
            raise AppError(
                status_code=404,
                error_code="ASSESSMENT_PACKAGE_NOT_FOUND",
                message="Package does not exist",
            )
        if (package.status or "").lower() != "active":
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Assessment package is not active",
            )

        await self._ensure_participant_or_employee(
            db,
            engagement_id=engagement_id,
            user_id=current_user_id,
            employee=employee,
        )

        package_id = int(package.package_id)
        assessment_type_code = (package.assessment_type_code or "").strip()

        participant_ids = await self._engagements.list_distinct_participant_ids_for_engagement(
            db,
            engagement_id=engagement_id,
        )

        created: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []

        for participant_id in participant_ids:
            existing = await self._assessments_repo.get_instance_by_user_engagement_package(
                db,
                user_id=participant_id,
                engagement_id=engagement_id,
                package_id=package_id,
            )
            if existing is not None:
                skipped.append(
                    {
                        "user_id": participant_id,
                        "assessment_instance_id": existing.assessment_instance_id,
                        "reason": "already exists",
                    }
                )
                continue

            user = await self._users.get_user_by_id(db, participant_id)
            profile_id = (getattr(user, "metsights_profile_id", None) or "").strip() if user else ""

            metsights_record_id: str | None = None
            if profile_id and assessment_type_code:
                try:
                    metsights_record_id = await self._metsights.create_record_for_profile(
                        profile_id=profile_id,
                        assessment_type_code=assessment_type_code,
                    )
                except AppError as exc:
                    errors.append(
                        {
                            "user_id": participant_id,
                            "stage": "metsights_create_record",
                            "reason": exc.message,
                        }
                    )
                except Exception as exc:  # pragma: no cover - defensive
                    logger.exception(
                        "Metsights record creation failed for user_id=%s engagement_id=%s package_id=%s",
                        participant_id,
                        engagement_id,
                        package_id,
                    )
                    errors.append(
                        {
                            "user_id": participant_id,
                            "stage": "metsights_create_record",
                            "reason": str(exc),
                        }
                    )

            try:
                instance = await self._assessments.ensure_instance_assigned(
                    db,
                    user_id=participant_id,
                    engagement_id=engagement_id,
                    package_id=package_id,
                    ip_address=ip_address,
                    user_agent=user_agent,
                    endpoint=endpoint,
                    metsights_record_id=metsights_record_id,
                )
            except AppError as exc:
                errors.append(
                    {
                        "user_id": participant_id,
                        "stage": "ensure_instance_assigned",
                        "reason": exc.message,
                    }
                )
                continue
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception(
                    "ensure_instance_assigned failed for user_id=%s engagement_id=%s package_id=%s",
                    participant_id,
                    engagement_id,
                    package_id,
                )
                errors.append(
                    {
                        "user_id": participant_id,
                        "stage": "ensure_instance_assigned",
                        "reason": str(exc),
                    }
                )
                continue

            created.append(
                {
                    "user_id": participant_id,
                    "assessment_instance_id": instance.assessment_instance_id,
                    "metsights_record_id": instance.metsights_record_id,
                }
            )

        action = (
            "EMPLOYEE_ADD_ENGAGEMENT_ASSESSMENT_PACKAGE"
            if employee is not None
            else "USER_ADD_ENGAGEMENT_ASSESSMENT_PACKAGE"
        )
        await self._audit.log_event(
            db,
            action=action,
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=current_user_id,
            session_id=None,
        )

        return {
            "package_id": package_id,
            "package_code": package.package_code,
            "created": created,
            "skipped": skipped,
            "errors": errors,
        }

    async def remove_package_from_engagement(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        package_code: str,
        employee: EmployeeContext,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict[str, Any]:
        if employee is None:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )

        code = (package_code or "").strip()
        if not code:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Invalid request",
            )

        engagement = await self._engagements.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )

        package = await self._assessments_repo.get_package_by_code(db, package_code=code)
        if package is None:
            raise AppError(
                status_code=404,
                error_code="ASSESSMENT_PACKAGE_NOT_FOUND",
                message="Package does not exist",
            )

        package_id = int(package.package_id)

        if engagement.assessment_package_id is not None and int(engagement.assessment_package_id) == package_id:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Cannot remove the engagement's primary assessment package",
            )

        instances = await self._assessments_repo.list_instances_for_engagement_and_package(
            db,
            engagement_id=engagement_id,
            package_id=package_id,
        )

        deleted = 0
        for instance in instances:
            instance_id = int(instance.assessment_instance_id)
            await self._reports.delete_individual_reports_for_instance(
                db,
                assessment_instance_id=instance_id,
            )
            await self._questionnaire.delete_responses_for_instance(
                db,
                assessment_instance_id=instance_id,
            )
            await self._assessments_repo.delete_category_progress_for_instance(
                db,
                assessment_instance_id=instance_id,
            )
            await self._assessments_repo.delete_instance(
                db,
                assessment_instance_id=instance_id,
            )
            deleted += 1

        await self._audit.log_event(
            db,
            action="EMPLOYEE_REMOVE_ENGAGEMENT_ASSESSMENT_PACKAGE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return {
            "package_id": package_id,
            "package_code": package.package_code,
            "deleted_instances": deleted,
        }
