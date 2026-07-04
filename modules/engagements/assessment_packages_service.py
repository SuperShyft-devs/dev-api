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
from modules.engagements.schemas import PUSH_QUESTIONNAIRE_CATEGORY_KEYS
from modules.metsights.service import MetsightsService
from modules.metsights.sync_service import MetsightsSyncService
from modules.questionnaire.repository import QuestionnaireRepository
from modules.reports.repository import ReportsRepository
from modules.users.repository import UsersRepository


def _normalize_push_categories(categories: list[str] | None) -> list[str] | None:
    """Validate and normalize optional category filter. ``None`` means push all."""

    if categories is None:
        return None

    cleaned = [str(c).strip() for c in categories if str(c).strip()]
    if not cleaned:
        raise AppError(
            status_code=422,
            error_code="INVALID_INPUT",
            message="At least one category must be selected",
        )

    unknown = sorted({c for c in cleaned if c not in PUSH_QUESTIONNAIRE_CATEGORY_KEYS})
    if unknown:
        raise AppError(
            status_code=422,
            error_code="INVALID_INPUT",
            message=f"Unknown push categories: {', '.join(unknown)}",
        )

    # Preserve order, drop duplicates
    seen: set[str] = set()
    ordered: list[str] = []
    for key in cleaned:
        if key not in seen:
            seen.add(key)
            ordered.append(key)
    return ordered


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

        total_participants = await self._engagements.count_distinct_participants_for_engagement(
            db,
            engagement_id=engagement_id,
        )

        result: list[dict[str, Any]] = []
        for p in packages:
            assigned, synced = await self._assessments_repo.count_instances_for_engagement_and_package(
                db,
                engagement_id=engagement_id,
                package_id=int(p.package_id),
            )
            d = _package_to_dict(p)
            d["assigned_count"] = assigned
            d["total_participants"] = total_participants
            d["synced_count"] = synced
            result.append(d)

        return result

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
        if (engagement.status or "").lower() != "running":
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Engagement is not running",
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

            # If an instance already exists but is missing a Metsights record,
            # try to create one and backfill before skipping.
            if existing is not None:
                existing_rid = (existing.metsights_record_id or "").strip()
                if not existing_rid and assessment_type_code:
                    user = await self._users.get_user_by_id(db, participant_id)
                    profile_id = (getattr(user, "metsights_profile_id", None) or "").strip() if user else ""
                    if profile_id:
                        try:
                            new_rid = await self._metsights.create_record_for_profile(
                                profile_id=profile_id,
                                assessment_type_code=assessment_type_code,
                            )
                            if new_rid:
                                await self._assessments_repo.set_metsights_record_id(
                                    db,
                                    assessment_instance_id=int(existing.assessment_instance_id),
                                    metsights_record_id=new_rid,
                                )
                                existing.metsights_record_id = new_rid
                        except AppError as exc:
                            errors.append(
                                {
                                    "user_id": participant_id,
                                    "stage": "metsights_backfill_record",
                                    "reason": exc.message,
                                }
                            )
                        except Exception as exc:  # pragma: no cover - defensive
                            logger.exception(
                                "Metsights backfill failed for user_id=%s instance_id=%s",
                                participant_id,
                                existing.assessment_instance_id,
                            )
                            errors.append(
                                {
                                    "user_id": participant_id,
                                    "stage": "metsights_backfill_record",
                                    "reason": str(exc),
                                }
                            )

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

    async def list_assessment_instances_for_engagement(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        employee: EmployeeContext,
        package_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """Return assessment instances for client-side sequential batching (employee only)."""
        if employee is None:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )

        engagement = await self._engagements.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )

        if package_id is not None:
            package = await self._assessments_repo.get_package_by_id(db, package_id=package_id)
            if package is None:
                raise AppError(
                    status_code=404,
                    error_code="PACKAGE_NOT_FOUND",
                    message="Assessment package does not exist",
                )
            instances = await self._assessments_repo.list_instances_for_engagement_and_package(
                db,
                engagement_id=engagement_id,
                package_id=package_id,
            )
        else:
            instances = await self._assessments_repo.list_all_instances_for_engagement(
                db,
                engagement_id=engagement_id,
            )

        package_cache: dict[int, AssessmentPackage | None] = {}
        rows: list[dict[str, Any]] = []
        for inst in instances:
            pid = int(inst.package_id)
            if pid not in package_cache:
                package_cache[pid] = await self._assessments_repo.get_package_by_id(db, package_id=pid)
            package = package_cache[pid]
            rows.append(
                {
                    "assessment_instance_id": int(inst.assessment_instance_id),
                    "user_id": int(inst.user_id),
                    "package_id": pid,
                    "package_code": (getattr(package, "package_code", None) or "").strip() if package else None,
                    "metsights_record_id": (inst.metsights_record_id or "").strip() or None,
                    "status": inst.status,
                }
            )
        return rows

    async def push_all_questionnaires_to_metsights(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        target_package_id: int,
        employee: EmployeeContext,
        sync_service: MetsightsSyncService,
        ip_address: str,
        user_agent: str,
        endpoint: str,
        assessment_instance_id: int | None = None,
        categories: list[str] | None = None,
    ) -> dict[str, Any]:
        """Push questionnaire answers to Metsights for a specific package's participants.

        Only instances belonging to ``target_package_id`` are used as push targets.
        All of a user's instance IDs within the engagement are passed as
        ``source_assessment_instance_ids`` so answers from every package merge
        into the target record.

        When ``assessment_instance_id`` is set, only that instance is pushed
        (for client-side sequential batching).

        When ``categories`` is set, only those Metsights sub-resources are patched.
        """
        if employee is None:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )

        selected_categories = _normalize_push_categories(categories)

        engagement = await self._engagements.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )

        package = await self._assessments_repo.get_package_by_id(db, package_id=target_package_id)
        if package is None:
            raise AppError(
                status_code=404,
                error_code="PACKAGE_NOT_FOUND",
                message="Assessment package does not exist",
            )

        all_instances = await self._assessments_repo.list_all_instances_for_engagement(
            db,
            engagement_id=engagement_id,
        )

        if not all_instances:
            return {"pushed": 0, "skipped": 0, "errors": 0, "details": []}

        from collections import defaultdict
        user_instances: dict[int, list] = defaultdict(list)
        for inst in all_instances:
            user_instances[int(inst.user_id)].append(inst)

        pushed = 0
        skipped = 0
        errors_count = 0
        details: list[dict[str, Any]] = []

        # Shared cache for Metsights OPTIONS metadata — fetched once and reused
        # across all participants so we don't make 42×3 redundant HTTP calls.
        options_cache: dict = {}

        single_instance_id = int(assessment_instance_id) if assessment_instance_id is not None else None

        for user_id, instances in user_instances.items():
            all_user_instance_ids = [int(i.assessment_instance_id) for i in instances]

            target_instances = [
                i for i in instances if int(i.package_id) == target_package_id
            ]
            if not target_instances:
                continue

            for inst in target_instances:
                inst_id = int(inst.assessment_instance_id)
                if single_instance_id is not None and inst_id != single_instance_id:
                    continue

                mrid = (inst.metsights_record_id or "").strip()
                if not mrid:
                    await sync_service.log_skipped_push(
                        db,
                        engagement_id=engagement_id,
                        user_id=int(user_id),
                        reason="no_metsights_record_id",
                        assessment_instance_id=inst_id,
                    )
                    skipped += 1
                    continue

                try:
                    result = await sync_service.push_questionnaire_for_instance(
                        db,
                        assessment_instance_id=inst_id,
                        source_assessment_instance_ids=all_user_instance_ids,
                        options_cache=options_cache,
                        categories=selected_categories,
                    )
                    if result.get("pushed"):
                        pushed += 1
                        details.append(result)
                    else:
                        skipped += 1
                except Exception as exc:
                    logger.exception(
                        "Push questionnaire failed for instance_id=%s engagement_id=%s package_id=%s",
                        inst_id,
                        engagement_id,
                        target_package_id,
                    )
                    errors_count += 1
                    details.append({
                        "assessment_instance_id": inst_id,
                        "pushed": False,
                        "reason": str(exc),
                    })

                if single_instance_id is not None:
                    break

            if single_instance_id is not None and (pushed + skipped + errors_count) > 0:
                break

        if single_instance_id is not None and (pushed + skipped + errors_count) == 0:
            raise AppError(
                status_code=404,
                error_code="ASSESSMENT_NOT_FOUND",
                message="Assessment instance not found for this engagement and package",
            )

        await self._audit.log_event(
            db,
            action="EMPLOYEE_PUSH_ENGAGEMENT_QUESTIONNAIRES",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return {
            "pushed": pushed,
            "skipped": skipped,
            "errors": errors_count,
            "details": details,
        }

    async def connect_metsights_records_for_package(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        package_id: int,
        employee: EmployeeContext,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict[str, Any]:
        """Create Metsights records for existing instances and store ``metsights_record_id``.

        Does not create new ``assessment_instances``. Skips instances that already have a
        record id or users without ``metsights_profile_id``.
        """
        if employee is None:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )

        engagement = await self._engagements.get_engagement_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(
                status_code=404,
                error_code="ENGAGEMENT_NOT_FOUND",
                message="Engagement does not exist",
            )

        package = await self._assessments_repo.get_package_by_id(db, package_id=package_id)
        if package is None:
            raise AppError(
                status_code=404,
                error_code="PACKAGE_NOT_FOUND",
                message="Assessment package does not exist",
            )

        assessment_type_code = (package.assessment_type_code or "").strip()
        if not assessment_type_code:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Assessment package has no Metsights assessment type",
            )

        instances = await self._assessments_repo.list_instances_for_engagement_and_package(
            db,
            engagement_id=engagement_id,
            package_id=package_id,
        )

        connected = 0
        skipped = 0
        failed = 0
        results: list[dict[str, Any]] = []

        for instance in instances:
            inst_id = int(instance.assessment_instance_id)
            user_id = int(instance.user_id)
            base: dict[str, Any] = {
                "user_id": user_id,
                "assessment_instance_id": inst_id,
                "status": "pending",
                "metsights_record_id": None,
                "reason": None,
            }

            existing_rid = (instance.metsights_record_id or "").strip()
            if existing_rid:
                base["status"] = "skipped"
                base["reason"] = "already_connected"
                base["metsights_record_id"] = existing_rid
                skipped += 1
                results.append(base)
                continue

            user = await self._users.get_user_by_id(db, user_id)
            profile_id = (getattr(user, "metsights_profile_id", None) or "").strip() if user else ""
            if not profile_id:
                base["status"] = "skipped"
                base["reason"] = "no_metsights_profile_id"
                skipped += 1
                results.append(base)
                continue

            try:
                record_id = await self._metsights.create_record_for_profile(
                    profile_id=profile_id,
                    assessment_type_code=assessment_type_code,
                )
                await self._assessments_repo.set_metsights_record_id(
                    db,
                    assessment_instance_id=inst_id,
                    metsights_record_id=record_id,
                )
                base["status"] = "connected"
                base["metsights_record_id"] = record_id
                connected += 1
            except AppError as exc:
                base["status"] = "error"
                base["reason"] = exc.message or exc.error_code or "metsights_error"
                failed += 1
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception(
                    "Metsights connect failed for user_id=%s instance_id=%s engagement_id=%s package_id=%s",
                    user_id,
                    inst_id,
                    engagement_id,
                    package_id,
                )
                base["status"] = "error"
                base["reason"] = str(exc)
                failed += 1

            results.append(base)

        await self._audit.log_event(
            db,
            action="EMPLOYEE_CONNECT_ENGAGEMENT_METSIGHTS_RECORDS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return {
            "engagement_id": engagement_id,
            "package_id": package_id,
            "package_code": package.package_code,
            "assessment_type_code": assessment_type_code,
            "total": len(instances),
            "connected": connected,
            "skipped": skipped,
            "failed": failed,
            "results": results,
        }
