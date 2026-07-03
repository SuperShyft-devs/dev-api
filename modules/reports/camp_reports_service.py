"""Business logic for camp report init and delete."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.audit.service import AuditService
from modules.employee.access_control import (
    ensure_camp_access,
    ensure_camp_access_admin_or_org_manager,
    ensure_internal_employee,
)
from modules.employee.service import EmployeeContext
from modules.engagements.camp_no import format_camp_name, format_department_camp_name
from modules.organizations.models import Organization
from modules.organizations.repository import OrganizationsRepository
from modules.organizations.service import get_department_slugs
from modules.reports.camp_report_section_builders import (
    SECTION_BUILDERS,
    aggregate_top_healthy_habits,
    aggregate_top_healthy_profiles,
    aggregate_top_low_risk,
    build_blood_and_lab_intelligence,
    build_company_average_scores,
    build_distribution_by_gender_by_metabolic_syndrome,
    build_distribution_by_oxidative_stress,
    build_distribution_by_physical_activity_frequency,
    build_distribution_by_sleeping_hours,
    build_kpis,
    build_overall_risk_score,
    build_participation_by_age,
    build_positive_wins,
    normalize_camp_gender,
    physical_activity_answer_to_bucket,
    sleeping_hours_answer_to_bucket,
)
from modules.assessments.repository import AssessmentsRepository
from modules.diagnostics.repository import DiagnosticsRepository
from modules.reports.camp_report_sections_repository import CampReportSectionsRepository
from modules.reports.camp_reports_repository import CampReportsRepository
from modules.reports.models import CampReport
from modules.reports.service import BLOOD_DATA_UNAVAILABLE_ERROR_CODES, ReportsService
from modules.users.models import User


class CampReportsService:
    """Employee-facing init/delete for camp-level reports."""

    def __init__(
        self,
        *,
        repository: CampReportsRepository,
        sections_repository: CampReportSectionsRepository,
        organizations_repository: OrganizationsRepository | None = None,
        audit_service: AuditService,
        reports_service: ReportsService,
        assessments_repository: AssessmentsRepository | None = None,
        diagnostics_repository: DiagnosticsRepository | None = None,
    ) -> None:
        self._repository = repository
        self._sections_repository = sections_repository
        self._organizations_repository = organizations_repository or OrganizationsRepository()
        self._audit_service = audit_service
        self._reports_service = reports_service
        self._assessments_repository = assessments_repository or AssessmentsRepository()
        self._diagnostics_repository = diagnostics_repository or DiagnosticsRepository()

    async def _resolve_camp_context(self, db: AsyncSession, *, camp_no: int) -> dict:
        row = await self._repository.get_camp_context(db, camp_no=camp_no)
        if row is None:
            raise AppError(
                status_code=404,
                error_code="CAMP_NOT_FOUND",
                message="Camp does not exist",
            )
        organization_id, organization_name, camp_start_date, camp_end_date = row
        return {
            "organization_id": int(organization_id),
            "organization_name": organization_name or "",
            "camp_start_date": camp_start_date,
            "camp_end_date": camp_end_date,
        }

    @staticmethod
    def _iso_date(value: date | None) -> str | None:
        if value is None:
            return None
        return value.isoformat()

    def _build_initial_report(
        self,
        *,
        camp_name: str,
        camp_start_date: date | None,
        camp_end_date: date | None,
    ) -> dict:
        return {
            "meta": {
                "camp_name": camp_name,
                "summary_available": False,
                "refreshed_at": None,
                "next_refresh": None,
                "camp_start_date": self._iso_date(camp_start_date),
                "camp_end_date": self._iso_date(camp_end_date),
            }
        }

    async def _validate_department_slug(
        self,
        db: AsyncSession,
        *,
        organization_id: int,
        slug: str,
    ) -> None:
        result = await db.get(Organization, organization_id)
        if result is None:
            raise AppError(
                status_code=404,
                error_code="ORGANIZATION_NOT_FOUND",
                message="Organization does not exist",
            )
        allowed = get_department_slugs(result)
        normalized = slug.strip()
        if normalized not in allowed:
            raise AppError(
                status_code=404,
                error_code="DEPARTMENT_NOT_FOUND",
                message="Department does not exist for this organization",
            )

    async def init_camp_report(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> CampReport:
        ensure_internal_employee(employee)

        context = await self._resolve_camp_context(db, camp_no=camp_no)
        existing = await self._repository.get_overall_by_camp_no(db, camp_no=camp_no)
        if existing is not None:
            raise AppError(
                status_code=409,
                error_code="CAMP_REPORT_EXISTS",
                message="Camp report already exists",
            )

        camp_name = format_camp_name(context["organization_name"], context["camp_start_date"])
        report_payload = self._build_initial_report(
            camp_name=camp_name,
            camp_start_date=context["camp_start_date"],
            camp_end_date=context["camp_end_date"],
        )

        row = CampReport(
            report=report_payload,
            camp_no=camp_no,
            department=None,
            organization_id=context["organization_id"],
        )
        try:
            created = await self._repository.create(db, row)
        except IntegrityError:
            raise AppError(
                status_code=409,
                error_code="CAMP_REPORT_EXISTS",
                message="Camp report already exists",
            ) from None

        await self._audit_service.log_event(
            db,
            action="EMPLOYEE_INIT_CAMP_REPORT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return created

    async def init_department_camp_report(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        slug: str,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> CampReport:
        ensure_internal_employee(employee)

        normalized_slug = slug.strip()
        if not normalized_slug:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        context = await self._resolve_camp_context(db, camp_no=camp_no)
        await self._validate_department_slug(
            db,
            organization_id=context["organization_id"],
            slug=normalized_slug,
        )

        existing = await self._repository.get_by_camp_no_and_department(
            db,
            camp_no=camp_no,
            department=normalized_slug,
        )
        if existing is not None:
            raise AppError(
                status_code=409,
                error_code="CAMP_REPORT_EXISTS",
                message="Camp report already exists",
            )

        camp_name = format_department_camp_name(
            context["organization_name"],
            normalized_slug,
            context["camp_start_date"],
        )
        report_payload = self._build_initial_report(
            camp_name=camp_name,
            camp_start_date=context["camp_start_date"],
            camp_end_date=context["camp_end_date"],
        )

        row = CampReport(
            report=report_payload,
            camp_no=camp_no,
            department=normalized_slug,
            organization_id=context["organization_id"],
        )
        try:
            created = await self._repository.create(db, row)
        except IntegrityError:
            raise AppError(
                status_code=409,
                error_code="CAMP_REPORT_EXISTS",
                message="Camp report already exists",
            ) from None

        await self._audit_service.log_event(
            db,
            action="EMPLOYEE_INIT_DEPARTMENT_CAMP_REPORT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )
        return created

    async def delete_camp_report(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        ensure_internal_employee(employee)

        deleted = await self._repository.delete_overall(db, camp_no=camp_no)
        if deleted == 0:
            raise AppError(
                status_code=404,
                error_code="CAMP_REPORT_NOT_FOUND",
                message="Camp report does not exist",
            )

        await self._audit_service.log_event(
            db,
            action="EMPLOYEE_DELETE_CAMP_REPORT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

    async def delete_department_camp_report(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        slug: str,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        ensure_internal_employee(employee)

        normalized_slug = slug.strip()
        if not normalized_slug:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        deleted = await self._repository.delete_by_department(
            db,
            camp_no=camp_no,
            department=normalized_slug,
        )
        if deleted == 0:
            raise AppError(
                status_code=404,
                error_code="CAMP_REPORT_NOT_FOUND",
                message="Camp report does not exist",
            )

        await self._audit_service.log_event(
            db,
            action="EMPLOYEE_DELETE_DEPARTMENT_CAMP_REPORT",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

    async def purge_camp_reports_if_orphaned(
        self,
        db: AsyncSession,
        *,
        camp_no: int | None,
    ) -> int:
        """Delete all camp reports when no engagements remain for camp_no."""
        if camp_no is None:
            return 0
        remaining = await self._repository.count_engagements_for_camp_no(db, camp_no=camp_no)
        if remaining > 0:
            return 0
        return await self._repository.delete_all_for_camp_no(db, camp_no=camp_no)

    @staticmethod
    def _serialize_camp_report(row: CampReport) -> dict:
        return {
            "report_id": row.report_id,
            "camp_no": int(row.camp_no),
            "department": row.department,
            "organization_id": row.organization_id,
            "report": row.report,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }

    async def list_camp_reports(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
    ) -> list[dict]:
        ensure_internal_employee(employee)
        await self._resolve_camp_context(db, camp_no=camp_no)
        rows = await self._repository.list_by_camp_no(db, camp_no=camp_no)
        return [self._serialize_camp_report(row) for row in rows]

    @staticmethod
    def _camp_participant_to_dict(row: tuple) -> dict:
        (
            engagement_participant_id,
            engagement_id,
            user_id,
            first_name,
            last_name,
            phone,
            gender,
            participant_blood_group,
            participant_department,
        ) = row
        return {
            "engagement_participant_id": engagement_participant_id,
            "engagement_id": engagement_id,
            "user_id": user_id,
            "first_name": first_name,
            "last_name": last_name,
            "phone": phone,
            "gender": gender,
            "participant_blood_group": participant_blood_group,
            "participant_department": participant_department,
        }

    async def list_camp_participants(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        page: int,
        limit: int,
    ) -> tuple[list[dict], int]:
        context = await self._resolve_camp_context(db, camp_no=camp_no)
        await ensure_camp_access(
            db,
            employee,
            context["organization_id"],
            repository=self._organizations_repository,
        )

        rows = await self._repository.list_participants_by_camp_no(
            db,
            camp_no=camp_no,
            page=page,
            limit=limit,
        )
        total = await self._repository.count_participants_by_camp_no(db, camp_no=camp_no)
        return [self._camp_participant_to_dict(row) for row in rows], total

    async def _get_camp_report_row(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None,
    ) -> CampReport:
        if department is None:
            row = await self._repository.get_overall_by_camp_no(db, camp_no=camp_no)
        else:
            normalized_department = department.strip()
            if not normalized_department:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            row = await self._repository.get_by_camp_no_and_department(
                db,
                camp_no=camp_no,
                department=normalized_department,
            )

        if row is None:
            raise AppError(
                status_code=404,
                error_code="CAMP_REPORT_NOT_FOUND",
                message="Camp report does not exist",
            )
        return row

    async def get_camp_report_meta(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        department: str | None = None,
    ) -> dict:
        context = await self._resolve_camp_context(db, camp_no=camp_no)
        await ensure_camp_access(
            db,
            employee,
            context["organization_id"],
            repository=self._organizations_repository,
        )

        if department is not None:
            normalized_department = department.strip()
            if not normalized_department:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            await self._validate_department_slug(
                db,
                organization_id=context["organization_id"],
                slug=normalized_department,
            )
            department = normalized_department

        row = await self._get_camp_report_row(db, camp_no=camp_no, department=department)
        report = row.report or {}
        return dict(report.get("meta") or {})

    async def list_camp_report_section_keys(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        department: str | None = None,
    ) -> list[str]:
        context = await self._resolve_camp_context(db, camp_no=camp_no)
        await ensure_camp_access_admin_or_org_manager(
            db,
            employee,
            context["organization_id"],
            repository=self._organizations_repository,
        )

        if department is not None:
            normalized_department = department.strip()
            if not normalized_department:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            await self._validate_department_slug(
                db,
                organization_id=context["organization_id"],
                slug=normalized_department,
            )
            department = normalized_department

        row = await self._get_camp_report_row(db, camp_no=camp_no, department=department)
        report = row.report or {}
        return list(report.keys())

    async def get_camp_report_dashboard(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        section: str,
        department: str | None = None,
    ) -> dict:
        normalized_section = section.strip()
        if not normalized_section:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        context = await self._resolve_camp_context(db, camp_no=camp_no)
        await ensure_camp_access(
            db,
            employee,
            context["organization_id"],
            repository=self._organizations_repository,
        )

        if department is not None:
            normalized_department = department.strip()
            if not normalized_department:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            await self._validate_department_slug(
                db,
                organization_id=context["organization_id"],
                slug=normalized_department,
            )
            department = normalized_department

        section_row = await self._sections_repository.get_by_section_key(
            db,
            section_key=normalized_section,
        )
        if section_row is None:
            raise AppError(
                status_code=400,
                error_code="INVALID_SECTION",
                message="Invalid report section",
            )

        row = await self._get_camp_report_row(db, camp_no=camp_no, department=department)
        report = row.report or {}
        if normalized_section not in report:
            raise AppError(
                status_code=404,
                error_code="SECTION_NOT_FOUND",
                message="Report section has not been refreshed",
            )
        return dict(report[normalized_section])

    async def refresh_camp_report_section(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        section: str,
        department: str | None = None,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> dict:
        normalized_section = section.strip()
        if not normalized_section:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

        context = await self._resolve_camp_context(db, camp_no=camp_no)
        await ensure_camp_access(
            db,
            employee,
            context["organization_id"],
            repository=self._organizations_repository,
        )

        if department is None:
            row = await self._repository.get_overall_by_camp_no(db, camp_no=camp_no)
            audit_action = "EMPLOYEE_REFRESH_CAMP_REPORT_SECTION"
        else:
            normalized_department = department.strip()
            if not normalized_department:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            await self._validate_department_slug(
                db,
                organization_id=context["organization_id"],
                slug=normalized_department,
            )
            row = await self._repository.get_by_camp_no_and_department(
                db,
                camp_no=camp_no,
                department=normalized_department,
            )
            department = normalized_department
            audit_action = "EMPLOYEE_REFRESH_DEPARTMENT_CAMP_REPORT_SECTION"

        if row is None:
            raise AppError(
                status_code=404,
                error_code="CAMP_REPORT_NOT_FOUND",
                message="Camp report does not exist",
            )

        section_row = await self._sections_repository.get_by_section_key(
            db,
            section_key=normalized_section,
        )
        if section_row is None:
            raise AppError(
                status_code=400,
                error_code="INVALID_SECTION",
                message="Invalid report section",
            )

        builder = SECTION_BUILDERS.get(normalized_section)
        if builder is None:
            raise AppError(
                status_code=400,
                error_code="SECTION_NOT_IMPLEMENTED",
                message="Report section is not implemented",
            )

        built_payload = await self._build_section_payload(
            db,
            section_key=normalized_section,
            camp_no=camp_no,
            department=department,
            camp_start_date=context["camp_start_date"],
            camp_end_date=context["camp_end_date"],
        )

        report = dict(row.report or {})
        meta = dict(report.get("meta") or {})
        meta["refreshed_at"] = datetime.now(timezone.utc).isoformat()
        meta["summary_available"] = True
        report["meta"] = meta

        section_payload = {
            **built_payload,
            "name": section_row.section,
            "description": section_row.description,
        }
        report[normalized_section] = section_payload

        await self._repository.update_report(db, row, report)

        await self._audit_service.log_event(
            db,
            action=audit_action,
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=employee.user_id,
            session_id=None,
        )

        return {
            "report_id": row.report_id,
            "section": section_payload,
        }

    async def _compute_positive_wins_payload(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None,
    ) -> dict:
        contexts = await self._repository.list_health_assessment_contexts(
            db,
            camp_no=camp_no,
            department=department,
        )
        participant_habits: list[list[dict[str, str | None]]] = []
        participant_profiles: list[list[str]] = []
        participant_low_risk: list[list[dict[str, Any]]] = []
        for ctx in contexts:
            low_risk_items = await self._reports_service.compute_low_risk_for_instance(
                db,
                assessment_instance=ctx.assessment_instance,
                package=ctx.package,
                individual_report=ctx.individual_report,
            )
            participant_low_risk.append(
                [
                    {
                        "code": item.code,
                        "name": item.name,
                        "risk_status": item.risk_status,
                        "risk_score_scaled": item.risk_score_scaled,
                    }
                    for item in low_risk_items
                ]
            )
            try:
                habits, profiles = await self._reports_service.compute_healthy_habits_and_profiles_for_instance(
                    db,
                    assessment_instance=ctx.assessment_instance,
                    package=ctx.package,
                    engagement=ctx.engagement,
                    individual_report=ctx.individual_report,
                    user_gender=ctx.user_gender,
                )
            except AppError as exc:
                if exc.error_code not in BLOOD_DATA_UNAVAILABLE_ERROR_CODES:
                    raise
                habits, profiles = [], []
            participant_habits.append(
                [{"habit_key": h.habit_key, "habit_label": h.habit_label} for h in habits]
            )
            participant_profiles.append(profiles)

        return build_positive_wins(
            low_risk=aggregate_top_low_risk(participant_low_risk),
            healthy_habits=aggregate_top_healthy_habits(participant_habits),
            healthy_profiles=aggregate_top_healthy_profiles(participant_profiles),
        )

    async def _compute_company_average_scores_payload(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None,
    ) -> dict:
        contexts = await self._repository.list_fitprint_assessment_contexts(
            db,
            camp_no=camp_no,
            department=department,
        )

        participant_scores: list[dict[str, float | None]] = []

        for ctx in contexts:
            try:
                report_dict = await self._reports_service._resolve_report_dict_for_instance(
                    db,
                    assessment_instance=ctx.assessment_instance,
                    package=ctx.package,
                    individual_report=ctx.individual_report,
                )
            except Exception:
                continue

            fitness_spec = report_dict.get("fitness_specification") or {}
            activity_spec = report_dict.get("activity_specification") or {}

            raw_lifestyle = fitness_spec.get("score") if isinstance(fitness_spec, dict) else None
            lifestyle_score = float(raw_lifestyle) if isinstance(raw_lifestyle, (int, float)) else None

            raw_fitness = activity_spec.get("score") if isinstance(activity_spec, dict) else None
            fitness_score = float(raw_fitness) if isinstance(raw_fitness, (int, float)) else None

            all_instances = await self._assessments_repository.list_instances_for_user_engagement(
                db,
                user_id=ctx.assessment_instance.user_id,
                engagement_id=ctx.assessment_instance.engagement_id,
            )
            source_ids = [inst.assessment_instance_id for inst in all_instances]

            nutrition_score: float | None = None
            if source_ids:
                try:
                    lookup, key_to_qid = await self._reports_service._build_questionnaire_lookup(
                        db,
                        source_assessment_instance_ids=source_ids,
                    )
                    option_reverse_map = await self._reports_service._build_option_reverse_map(db, key_to_qid)
                    nutrition_payload = self._reports_service._build_nutrition_api_payload(
                        lookup, user_gender=ctx.user_gender, option_reverse_map=option_reverse_map,
                    )
                    nutrition_response = await self._reports_service._call_nutrition_api(
                        db,
                        nutrition_payload,
                        user_id=ctx.assessment_instance.user_id,
                        engagement_id=ctx.assessment_instance.engagement_id,
                    )
                    raw_nutrition = nutrition_response.get("nutrition_score")
                    nutrition_score = float(raw_nutrition) if isinstance(raw_nutrition, (int, float)) else None
                except Exception:
                    nutrition_score = None

            participant_scores.append({
                "nutrition": nutrition_score,
                "fitness": fitness_score,
                "lifestyle": lifestyle_score,
            })

        return build_company_average_scores(participant_scores)

    async def validate_company_average_scores(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        department: str | None = None,
    ) -> dict:
        context = await self._resolve_camp_context(db, camp_no=camp_no)
        await ensure_camp_access(
            db,
            employee,
            context["organization_id"],
            repository=self._organizations_repository,
        )

        if department is not None:
            normalized_department = department.strip()
            if not normalized_department:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            await self._validate_department_slug(
                db,
                organization_id=context["organization_id"],
                slug=normalized_department,
            )
            department = normalized_department

        contexts = await self._repository.list_fitprint_assessment_contexts(
            db,
            camp_no=camp_no,
            department=department,
        )

        no_fitprint_rows = await self._repository.list_enrolled_users_without_fitprint(
            db,
            camp_no=camp_no,
            department=department,
        )

        user_ids = [ctx.assessment_instance.user_id for ctx in contexts]
        user_names: dict[int, str] = {}
        if user_ids:
            rows = await db.execute(
                select(User.user_id, User.first_name, User.last_name).where(
                    User.user_id.in_(user_ids)
                )
            )
            for uid, first, last in rows.all():
                parts = [p for p in (first, last) if p]
                user_names[int(uid)] = " ".join(parts) if parts else f"User {uid}"

        nutrition_question_keys = self._reports_service._NUTRITION_API_QUESTION_KEYS

        total_participants = len(contexts)
        valid_counts: dict[str, int] = {"nutrition": 0, "fitness": 0, "lifestyle": 0}
        totals: dict[str, float] = {"nutrition": 0.0, "fitness": 0.0, "lifestyle": 0.0}
        participants_detail: dict[str, list[dict[str, Any]]] = {
            "nutrition": [], "fitness": [], "lifestyle": [],
        }

        for ctx in contexts:
            uid = ctx.assessment_instance.user_id
            name = user_names.get(uid, f"User {uid}")

            try:
                report_dict = await self._reports_service._resolve_report_dict_for_instance(
                    db,
                    assessment_instance=ctx.assessment_instance,
                    package=ctx.package,
                    individual_report=ctx.individual_report,
                )
            except AppError as exc:
                detail = f"Metsights service returned: {exc.message}"
                for key in ("nutrition", "fitness", "lifestyle"):
                    participants_detail[key].append({
                        "user_id": uid, "name": name,
                        "score": None, "reason": "FitPrint report not available",
                        "detail": detail,
                    })
                continue
            except Exception as exc:
                detail = str(exc) or "An unexpected error occurred while resolving the FitPrint report"
                for key in ("nutrition", "fitness", "lifestyle"):
                    participants_detail[key].append({
                        "user_id": uid, "name": name,
                        "score": None, "reason": "FitPrint report not available",
                        "detail": detail,
                    })
                continue

            record_id = (ctx.assessment_instance.metsights_record_id or "").strip()
            report_empty = not report_dict

            if report_empty and not record_id:
                for key in ("nutrition", "fitness", "lifestyle"):
                    participants_detail[key].append({
                        "user_id": uid, "name": name,
                        "score": None, "reason": "FitPrint report not available",
                        "detail": "FitPrint assessment not completed yet (no Metsights record ID)",
                    })
                continue

            if report_empty:
                for key in ("nutrition", "fitness", "lifestyle"):
                    participants_detail[key].append({
                        "user_id": uid, "name": name,
                        "score": None, "reason": "FitPrint report not available",
                        "detail": "Metsights returned an empty report for this participant",
                    })
                continue

            fitness_spec = report_dict.get("fitness_specification") or {}
            activity_spec = report_dict.get("activity_specification") or {}

            raw_lifestyle = fitness_spec.get("score") if isinstance(fitness_spec, dict) else None
            lifestyle_score = float(raw_lifestyle) if isinstance(raw_lifestyle, (int, float)) else None
            if lifestyle_score is not None:
                valid_counts["lifestyle"] += 1
                totals["lifestyle"] += lifestyle_score
                participants_detail["lifestyle"].append({
                    "user_id": uid, "name": name,
                    "score": lifestyle_score, "reason": None, "detail": None,
                })
            else:
                participants_detail["lifestyle"].append({
                    "user_id": uid, "name": name,
                    "score": None, "reason": "Lifestyle score missing in FitPrint report",
                    "detail": "The Metsights FitPrint report was retrieved but does not contain a fitness_specification score. The participant may not have completed the fitness assessment.",
                })

            raw_fitness = activity_spec.get("score") if isinstance(activity_spec, dict) else None
            fitness_score = float(raw_fitness) if isinstance(raw_fitness, (int, float)) else None
            if fitness_score is not None:
                valid_counts["fitness"] += 1
                totals["fitness"] += fitness_score
                participants_detail["fitness"].append({
                    "user_id": uid, "name": name,
                    "score": fitness_score, "reason": None, "detail": None,
                })
            else:
                participants_detail["fitness"].append({
                    "user_id": uid, "name": name,
                    "score": None, "reason": "Fitness score missing in FitPrint report",
                    "detail": "The Metsights FitPrint report was retrieved but does not contain an activity_specification score. The participant may not have completed the fitness assessment.",
                })

            all_instances = await self._assessments_repository.list_instances_for_user_engagement(
                db,
                user_id=uid,
                engagement_id=ctx.assessment_instance.engagement_id,
            )
            source_ids = [inst.assessment_instance_id for inst in all_instances]

            nutrition_score: float | None = None
            nutrition_detail: dict[str, Any] = {"user_id": uid, "name": name}

            if not source_ids:
                nutrition_detail.update({
                    "score": None,
                    "reason": "No assessment instances found",
                    "detail": "No assessments (questionnaire or other) found for this participant's engagement — nutrition questions cannot be loaded",
                })
            else:
                try:
                    lookup, key_to_qid = await self._reports_service._build_questionnaire_lookup(
                        db,
                        source_assessment_instance_ids=source_ids,
                    )

                    answered_keys = [k for k in nutrition_question_keys if lookup.get(k) is not None]
                    missing_keys = [k for k in nutrition_question_keys if lookup.get(k) is None]

                    if not answered_keys:
                        nutrition_detail.update({
                            "score": None,
                            "reason": "Nutrition questionnaire not filled",
                            "detail": f"Participant has not answered any of the {len(nutrition_question_keys)} nutrition-related questions required for score calculation",
                            "missing_questions": missing_keys,
                        })
                    else:
                        option_reverse_map = await self._reports_service._build_option_reverse_map(db, key_to_qid)
                        nutrition_payload = self._reports_service._build_nutrition_api_payload(
                            lookup, user_gender=ctx.user_gender, option_reverse_map=option_reverse_map,
                        )
                        nutrition_response = await self._reports_service._call_nutrition_api(
                            db,
                            nutrition_payload,
                            user_id=ctx.assessment_instance.user_id,
                            engagement_id=ctx.assessment_instance.engagement_id,
                        )
                        raw_nutrition = nutrition_response.get("nutrition_score")
                        nutrition_score = float(raw_nutrition) if isinstance(raw_nutrition, (int, float)) else None

                        if nutrition_score is not None:
                            nutrition_detail.update({"score": nutrition_score, "reason": None, "detail": None})
                            if missing_keys:
                                nutrition_detail["missing_questions"] = missing_keys
                        else:
                            detail_parts = ["The nutrition API processed the request but did not return a numeric score"]
                            if missing_keys:
                                detail_parts.append(f"{len(missing_keys)} of {len(nutrition_question_keys)} questions were not answered, which may have affected the result")
                            nutrition_detail.update({
                                "score": None,
                                "reason": "Nutrition API returned no score",
                                "detail": ". ".join(detail_parts),
                            })
                            if missing_keys:
                                nutrition_detail["missing_questions"] = missing_keys
                except AppError as exc:
                    if exc.error_code == "INVALID_INPUT":
                        detail = exc.message or "The nutrition API rejected the request payload"
                        for qkey, qval in nutrition_payload.items():
                            val_str = str(qval) if not isinstance(qval, list) else str(qval)
                            if val_str in detail:
                                detail = f"[{qkey}] {detail}"
                                break
                    elif exc.error_code == "EXTERNAL_SERVICE_UNAVAILABLE":
                        detail = "The nutrition scoring service is currently unavailable"
                    else:
                        detail = exc.message or str(exc)
                    nutrition_detail.update({
                        "score": None,
                        "reason": "Nutrition API call failed",
                        "detail": detail,
                    })
                except Exception as exc:
                    nutrition_detail.update({
                        "score": None,
                        "reason": "Nutrition API call failed",
                        "detail": str(exc) or "An unexpected error occurred while calling the nutrition API",
                    })

            if nutrition_score is not None:
                valid_counts["nutrition"] += 1
                totals["nutrition"] += nutrition_score

            participants_detail["nutrition"].append(nutrition_detail)

        result: dict[str, Any] = {}
        for key in ("nutrition", "fitness", "lifestyle"):
            vc = valid_counts[key]
            avg = round(totals[key] / vc) if vc > 0 else 0
            result[key] = {
                "score": avg,
                "valid_count": vc,
                "total_participants": total_participants,
                "participants": participants_detail[key],
            }

        result["no_fitprint_assigned"] = [
            {
                "user_id": uid,
                "name": " ".join(p for p in (first, last) if p) or f"User {uid}",
            }
            for uid, first, last in no_fitprint_rows
        ]

        return result

    async def validate_positive_wins(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        department: str | None = None,
    ) -> dict:
        context = await self._resolve_camp_context(db, camp_no=camp_no)
        await ensure_camp_access(
            db,
            employee,
            context["organization_id"],
            repository=self._organizations_repository,
        )

        if department is not None:
            normalized_department = department.strip()
            if not normalized_department:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            await self._validate_department_slug(
                db,
                organization_id=context["organization_id"],
                slug=normalized_department,
            )
            department = normalized_department

        contexts = await self._repository.list_health_assessment_contexts(
            db,
            camp_no=camp_no,
            department=department,
        )

        user_ids = [ctx.assessment_instance.user_id for ctx in contexts]
        user_names: dict[int, str] = {}
        if user_ids:
            rows = await db.execute(
                select(User.user_id, User.first_name, User.last_name).where(
                    User.user_id.in_(user_ids)
                )
            )
            for uid, first, last in rows.all():
                parts = [p for p in (first, last) if p]
                user_names[int(uid)] = " ".join(parts) if parts else f"User {uid}"

        participants_low_risk: list[dict[str, Any]] = []
        participants_habits: list[dict[str, Any]] = []
        participants_profiles: list[dict[str, Any]] = []

        agg_low_risk: list[list[dict[str, Any]]] = []
        agg_habits: list[list[dict[str, str | None]]] = []
        agg_profiles: list[list[str]] = []

        for ctx in contexts:
            uid = ctx.assessment_instance.user_id
            name = user_names.get(uid, f"User {uid}")

            # --- low_risk ---
            lr_entry: dict[str, Any] = {"user_id": uid, "name": name}
            if ctx.package is None:
                lr_entry.update({"items": None, "reason": "No assessment package", "detail": None})
                agg_low_risk.append([])
            else:
                try:
                    report_dict = await self._reports_service._resolve_report_dict_for_instance(
                        db,
                        assessment_instance=ctx.assessment_instance,
                        package=ctx.package,
                        individual_report=ctx.individual_report,
                    )
                except AppError as exc:
                    lr_entry.update({"items": None, "reason": "Metsights report unavailable", "detail": exc.message})
                    agg_low_risk.append([])
                    participants_low_risk.append(lr_entry)

                    # habits & profiles also fail when metsights is down for this participant
                    participants_habits.append({"user_id": uid, "name": name, "items": None, "reason": "Metsights report unavailable", "detail": exc.message})
                    agg_habits.append([])
                    participants_profiles.append({"user_id": uid, "name": name, "items": None, "reason": "Metsights report unavailable", "detail": exc.message})
                    agg_profiles.append([])
                    continue
                except Exception as exc:
                    detail = str(exc) or "Unexpected error resolving report"
                    lr_entry.update({"items": None, "reason": "Report resolution failed", "detail": detail})
                    agg_low_risk.append([])
                    participants_low_risk.append(lr_entry)

                    participants_habits.append({"user_id": uid, "name": name, "items": None, "reason": "Report resolution failed", "detail": detail})
                    agg_habits.append([])
                    participants_profiles.append({"user_id": uid, "name": name, "items": None, "reason": "Report resolution failed", "detail": detail})
                    agg_profiles.append([])
                    continue
                else:
                    low_risk_items = self._reports_service._top_low_risk_from_report_dict(report_dict)
                    if low_risk_items:
                        items_dicts = [{"code": i.code, "name": i.name, "risk_status": i.risk_status, "risk_score_scaled": i.risk_score_scaled} for i in low_risk_items]
                        lr_entry.update({"items": [i.name for i in low_risk_items], "reason": None, "detail": None})
                        agg_low_risk.append(items_dicts)
                    else:
                        lr_entry.update({"items": None, "reason": "No low-risk diseases found in report", "detail": None})
                        agg_low_risk.append([])
            participants_low_risk.append(lr_entry)

            # --- healthy_habits ---
            hh_entry: dict[str, Any] = {"user_id": uid, "name": name}
            if self._reports_service._healthy_habits_service is None:
                hh_entry.update({"items": None, "reason": "Healthy habits service not configured", "detail": None})
                agg_habits.append([])
            elif ctx.package is None:
                hh_entry.update({"items": None, "reason": "No assessment package", "detail": None})
                agg_habits.append([])
            else:
                try:
                    computed = await self._reports_service._healthy_habits_service.top_habits_for_assessment(
                        db,
                        assessment_instance_id=int(ctx.assessment_instance.assessment_instance_id),
                        package_id=int(ctx.assessment_instance.package_id),
                        limit=3,
                    )
                except Exception as exc:
                    hh_entry.update({"items": None, "reason": "Healthy habits computation failed", "detail": str(exc) or "Unexpected error"})
                    agg_habits.append([])
                else:
                    if computed:
                        habit_dicts = [{"habit_key": h.habit_key, "habit_label": h.habit_label} for h in computed]
                        hh_entry.update({"items": [h.habit_label for h in computed], "reason": None, "detail": None})
                        agg_habits.append(habit_dicts)
                    else:
                        hh_entry.update({"items": None, "reason": "No habit rules matched participant answers", "detail": None})
                        agg_habits.append([])
            participants_habits.append(hh_entry)

            # --- healthy_profiles ---
            hp_entry: dict[str, Any] = {"user_id": uid, "name": name}
            if ctx.engagement is None or ctx.engagement.diagnostic_package_id is None:
                hp_entry.update({"items": None, "reason": "No diagnostic package assigned", "detail": None})
                agg_profiles.append([])
            elif ctx.individual_report is None:
                hp_entry.update({"items": None, "reason": "No individual health report", "detail": None})
                agg_profiles.append([])
            else:
                try:
                    _, profiles = await self._reports_service.compute_healthy_habits_and_profiles_for_instance(
                        db,
                        assessment_instance=ctx.assessment_instance,
                        package=ctx.package,
                        engagement=ctx.engagement,
                        individual_report=ctx.individual_report,
                        user_gender=ctx.user_gender,
                    )
                except AppError as exc:
                    if exc.error_code in BLOOD_DATA_UNAVAILABLE_ERROR_CODES:
                        hp_entry.update({"items": None, "reason": "Blood data unavailable", "detail": exc.error_code})
                    else:
                        hp_entry.update({"items": None, "reason": "Blood data fetch failed", "detail": exc.message})
                    agg_profiles.append([])
                except Exception as exc:
                    hp_entry.update({"items": None, "reason": "Healthy profiles computation failed", "detail": str(exc) or "Unexpected error"})
                    agg_profiles.append([])
                else:
                    if profiles:
                        hp_entry.update({"items": profiles, "reason": None, "detail": None})
                        agg_profiles.append(profiles)
                    else:
                        hp_entry.update({"items": None, "reason": "No test groups with in-range parameters", "detail": None})
                        agg_profiles.append([])
            participants_profiles.append(hp_entry)

        return {
            "total_participants": len(contexts),
            "low_risk": {
                "aggregated": aggregate_top_low_risk(agg_low_risk),
                "participants": participants_low_risk,
            },
            "healthy_habits": {
                "aggregated": aggregate_top_healthy_habits(agg_habits),
                "participants": participants_habits,
            },
            "healthy_profiles": {
                "aggregated": aggregate_top_healthy_profiles(agg_profiles),
                "participants": participants_profiles,
            },
        }

    _QUESTIONNAIRE_SECTION_CONFIG: dict[str, dict[str, Any]] = {
        "physical_activity_frequency": {
            "question_key": "physical_activity_frequency",
            "bucket_fn": staticmethod(physical_activity_answer_to_bucket),
        },
        "sleeping_hours": {
            "question_key": "sleeping_hours",
            "bucket_fn": staticmethod(sleeping_hours_answer_to_bucket),
        },
    }

    async def validate_questionnaire_distribution(
        self,
        db: AsyncSession,
        *,
        employee: EmployeeContext,
        camp_no: int,
        question_key: str,
        department: str | None = None,
    ) -> dict:
        context = await self._resolve_camp_context(db, camp_no=camp_no)
        await ensure_camp_access(
            db,
            employee,
            context["organization_id"],
            repository=self._organizations_repository,
        )

        if department is not None:
            normalized_department = department.strip()
            if not normalized_department:
                raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
            await self._validate_department_slug(
                db,
                organization_id=context["organization_id"],
                slug=normalized_department,
            )
            department = normalized_department

        config = self._QUESTIONNAIRE_SECTION_CONFIG.get(question_key)
        if config is None:
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message=f"Unsupported question_key: {question_key}",
            )
        bucket_fn = config["bucket_fn"]

        rows = await self._repository.list_enrolled_users_with_questionnaire_answer(
            db,
            camp_no=camp_no,
            question_key=question_key,
            department=department,
        )

        summary: dict[str, dict[str, int]] = {
            "male": {"enrolled": 0, "responded": 0, "not_responded": 0},
            "female": {"enrolled": 0, "responded": 0, "not_responded": 0},
        }
        participants: list[dict[str, Any]] = []

        for user_id, first_name, last_name, gender_raw, answer in rows:
            parts = [p for p in (first_name, last_name) if p]
            name = " ".join(parts) if parts else f"User {user_id}"
            gender = normalize_camp_gender(gender_raw)

            if gender is not None:
                summary[gender]["enrolled"] += 1

            if answer is None:
                if gender is not None:
                    summary[gender]["not_responded"] += 1
                participants.append({
                    "user_id": user_id,
                    "name": name,
                    "gender": gender,
                    "answer": None,
                    "bucket": None,
                    "reason": "No questionnaire response found for this question",
                })
            else:
                answer_str = str(answer).strip()
                bucket = bucket_fn(answer_str)
                if bucket is not None:
                    if gender is not None:
                        summary[gender]["responded"] += 1
                    participants.append({
                        "user_id": user_id,
                        "name": name,
                        "gender": gender,
                        "answer": answer_str,
                        "bucket": bucket,
                        "reason": None,
                    })
                else:
                    if gender is not None:
                        summary[gender]["not_responded"] += 1
                    participants.append({
                        "user_id": user_id,
                        "name": name,
                        "gender": gender,
                        "answer": answer_str,
                        "bucket": None,
                        "reason": f"Answer value '{answer_str}' does not map to a known bucket",
                    })

        total_enrolled = len(rows)

        return {
            "question_key": question_key,
            "total_enrolled": total_enrolled,
            "summary": summary,
            "participants": participants,
        }

    _BLOOD_INTELLIGENCE_GROUP_KEYS = (
        "vitamin_profile",
        "diabetes_profile",
        "lipid_profile",
        "inflammatory",
    )

    _BLOOD_INTELLIGENCE_COMBINED_KEYS: dict[str, list[str]] = {
        "lipid_profile": ["cholesterol_total", "triglycerides", "ldl_cholestrol"],
        "inflammatory": ["homocysteine", "hs-crp", "esr"],
    }

    async def _compute_blood_and_lab_intelligence_payload(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None,
    ) -> dict:
        participants = await self._repository.list_blood_parameters_by_gender(
            db,
            camp_no=camp_no,
            department=department,
        )

        group_tests: list[tuple[str, list]] = []
        for group_key in self._BLOOD_INTELLIGENCE_GROUP_KEYS:
            group = await self._diagnostics_repository.get_group_by_group_key(db, group_key=group_key)
            if group is None:
                continue
            tests = await self._diagnostics_repository.get_parameters_for_group(db, group_id=group.group_id)
            group_tests.append((group_key, tests))

        tests_by_group: dict[str, dict[str, object]] = {}
        for group_key, tests in group_tests:
            tests_by_group[group_key] = {t.parameter_key: t for t in tests if t.parameter_key}

        group_stats: dict[str, dict[str, dict[str, int]]] = {}
        for group_key, tests in group_tests:
            test_stats: dict[str, dict[str, int]] = {}
            for test in tests:
                param_key = test.parameter_key
                if not param_key:
                    continue
                in_range_count = 0
                total_valid = 0

                for gender, blood_params in participants:
                    value, lower_range, higher_range = self._extract_test_value_and_range(
                        blood_params, test, gender
                    )
                    if value is None or lower_range is None or higher_range is None:
                        continue
                    total_valid += 1
                    if lower_range <= value <= higher_range:
                        in_range_count += 1

                test_stats[param_key] = {"in_range": in_range_count, "total": total_valid}

            combined_params = self._BLOOD_INTELLIGENCE_COMBINED_KEYS.get(group_key)
            if combined_params:
                combined_key = "__".join(combined_params)
                group_test_map = tests_by_group.get(group_key, {})
                combined_tests = [group_test_map[k] for k in combined_params if k in group_test_map]

                if combined_tests:
                    combined_in_range = 0
                    combined_total = 0

                    for gender, blood_params in participants:
                        all_valid = True
                        all_in_range = True
                        for ct in combined_tests:
                            value, lower_range, higher_range = self._extract_test_value_and_range(
                                blood_params, ct, gender
                            )
                            if value is None or lower_range is None or higher_range is None:
                                all_valid = False
                                break
                            if not (lower_range <= value <= higher_range):
                                all_in_range = False
                        if not all_valid:
                            continue
                        combined_total += 1
                        if all_in_range:
                            combined_in_range += 1

                    test_stats[combined_key] = {"in_range": combined_in_range, "total": combined_total}

            group_stats[group_key] = test_stats

        return build_blood_and_lab_intelligence(group_stats)

    @staticmethod
    def _extract_test_value_and_range(
        blood_params: dict[str, Any],
        test,
        gender: str | None,
    ) -> tuple[float | None, float | None, float | None]:
        """Extract value and range for a single test from a participant's blood_parameters."""
        from modules.reports.blood_parameters_read_service import BloodParametersReadService
        from modules.reports.blood_parameters_schemas import (
            is_canonical_blood_parameters,
            is_legacy_healthians_format,
            is_legacy_metsights_flat_format,
        )

        if is_canonical_blood_parameters(blood_params):
            return BloodParametersReadService.extract_canonical_value_and_range(
                blood_params,
                parameter_key=test.parameter_key,
                gender=gender,
                catalog_lower_male=test.low_risk_lower_range_male,
                catalog_higher_male=test.low_risk_higher_range_male,
                catalog_lower_female=test.low_risk_lower_range_female,
                catalog_higher_female=test.low_risk_higher_range_female,
            )

        if is_legacy_metsights_flat_format(blood_params):
            value: float | None = None
            lower_range: float | None = None
            higher_range: float | None = None
            param_key = test.parameter_key
            if not param_key:
                return None, None, None
            raw_val = blood_params.get(param_key)
            if raw_val is not None:
                try:
                    value = float(raw_val)
                except (TypeError, ValueError):
                    pass
            normalized_gender = (gender or "").strip().lower()
            if normalized_gender in ("male", "m", "1"):
                if test.low_risk_lower_range_male is not None:
                    lower_range = float(test.low_risk_lower_range_male)
                if test.low_risk_higher_range_male is not None:
                    higher_range = float(test.low_risk_higher_range_male)
            elif normalized_gender in ("female", "f", "2"):
                if test.low_risk_lower_range_female is not None:
                    lower_range = float(test.low_risk_lower_range_female)
                if test.low_risk_higher_range_female is not None:
                    higher_range = float(test.low_risk_higher_range_female)
            return value, lower_range, higher_range

        if not is_legacy_healthians_format(blood_params):
            return None, None, None

        value = None
        lower_range = None
        higher_range = None
        healthians_pid = test.healthians_parameter_id
        if healthians_pid is None:
            return None, None, None
        for entry in blood_params["digital_data"]:
            entry_pid = entry.get("parameter_id")
            if entry_pid is not None and str(entry_pid) == str(healthians_pid):
                raw_val = entry.get("value")
                if raw_val is not None:
                    try:
                        value = float(raw_val)
                    except (TypeError, ValueError):
                        pass
                raw_min = entry.get("min_range")
                if raw_min is not None:
                    try:
                        lower_range = float(raw_min)
                    except (TypeError, ValueError):
                        pass
                raw_max = entry.get("max_range")
                if raw_max is not None:
                    try:
                        higher_range = float(raw_max)
                    except (TypeError, ValueError):
                        pass
                break

        return value, lower_range, higher_range

    async def _build_section_payload(
        self,
        db: AsyncSession,
        *,
        section_key: str,
        camp_no: int,
        department: str | None,
        camp_start_date: date | None,
        camp_end_date: date | None,
    ) -> dict:
        participation_reference = camp_start_date or date.today()
        age_reference = camp_end_date or date.today()

        if section_key == "participation_by_age":
            users = await self._repository.list_distinct_enrolled_users(
                db,
                camp_no=camp_no,
                department=department,
            )
            return build_participation_by_age(users, reference_date=participation_reference)

        if section_key == "kpis":
            metrics = await self._repository.compute_kpi_metrics(
                db,
                camp_no=camp_no,
                department=department,
                age_reference_date=age_reference,
            )
            return build_kpis(metrics)

        if section_key == "overall_risk_score":
            scores = await self._repository.list_metabolic_scores(
                db,
                camp_no=camp_no,
                department=department,
            )
            return build_overall_risk_score(scores)

        if section_key == "distribution_by_oxidative_stress":
            scores = await self._repository.list_oxidative_stress_scores(
                db,
                camp_no=camp_no,
                department=department,
            )
            return build_distribution_by_oxidative_stress(scores)

        if section_key == "distribution_by_physical_activity_frequency":
            rows = await self._repository.list_physical_activity_frequency_by_gender(
                db,
                camp_no=camp_no,
                department=department,
            )
            return build_distribution_by_physical_activity_frequency(rows)

        if section_key == "distribution_by_sleeping_hours":
            rows = await self._repository.list_sleeping_hours_by_gender(
                db,
                camp_no=camp_no,
                department=department,
            )
            return build_distribution_by_sleeping_hours(rows)

        if section_key == "distribution_by_gender_by_metabolic_syndrome":
            rows = await self._repository.list_health_reports_by_gender(
                db,
                camp_no=camp_no,
                department=department,
            )
            return build_distribution_by_gender_by_metabolic_syndrome(rows)

        if section_key == "positive_wins":
            return await self._compute_positive_wins_payload(
                db,
                camp_no=camp_no,
                department=department,
            )

        if section_key == "company_average_scores":
            return await self._compute_company_average_scores_payload(
                db,
                camp_no=camp_no,
                department=department,
            )

        if section_key == "blood_and_lab_intelligence":
            return await self._compute_blood_and_lab_intelligence_payload(
                db,
                camp_no=camp_no,
                department=department,
            )

        raise AppError(
            status_code=400,
            error_code="SECTION_NOT_IMPLEMENTED",
            message="Report section is not implemented",
        )
