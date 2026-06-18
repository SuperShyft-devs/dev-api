"""Reports repository.

Only database queries belong here.
"""

from __future__ import annotations

from datetime import date

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.engagements.models import EngagementParticipant
from modules.reports.models import IndividualHealthReport, OrganizationHealthReport, ReportsUserSyncState
from modules.users.models import User

# MetSights Basic / Pro (excludes FitPrint and other types).
_METSIGHTS_PRO_BASIC_TYPE_CODES = ("1", "2")


class ReportsRepository:
    """Database access for reports."""

    async def get_individual_report_by_assessment(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
    ) -> IndividualHealthReport | None:
        # assessment_instance_id is not unique; concurrent cache writes can leave duplicates.
        # Always use the newest row so reads/updates stay consistent.
        result = await db.execute(
            select(IndividualHealthReport)
            .where(IndividualHealthReport.assessment_instance_id == assessment_instance_id)
            .order_by(IndividualHealthReport.report_id.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def create_individual_report(
        self,
        db: AsyncSession,
        report: IndividualHealthReport,
    ) -> IndividualHealthReport:
        db.add(report)
        await db.flush()
        return report

    async def update_individual_report(
        self,
        db: AsyncSession,
        report: IndividualHealthReport,
    ) -> IndividualHealthReport:
        db.add(report)
        await db.flush()
        return report

    async def delete_individual_reports_for_instance(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
    ) -> int:
        result = await db.execute(
            delete(IndividualHealthReport).where(
                IndividualHealthReport.assessment_instance_id == assessment_instance_id
            )
        )
        return int(result.rowcount or 0)

    async def list_individual_reports_for_user_with_assessment(
        self,
        db: AsyncSession,
        *,
        user_id: int,
    ) -> list[tuple[IndividualHealthReport, AssessmentInstance]]:
        result = await db.execute(
            select(IndividualHealthReport, AssessmentInstance)
            .join(
                AssessmentInstance,
                AssessmentInstance.assessment_instance_id == IndividualHealthReport.assessment_instance_id,
            )
            .where(IndividualHealthReport.user_id == user_id)
            .order_by(
                AssessmentInstance.completed_at.asc().nulls_last(),
                AssessmentInstance.assigned_at.asc().nulls_last(),
                AssessmentInstance.assessment_instance_id.asc(),
            )
        )
        return list(result.all())

    async def list_metsights_pro_basic_assessments_for_user(
        self,
        db: AsyncSession,
        *,
        user_id: int,
    ) -> list[tuple[AssessmentInstance, AssessmentPackage]]:
        result = await db.execute(
            select(AssessmentInstance, AssessmentPackage)
            .join(AssessmentPackage, AssessmentPackage.package_id == AssessmentInstance.package_id)
            .where(AssessmentInstance.user_id == user_id)
            .where(AssessmentInstance.metsights_record_id.is_not(None))
            .where(AssessmentInstance.metsights_record_id != "")
            .where(AssessmentPackage.assessment_type_code.in_(_METSIGHTS_PRO_BASIC_TYPE_CODES))
            .order_by(
                AssessmentInstance.completed_at.asc().nulls_last(),
                AssessmentInstance.assigned_at.asc().nulls_last(),
                AssessmentInstance.assessment_instance_id.asc(),
            )
        )
        return list(result.all())

    async def get_user_sync_state(self, db: AsyncSession, *, user_id: int) -> ReportsUserSyncState | None:
        result = await db.execute(select(ReportsUserSyncState).where(ReportsUserSyncState.user_id == user_id))
        return result.scalar_one_or_none()

    async def create_user_sync_state(
        self,
        db: AsyncSession,
        *,
        user_id: int,
    ) -> ReportsUserSyncState:
        row = ReportsUserSyncState(user_id=user_id, sync_status="idle")
        db.add(row)
        await db.flush()
        return row

    async def update_user_sync_state(
        self,
        db: AsyncSession,
        row: ReportsUserSyncState,
    ) -> ReportsUserSyncState:
        db.add(row)
        await db.flush()
        return row

    async def get_latest_assessment_with_record_id(
        self,
        db: AsyncSession,
        *,
        user_id: int,
    ) -> AssessmentInstance | None:
        # FitPrint (assessment_type_code "7") has no blood-parameters resource on Metsights records.
        result = await db.execute(
            select(AssessmentInstance)
            .join(AssessmentPackage, AssessmentPackage.package_id == AssessmentInstance.package_id)
            .where(AssessmentInstance.user_id == user_id)
            .where(AssessmentInstance.metsights_record_id.is_not(None))
            .where(AssessmentInstance.metsights_record_id != "")
            .where(func.coalesce(AssessmentPackage.assessment_type_code, "") != "7")
            .order_by(AssessmentInstance.assessment_instance_id.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def list_unsynced_assessments_with_record_id(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        after_assessment_instance_id: int,
    ) -> list[AssessmentInstance]:
        result = await db.execute(
            select(AssessmentInstance)
            .join(AssessmentPackage, AssessmentPackage.package_id == AssessmentInstance.package_id)
            .where(AssessmentInstance.user_id == user_id)
            .where(AssessmentInstance.metsights_record_id.is_not(None))
            .where(AssessmentInstance.metsights_record_id != "")
            .where(AssessmentInstance.assessment_instance_id > after_assessment_instance_id)
            .where(func.coalesce(AssessmentPackage.assessment_type_code, "") != "7")
            .order_by(AssessmentInstance.assessment_instance_id.asc())
        )
        return list(result.scalars().all())

    async def list_participants_for_engagements(
        self,
        db: AsyncSession,
        *,
        engagement_ids: list[int],
    ) -> list[EngagementParticipant]:
        if not engagement_ids:
            return []
        result = await db.execute(
            select(EngagementParticipant)
            .where(EngagementParticipant.engagement_id.in_(engagement_ids))
            .order_by(EngagementParticipant.engagement_participant_id.asc())
        )
        return list(result.scalars().all())

    async def list_individual_reports_for_engagements(
        self,
        db: AsyncSession,
        *,
        engagement_ids: list[int],
    ) -> list[IndividualHealthReport]:
        if not engagement_ids:
            return []
        result = await db.execute(
            select(IndividualHealthReport)
            .where(IndividualHealthReport.engagement_id.in_(engagement_ids))
            .order_by(IndividualHealthReport.report_id.asc())
        )
        return list(result.scalars().all())

    async def map_user_date_of_birth(
        self,
        db: AsyncSession,
        *,
        user_ids: list[int],
    ) -> dict[int, date | None]:
        if not user_ids:
            return {}

        result = await db.execute(
            select(User.user_id, User.date_of_birth).where(User.user_id.in_(user_ids))
        )
        mapped: dict[int, date | None] = {}
        for user_id, dob in result.all():
            mapped[int(user_id)] = dob
        return mapped

    async def get_camp_report(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
    ) -> OrganizationHealthReport | None:
        result = await db.execute(
            select(OrganizationHealthReport)
            .where(OrganizationHealthReport.camp_no == camp_no)
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def upsert_organization_camp_report(
        self,
        db: AsyncSession,
        *,
        organization_id: int,
        camp_no: int,
        camp_report: dict,
    ) -> OrganizationHealthReport:
        existing = await self.get_camp_report(
            db,
            camp_no=camp_no,
        )
        if existing is None:
            row = OrganizationHealthReport(
                organization_id=organization_id,
                camp_no=camp_no,
                camp_report=camp_report,
            )
            db.add(row)
            await db.flush()
            return row

        existing.camp_report = camp_report
        db.add(existing)
        await db.flush()
        return existing

    async def map_user_gender_by_id(
        self,
        db: AsyncSession,
        *,
        user_ids: list[int],
    ) -> dict[int, str | None]:
        """Return a {user_id: gender} mapping for the given user IDs."""
        if not user_ids:
            return {}
        result = await db.execute(
            select(User.user_id, User.gender).where(User.user_id.in_(user_ids))
        )
        return {int(uid): gender for uid, gender in result.all()}

    async def list_questionnaire_answers_for_participants(
        self,
        db: AsyncSession,
        *,
        engagement_ids: list[int],
        question_ids: list[int],
    ) -> dict[tuple[int, int], str]:
        """Return {(user_id, question_id): option_display_name} for the given engagements and questions.

        Fetches the latest questionnaire responses per user and translates option IDs
        to human-readable labels using questionnaire_options — all without any
        JSON-to-integer casts, which are fragile across different JSON storage formats.
        """
        from sqlalchemy import and_
        from modules.assessments.models import AssessmentInstance
        from modules.questionnaire.models import QuestionnaireOption, QuestionnaireResponse

        if not engagement_ids or not question_ids:
            return {}

        # Step 1: Find the latest assessment_instance_id per user per engagement.
        latest_ai = (
            select(
                AssessmentInstance.user_id,
                func.max(AssessmentInstance.assessment_instance_id).label("latest_ai_id"),
            )
            .where(AssessmentInstance.engagement_id.in_(engagement_ids))
            .group_by(AssessmentInstance.user_id)
            .subquery()
        )

        # Step 2: Fetch raw responses for those assessment instances and questions.
        resp_result = await db.execute(
            select(
                latest_ai.c.user_id,
                QuestionnaireResponse.question_id,
                QuestionnaireResponse.answer,
            )
            .join(
                QuestionnaireResponse,
                and_(
                    QuestionnaireResponse.assessment_instance_id == latest_ai.c.latest_ai_id,
                    QuestionnaireResponse.question_id.in_(question_ids),
                ),
            )
        )
        raw_responses = resp_result.all()
        if not raw_responses:
            return {}

        # Step 3: Collect all unique option_ids seen in the responses (handle both int and str JSON values).
        option_ids_needed: set[int] = set()
        for _, _, answer in raw_responses:
            try:
                option_ids_needed.add(int(str(answer).strip('"')))
            except (TypeError, ValueError):
                pass

        if not option_ids_needed:
            return {}

        # Step 4: Fetch matching option display names in one query.
        opt_result = await db.execute(
            select(
                QuestionnaireOption.question_id,
                QuestionnaireOption.option_id,
                QuestionnaireOption.display_name,
            )
            .where(
                QuestionnaireOption.question_id.in_(question_ids),
                QuestionnaireOption.option_id.in_(list(option_ids_needed)),
            )
        )
        # Build a lookup: {(question_id, option_id): display_name}
        option_lookup: dict[tuple[int, int], str] = {
            (int(qid), int(oid)): name
            for qid, oid, name in opt_result.all()
        }

        # Step 5: Combine into the final {(user_id, question_id): display_name} map.
        answers: dict[tuple[int, int], str] = {}
        for user_id, question_id, answer in raw_responses:
            try:
                option_id = int(str(answer).strip('"'))
            except (TypeError, ValueError):
                continue
            display = option_lookup.get((int(question_id), option_id))
            if display:
                answers[(int(user_id), int(question_id))] = display
        return answers

    async def list_health_parameters_with_ranges(
        self,
        db: AsyncSession,
    ) -> dict[str, dict]:
        """Return {parameter_key: {low_male, high_male, low_female, high_female}} for all active health parameters."""
        from modules.diagnostics.models import HealthParameter

        result = await db.execute(
            select(
                HealthParameter.parameter_key,
                HealthParameter.test_name,
                HealthParameter.low_risk_lower_range_male,
                HealthParameter.low_risk_higher_range_male,
                HealthParameter.low_risk_lower_range_female,
                HealthParameter.low_risk_higher_range_female,
            )
            .where(HealthParameter.parameter_key.isnot(None))
            .where(HealthParameter.is_available.is_(True))
        )

        params: dict[str, dict] = {}
        for row in result.all():
            key = str(row.parameter_key or "").strip()
            if not key:
                continue
            params[key] = {
                "test_name": row.test_name,
                "low_male": float(row.low_risk_lower_range_male) if row.low_risk_lower_range_male is not None else None,
                "high_male": float(row.low_risk_higher_range_male) if row.low_risk_higher_range_male is not None else None,
                "low_female": float(row.low_risk_lower_range_female) if row.low_risk_lower_range_female is not None else None,
                "high_female": float(row.low_risk_higher_range_female) if row.low_risk_higher_range_female is not None else None,
            }
        return params
