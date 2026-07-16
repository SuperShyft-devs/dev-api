"""Database access for camp reports."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from sqlalchemy import and_, delete, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.engagements.models import Engagement, EngagementParticipant
from modules.organizations.models import Organization
from modules.questionnaire.models import QuestionnaireDefinition, QuestionnaireResponse
from modules.reports.camp_report_section_builders import extract_metabolic_age, extract_metabolic_score, extract_oxidative_stress_score, is_high_metabolic_risk, resolve_user_age
from modules.reports.models import CampReport, IndividualHealthReport
from modules.users.models import User

_MALE_GENDERS = ("male", "m", "1")
_FEMALE_GENDERS = ("female", "f", "2")


@dataclass
class EnrolledAssessmentContext:
    """Latest assessment context for one enrolled camp participant."""

    assessment_instance: AssessmentInstance
    package: AssessmentPackage
    engagement: Engagement
    individual_report: IndividualHealthReport | None
    user_gender: str | None


class CampReportsRepository:
    """CRUD queries for camp_reports."""

    @staticmethod
    def _enrolled_users_ranked_subquery(*, camp_no: int, department: str | None = None):
        """Distinct enrolled users per camp (latest participant row per user_id)."""
        ranked_rows = (
            select(
                User.user_id,
                User.date_of_birth,
                User.age,
                User.gender,
                Engagement.engagement_id,
                func.row_number()
                .over(
                    partition_by=EngagementParticipant.user_id,
                    order_by=EngagementParticipant.engagement_participant_id.desc(),
                )
                .label("rn"),
            )
            .select_from(Engagement)
            .join(
                EngagementParticipant,
                EngagementParticipant.engagement_id == Engagement.engagement_id,
            )
            .join(User, User.user_id == EngagementParticipant.user_id)
            .where(Engagement.camp_no == camp_no)
        )
        if department is not None:
            ranked_rows = ranked_rows.where(EngagementParticipant.participant_department == department)

        ranked = ranked_rows.subquery()
        return (
            select(
                ranked.c.user_id,
                ranked.c.date_of_birth,
                ranked.c.age,
                ranked.c.gender,
                ranked.c.engagement_id,
            )
            .where(ranked.c.rn == 1)
            .subquery()
        )

    async def get_camp_context(self, db: AsyncSession, *, camp_no: int) -> tuple | None:
        """Return (organization_id, organization_name, start_date, end_date) for a camp."""
        result = await db.execute(
            select(
                Engagement.organization_id,
                Organization.name.label("organization_name"),
                func.min(Engagement.start_date).label("camp_start_date"),
                func.max(Engagement.end_date).label("camp_end_date"),
            )
            .select_from(Engagement)
            .join(Organization, Organization.organization_id == Engagement.organization_id)
            .where(Engagement.camp_no == camp_no)
            .group_by(Engagement.organization_id, Organization.name)
        )
        row = result.one_or_none()
        return tuple(row) if row is not None else None

    async def get_overall_by_camp_no(self, db: AsyncSession, *, camp_no: int) -> CampReport | None:
        result = await db.execute(
            select(CampReport).where(
                CampReport.camp_no == camp_no,
                CampReport.department.is_(None),
            )
        )
        return result.scalar_one_or_none()

    async def get_by_camp_no_and_department(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str,
    ) -> CampReport | None:
        result = await db.execute(
            select(CampReport).where(
                CampReport.camp_no == camp_no,
                CampReport.department == department,
            )
        )
        return result.scalar_one_or_none()

    async def list_by_camp_no(self, db: AsyncSession, *, camp_no: int) -> list[CampReport]:
        result = await db.execute(
            select(CampReport)
            .where(CampReport.camp_no == camp_no)
            .order_by(CampReport.department.is_(None).desc(), CampReport.department.asc())
        )
        return list(result.scalars().all())

    async def create(self, db: AsyncSession, row: CampReport) -> CampReport:
        db.add(row)
        await db.flush()
        return row

    async def update_report(self, db: AsyncSession, row: CampReport, report: dict) -> CampReport:
        row.report = report
        await db.flush()
        return row

    async def list_distinct_enrolled_users(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
    ) -> list[tuple[int, date | None, int]]:
        """Return distinct (user_id, date_of_birth, age) enrolled in a camp."""
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department)
        query = select(
            enrolled.c.user_id,
            enrolled.c.date_of_birth,
            enrolled.c.age,
        )
        result = await db.execute(query)
        return [(int(r[0]), r[1], int(r[2])) for r in result.all()]

    async def compute_kpi_metrics(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        age_reference_date: date,
    ) -> dict[str, int]:
        """Aggregate KPI counts for a camp (optionally scoped to a department)."""
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department)

        employees_result = await db.execute(
            select(func.count()).select_from(enrolled)
        )
        employees_enrolled = int(employees_result.scalar_one())

        male_result = await db.execute(
            select(func.count())
            .select_from(enrolled)
            .where(func.lower(func.trim(enrolled.c.gender)).in_(_MALE_GENDERS))
        )
        male_enrolled = int(male_result.scalar_one())

        female_result = await db.execute(
            select(func.count())
            .select_from(enrolled)
            .where(func.lower(func.trim(enrolled.c.gender)).in_(_FEMALE_GENDERS))
        )
        female_enrolled = int(female_result.scalar_one())

        blood_result = await db.execute(
            select(func.count(func.distinct(enrolled.c.user_id)))
            .select_from(enrolled)
            .join(
                IndividualHealthReport,
                and_(
                    IndividualHealthReport.engagement_id == enrolled.c.engagement_id,
                    IndividualHealthReport.user_id == enrolled.c.user_id,
                ),
            )
            .where(IndividualHealthReport.blood_parameters.isnot(None))
        )
        total_blood_test = int(blood_result.scalar_one())

        doctor_query = (
            select(func.count(func.distinct(EngagementParticipant.user_id)))
            .select_from(Engagement)
            .join(
                EngagementParticipant,
                EngagementParticipant.engagement_id == Engagement.engagement_id,
            )
            .where(Engagement.camp_no == camp_no)
            .where(
                EngagementParticipant.consultations["doctor"]["want"].as_boolean().is_(True)
            )
        )
        if department is not None:
            doctor_query = doctor_query.where(EngagementParticipant.participant_department == department)
        doctor_result = await db.execute(doctor_query)
        doctor_consultation = int(doctor_result.scalar_one())

        nutritionist_query = (
            select(func.count(func.distinct(EngagementParticipant.user_id)))
            .select_from(Engagement)
            .join(
                EngagementParticipant,
                EngagementParticipant.engagement_id == Engagement.engagement_id,
            )
            .where(Engagement.camp_no == camp_no)
            .where(
                EngagementParticipant.consultations["nutritionist"]["want"].as_boolean().is_(True)
            )
        )
        if department is not None:
            nutritionist_query = nutritionist_query.where(
                EngagementParticipant.participant_department == department
            )
        nutritionist_result = await db.execute(nutritionist_query)
        nutritionist_consultation = int(nutritionist_result.scalar_one())

        both_query = (
            select(func.count(func.distinct(EngagementParticipant.user_id)))
            .select_from(Engagement)
            .join(
                EngagementParticipant,
                EngagementParticipant.engagement_id == Engagement.engagement_id,
            )
            .where(Engagement.camp_no == camp_no)
            .where(
                EngagementParticipant.consultations["doctor"]["want"].as_boolean().is_(True),
                EngagementParticipant.consultations["nutritionist"]["want"].as_boolean().is_(True),
            )
        )
        if department is not None:
            both_query = both_query.where(EngagementParticipant.participant_department == department)
        both_result = await db.execute(both_query)
        doctor_and_nutritionist_consultation = int(both_result.scalar_one())

        ranked_reports = (
            select(
                enrolled.c.user_id,
                enrolled.c.date_of_birth,
                enrolled.c.age,
                IndividualHealthReport.reports,
                func.row_number()
                .over(
                    partition_by=enrolled.c.user_id,
                    order_by=IndividualHealthReport.report_id.desc(),
                )
                .label("rn"),
            )
            .select_from(enrolled)
            .join(
                AssessmentInstance,
                and_(
                    AssessmentInstance.engagement_id == enrolled.c.engagement_id,
                    AssessmentInstance.user_id == enrolled.c.user_id,
                ),
            )
            .join(AssessmentPackage, AssessmentPackage.package_id == AssessmentInstance.package_id)
            .join(
                IndividualHealthReport,
                IndividualHealthReport.assessment_instance_id
                == AssessmentInstance.assessment_instance_id,
            )
            .where(AssessmentPackage.assessment_type_code.in_(("1", "2")))
        ).subquery()

        reports_result = await db.execute(
            select(
                ranked_reports.c.user_id,
                ranked_reports.c.date_of_birth,
                ranked_reports.c.age,
                ranked_reports.c.reports,
            ).where(ranked_reports.c.rn == 1)
        )
        high_risk_group = 0
        for _user_id, dob, stored_age, reports in reports_result.all():
            reports_dict: dict[str, Any] = reports if isinstance(reports, dict) else {}
            metabolic_age = extract_metabolic_age(reports_dict)
            chronological_age = resolve_user_age(
                date_of_birth=dob,
                stored_age=int(stored_age),
                reference_date=age_reference_date,
            )
            if is_high_metabolic_risk(
                metabolic_age=metabolic_age,
                chronological_age=chronological_age,
            ):
                high_risk_group += 1

        return {
            "employees_enrolled": employees_enrolled,
            "male_enrolled": male_enrolled,
            "female_enrolled": female_enrolled,
            "total_blood_test": total_blood_test,
            "doctor_consultation": doctor_consultation,
            "nutritionist_consultation": nutritionist_consultation,
            "doctor_and_nutritionist_consultation": doctor_and_nutritionist_consultation,
            "high_risk_group": high_risk_group,
        }

    async def list_metabolic_scores(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
    ) -> list[float]:
        """Return metabolic scores for enrolled users with Pro/Basic health reports."""
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department)

        ranked_reports = (
            select(
                enrolled.c.user_id,
                IndividualHealthReport.reports,
                func.row_number()
                .over(
                    partition_by=enrolled.c.user_id,
                    order_by=IndividualHealthReport.report_id.desc(),
                )
                .label("rn"),
            )
            .select_from(enrolled)
            .join(
                AssessmentInstance,
                and_(
                    AssessmentInstance.engagement_id == enrolled.c.engagement_id,
                    AssessmentInstance.user_id == enrolled.c.user_id,
                ),
            )
            .join(AssessmentPackage, AssessmentPackage.package_id == AssessmentInstance.package_id)
            .join(
                IndividualHealthReport,
                IndividualHealthReport.assessment_instance_id
                == AssessmentInstance.assessment_instance_id,
            )
            .where(AssessmentPackage.assessment_type_code.in_(("1", "2")))
        ).subquery()

        reports_result = await db.execute(
            select(ranked_reports.c.reports).where(ranked_reports.c.rn == 1)
        )

        scores: list[float] = []
        for (reports,) in reports_result.all():
            reports_dict: dict[str, Any] = reports if isinstance(reports, dict) else {}
            score = extract_metabolic_score(reports_dict)
            if score is not None:
                scores.append(score)
        return scores

    async def list_oxidative_stress_scores(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
    ) -> list[float]:
        """Return oxidative stress scores for enrolled users with Pro/Basic health reports."""
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department)

        ranked_reports = (
            select(
                enrolled.c.user_id,
                IndividualHealthReport.reports,
                func.row_number()
                .over(
                    partition_by=enrolled.c.user_id,
                    order_by=IndividualHealthReport.report_id.desc(),
                )
                .label("rn"),
            )
            .select_from(enrolled)
            .join(
                AssessmentInstance,
                and_(
                    AssessmentInstance.engagement_id == enrolled.c.engagement_id,
                    AssessmentInstance.user_id == enrolled.c.user_id,
                ),
            )
            .join(AssessmentPackage, AssessmentPackage.package_id == AssessmentInstance.package_id)
            .join(
                IndividualHealthReport,
                IndividualHealthReport.assessment_instance_id
                == AssessmentInstance.assessment_instance_id,
            )
            .where(AssessmentPackage.assessment_type_code.in_(("1", "2")))
        ).subquery()

        reports_result = await db.execute(
            select(ranked_reports.c.reports).where(ranked_reports.c.rn == 1)
        )

        scores: list[float] = []
        for (reports,) in reports_result.all():
            reports_dict: dict[str, Any] = reports if isinstance(reports, dict) else {}
            score = extract_oxidative_stress_score(reports_dict)
            if score is not None:
                scores.append(score)
        return scores

    async def list_health_reports_by_gender(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
    ) -> list[tuple[str | None, dict[str, Any]]]:
        """Return (gender, reports) for enrolled users with latest Pro/Basic health reports."""
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department)

        ranked_reports = (
            select(
                enrolled.c.gender,
                IndividualHealthReport.reports,
                func.row_number()
                .over(
                    partition_by=enrolled.c.user_id,
                    order_by=IndividualHealthReport.report_id.desc(),
                )
                .label("rn"),
            )
            .select_from(enrolled)
            .join(
                AssessmentInstance,
                and_(
                    AssessmentInstance.engagement_id == enrolled.c.engagement_id,
                    AssessmentInstance.user_id == enrolled.c.user_id,
                ),
            )
            .join(AssessmentPackage, AssessmentPackage.package_id == AssessmentInstance.package_id)
            .join(
                IndividualHealthReport,
                IndividualHealthReport.assessment_instance_id
                == AssessmentInstance.assessment_instance_id,
            )
            .where(AssessmentPackage.assessment_type_code.in_(("1", "2")))
        ).subquery()

        reports_result = await db.execute(
            select(ranked_reports.c.gender, ranked_reports.c.reports).where(ranked_reports.c.rn == 1)
        )

        rows: list[tuple[str | None, dict[str, Any]]] = []
        for gender, reports in reports_result.all():
            reports_dict: dict[str, Any] = reports if isinstance(reports, dict) else {}
            rows.append((gender, reports_dict))
        return rows

    async def list_blood_parameters_by_gender(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
    ) -> list[tuple[str | None, Any]]:
        """Return (gender, blood_parameters) for enrolled users with blood data."""
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department)

        ranked_reports = (
            select(
                enrolled.c.gender,
                IndividualHealthReport.blood_parameters,
                func.row_number()
                .over(
                    partition_by=enrolled.c.user_id,
                    order_by=IndividualHealthReport.report_id.desc(),
                )
                .label("rn"),
            )
            .select_from(enrolled)
            .join(
                IndividualHealthReport,
                and_(
                    IndividualHealthReport.engagement_id == enrolled.c.engagement_id,
                    IndividualHealthReport.user_id == enrolled.c.user_id,
                ),
            )
            .where(IndividualHealthReport.blood_parameters.isnot(None))
        ).subquery()

        result = await db.execute(
            select(ranked_reports.c.gender, ranked_reports.c.blood_parameters).where(ranked_reports.c.rn == 1)
        )

        rows: list[tuple[str | None, Any]] = []
        for gender, blood_params in result.all():
            if blood_params:
                rows.append((gender, blood_params))
        return rows

    async def list_physical_activity_frequency_by_gender(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
    ) -> list[tuple[str | None, object | None]]:
        """Return (gender, answer) for enrolled users with physical_activity_frequency responses."""
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department)

        ranked_instances = (
            select(
                enrolled.c.user_id,
                enrolled.c.gender,
                QuestionnaireResponse.answer,
                func.row_number()
                .over(
                    partition_by=enrolled.c.user_id,
                    order_by=AssessmentInstance.assessment_instance_id.desc(),
                )
                .label("rn"),
            )
            .select_from(enrolled)
            .join(
                AssessmentInstance,
                AssessmentInstance.user_id == enrolled.c.user_id,
            )
            .join(
                Engagement,
                and_(
                    Engagement.engagement_id == AssessmentInstance.engagement_id,
                    Engagement.camp_no == camp_no,
                ),
            )
            .join(
                QuestionnaireResponse,
                QuestionnaireResponse.assessment_instance_id == AssessmentInstance.assessment_instance_id,
            )
            .join(
                QuestionnaireDefinition,
                QuestionnaireDefinition.question_id == QuestionnaireResponse.question_id,
            )
            .where(QuestionnaireDefinition.question_key == "physical_activity_frequency")
        ).subquery()

        result = await db.execute(
            select(ranked_instances.c.gender, ranked_instances.c.answer).where(ranked_instances.c.rn == 1)
        )
        return [(row[0], row[1]) for row in result.all()]

    async def list_sleeping_hours_by_gender(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
    ) -> list[tuple[str | None, object | None]]:
        """Return (gender, answer) for enrolled users with sleeping_hours responses."""
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department)

        ranked_instances = (
            select(
                enrolled.c.user_id,
                enrolled.c.gender,
                QuestionnaireResponse.answer,
                func.row_number()
                .over(
                    partition_by=enrolled.c.user_id,
                    order_by=AssessmentInstance.assessment_instance_id.desc(),
                )
                .label("rn"),
            )
            .select_from(enrolled)
            .join(
                AssessmentInstance,
                AssessmentInstance.user_id == enrolled.c.user_id,
            )
            .join(
                Engagement,
                and_(
                    Engagement.engagement_id == AssessmentInstance.engagement_id,
                    Engagement.camp_no == camp_no,
                ),
            )
            .join(
                QuestionnaireResponse,
                QuestionnaireResponse.assessment_instance_id == AssessmentInstance.assessment_instance_id,
            )
            .join(
                QuestionnaireDefinition,
                QuestionnaireDefinition.question_id == QuestionnaireResponse.question_id,
            )
            .where(QuestionnaireDefinition.question_key == "sleeping_hours")
        ).subquery()

        result = await db.execute(
            select(ranked_instances.c.gender, ranked_instances.c.answer).where(ranked_instances.c.rn == 1)
        )
        return [(row[0], row[1]) for row in result.all()]

    async def list_enrolled_users_with_questionnaire_answer(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        question_key: str,
        department: str | None = None,
    ) -> list[tuple[int, str | None, str | None, str | None, object | None]]:
        """Return (user_id, first_name, last_name, gender, answer) for ALL enrolled users.

        Uses LEFT JOIN so users without a questionnaire response for the given
        question_key still appear with answer=NULL.
        """
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department)

        qr_subquery = (
            select(
                AssessmentInstance.user_id.label("qr_user_id"),
                QuestionnaireResponse.answer.label("qr_answer"),
                func.row_number()
                .over(
                    partition_by=AssessmentInstance.user_id,
                    order_by=AssessmentInstance.assessment_instance_id.desc(),
                )
                .label("qr_rn"),
            )
            .select_from(Engagement)
            .join(
                AssessmentInstance,
                AssessmentInstance.engagement_id == Engagement.engagement_id,
            )
            .join(
                QuestionnaireResponse,
                QuestionnaireResponse.assessment_instance_id == AssessmentInstance.assessment_instance_id,
            )
            .join(
                QuestionnaireDefinition,
                QuestionnaireDefinition.question_id == QuestionnaireResponse.question_id,
            )
            .where(
                Engagement.camp_no == camp_no,
                QuestionnaireDefinition.question_key == question_key,
            )
        ).subquery()

        latest_answer = (
            select(qr_subquery.c.qr_user_id, qr_subquery.c.qr_answer)
            .where(qr_subquery.c.qr_rn == 1)
            .subquery()
        )

        query = (
            select(
                enrolled.c.user_id,
                User.first_name,
                User.last_name,
                enrolled.c.gender,
                latest_answer.c.qr_answer,
            )
            .select_from(enrolled)
            .join(User, User.user_id == enrolled.c.user_id)
            .outerjoin(
                latest_answer,
                latest_answer.c.qr_user_id == enrolled.c.user_id,
            )
        )

        result = await db.execute(query)
        return [
            (int(row[0]), row[1], row[2], row[3], row[4])
            for row in result.all()
        ]

    async def delete_overall(self, db: AsyncSession, *, camp_no: int) -> int:
        result = await db.execute(
            delete(CampReport).where(
                CampReport.camp_no == camp_no,
                CampReport.department.is_(None),
            )
        )
        return int(result.rowcount or 0)

    async def delete_by_department(self, db: AsyncSession, *, camp_no: int, department: str) -> int:
        result = await db.execute(
            delete(CampReport).where(
                CampReport.camp_no == camp_no,
                CampReport.department == department,
            )
        )
        return int(result.rowcount or 0)

    async def delete_all_for_camp_no(self, db: AsyncSession, *, camp_no: int) -> int:
        result = await db.execute(delete(CampReport).where(CampReport.camp_no == camp_no))
        return int(result.rowcount or 0)

    async def count_engagements_for_camp_no(self, db: AsyncSession, *, camp_no: int) -> int:
        result = await db.execute(
            select(func.count())
            .select_from(Engagement)
            .where(Engagement.camp_no == camp_no)
        )
        return int(result.scalar_one())

    async def list_participants_by_camp_no(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        page: int,
        limit: int,
    ) -> list[tuple]:
        """Fetch all engagement participant enrollment rows for a camp."""
        offset = (page - 1) * limit

        query = (
            select(
                EngagementParticipant.engagement_participant_id,
                EngagementParticipant.engagement_id,
                User.user_id,
                User.first_name,
                User.last_name,
                User.phone,
                User.gender,
                EngagementParticipant.participant_blood_group,
                EngagementParticipant.participant_department,
            )
            .select_from(Engagement)
            .join(
                EngagementParticipant,
                EngagementParticipant.engagement_id == Engagement.engagement_id,
            )
            .join(User, User.user_id == EngagementParticipant.user_id)
            .where(Engagement.camp_no == camp_no)
            .order_by(EngagementParticipant.engagement_participant_id.asc())
            .offset(offset)
            .limit(limit)
        )

        result = await db.execute(query)
        return list(result.all())

    async def list_enrolled_assessment_contexts(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
    ) -> list[EnrolledAssessmentContext]:
        """Latest assessment + report context per enrolled user in a camp."""
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department)

        ranked = (
            select(
                AssessmentInstance.assessment_instance_id.label("assessment_instance_id"),
                enrolled.c.gender.label("user_gender"),
                func.row_number()
                .over(
                    partition_by=enrolled.c.user_id,
                    order_by=AssessmentInstance.assessment_instance_id.desc(),
                )
                .label("rn"),
            )
            .select_from(enrolled)
            .join(
                AssessmentInstance,
                and_(
                    AssessmentInstance.engagement_id == enrolled.c.engagement_id,
                    AssessmentInstance.user_id == enrolled.c.user_id,
                ),
            )
        ).subquery()

        query = (
            select(
                AssessmentInstance,
                AssessmentPackage,
                Engagement,
                IndividualHealthReport,
                ranked.c.user_gender,
            )
            .select_from(ranked)
            .join(
                AssessmentInstance,
                AssessmentInstance.assessment_instance_id == ranked.c.assessment_instance_id,
            )
            .join(AssessmentPackage, AssessmentPackage.package_id == AssessmentInstance.package_id)
            .join(Engagement, Engagement.engagement_id == AssessmentInstance.engagement_id)
            .outerjoin(
                IndividualHealthReport,
                IndividualHealthReport.assessment_instance_id
                == AssessmentInstance.assessment_instance_id,
            )
            .where(ranked.c.rn == 1)
        )

        result = await db.execute(query)
        return [
            EnrolledAssessmentContext(
                assessment_instance=ai,
                package=pkg,
                engagement=eng,
                individual_report=ihr,
                user_gender=gender,
            )
            for ai, pkg, eng, ihr, gender in result.all()
        ]

    async def list_fitprint_assessment_contexts(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
    ) -> list[EnrolledAssessmentContext]:
        """Latest FitPrint (type_code '7') assessment context per enrolled user in a camp."""
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department)

        ranked = (
            select(
                AssessmentInstance.assessment_instance_id.label("assessment_instance_id"),
                enrolled.c.gender.label("user_gender"),
                func.row_number()
                .over(
                    partition_by=enrolled.c.user_id,
                    order_by=AssessmentInstance.assessment_instance_id.desc(),
                )
                .label("rn"),
            )
            .select_from(enrolled)
            .join(
                AssessmentInstance,
                and_(
                    AssessmentInstance.engagement_id == enrolled.c.engagement_id,
                    AssessmentInstance.user_id == enrolled.c.user_id,
                ),
            )
            .join(
                AssessmentPackage,
                AssessmentPackage.package_id == AssessmentInstance.package_id,
            )
            .where(AssessmentPackage.assessment_type_code == "7")
        ).subquery()

        query = (
            select(
                AssessmentInstance,
                AssessmentPackage,
                Engagement,
                IndividualHealthReport,
                ranked.c.user_gender,
            )
            .select_from(ranked)
            .join(
                AssessmentInstance,
                AssessmentInstance.assessment_instance_id == ranked.c.assessment_instance_id,
            )
            .join(AssessmentPackage, AssessmentPackage.package_id == AssessmentInstance.package_id)
            .join(Engagement, Engagement.engagement_id == AssessmentInstance.engagement_id)
            .outerjoin(
                IndividualHealthReport,
                IndividualHealthReport.assessment_instance_id
                == AssessmentInstance.assessment_instance_id,
            )
            .where(ranked.c.rn == 1)
        )

        result = await db.execute(query)
        return [
            EnrolledAssessmentContext(
                assessment_instance=ai,
                package=pkg,
                engagement=eng,
                individual_report=ihr,
                user_gender=gender,
            )
            for ai, pkg, eng, ihr, gender in result.all()
        ]

    async def list_health_assessment_contexts(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
    ) -> list[EnrolledAssessmentContext]:
        """Latest health (type_code '1' or '2') assessment context per enrolled user."""
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department)

        ranked = (
            select(
                AssessmentInstance.assessment_instance_id.label("assessment_instance_id"),
                enrolled.c.gender.label("user_gender"),
                func.row_number()
                .over(
                    partition_by=enrolled.c.user_id,
                    order_by=AssessmentInstance.assessment_instance_id.desc(),
                )
                .label("rn"),
            )
            .select_from(enrolled)
            .join(
                AssessmentInstance,
                and_(
                    AssessmentInstance.engagement_id == enrolled.c.engagement_id,
                    AssessmentInstance.user_id == enrolled.c.user_id,
                ),
            )
            .join(
                AssessmentPackage,
                AssessmentPackage.package_id == AssessmentInstance.package_id,
            )
            .where(AssessmentPackage.assessment_type_code.in_(("1", "2")))
        ).subquery()

        query = (
            select(
                AssessmentInstance,
                AssessmentPackage,
                Engagement,
                IndividualHealthReport,
                ranked.c.user_gender,
            )
            .select_from(ranked)
            .join(
                AssessmentInstance,
                AssessmentInstance.assessment_instance_id == ranked.c.assessment_instance_id,
            )
            .join(AssessmentPackage, AssessmentPackage.package_id == AssessmentInstance.package_id)
            .join(Engagement, Engagement.engagement_id == AssessmentInstance.engagement_id)
            .outerjoin(
                IndividualHealthReport,
                IndividualHealthReport.assessment_instance_id
                == AssessmentInstance.assessment_instance_id,
            )
            .where(ranked.c.rn == 1)
        )

        result = await db.execute(query)
        return [
            EnrolledAssessmentContext(
                assessment_instance=ai,
                package=pkg,
                engagement=eng,
                individual_report=ihr,
                user_gender=gender,
            )
            for ai, pkg, eng, ihr, gender in result.all()
        ]

    async def list_enrolled_users_without_fitprint(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
    ) -> list[tuple[int, str | None, str | None]]:
        """Enrolled camp users who have NO FitPrint (type_code '7') assessment.

        Returns list of (user_id, first_name, last_name).
        """
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department)

        fitprint_users = (
            select(AssessmentInstance.user_id)
            .select_from(enrolled)
            .join(
                AssessmentInstance,
                and_(
                    AssessmentInstance.engagement_id == enrolled.c.engagement_id,
                    AssessmentInstance.user_id == enrolled.c.user_id,
                ),
            )
            .join(
                AssessmentPackage,
                AssessmentPackage.package_id == AssessmentInstance.package_id,
            )
            .where(AssessmentPackage.assessment_type_code == "7")
        ).subquery()

        query = (
            select(enrolled.c.user_id, User.first_name, User.last_name)
            .select_from(enrolled)
            .join(User, User.user_id == enrolled.c.user_id)
            .where(enrolled.c.user_id.notin_(select(fitprint_users.c.user_id)))
        )

        result = await db.execute(query)
        return [(int(r[0]), r[1], r[2]) for r in result.all()]

    async def count_participants_by_camp_no(self, db: AsyncSession, *, camp_no: int) -> int:
        """Count all engagement participant enrollment rows for a camp."""
        result = await db.execute(
            select(func.count())
            .select_from(Engagement)
            .join(
                EngagementParticipant,
                EngagementParticipant.engagement_id == Engagement.engagement_id,
            )
            .where(Engagement.camp_no == camp_no)
        )
        return int(result.scalar_one())

    async def list_org_avg_metabolic_scores_by_city(
        self,
        db: AsyncSession,
        *,
        city: str,
        current_year: int,
    ) -> list[dict]:
        """For ranking: return avg metabolic score per org for orgs in the given city
        that have at least one camp that started in current_year.

        Returns a list of dicts:
          { "organization_id": int, "industry_key": str|None, "avg_score": float, "camp_no": int }
        """
        from sqlalchemy import cast, extract, Integer, Float

        # Subquery: for each org in the city, find their latest camp_no in the current year
        latest_camp_sq = (
            select(
                Engagement.organization_id,
                func.max(Engagement.camp_no).label("latest_camp_no"),
            )
            .select_from(Engagement)
            .join(Organization, Organization.organization_id == Engagement.organization_id)
            .where(
                Organization.city.isnot(None),
                func.lower(func.trim(Organization.city)) == city.strip().lower(),
                Engagement.camp_no.isnot(None),
                extract("year", Engagement.start_date) == current_year,
            )
            .group_by(Engagement.organization_id)
        ).subquery()

        # Get all assessment instances for those latest camps
        ranked_reports_sq = (
            select(
                latest_camp_sq.c.organization_id,
                AssessmentInstance.user_id,
                IndividualHealthReport.reports,
                func.row_number()
                .over(
                    partition_by=[latest_camp_sq.c.organization_id, AssessmentInstance.user_id],
                    order_by=IndividualHealthReport.report_id.desc(),
                )
                .label("rn"),
            )
            .select_from(latest_camp_sq)
            .join(
                Engagement,
                and_(
                    Engagement.organization_id == latest_camp_sq.c.organization_id,
                    Engagement.camp_no == latest_camp_sq.c.latest_camp_no,
                ),
            )
            .join(
                AssessmentInstance,
                AssessmentInstance.engagement_id == Engagement.engagement_id,
            )
            .join(AssessmentPackage, AssessmentPackage.package_id == AssessmentInstance.package_id)
            .join(
                IndividualHealthReport,
                IndividualHealthReport.assessment_instance_id
                == AssessmentInstance.assessment_instance_id,
            )
            .where(AssessmentPackage.assessment_type_code.in_(("1", "2")))
        ).subquery()

        # Get latest report per user per org (rn == 1)
        latest_reports_sq = (
            select(
                ranked_reports_sq.c.organization_id,
                ranked_reports_sq.c.reports,
            )
            .where(ranked_reports_sq.c.rn == 1)
        ).subquery()

        result = await db.execute(
            select(
                latest_reports_sq.c.organization_id,
                latest_reports_sq.c.reports,
                latest_camp_sq.c.latest_camp_no,
                Organization.industry_key,
            )
            .select_from(latest_reports_sq)
            .join(latest_camp_sq, latest_camp_sq.c.organization_id == latest_reports_sq.c.organization_id)
            .join(Organization, Organization.organization_id == latest_reports_sq.c.organization_id)
        )

        # Group scores by org_id in Python, then average
        org_scores: dict[int, list[float]] = {}
        org_meta: dict[int, dict] = {}
        for org_id, reports, camp_no, industry_key in result.all():
            reports_dict: dict = reports if isinstance(reports, dict) else {}
            score = extract_metabolic_score(reports_dict)
            if score is None:
                continue
            if org_id not in org_scores:
                org_scores[org_id] = []
                org_meta[org_id] = {"camp_no": int(camp_no), "industry_key": industry_key}
            org_scores[org_id].append(score)

        out = []
        for org_id, scores in org_scores.items():
            if not scores:
                continue
            avg = round(sum(scores) / len(scores), 2)
            out.append({
                "organization_id": org_id,
                "industry_key": org_meta[org_id]["industry_key"],
                "avg_score": avg,
                "camp_no": org_meta[org_id]["camp_no"],
            })
        return out
