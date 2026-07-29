"""Database access for camp reports."""

from __future__ import annotations

import itertools
import json
from dataclasses import dataclass
from datetime import date
from typing import Any

from sqlalchemy import and_, case, delete, extract, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.engagements.models import Engagement, EngagementParticipant
from modules.experts.models import ConsultationBooking, ExpertTypeModel
from modules.organizations.models import Organization
from modules.questionnaire.models import QuestionnaireDefinition, QuestionnaireResponse
from modules.reports.camp_report_section_builders import extract_metabolic_age, extract_metabolic_score, extract_oxidative_stress_score, is_high_metabolic_risk, resolve_user_age
from modules.reports.models import CampReport, IndividualHealthReport
from modules.users.models import User

_MALE_GENDERS = ("male", "m", "1")
_FEMALE_GENDERS = ("female", "f", "2")


def _coerce_reports_dict(reports: Any) -> dict[str, Any]:
    if isinstance(reports, dict):
        return reports
    if isinstance(reports, str) and reports.strip():
        try:
            parsed = json.loads(reports)
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _latest_report_order():
    """Prefer generated Bio AI JSON over empty report shells, then newest report_id."""
    return (
        case((IndividualHealthReport.reports.isnot(None), 0), else_=1),
        IndividualHealthReport.report_id.desc(),
    )

@dataclass
class EnrolledAssessmentContext:
    """Latest assessment context for one enrolled camp participant."""

    assessment_instance: AssessmentInstance
    package: AssessmentPackage
    engagement: Engagement
    individual_report: IndividualHealthReport | None
    user_gender: str | None


def _dedupe_enrolled_assessment_contexts(
    rows: list[tuple],
) -> list[EnrolledAssessmentContext]:
    """Collapse duplicate IHR outer-join rows; prefer a row that has blood_parameters."""
    by_id: dict[int, EnrolledAssessmentContext] = {}
    for ai, pkg, eng, ihr, gender in rows:
        aid = int(ai.assessment_instance_id)
        ctx = EnrolledAssessmentContext(
            assessment_instance=ai,
            package=pkg,
            engagement=eng,
            individual_report=ihr,
            user_gender=gender,
        )
        existing = by_id.get(aid)
        if existing is None:
            by_id[aid] = ctx
            continue
        existing_has_blood = (
            existing.individual_report is not None
            and existing.individual_report.blood_parameters is not None
        )
        new_has_blood = ihr is not None and ihr.blood_parameters is not None
        if new_has_blood and not existing_has_blood:
            by_id[aid] = ctx
    return list(by_id.values())


class CampReportsRepository:
    """CRUD queries for camp_reports."""

    @staticmethod
    def _enrolled_users_ranked_subquery(
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ):
        """Distinct enrolled users per camp (latest participant row per user_id)."""
        ranked_rows = (
            select(
                User.user_id,
                User.date_of_birth,
                User.age,
                User.gender,
                User.first_name,
                User.last_name,
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
        if city is not None:
            ranked_rows = ranked_rows.where(func.lower(func.trim(Engagement.city)) == city.lower())

        ranked = ranked_rows.subquery()
        return (
            select(
                ranked.c.user_id,
                ranked.c.date_of_birth,
                ranked.c.age,
                ranked.c.gender,
                ranked.c.first_name,
                ranked.c.last_name,
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
                CampReport.city.is_(None),
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
                CampReport.city.is_(None),
            )
        )
        return result.scalar_one_or_none()

    async def get_by_camp_no_and_city(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        city: str,
    ) -> CampReport | None:
        result = await db.execute(
            select(CampReport).where(
                CampReport.camp_no == camp_no,
                CampReport.department.is_(None),
                func.lower(CampReport.city) == city.lower(),
            )
        )
        return result.scalar_one_or_none()

    async def get_by_camp_no_city_and_department(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        city: str,
        department: str,
    ) -> CampReport | None:
        result = await db.execute(
            select(CampReport).where(
                CampReport.camp_no == camp_no,
                CampReport.department == department,
                func.lower(CampReport.city) == city.lower(),
            )
        )
        return result.scalar_one_or_none()

    async def list_distinct_cities_for_camp(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
    ) -> list[str]:
        result = await db.execute(
            select(Engagement.city)
            .where(
                Engagement.camp_no == camp_no,
                Engagement.city.isnot(None),
                func.trim(Engagement.city) != "",
            )
            .distinct()
            .order_by(Engagement.city.asc())
        )
        cities: list[str] = []
        seen: set[str] = set()
        for (raw,) in result.all():
            if raw is None:
                continue
            trimmed = str(raw).strip()
            if not trimmed:
                continue
            key = trimmed.lower()
            if key in seen:
                continue
            seen.add(key)
            cities.append(trimmed)
        return cities

    async def count_engagements_for_camp_city(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        city: str,
    ) -> int:
        result = await db.execute(
            select(func.count())
            .select_from(Engagement)
            .where(
                Engagement.camp_no == camp_no,
                func.lower(func.trim(Engagement.city)) == city.lower(),
            )
        )
        return int(result.scalar_one() or 0)

    async def list_by_camp_no(self, db: AsyncSession, *, camp_no: int) -> list[CampReport]:
        result = await db.execute(
            select(CampReport)
            .where(CampReport.camp_no == camp_no)
            .order_by(
                CampReport.city.is_(None).desc(),
                CampReport.city.asc(),
                CampReport.department.is_(None).desc(),
                CampReport.department.asc(),
            )
        )
        return list(result.scalars().all())

    async def list_all(self, db: AsyncSession) -> list[CampReport]:
        result = await db.execute(
            select(CampReport).order_by(
                CampReport.camp_no.asc(),
                CampReport.city.is_(None).desc(),
                CampReport.city.asc(),
                CampReport.department.is_(None).desc(),
                CampReport.department.asc(),
            )
        )
        return list(result.scalars().all())

    async def has_running_engagement(self, db: AsyncSession, *, camp_no: int) -> bool:
        result = await db.execute(
            select(Engagement.engagement_id)
            .where(
                Engagement.camp_no == camp_no,
                func.lower(func.trim(Engagement.status)) == "running",
            )
            .limit(1)
        )
        return result.scalar_one_or_none() is not None

    async def create(self, db: AsyncSession, row: CampReport) -> CampReport:
        db.add(row)
        await db.flush()
        return row

    async def update_report(self, db: AsyncSession, row: CampReport, report: dict) -> CampReport:
        row.report = report
        await db.flush()
        return row

    async def update_report_and_bts(
        self,
        db: AsyncSession,
        row: CampReport,
        report: dict,
        report_bts: dict,
    ) -> CampReport:
        row.report = report
        row.report_bts = report_bts
        await db.flush()
        return row

    async def list_distinct_enrolled_users(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ) -> list[tuple[int, date | None, int]]:
        """Return distinct (user_id, date_of_birth, age) enrolled in a camp."""
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department, city=city)
        query = select(
            enrolled.c.user_id,
            enrolled.c.date_of_birth,
            enrolled.c.age,
        )
        result = await db.execute(query)
        return [(int(r[0]), r[1], int(r[2])) for r in result.all()]

    async def list_distinct_enrolled_users_for_age_bts(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ) -> list[tuple[int, date | None, int, str | None, str | None, int]]:
        """Return distinct enrolled users with name + engagement for age BTS roster.

        Each row: (user_id, date_of_birth, age, first_name, last_name, engagement_id).
        """
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department, city=city)
        query = select(
            enrolled.c.user_id,
            enrolled.c.date_of_birth,
            enrolled.c.age,
            enrolled.c.first_name,
            enrolled.c.last_name,
            enrolled.c.engagement_id,
        )
        result = await db.execute(query)
        return [
            (int(r[0]), r[1], int(r[2]), r[3], r[4], int(r[5]))
            for r in result.all()
        ]

    async def count_engagements_for_camp_scope(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ) -> int:
        """Count engagements in scope for age BTS method stats.

        When department is set, counts engagements that have at least one
        participant in that department (and city, if set).
        """
        if department is None and city is None:
            return await self.count_engagements_for_camp_no(db, camp_no=camp_no)

        query = (
            select(func.count(func.distinct(Engagement.engagement_id)))
            .select_from(Engagement)
            .join(
                EngagementParticipant,
                EngagementParticipant.engagement_id == Engagement.engagement_id,
            )
            .where(Engagement.camp_no == camp_no)
        )
        if department is not None:
            query = query.where(EngagementParticipant.participant_department == department)
        if city is not None:
            query = query.where(func.lower(func.trim(Engagement.city)) == city.lower())
        result = await db.execute(query)
        return int(result.scalar_one())

    async def list_kpi_blood_candidates(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ) -> list[tuple[int, bool, str | None]]:
        """Distinct enrolled users: (user_id, has_booking_id, metsights_record_id_or_none).

        ``has_booking_id`` is True if ANY engagement_participant in the camp for that user
        has ``booking_id`` not null. ``metsights_record_id`` comes from the latest Basic/Pro
        (type 1/2) assessment instance for that user's enrolled engagement.
        """
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department, city=city)

        booking_users = (
            select(EngagementParticipant.user_id.label("user_id"))
            .select_from(Engagement)
            .join(
                EngagementParticipant,
                EngagementParticipant.engagement_id == Engagement.engagement_id,
            )
            .where(
                Engagement.camp_no == camp_no,
                EngagementParticipant.booking_id.isnot(None),
            )
            .distinct()
            .subquery()
        )

        ranked_assessments = (
            select(
                enrolled.c.user_id,
                AssessmentInstance.metsights_record_id,
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
            .join(AssessmentPackage, AssessmentPackage.package_id == AssessmentInstance.package_id)
            .where(AssessmentPackage.assessment_type_code.in_(("1", "2")))
        ).subquery()

        latest_assessment = (
            select(
                ranked_assessments.c.user_id,
                ranked_assessments.c.metsights_record_id,
            )
            .where(ranked_assessments.c.rn == 1)
            .subquery()
        )

        result = await db.execute(
            select(
                enrolled.c.user_id,
                booking_users.c.user_id.label("booking_user_id"),
                latest_assessment.c.metsights_record_id,
            )
            .select_from(enrolled)
            .outerjoin(booking_users, booking_users.c.user_id == enrolled.c.user_id)
            .outerjoin(latest_assessment, latest_assessment.c.user_id == enrolled.c.user_id)
        )

        rows: list[tuple[int, bool, str | None]] = []
        for user_id, booking_user_id, metsights_record_id in result.all():
            record_id = str(metsights_record_id).strip() if metsights_record_id else None
            if record_id == "":
                record_id = None
            rows.append((int(user_id), booking_user_id is not None, record_id))
        return rows

    async def compute_consultation_counts(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ) -> dict[str, int]:
        """Count distinct users wanting each expert type and every combination (size >= 2).

        Keys are expert ``type_key`` values (and sorted ``_``-joined combinations).
        A user counts for a combination when they have ``want=True`` for every type in it.
        """
        expert_types_result = await db.execute(
            select(ExpertTypeModel.type_key).order_by(ExpertTypeModel.type_key.asc())
        )
        type_keys = [str(row[0]).strip() for row in expert_types_result.all() if row[0]]
        type_keys = [key for key in type_keys if key]

        counts: dict[str, int] = {key: 0 for key in type_keys}
        for size in range(2, len(type_keys) + 1):
            for combo in itertools.combinations(type_keys, size):
                counts["_".join(combo)] = 0

        if not type_keys:
            return counts

        want_query = (
            select(
                EngagementParticipant.user_id,
                ConsultationBooking.expert_type,
            )
            .select_from(Engagement)
            .join(
                EngagementParticipant,
                EngagementParticipant.engagement_id == Engagement.engagement_id,
            )
            .join(
                ConsultationBooking,
                ConsultationBooking.engagement_participant_id
                == EngagementParticipant.engagement_participant_id,
            )
            .where(
                Engagement.camp_no == camp_no,
                ConsultationBooking.want.is_(True),
            )
        )
        if department is not None:
            want_query = want_query.where(
                EngagementParticipant.participant_department == department
            )
        if city is not None:
            want_query = want_query.where(
                func.lower(func.trim(Engagement.city)) == city.lower()
            )

        want_result = await db.execute(want_query)
        user_types: dict[int, set[str]] = {}
        known_types = set(type_keys)
        for user_id, expert_type in want_result.all():
            if expert_type is None:
                continue
            key = str(expert_type).strip()
            if key not in known_types:
                continue
            user_types.setdefault(int(user_id), set()).add(key)

        for types in user_types.values():
            for key in type_keys:
                if key in types:
                    counts[key] += 1
            for size in range(2, len(type_keys) + 1):
                for combo in itertools.combinations(type_keys, size):
                    if set(combo).issubset(types):
                        counts["_".join(combo)] += 1

        return counts

    async def compute_kpi_metrics(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
        age_reference_date: date,
        blood_tested_user_ids: set[int],
        blood_details: dict[str, int],
    ) -> dict:
        """Aggregate KPI counts for a camp (optionally scoped to a department).

        Blood totals come from the service (booking_id + Metsights collection checks).
        Consultation counts include all expert types and combinations.
        """
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department, city=city)

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

        consultations = await self.compute_consultation_counts(
            db,
            camp_no=camp_no,
            department=department,
            city=city,
        )
        doctor_consultation = int(consultations.get("doctor", 0))
        nutritionist_consultation = int(consultations.get("nutritionist", 0))
        doctor_and_nutritionist_consultation = int(
            consultations.get("doctor_nutritionist", 0)
        )

        ranked_reports = (
            select(
                enrolled.c.user_id,
                enrolled.c.date_of_birth,
                enrolled.c.age,
                IndividualHealthReport.reports,
                func.row_number()
                .over(
                    partition_by=enrolled.c.user_id,
                    order_by=_latest_report_order(),
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
            "total_blood_test": len(blood_tested_user_ids),
            "consultations": consultations,
            "doctor_consultation": doctor_consultation,
            "nutritionist_consultation": nutritionist_consultation,
            "doctor_and_nutritionist_consultation": doctor_and_nutritionist_consultation,
            "high_risk_group": high_risk_group,
            "blood_details": dict(blood_details),
        }

    async def list_metabolic_scores(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ) -> list[float]:
        """Return metabolic scores for enrolled users with Pro/Basic health reports."""
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department, city=city)

        ranked_reports = (
            select(
                enrolled.c.user_id,
                IndividualHealthReport.reports,
                func.row_number()
                .over(
                    partition_by=enrolled.c.user_id,
                    order_by=_latest_report_order(),
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
            score = extract_metabolic_score(_coerce_reports_dict(reports))
            if score is not None:
                scores.append(score)
        return scores

    async def count_bio_ai_reports(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ) -> int:
        """Count enrolled users whose latest Pro/Basic report has generated Bio AI JSON.

        Empty ``individual_health_report`` shells (``reports`` null/empty) are excluded —
        those rows are not treated as Bio AI generated.
        """
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department, city=city)

        ranked_reports = (
            select(
                enrolled.c.user_id,
                IndividualHealthReport.reports,
                func.row_number()
                .over(
                    partition_by=enrolled.c.user_id,
                    order_by=_latest_report_order(),
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
        return sum(1 for (reports,) in reports_result.all() if _coerce_reports_dict(reports))

    async def count_enrolled_users(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ) -> int:
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department, city=city)
        result = await db.execute(select(func.count()).select_from(enrolled))
        return int(result.scalar_one())

    async def count_any_questionnaire_responders_by_gender(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ) -> dict[str, int]:
        """Count enrolled users with at least one questionnaire answer in this camp."""
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department, city=city)

        answered = (
            select(AssessmentInstance.user_id.label("user_id"))
            .select_from(AssessmentInstance)
            .join(
                Engagement,
                and_(
                    Engagement.engagement_id == AssessmentInstance.engagement_id,
                    Engagement.camp_no == camp_no,
                ),
            )
            .join(
                QuestionnaireResponse,
                QuestionnaireResponse.assessment_instance_id
                == AssessmentInstance.assessment_instance_id,
            )
            .distinct()
            .subquery()
        )

        result = await db.execute(
            select(enrolled.c.gender)
            .select_from(enrolled)
            .join(answered, answered.c.user_id == enrolled.c.user_id)
        )

        counts = {"male": 0, "female": 0, "total": 0, "other": 0}
        for (gender_raw,) in result.all():
            counts["total"] += 1
            normalized = str(gender_raw).strip().lower() if gender_raw is not None else ""
            if normalized in _MALE_GENDERS:
                counts["male"] += 1
            elif normalized in _FEMALE_GENDERS:
                counts["female"] += 1
            else:
                counts["other"] += 1
        return counts

    async def list_metabolic_score_status(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ) -> list[tuple[int, str | None, str | None, str | None, float | None, str | None]]:
        """Return (user_id, first_name, last_name, gender, score, reason) for enrolled users.

        ``reason`` is None when a metabolic_score is present.
        """
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department, city=city)

        ranked_reports = (
            select(
                enrolled.c.user_id,
                IndividualHealthReport.reports,
                func.row_number()
                .over(
                    partition_by=enrolled.c.user_id,
                    order_by=_latest_report_order(),
                )
                .label("rn"),
            )
            .select_from(enrolled)
            .join(AssessmentInstance, AssessmentInstance.user_id == enrolled.c.user_id)
            .join(
                Engagement,
                and_(
                    Engagement.engagement_id == AssessmentInstance.engagement_id,
                    Engagement.camp_no == camp_no,
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

        latest_report = (
            select(ranked_reports.c.user_id, ranked_reports.c.reports)
            .where(ranked_reports.c.rn == 1)
            .subquery()
        )

        ranked_instances = (
            select(
                enrolled.c.user_id,
                func.row_number()
                .over(
                    partition_by=enrolled.c.user_id,
                    order_by=AssessmentInstance.assessment_instance_id.desc(),
                )
                .label("rn"),
            )
            .select_from(enrolled)
            .join(AssessmentInstance, AssessmentInstance.user_id == enrolled.c.user_id)
            .join(
                Engagement,
                and_(
                    Engagement.engagement_id == AssessmentInstance.engagement_id,
                    Engagement.camp_no == camp_no,
                ),
            )
            .join(AssessmentPackage, AssessmentPackage.package_id == AssessmentInstance.package_id)
            .where(AssessmentPackage.assessment_type_code.in_(("1", "2")))
        ).subquery()

        bio_ai_users = (
            select(ranked_instances.c.user_id).where(ranked_instances.c.rn == 1).subquery()
        )

        query = (
            select(
                enrolled.c.user_id,
                User.first_name,
                User.last_name,
                enrolled.c.gender,
                latest_report.c.reports,
                bio_ai_users.c.user_id.label("bio_ai_user_id"),
            )
            .select_from(enrolled)
            .join(User, User.user_id == enrolled.c.user_id)
            .outerjoin(latest_report, latest_report.c.user_id == enrolled.c.user_id)
            .outerjoin(bio_ai_users, bio_ai_users.c.user_id == enrolled.c.user_id)
        )

        result = await db.execute(query)
        rows: list[tuple[int, str | None, str | None, str | None, float | None, str | None]] = []
        for user_id, first_name, last_name, gender, reports, bio_ai_user_id in result.all():
            if bio_ai_user_id is None:
                rows.append(
                    (
                        int(user_id),
                        first_name,
                        last_name,
                        gender,
                        None,
                        "No Metsights Basic/Pro assessment instance for this camp",
                    )
                )
                continue
            if reports is None:
                rows.append(
                    (
                        int(user_id),
                        first_name,
                        last_name,
                        gender,
                        None,
                        "Bio AI not generated — report row missing or reports JSON is null (empty shell excluded from Overall Risk Score)",
                    )
                )
                continue
            reports_dict = _coerce_reports_dict(reports)
            if not reports_dict:
                rows.append(
                    (
                        int(user_id),
                        first_name,
                        last_name,
                        gender,
                        None,
                        "Bio AI not generated — reports JSON is empty (excluded from Overall Risk Score)",
                    )
                )
                continue
            score = extract_metabolic_score(reports_dict)
            if score is None:
                rows.append(
                    (
                        int(user_id),
                        first_name,
                        last_name,
                        gender,
                        None,
                        "Bio AI generated but metabolic_score field is missing from reports JSON",
                    )
                )
            else:
                rows.append((int(user_id), first_name, last_name, gender, score, None))
        return rows

    async def list_oxidative_stress_scores(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ) -> list[float]:
        """Return oxidative stress scores for enrolled users with Pro/Basic health reports."""
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department, city=city)

        ranked_reports = (
            select(
                enrolled.c.user_id,
                IndividualHealthReport.reports,
                func.row_number()
                .over(
                    partition_by=enrolled.c.user_id,
                    order_by=_latest_report_order(),
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
            score = extract_oxidative_stress_score(_coerce_reports_dict(reports))
            if score is not None:
                scores.append(score)
        return scores

    async def list_health_reports_by_gender(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ) -> list[tuple[str | None, dict[str, Any]]]:
        """Return (gender, reports) for enrolled users with latest Pro/Basic health reports."""
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department, city=city)

        ranked_reports = (
            select(
                enrolled.c.gender,
                IndividualHealthReport.reports,
                func.row_number()
                .over(
                    partition_by=enrolled.c.user_id,
                    order_by=_latest_report_order(),
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
        city: str | None = None,
    ) -> list[tuple[str | None, Any]]:
        """Return (gender, blood_parameters) for enrolled users with blood data."""
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department, city=city)

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
        city: str | None = None,
    ) -> list[tuple[str | None, object | None]]:
        """Return (gender, answer) for enrolled users with physical_activity_frequency responses."""
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department, city=city)

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
        city: str | None = None,
    ) -> list[tuple[str | None, object | None]]:
        """Return (gender, answer) for enrolled users with sleeping_hours responses."""
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department, city=city)

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
        city: str | None = None,
    ) -> list[tuple[int, str | None, str | None, str | None, object | None]]:
        """Return (user_id, first_name, last_name, gender, answer) for ALL enrolled users.

        Uses LEFT JOIN so users without a questionnaire response for the given
        question_key still appear with answer=NULL.
        """
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department, city=city)

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
                CampReport.city.is_(None),
            )
        )
        return int(result.rowcount or 0)

    async def delete_by_department(self, db: AsyncSession, *, camp_no: int, department: str) -> int:
        result = await db.execute(
            delete(CampReport).where(
                CampReport.camp_no == camp_no,
                CampReport.department == department,
                CampReport.city.is_(None),
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
        department: str | None = None,
        city: str | None = None,
    ) -> list[tuple]:
        """Fetch engagement participant enrollment rows for a camp (optionally scoped)."""
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
        )
        if department is not None:
            query = query.where(EngagementParticipant.participant_department == department)
        if city is not None:
            query = query.where(func.lower(func.trim(Engagement.city)) == city.lower())

        query = (
            query.order_by(EngagementParticipant.engagement_participant_id.asc())
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
        city: str | None = None,
    ) -> list[EnrolledAssessmentContext]:
        """Latest assessment + report context per enrolled user in a camp."""
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department, city=city)

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
        return _dedupe_enrolled_assessment_contexts(list(result.all()))

    async def list_fitprint_assessment_contexts(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ) -> list[EnrolledAssessmentContext]:
        """Latest FitPrint (type_code '7') assessment context per enrolled user in a camp."""
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department, city=city)

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
        return _dedupe_enrolled_assessment_contexts(list(result.all()))

    async def count_fitprint_assessment_contexts(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ) -> int:
        """Count enrolled users with a FitPrint (type_code '7') assessment in a camp."""
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department, city=city)

        ranked = (
            select(
                AssessmentInstance.assessment_instance_id.label("assessment_instance_id"),
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

        query = select(func.count()).select_from(ranked).where(ranked.c.rn == 1)
        result = await db.execute(query)
        return int(result.scalar_one())

    async def list_health_assessment_contexts(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ) -> list[EnrolledAssessmentContext]:
        """Latest health (type_code '1' or '2') assessment context per enrolled user."""
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department, city=city)

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
        return _dedupe_enrolled_assessment_contexts(list(result.all()))

    async def count_health_assessment_contexts(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ) -> int:
        """Count enrolled users with a health (type_code '1' or '2') assessment in a camp."""
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department, city=city)

        ranked = (
            select(
                AssessmentInstance.assessment_instance_id.label("assessment_instance_id"),
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

        query = select(func.count()).select_from(ranked).where(ranked.c.rn == 1)
        result = await db.execute(query)
        return int(result.scalar_one())

    async def list_enrolled_users_without_fitprint(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ) -> list[tuple[int, str | None, str | None]]:
        """Enrolled camp users who have NO FitPrint (type_code '7') assessment.

        Returns list of (user_id, first_name, last_name).
        """
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department, city=city)

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

    async def count_participants_by_camp_no(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ) -> int:
        """Count engagement participant enrollment rows for a camp (optionally scoped)."""
        query = (
            select(func.count())
            .select_from(Engagement)
            .join(
                EngagementParticipant,
                EngagementParticipant.engagement_id == Engagement.engagement_id,
            )
            .where(Engagement.camp_no == camp_no)
        )
        if department is not None:
            query = query.where(EngagementParticipant.participant_department == department)
        if city is not None:
            query = query.where(func.lower(func.trim(Engagement.city)) == city.lower())

        result = await db.execute(query)
        return int(result.scalar_one())

    async def get_camp_city_year(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        city: str,
    ) -> int | None:
        """Return the earliest engagement start year for this camp in the given city."""
        result = await db.execute(
            select(func.min(extract("year", Engagement.start_date)))
            .where(
                Engagement.camp_no == camp_no,
                Engagement.start_date.isnot(None),
                Engagement.city.isnot(None),
                func.lower(func.trim(Engagement.city)) == city.strip().lower(),
            )
        )
        year = result.scalar_one_or_none()
        return int(year) if year is not None else None

    async def list_camp_avg_metabolic_scores_by_city(
        self,
        db: AsyncSession,
        *,
        city: str,
        year: int,
    ) -> list[dict]:
        """For ranking: avg metabolic score per camp held in ``city`` in ``year``.

        A camp is a peer if it has ≥1 engagement with matching ``engagement.city``
        (case-insensitive) and ``extract(year, start_date) == year``.

        Scores use city-scoped participants (same as ``list_metabolic_scores`` with city).

        Returns a list of dicts:
          { "camp_no": int, "organization_id": int, "industry_key": str|None, "avg_score": float }
        """
        city_norm = city.strip().lower()
        peer_result = await db.execute(
            select(
                Engagement.camp_no,
                func.min(Engagement.organization_id).label("organization_id"),
            )
            .where(
                Engagement.camp_no.isnot(None),
                Engagement.city.isnot(None),
                func.lower(func.trim(Engagement.city)) == city_norm,
                extract("year", Engagement.start_date) == year,
            )
            .group_by(Engagement.camp_no)
        )
        peer_rows = peer_result.all()
        if not peer_rows:
            return []

        org_ids = {int(org_id) for _, org_id in peer_rows if org_id is not None}
        org_industry: dict[int, str | None] = {}
        if org_ids:
            org_result = await db.execute(
                select(Organization.organization_id, Organization.industry_key).where(
                    Organization.organization_id.in_(org_ids)
                )
            )
            org_industry = {int(oid): ik for oid, ik in org_result.all()}

        city_key = city.strip()
        out: list[dict] = []
        for camp_no_val, org_id in peer_rows:
            if camp_no_val is None or org_id is None:
                continue
            camp_no_int = int(camp_no_val)
            org_id_int = int(org_id)
            scores = await self.list_metabolic_scores(
                db,
                camp_no=camp_no_int,
                department=None,
                city=city_key,
            )
            if not scores:
                continue
            out.append(
                {
                    "camp_no": camp_no_int,
                    "organization_id": org_id_int,
                    "industry_key": org_industry.get(org_id_int),
                    "avg_score": round(sum(scores) / len(scores), 2),
                }
            )
        return out
