"""Database access for camp reports."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import String, and_, case, cast, delete, exists, extract, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import defer
from sqlalchemy.orm.attributes import flag_modified

from modules.assessments.models import (
    AssessmentCategoryProgress,
    AssessmentInstance,
    AssessmentPackage,
    AssessmentPackageCategory,
)
from modules.engagement_notifications.service_config import extract_service_keys
from modules.engagements.blood_bookings_repository import current_blood_booking_subquery
from modules.engagements.models import (
    Engagement,
    EngagementNotification,
    EngagementParticipant,
    ParticipantBloodBooking,
)
from modules.experts.models import ConsultationBooking, ExpertTypeModel
from modules.organizations.models import Organization
from modules.questionnaire.models import (
    QuestionnaireCategory,
    QuestionnaireCategoryQuestion,
    QuestionnaireDefinition,
    QuestionnaireResponse,
)
from modules.reports.camp_report_section_builders import (
    extract_metabolic_age,
    extract_metabolic_score,
    extract_oxidative_stress_score,
    metabolic_age_gap,
    metabolic_risk_bucket,
    normalize_camp_gender,
    resolve_user_age,
)
from modules.notifications.expire_stale import DEFAULT_PENDING_TIMEOUT_HOURS
from modules.notifications.models import Notification
from modules.reports.blood_report_archival import is_persistable_blood_report_url
from modules.reports.models import CampReport, IndividualHealthReport
from modules.users.models import User

_MALE_GENDERS = ("male", "m", "1")
_FEMALE_GENDERS = ("female", "f", "2")


def _normalized_gender(column):
    """user_gender_enum has no trim/lower; compare the text label."""
    return func.lower(func.trim(cast(column, String)))


_METSIGHTS_PRO_BASIC_TYPE_CODES = ("1", "2")
_METSIGHTS_CATEGORY_KEYS = (
    "physical-measurement",
    "diet-lifestyle-parameters",
    "vitals",
    "fitness-parameters",
)
# KPI questionnaire_completed requires these Metsights categories when assigned.
# Vitals is camp-day data and is intentionally excluded from this set.
_KPI_QUESTIONNAIRE_REQUIRED_CATEGORY_KEYS = (
    "physical-measurement",
    "diet-lifestyle-parameters",
    "fitness-parameters",
)


def _person_name(first_name: str | None, last_name: str | None) -> str:
    parts = [p.strip() for p in (first_name or "", last_name or "") if p and str(p).strip()]
    return " ".join(parts) if parts else "Unknown"


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
    user_age: int | None
    user_date_of_birth: date | None


def _dedupe_enrolled_assessment_contexts(
    rows: list[tuple],
) -> list[EnrolledAssessmentContext]:
    """Collapse duplicate IHR outer-join rows; prefer a row that has BioAI report JSON."""
    by_id: dict[int, EnrolledAssessmentContext] = {}
    for ai, pkg, eng, ihr, gender, age, date_of_birth in rows:
        aid = int(ai.assessment_instance_id)
        ctx = EnrolledAssessmentContext(
            assessment_instance=ai,
            package=pkg,
            engagement=eng,
            individual_report=ihr,
            user_gender=gender,
            user_age=int(age) if age is not None else None,
            user_date_of_birth=date_of_birth,
        )
        existing = by_id.get(aid)
        if existing is None:
            by_id[aid] = ctx
            continue
        existing_has_reports = (
            existing.individual_report is not None
            and existing.individual_report.reports is not None
        )
        new_has_reports = ihr is not None and ihr.reports is not None
        if new_has_reports and not existing_has_reports:
            by_id[aid] = ctx
    return list(by_id.values())


@dataclass
class CampParticipantEnrichment:
    """Enriched camp participant fields loaded in batch for one page."""

    questionnaires: dict[str, bool]
    blood_report_generated: bool
    bio_ai_report_generated: bool
    blood_report_sent: bool
    bio_ai_report_sent: bool
    consultations: bool


def _notification_user_ids(raw_user: Any) -> set[int]:
    if not isinstance(raw_user, dict):
        return set()
    ids = raw_user.get("user_ids")
    if not isinstance(ids, list):
        return set()
    result: set[int] = set()
    for uid in ids:
        if uid is None:
            continue
        try:
            result.add(int(uid))
        except (TypeError, ValueError):
            continue
    return result


def _notification_counts_as_sent(status: str | None, dispatched_at: datetime | None) -> bool:
    normalized = (status or "").strip().lower()
    if normalized == "sent":
        return True
    if normalized != "pending" or dispatched_at is None:
        return False
    cutoff = datetime.now(timezone.utc) - timedelta(hours=DEFAULT_PENDING_TIMEOUT_HOURS)
    dispatched = dispatched_at if dispatched_at.tzinfo else dispatched_at.replace(tzinfo=timezone.utc)
    return dispatched >= cutoff


class CampReportsRepository:
    """CRUD queries for camp_reports."""

    @staticmethod
    def _enrolled_users_ranked_select(
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
        return select(
            ranked.c.user_id,
            ranked.c.date_of_birth,
            ranked.c.age,
            ranked.c.gender,
            ranked.c.first_name,
            ranked.c.last_name,
            ranked.c.engagement_id,
        ).where(ranked.c.rn == 1)

    @staticmethod
    def _enrolled_users_ranked_subquery(
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ):
        """Distinct enrolled users per camp (latest participant row per user_id)."""
        return CampReportsRepository._enrolled_users_ranked_select(
            camp_no=camp_no,
            department=department,
            city=city,
        ).subquery()

    @staticmethod
    def _enrolled_users_ranked_cte(
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
        name: str = "enrolled_users",
    ):
        """Distinct enrolled users as a CTE (avoids repeating the ranked scan in one query)."""
        return CampReportsRepository._enrolled_users_ranked_select(
            camp_no=camp_no,
            department=department,
            city=city,
        ).cte(name)

    @staticmethod
    def _bio_ai_report_join_ctes(
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ):
        """CTEs for enrolled users + latest Bio AI report + Basic/Pro instance presence."""
        enrolled = CampReportsRepository._enrolled_users_ranked_cte(
            camp_no=camp_no,
            department=department,
            city=city,
        )

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
        ).cte("ranked_reports")

        latest_report = (
            select(ranked_reports.c.user_id, ranked_reports.c.reports)
            .where(ranked_reports.c.rn == 1)
        ).cte("latest_report")

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
        ).cte("ranked_instances")

        bio_ai_users = (
            select(ranked_instances.c.user_id).where(ranked_instances.c.rn == 1)
        ).cte("bio_ai_users")

        return enrolled, latest_report, bio_ai_users

    @staticmethod
    def _bio_ai_user_status_query(
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ):
        enrolled, latest_report, bio_ai_users = CampReportsRepository._bio_ai_report_join_ctes(
            camp_no=camp_no,
            department=department,
            city=city,
        )
        return (
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

    @staticmethod
    def _camp_report_select(*, load_report_bts: bool = True):
        stmt = select(CampReport)
        if not load_report_bts:
            stmt = stmt.options(defer(CampReport.report_bts))
        return stmt

    async def get_overall_by_camp_no(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        load_report_bts: bool = True,
    ) -> CampReport | None:
        result = await db.execute(
            self._camp_report_select(load_report_bts=load_report_bts).where(
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
        load_report_bts: bool = True,
    ) -> CampReport | None:
        result = await db.execute(
            self._camp_report_select(load_report_bts=load_report_bts).where(
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
        load_report_bts: bool = True,
    ) -> CampReport | None:
        result = await db.execute(
            self._camp_report_select(load_report_bts=load_report_bts).where(
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
        load_report_bts: bool = True,
    ) -> CampReport | None:
        result = await db.execute(
            self._camp_report_select(load_report_bts=load_report_bts).where(
                CampReport.camp_no == camp_no,
                CampReport.department == department,
                func.lower(CampReport.city) == city.lower(),
            )
        )
        return result.scalar_one_or_none()

    async def get_report_bts_for_scope(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None,
        city: str | None,
    ) -> dict[str, Any] | None:
        """Load only the report_bts JSON column for a camp report scope."""
        if city is None:
            if department is None:
                where = (
                    CampReport.camp_no == camp_no,
                    CampReport.department.is_(None),
                    CampReport.city.is_(None),
                )
            else:
                where = (
                    CampReport.camp_no == camp_no,
                    CampReport.department == department,
                    CampReport.city.is_(None),
                )
        elif department is None:
            where = (
                CampReport.camp_no == camp_no,
                CampReport.department.is_(None),
                func.lower(CampReport.city) == city.lower(),
            )
        else:
            where = (
                CampReport.camp_no == camp_no,
                CampReport.department == department,
                func.lower(CampReport.city) == city.lower(),
            )
        result = await db.execute(select(CampReport.report_bts).where(*where))
        raw = result.scalar_one_or_none()
        if raw is None:
            return None
        if isinstance(raw, dict):
            return raw
        return None

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
            .options(defer(CampReport.report_bts))
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

    async def list_distinct_camp_nos(self, db: AsyncSession) -> list[int]:
        """Return every camp_no referenced by at least one engagement."""
        result = await db.execute(
            select(Engagement.camp_no)
            .where(Engagement.camp_no.isnot(None))
            .distinct()
            .order_by(Engagement.camp_no.asc())
        )
        return [int(row[0]) for row in result.all()]

    async def has_scheduled_or_running_engagement(self, db: AsyncSession, *, camp_no: int) -> bool:
        result = await db.execute(
            select(Engagement.engagement_id)
            .where(
                Engagement.camp_no == camp_no,
                Engagement.status.in_(("scheduled", "running")),
            )
            .limit(1)
        )
        return result.scalar_one_or_none() is not None

    async def has_running_engagement(self, db: AsyncSession, *, camp_no: int) -> bool:
        """Return True when the camp has at least one running engagement."""
        result = await db.execute(
            select(Engagement.engagement_id)
            .where(
                Engagement.camp_no == camp_no,
                Engagement.status == "running",
            )
            .limit(1)
        )
        return result.scalar_one_or_none() is not None

    async def create(self, db: AsyncSession, row: CampReport) -> CampReport:
        db.add(row)
        await db.flush()
        return row

    async def update_report(self, db: AsyncSession, row: CampReport, report: dict) -> CampReport:
        row.report = dict(report)
        flag_modified(row, "report")
        await db.flush()
        return row

    async def update_report_and_bts(
        self,
        db: AsyncSession,
        row: CampReport,
        report: dict,
        report_bts: dict,
    ) -> CampReport:
        row.report = dict(report)
        row.report_bts = dict(report_bts)
        flag_modified(row, "report")
        flag_modified(row, "report_bts")
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
        enrolled = self._enrolled_users_ranked_cte(camp_no=camp_no, department=department, city=city)

        curr_pbb = current_blood_booking_subquery()
        booking_users = (
            select(EngagementParticipant.user_id.label("user_id"))
            .select_from(Engagement)
            .join(
                EngagementParticipant,
                EngagementParticipant.engagement_id == Engagement.engagement_id,
            )
            .join(
                curr_pbb,
                curr_pbb.c.engagement_participant_id
                == EngagementParticipant.engagement_participant_id,
            )
            .where(
                Engagement.camp_no == camp_no,
                curr_pbb.c.booking_id.isnot(None),
                func.trim(curr_pbb.c.booking_id) != "",
            )
            .distinct()
        ).cte("booking_users")

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
        ).cte("ranked_assessments")

        latest_assessment = (
            select(
                ranked_assessments.c.user_id,
                ranked_assessments.c.metsights_record_id,
            )
            .where(ranked_assessments.c.rn == 1)
        ).cte("latest_assessment")

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

    async def _compute_expert_type_counts(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
        done: bool = False,
    ) -> dict[str, int]:
        """Count distinct users per expert ``type_key`` for camp consultation bookings.

        When ``done`` is false, counts bookings with ``want=True``.
        When ``done`` is true, counts bookings with ``done=True``.
        """
        expert_types_result = await db.execute(
            select(ExpertTypeModel.type_key).order_by(ExpertTypeModel.type_key.asc())
        )
        type_keys = [str(row[0]).strip() for row in expert_types_result.all() if row[0]]
        type_keys = [key for key in type_keys if key]

        counts: dict[str, int] = {key: 0 for key in type_keys}
        if not type_keys:
            return counts

        flag_column = (
            ConsultationBooking.done if done else ConsultationBooking.want
        )
        booking_query = (
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
                flag_column.is_(True),
            )
        )
        if department is not None:
            booking_query = booking_query.where(
                EngagementParticipant.participant_department == department
            )
        if city is not None:
            booking_query = booking_query.where(
                func.lower(func.trim(Engagement.city)) == city.lower()
            )

        booking_result = await db.execute(booking_query)
        user_types: dict[int, set[str]] = {}
        known_types = set(type_keys)
        for user_id, expert_type in booking_result.all():
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

        return counts

    async def compute_consultation_counts(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ) -> dict[str, int]:
        """Count distinct users wanting each expert type (``want=True``)."""
        return await self._compute_expert_type_counts(
            db,
            camp_no=camp_no,
            department=department,
            city=city,
            done=False,
        )

    async def compute_consultation_done_counts(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ) -> dict[str, int]:
        """Count distinct users with each expert type consultation completed (``done=True``)."""
        return await self._compute_expert_type_counts(
            db,
            camp_no=camp_no,
            department=department,
            city=city,
            done=True,
        )

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
        Consultation counts include all expert types (want and done).
        """
        enrolled = self._enrolled_users_ranked_subquery(
            camp_no=camp_no, department=department, city=city
        )

        employees_result = await db.execute(select(func.count()).select_from(enrolled))
        employees_enrolled = int(employees_result.scalar_one())

        male_result = await db.execute(
            select(func.count())
            .select_from(enrolled)
            .where(_normalized_gender(enrolled.c.gender).in_(_MALE_GENDERS))
        )
        male_enrolled = int(male_result.scalar_one())

        female_result = await db.execute(
            select(func.count())
            .select_from(enrolled)
            .where(_normalized_gender(enrolled.c.gender).in_(_FEMALE_GENDERS))
        )
        female_enrolled = int(female_result.scalar_one())

        consultations = await self.compute_consultation_counts(
            db,
            camp_no=camp_no,
            department=department,
            city=city,
        )
        consultation_done = await self.compute_consultation_done_counts(
            db,
            camp_no=camp_no,
            department=department,
            city=city,
        )
        doctor_consultation = int(consultations.get("doctor", 0))
        nutritionist_consultation = int(consultations.get("nutritionist", 0))

        curr_pbb = current_blood_booking_subquery()
        ranked_reports = (
            select(
                enrolled.c.user_id,
                enrolled.c.date_of_birth,
                enrolled.c.age,
                enrolled.c.first_name,
                enrolled.c.last_name,
                IndividualHealthReport.reports,
                curr_pbb.c.blood_parameters,
                curr_pbb.c.diagnostic_report_url,
                AssessmentInstance.assessment_instance_id,
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
            .join(
                EngagementParticipant,
                and_(
                    EngagementParticipant.user_id == enrolled.c.user_id,
                    EngagementParticipant.engagement_id == enrolled.c.engagement_id,
                ),
            )
            .outerjoin(
                curr_pbb,
                curr_pbb.c.engagement_participant_id
                == EngagementParticipant.engagement_participant_id,
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
                ranked_reports.c.first_name,
                ranked_reports.c.last_name,
                ranked_reports.c.reports,
                ranked_reports.c.blood_parameters,
                ranked_reports.c.diagnostic_report_url,
                ranked_reports.c.assessment_instance_id,
            ).where(ranked_reports.c.rn == 1)
        )

        enrolled_roster_result = await db.execute(
            select(
                enrolled.c.user_id,
                enrolled.c.date_of_birth,
                enrolled.c.age,
                enrolled.c.first_name,
                enrolled.c.last_name,
            )
        )
        enrolled_roster = {
            int(uid): {
                "date_of_birth": dob,
                "age": int(stored_age),
                "first_name": first_name,
                "last_name": last_name,
            }
            for uid, dob, stored_age, first_name, last_name in enrolled_roster_result.all()
        }

        bio_ai_report_generated = 0
        high_risk_group = 0
        caution_risk_group = 0
        good_risk_group = 0
        risk_people: list[dict[str, Any]] = []
        bio_ai_user_ids: set[int] = set()
        report_info_by_user: dict[int, dict[str, Any]] = {}

        for (
            user_id,
            dob,
            stored_age,
            first_name,
            last_name,
            reports,
            blood_parameters,
            diagnostic_report_url,
            assessment_instance_id,
        ) in reports_result.all():
            uid = int(user_id)
            reports_dict = _coerce_reports_dict(reports)
            has_bio_ai = bool(reports_dict)
            chronological_age = resolve_user_age(
                date_of_birth=dob,
                stored_age=int(stored_age),
                reference_date=age_reference_date,
            )
            report_info_by_user[uid] = {
                "name": _person_name(first_name, last_name),
                "has_bio_ai": has_bio_ai,
                "has_assessment": assessment_instance_id is not None,
                "has_blood_parameters": blood_parameters is not None,
                "has_diagnostic_report_url": bool(
                    is_persistable_blood_report_url(
                        str(diagnostic_report_url).strip() if diagnostic_report_url else ""
                    )
                ),
                "actual_age": chronological_age,
            }
            if not has_bio_ai:
                continue

            bio_ai_report_generated += 1
            bio_ai_user_ids.add(uid)
            metabolic_age = extract_metabolic_age(reports_dict)
            gap = metabolic_age_gap(
                metabolic_age=metabolic_age,
                chronological_age=chronological_age,
            )
            bucket = metabolic_risk_bucket(
                metabolic_age=metabolic_age,
                chronological_age=chronological_age,
            )
            if bucket == "high":
                high_risk_group += 1
            elif bucket == "caution":
                caution_risk_group += 1
            else:
                good_risk_group += 1
            risk_people.append(
                {
                    "user_id": uid,
                    "name": _person_name(first_name, last_name),
                    "actual_age": chronological_age,
                    "metabolic_age": metabolic_age,
                    "gap_years": round(gap, 2),
                    "risk_group": bucket,
                }
            )

        for uid, info in enrolled_roster.items():
            if uid in report_info_by_user:
                continue
            report_info_by_user[uid] = {
                "name": _person_name(info["first_name"], info["last_name"]),
                "has_bio_ai": False,
                "has_assessment": False,
                "has_blood_parameters": False,
                "has_diagnostic_report_url": False,
                "actual_age": resolve_user_age(
                    date_of_birth=info["date_of_birth"],
                    stored_age=info["age"],
                    reference_date=age_reference_date,
                ),
            }

        questionnaire = await self._compute_kpi_questionnaire_status(
            db,
            camp_no=camp_no,
            department=department,
            city=city,
            enrolled_user_ids=set(enrolled_roster.keys()),
        )
        questionnaire_completed = int(questionnaire["questionnaire_completed"])
        filled_user_ids: set[int] = set(questionnaire["filled_user_ids"])
        unanswered_by_user: dict[int, list[str]] = questionnaire["unanswered_by_user"]

        bio_ai_mismatch_people: list[dict[str, Any]] = []
        mismatch_user_ids = filled_user_ids.symmetric_difference(bio_ai_user_ids)
        for uid in sorted(mismatch_user_ids):
            info = report_info_by_user.get(uid) or {
                "name": "Unknown",
                "has_bio_ai": uid in bio_ai_user_ids,
                "has_assessment": False,
                "has_blood_parameters": False,
                "has_diagnostic_report_url": False,
            }
            q_filled = uid in filled_user_ids
            has_bio = uid in bio_ai_user_ids
            reasons: list[str] = []
            if q_filled and not has_bio:
                if not info.get("has_assessment"):
                    reasons.append(
                        "This person finished the questionnaire, but we could not find "
                        "their Metsights Pro/Basic health assessment for this camp."
                    )
                elif not info.get("has_bio_ai"):
                    if not info.get("has_blood_parameters") and not info.get(
                        "has_diagnostic_report_url"
                    ):
                        reasons.append(
                            "Questionnaire is complete, but the blood report is not "
                            "available yet, so the Bio AI health report could not be generated."
                        )
                    else:
                        reasons.append(
                            "Questionnaire is complete, but the Bio AI health report "
                            "has not been generated yet (the report file is empty)."
                        )
            elif has_bio and not q_filled:
                missing_questions = unanswered_by_user.get(uid) or []
                if missing_questions:
                    preview = "; ".join(missing_questions[:8])
                    more = len(missing_questions) - 8
                    extra = f" (+{more} more)" if more > 0 else ""
                    reasons.append(
                        "Bio AI report is ready, but the questionnaire is not fully "
                        f"filled. Missing required question(s): {preview}{extra}."
                    )
                else:
                    reasons.append(
                        "Bio AI report is ready, but the questionnaire is not marked "
                        "as fully completed yet."
                    )
            if not reasons:
                reasons.append(
                    "Questionnaire completed and Bio AI report counts do not match "
                    "for this person."
                )
            bio_ai_mismatch_people.append(
                {
                    "user_id": uid,
                    "name": info.get("name") or "Unknown",
                    "questionnaire_completed": q_filled,
                    "bio_ai_report_generated": has_bio,
                    "reasons": reasons,
                }
            )

        risk_people.sort(key=lambda p: (p.get("name") or "", p.get("user_id") or 0))
        bio_ai_mismatch_people.sort(key=lambda p: (p.get("name") or "", p.get("user_id") or 0))

        return {
            "employees_enrolled": employees_enrolled,
            "male_enrolled": male_enrolled,
            "female_enrolled": female_enrolled,
            "total_blood_test": len(blood_tested_user_ids),
            "consultations": consultations,
            "consultation_done": consultation_done,
            "doctor_consultation": doctor_consultation,
            "nutritionist_consultation": nutritionist_consultation,
            "questionnaire_completed": questionnaire_completed,
            "bio_ai_report_generated": bio_ai_report_generated,
            "high_risk_group": high_risk_group,
            "caution_risk_group": caution_risk_group,
            "good_risk_group": good_risk_group,
            "blood_details": dict(blood_details),
            "kpi_bts_details": {
                "risk_groups": {
                    "people": risk_people,
                    "counts": {
                        "high": high_risk_group,
                        "caution": caution_risk_group,
                        "good": good_risk_group,
                        "bio_ai_report_generated": bio_ai_report_generated,
                    },
                },
                "questionnaire": {
                    "completed": questionnaire_completed,
                    "by_engagement": questionnaire["by_engagement"],
                    "sum_filled_cards": questionnaire["sum_filled_cards"],
                },
                "bio_ai_mismatch": {
                    "questionnaire_completed": questionnaire_completed,
                    "bio_ai_report_generated": bio_ai_report_generated,
                    "people": bio_ai_mismatch_people,
                },
            },
        }

    async def _compute_kpi_questionnaire_status(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None,
        city: str | None,
        enrolled_user_ids: set[int],
    ) -> dict[str, Any]:
        """Camp-scoped Metsights questionnaire completion for KPI ``questionnaire_completed``.

        Counts a user as filled when every **required** Metsights category assigned on
        their primary package (assessment_type_code 1/2) is complete. Required keys are
        ``physical-measurement``, ``diet-lifestyle-parameters``, and
        ``fitness-parameters`` when assigned. ``vitals`` is excluded (camp-day data);
        incomplete vitals does not block ``questionnaire_completed``.

        Restricted to enrolled users in scope. After deploying this rule change,
        refresh stored camp-report KPI sections (and ``report_bts.kpis``) so counts
        match the updated definition.
        """
        empty = {
            "questionnaire_completed": 0,
            "filled_user_ids": set(),
            "unanswered_by_user": {},
            "by_engagement": [],
            "sum_filled_cards": 0,
        }
        if not enrolled_user_ids:
            return empty

        engagement_query = select(
            Engagement.engagement_id,
            Engagement.engagement_name,
        ).where(Engagement.camp_no == camp_no)
        if city is not None:
            engagement_query = engagement_query.where(
                func.lower(func.trim(Engagement.city)) == city.lower()
            )
        engagement_rows = (await db.execute(engagement_query)).all()
        if not engagement_rows:
            return empty

        engagement_ids = [int(r[0]) for r in engagement_rows]
        engagement_names = {int(r[0]): (r[1] or f"Engagement {r[0]}") for r in engagement_rows}

        # Participants in scope for each engagement (department filter when set).
        participant_query = (
            select(
                EngagementParticipant.engagement_id,
                EngagementParticipant.user_id,
            )
            .where(EngagementParticipant.engagement_id.in_(engagement_ids))
            .where(EngagementParticipant.user_id.in_(enrolled_user_ids))
        )
        if department is not None:
            participant_query = participant_query.where(
                EngagementParticipant.participant_department == department
            )
        participants_by_eng: dict[int, set[int]] = {eid: set() for eid in engagement_ids}
        for eid, uid in (await db.execute(participant_query)).all():
            participants_by_eng.setdefault(int(eid), set()).add(int(uid))

        instances_query = (
            select(
                AssessmentInstance.assessment_instance_id,
                AssessmentInstance.user_id,
                AssessmentInstance.package_id,
                AssessmentInstance.engagement_id,
            )
            .join(
                AssessmentPackage,
                AssessmentPackage.package_id == AssessmentInstance.package_id,
            )
            .where(AssessmentInstance.engagement_id.in_(engagement_ids))
            .where(AssessmentInstance.user_id.in_(enrolled_user_ids))
            .where(AssessmentPackage.assessment_type_code.in_(("1", "2")))
        )
        instance_rows = (await db.execute(instances_query)).all()
        if not instance_rows:
            by_engagement = []
            for eid in engagement_ids:
                scoped = participants_by_eng.get(eid) or set()
                by_engagement.append(
                    {
                        "engagement_id": eid,
                        "engagement_name": engagement_names.get(eid),
                        "filled": 0,
                        "partially_filled": 0,
                        "not_started": len(scoped),
                        "enrolled": len(scoped),
                    }
                )
            return {
                "questionnaire_completed": 0,
                "filled_user_ids": set(),
                "unanswered_by_user": {},
                "by_engagement": by_engagement,
                "sum_filled_cards": 0,
            }

        instance_ids = [int(r[0]) for r in instance_rows]
        package_ids = list({int(r[2]) for r in instance_rows})

        progress_query = (
            select(
                AssessmentCategoryProgress.assessment_instance_id,
                AssessmentCategoryProgress.category_id,
                AssessmentCategoryProgress.status,
                AssessmentCategoryProgress.is_submitted,
                QuestionnaireCategory.category_key,
            )
            .join(
                QuestionnaireCategory,
                QuestionnaireCategory.category_id == AssessmentCategoryProgress.category_id,
            )
            .where(AssessmentCategoryProgress.assessment_instance_id.in_(instance_ids))
        )
        progress_map: dict[int, dict[str, dict]] = {}
        for p in (await db.execute(progress_query)).all():
            ck = p.category_key
            if ck is None:
                continue
            progress_map.setdefault(int(p.assessment_instance_id), {})[ck] = {
                "status": p.status,
                "is_submitted": bool(p.is_submitted),
            }

        resp_count_query = (
            select(
                QuestionnaireResponse.assessment_instance_id,
                func.count(QuestionnaireResponse.response_id),
            )
            .where(QuestionnaireResponse.assessment_instance_id.in_(instance_ids))
            .group_by(QuestionnaireResponse.assessment_instance_id)
        )
        resp_counts = {
            int(r[0]): int(r[1]) for r in (await db.execute(resp_count_query)).all()
        }

        pkg_cat_query = (
            select(
                AssessmentPackageCategory.package_id,
                QuestionnaireCategory.category_key,
            )
            .join(
                QuestionnaireCategory,
                QuestionnaireCategory.category_id == AssessmentPackageCategory.category_id,
            )
            .where(AssessmentPackageCategory.package_id.in_(package_ids))
            .where(QuestionnaireCategory.category_of == "metsights")
        )
        pkg_categories: dict[int, set[str]] = {}
        for pc in (await db.execute(pkg_cat_query)).all():
            pkg_categories.setdefault(int(pc.package_id), set()).add(pc.category_key)

        cat_key_to_id: dict[str, int] = {}
        for ck in _METSIGHTS_CATEGORY_KEYS:
            cat_row = await db.execute(
                select(QuestionnaireCategory.category_id)
                .where(QuestionnaireCategory.category_key == ck)
                .where(QuestionnaireCategory.category_of == "metsights")
                .limit(1)
            )
            cid = cat_row.scalar_one_or_none()
            if cid is not None:
                cat_key_to_id[ck] = int(cid)

        cat_resp_map: dict[int, dict[str, bool]] = {iid: {} for iid in instance_ids}
        for ck, cid in cat_key_to_id.items():
            has_resp_query = (
                select(
                    QuestionnaireResponse.assessment_instance_id,
                    func.count(QuestionnaireResponse.response_id),
                )
                .where(QuestionnaireResponse.assessment_instance_id.in_(instance_ids))
                .where(QuestionnaireResponse.category_ids.any(cid))
                .group_by(QuestionnaireResponse.assessment_instance_id)
            )
            for r in (await db.execute(has_resp_query)).all():
                cat_resp_map.setdefault(int(r[0]), {})[ck] = int(r[1]) > 0

        unanswered_map: dict[int, dict[str, list[dict]]] = {}
        for ck, cid in cat_key_to_id.items():
            unanswered_query = (
                select(
                    AssessmentInstance.assessment_instance_id,
                    QuestionnaireDefinition.question_id,
                    QuestionnaireDefinition.question_text,
                )
                .select_from(AssessmentInstance)
                .join(
                    AssessmentPackageCategory,
                    AssessmentPackageCategory.package_id == AssessmentInstance.package_id,
                )
                .join(
                    QuestionnaireCategoryQuestion,
                    QuestionnaireCategoryQuestion.category_id
                    == AssessmentPackageCategory.category_id,
                )
                .join(
                    QuestionnaireDefinition,
                    QuestionnaireDefinition.question_id
                    == QuestionnaireCategoryQuestion.question_id,
                )
                .outerjoin(
                    QuestionnaireResponse,
                    (QuestionnaireResponse.assessment_instance_id
                     == AssessmentInstance.assessment_instance_id)
                    & (QuestionnaireResponse.question_id == QuestionnaireDefinition.question_id)
                    & (QuestionnaireResponse.category_ids.any(cid)),
                )
                .where(AssessmentInstance.assessment_instance_id.in_(instance_ids))
                .where(AssessmentPackageCategory.category_id == cid)
                .where(QuestionnaireDefinition.is_required.is_(True))
                .where(QuestionnaireResponse.response_id.is_(None))
                .order_by(
                    AssessmentInstance.assessment_instance_id.asc(),
                    QuestionnaireDefinition.question_id.asc(),
                )
            )
            for r in (await db.execute(unanswered_query)).all():
                unanswered_map.setdefault(int(r.assessment_instance_id), {}).setdefault(
                    ck, []
                ).append(
                    {
                        "question_id": int(r.question_id),
                        "question_text": r.question_text,
                    }
                )

        # Per engagement + unique user rollup
        user_global: dict[int, dict[str, Any]] = {}
        by_engagement: list[dict[str, Any]] = []
        sum_filled_cards = 0

        instances_by_eng: dict[int, list[tuple]] = {eid: [] for eid in engagement_ids}
        for row in instance_rows:
            instances_by_eng.setdefault(int(row[3]), []).append(row)

        for eid in engagement_ids:
            scoped_users = participants_by_eng.get(eid) or set()
            user_data: dict[int, dict[str, Any]] = {}
            for row in instances_by_eng.get(eid) or []:
                iid = int(row[0])
                uid = int(row[1])
                pid = int(row[2])
                if uid not in scoped_users:
                    continue
                assigned_cats = pkg_categories.get(pid, set())
                entry = user_data.setdefault(
                    uid,
                    {
                        "assigned_cats": set(),
                        "has_any_responses": False,
                        "all_assigned_complete": True,
                        "unanswered_questions": [],
                    },
                )
                entry["assigned_cats"].update(assigned_cats)
                if resp_counts.get(iid, 0) > 0:
                    entry["has_any_responses"] = True
                inst_progress = progress_map.get(iid, {})
                for ck in _KPI_QUESTIONNAIRE_REQUIRED_CATEGORY_KEYS:
                    if ck not in assigned_cats:
                        continue
                    prog = inst_progress.get(ck, {})
                    has_resp = cat_resp_map.get(iid, {}).get(ck, False)
                    cat_status = prog.get("status", "incomplete")
                    unanswered = unanswered_map.get(iid, {}).get(ck, [])
                    if cat_status != "complete" and has_resp and not unanswered:
                        cat_status = "complete"
                    if cat_status != "complete":
                        entry["all_assigned_complete"] = False
                        for q in unanswered:
                            text = str(q.get("question_text") or "").strip()
                            if text and text not in entry["unanswered_questions"]:
                                entry["unanswered_questions"].append(text)
            # Users enrolled in engagement but without a primary assessment instance
            for uid in scoped_users:
                if uid not in user_data:
                    user_data[uid] = {
                        "assigned_cats": set(),
                        "has_any_responses": False,
                        "all_assigned_complete": True,
                        "unanswered_questions": [],
                    }

            filled = partial = not_started = 0
            for uid, entry in user_data.items():
                assigned = entry["assigned_cats"]
                if not assigned:
                    state = "not_started"
                    not_started += 1
                elif entry["all_assigned_complete"]:
                    state = "filled"
                    filled += 1
                elif entry["has_any_responses"]:
                    state = "partially_filled"
                    partial += 1
                else:
                    state = "not_started"
                    not_started += 1

                global_entry = user_global.setdefault(
                    uid,
                    {
                        "filled": False,
                        "unanswered_questions": [],
                    },
                )
                if state == "filled":
                    global_entry["filled"] = True
                for text in entry["unanswered_questions"]:
                    if text not in global_entry["unanswered_questions"]:
                        global_entry["unanswered_questions"].append(text)

            sum_filled_cards += filled
            by_engagement.append(
                {
                    "engagement_id": eid,
                    "engagement_name": engagement_names.get(eid),
                    "filled": filled,
                    "partially_filled": partial,
                    "not_started": not_started,
                    "enrolled": len(scoped_users),
                }
            )

        filled_user_ids = {
            uid for uid, entry in user_global.items() if entry.get("filled")
        }
        unanswered_by_user = {
            uid: list(entry.get("unanswered_questions") or [])
            for uid, entry in user_global.items()
            if entry.get("unanswered_questions")
        }
        return {
            "questionnaire_completed": len(filled_user_ids),
            "filled_user_ids": filled_user_ids,
            "unanswered_by_user": unanswered_by_user,
            "by_engagement": by_engagement,
            "sum_filled_cards": sum_filled_cards,
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

    async def get_questionnaire_filled_user_ids(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ) -> tuple[int, set[int]]:
        """Return (questionnaire_completed count, filled user ids) for camp scope."""
        enrolled = self._enrolled_users_ranked_subquery(
            camp_no=camp_no,
            department=department,
            city=city,
        )
        result = await db.execute(select(enrolled.c.user_id))
        enrolled_user_ids = {int(row[0]) for row in result.all()}
        questionnaire = await self._compute_kpi_questionnaire_status(
            db,
            camp_no=camp_no,
            department=department,
            city=city,
            enrolled_user_ids=enrolled_user_ids,
        )
        return int(questionnaire["questionnaire_completed"]), set(questionnaire["filled_user_ids"])

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
            bucket = normalize_camp_gender(gender_raw)
            if bucket == "male":
                counts["male"] += 1
            elif bucket == "female":
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
        result = await db.execute(
            self._bio_ai_user_status_query(
                camp_no=camp_no,
                department=department,
                city=city,
            )
        )
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

    async def list_oxidative_stress_status(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ) -> list[tuple[int, str | None, str | None, str | None, float | None, str | None]]:
        """Return (user_id, first_name, last_name, gender, score, reason) for enrolled users.

        ``reason`` is None when an oxidative_stress score is present.
        """
        result = await db.execute(
            self._bio_ai_user_status_query(
                camp_no=camp_no,
                department=department,
                city=city,
            )
        )
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
                        "Bio AI not generated — report row missing or reports JSON is null "
                        "(empty shell excluded from Oxidative Stress Distribution)",
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
                        "Bio AI not generated — reports JSON is empty "
                        "(excluded from Oxidative Stress Distribution)",
                    )
                )
                continue
            score = extract_oxidative_stress_score(reports_dict)
            if score is None:
                rows.append(
                    (
                        int(user_id),
                        first_name,
                        last_name,
                        gender,
                        None,
                        "Bio AI generated but oxidative_stress risk_score_scaled is missing from reports JSON",
                    )
                )
            else:
                rows.append((int(user_id), first_name, last_name, gender, score, None))
        return rows

    async def list_disease_risk_status_rows(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ) -> list[tuple[int, str | None, str | None, str | None, dict[str, Any] | None, str | None]]:
        """Return (user_id, first_name, last_name, gender, reports, reason) for enrolled users.

        ``reports`` is the latest Basic/Pro Bio AI JSON when present.
        ``reason`` is None when a usable Bio AI report exists.
        """
        result = await db.execute(
            self._bio_ai_user_status_query(
                camp_no=camp_no,
                department=department,
                city=city,
            )
        )
        rows: list[
            tuple[int, str | None, str | None, str | None, dict[str, Any] | None, str | None]
        ] = []
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
                        "Bio AI not generated — report row missing or reports JSON is null "
                        "(empty shell excluded from Disease Risk by Gender)",
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
                        "Bio AI not generated — reports JSON is empty "
                        "(excluded from Disease Risk by Gender)",
                    )
                )
            else:
                rows.append((int(user_id), first_name, last_name, gender, reports_dict, None))
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

    async def list_blood_parameters_contexts(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ) -> list[tuple[int, str | None, str | None, str | None, Any]]:
        """Return blood context rows for enrolled users with blood data.

        Each row is ``(user_id, first_name, last_name, gender, blood_parameters)``.
        """
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department, city=city)
        curr_pbb = current_blood_booking_subquery()

        ranked_reports = (
            select(
                enrolled.c.user_id,
                User.first_name,
                User.last_name,
                enrolled.c.gender,
                curr_pbb.c.blood_parameters,
                func.row_number()
                .over(
                    partition_by=enrolled.c.user_id,
                    order_by=ParticipantBloodBooking.id.desc(),
                )
                .label("rn"),
            )
            .select_from(enrolled)
            .join(User, User.user_id == enrolled.c.user_id)
            .join(
                EngagementParticipant,
                and_(
                    EngagementParticipant.user_id == enrolled.c.user_id,
                    EngagementParticipant.engagement_id == enrolled.c.engagement_id,
                ),
            )
            .join(
                curr_pbb,
                curr_pbb.c.engagement_participant_id
                == EngagementParticipant.engagement_participant_id,
            )
            .where(curr_pbb.c.blood_parameters.isnot(None))
        ).subquery()

        result = await db.execute(
            select(
                ranked_reports.c.user_id,
                ranked_reports.c.first_name,
                ranked_reports.c.last_name,
                ranked_reports.c.gender,
                ranked_reports.c.blood_parameters,
            ).where(ranked_reports.c.rn == 1)
        )

        rows: list[tuple[int, str | None, str | None, str | None, Any]] = []
        for user_id, first_name, last_name, gender, blood_params in result.all():
            if blood_params:
                rows.append((int(user_id), first_name, last_name, gender, blood_params))
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
        contexts = await self.list_blood_parameters_contexts(
            db,
            camp_no=camp_no,
            department=department,
            city=city,
        )
        return [(gender, blood_params) for _uid, _fn, _ln, gender, blood_params in contexts]

    async def list_enrolled_users_without_blood_results(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ) -> list[tuple[int, str | None, str | None]]:
        """Enrolled camp users who have NO stored blood_parameters on file.

        Returns list of (user_id, first_name, last_name).
        """
        enrolled = self._enrolled_users_ranked_subquery(camp_no=camp_no, department=department, city=city)
        curr_pbb = current_blood_booking_subquery()

        blood_users = (
            select(enrolled.c.user_id)
            .select_from(enrolled)
            .join(
                EngagementParticipant,
                and_(
                    EngagementParticipant.user_id == enrolled.c.user_id,
                    EngagementParticipant.engagement_id == enrolled.c.engagement_id,
                ),
            )
            .join(
                curr_pbb,
                curr_pbb.c.engagement_participant_id
                == EngagementParticipant.engagement_participant_id,
            )
            .where(curr_pbb.c.blood_parameters.isnot(None))
        ).subquery()

        query = (
            select(enrolled.c.user_id, User.first_name, User.last_name)
            .select_from(enrolled)
            .join(User, User.user_id == enrolled.c.user_id)
            .where(enrolled.c.user_id.notin_(select(blood_users.c.user_id)))
        )

        result = await db.execute(query)
        return [(int(r[0]), r[1], r[2]) for r in result.all()]

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

    @staticmethod
    def _orphan_camp_report_exists_clause():
        """True when at least one engagement exists for the camp_reports row's camp_no."""
        return (
            select(1)
            .select_from(Engagement)
            .where(Engagement.camp_no == CampReport.camp_no)
            .correlate(CampReport)
        )

    async def list_orphan_camp_nos(self, db: AsyncSession) -> list[int]:
        """Return camp_no values that have camp_reports rows but no engagements."""
        engagement_exists = self._orphan_camp_report_exists_clause()
        result = await db.execute(
            select(CampReport.camp_no)
            .where(~exists(engagement_exists))
            .distinct()
            .order_by(CampReport.camp_no.asc())
        )
        return [int(row[0]) for row in result.all()]

    async def count_orphan_camp_report_rows(self, db: AsyncSession) -> int:
        """Count camp_reports rows whose camp_no has no engagements."""
        engagement_exists = self._orphan_camp_report_exists_clause()
        result = await db.execute(
            select(func.count())
            .select_from(CampReport)
            .where(~exists(engagement_exists))
        )
        return int(result.scalar_one() or 0)

    async def delete_orphaned_camp_reports(self, db: AsyncSession) -> int:
        """Delete all camp_reports rows whose camp_no has no engagements."""
        engagement_exists = self._orphan_camp_report_exists_clause()
        result = await db.execute(delete(CampReport).where(~exists(engagement_exists)))
        return int(result.rowcount or 0)

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
                User.email,
                User.age,
                User.gender,
                EngagementParticipant.participant_blood_group,
                EngagementParticipant.participant_department,
                EngagementParticipant.participants_employee_id,
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

    async def enrich_camp_participants_page(
        self,
        db: AsyncSession,
        *,
        rows: list[tuple],
    ) -> dict[int, CampParticipantEnrichment]:
        """Batch-load questionnaires, reports, notifications, and consultations for a page."""
        if not rows:
            return {}

        participant_ids = [int(row[0]) for row in rows]
        user_ids = [int(row[2]) for row in rows]
        engagement_ids = list({int(row[1]) for row in rows})
        enrollment_keys = {(int(row[2]), int(row[1])) for row in rows}

        default = CampParticipantEnrichment(
            questionnaires={},
            blood_report_generated=False,
            bio_ai_report_generated=False,
            blood_report_sent=False,
            bio_ai_report_sent=False,
            consultations=False,
        )
        by_participant_id = {
            pid: CampParticipantEnrichment(
                questionnaires=dict(default.questionnaires),
                blood_report_generated=default.blood_report_generated,
                bio_ai_report_generated=default.bio_ai_report_generated,
                blood_report_sent=default.blood_report_sent,
                bio_ai_report_sent=default.bio_ai_report_sent,
                consultations=default.consultations,
            )
            for pid in participant_ids
        }
        participant_by_key = {
            (int(row[2]), int(row[1])): int(row[0])
            for row in rows
        }

        consultation_result = await db.execute(
            select(EngagementParticipant.engagement_participant_id)
            .select_from(EngagementParticipant)
            .join(
                ConsultationBooking,
                ConsultationBooking.engagement_participant_id
                == EngagementParticipant.engagement_participant_id,
            )
            .where(
                EngagementParticipant.engagement_participant_id.in_(participant_ids),
                ConsultationBooking.want.is_(True),
            )
        )
        for (participant_id,) in consultation_result.all():
            by_participant_id[int(participant_id)].consultations = True

        curr_pbb = current_blood_booking_subquery()
        blood_result = await db.execute(
            select(
                EngagementParticipant.user_id,
                EngagementParticipant.engagement_id,
                curr_pbb.c.diagnostic_report_url,
            )
            .select_from(EngagementParticipant)
            .join(
                curr_pbb,
                curr_pbb.c.engagement_participant_id
                == EngagementParticipant.engagement_participant_id,
            )
            .where(
                EngagementParticipant.user_id.in_(user_ids),
                EngagementParticipant.engagement_id.in_(engagement_ids),
            )
        )
        # Match compute_kpi_metrics / count_bio_ai_reports: only Basic/Pro
        # (assessment_type_code 1/2) with non-empty reports JSON count as Bio AI.
        # FitPrint (type 7) also stores IHR.reports and must not flip this flag.
        # A leftover report_url alone also must not count as generated.
        ihr_result = await db.execute(
            select(
                IndividualHealthReport.user_id,
                IndividualHealthReport.engagement_id,
                IndividualHealthReport.reports,
            )
            .join(
                AssessmentInstance,
                AssessmentInstance.assessment_instance_id
                == IndividualHealthReport.assessment_instance_id,
            )
            .join(
                AssessmentPackage,
                AssessmentPackage.package_id == AssessmentInstance.package_id,
            )
            .where(
                IndividualHealthReport.user_id.in_(user_ids),
                IndividualHealthReport.engagement_id.in_(engagement_ids),
                AssessmentPackage.assessment_type_code.in_(_METSIGHTS_PRO_BASIC_TYPE_CODES),
            )
        )
        blood_generated: set[tuple[int, int]] = set()
        bio_generated: set[tuple[int, int]] = set()
        for user_id, engagement_id, diagnostic_url in blood_result.all():
            key = (int(user_id), int(engagement_id))
            if key not in enrollment_keys:
                continue
            if diagnostic_url and is_persistable_blood_report_url(str(diagnostic_url).strip()):
                blood_generated.add(key)
        for user_id, engagement_id, reports in ihr_result.all():
            key = (int(user_id), int(engagement_id))
            if key not in enrollment_keys:
                continue
            if _coerce_reports_dict(reports):
                bio_generated.add(key)

        for key, participant_id in participant_by_key.items():
            if key in blood_generated:
                by_participant_id[participant_id].blood_report_generated = True
            if key in bio_generated:
                by_participant_id[participant_id].bio_ai_report_generated = True

        blood_services_by_engagement: dict[int, list[str]] = {}
        bio_services_by_engagement: dict[int, list[str]] = {}
        if engagement_ids:
            event_result = await db.execute(
                select(
                    EngagementNotification.engagement_id,
                    EngagementNotification.event_code,
                    EngagementNotification.notification_services,
                )
                .where(
                    EngagementNotification.engagement_id.in_(engagement_ids),
                    EngagementNotification.event_code.in_(
                        ("reports.blood_ready", "reports.bioai_ready")
                    ),
                )
            )
            all_service_keys: set[str] = set()
            for engagement_id, event_code, services in event_result.all():
                # notification_services is JSON list of {service_key, ...} (or legacy strings).
                keys = extract_service_keys(services if isinstance(services, list) else None)
                if not keys:
                    continue
                all_service_keys.update(keys)
                eid = int(engagement_id)
                if event_code == "reports.blood_ready":
                    blood_services_by_engagement.setdefault(eid, []).extend(keys)
                elif event_code == "reports.bioai_ready":
                    bio_services_by_engagement.setdefault(eid, []).extend(keys)

            if all_service_keys:
                notification_result = await db.execute(
                    select(
                        Notification.engagement_id,
                        Notification.service_key,
                        Notification.status,
                        Notification.dispatched_at,
                        Notification.user,
                    ).where(
                        Notification.engagement_id.in_(engagement_ids),
                        Notification.service_key.in_(list(all_service_keys)),
                    )
                )
                sent_pairs: set[tuple[int, int, str]] = set()
                for engagement_id, service_key, status, dispatched_at, user_payload in notification_result.all():
                    if not _notification_counts_as_sent(status, dispatched_at):
                        continue
                    eid = int(engagement_id)
                    sk = str(service_key).strip()
                    for uid in _notification_user_ids(user_payload):
                        sent_pairs.add((uid, eid, sk))

                for row in rows:
                    participant_id = int(row[0])
                    user_id = int(row[2])
                    engagement_id = int(row[1])
                    blood_keys = blood_services_by_engagement.get(engagement_id, [])
                    bio_keys = bio_services_by_engagement.get(engagement_id, [])
                    if any((user_id, engagement_id, sk) in sent_pairs for sk in blood_keys):
                        by_participant_id[participant_id].blood_report_sent = True
                    if any((user_id, engagement_id, sk) in sent_pairs for sk in bio_keys):
                        by_participant_id[participant_id].bio_ai_report_sent = True

        if enrollment_keys:
            ranked_primary = (
                select(
                    AssessmentInstance.user_id,
                    AssessmentInstance.engagement_id,
                    AssessmentInstance.assessment_instance_id,
                    AssessmentInstance.package_id,
                    func.row_number()
                    .over(
                        partition_by=(
                            AssessmentInstance.user_id,
                            AssessmentInstance.engagement_id,
                        ),
                        order_by=AssessmentInstance.assessment_instance_id.asc(),
                    )
                    .label("rn"),
                )
                .select_from(AssessmentInstance)
                .join(
                    AssessmentPackage,
                    AssessmentPackage.package_id == AssessmentInstance.package_id,
                )
                .where(
                    AssessmentInstance.user_id.in_(user_ids),
                    AssessmentInstance.engagement_id.in_(engagement_ids),
                    AssessmentPackage.assessment_type_code.in_(_METSIGHTS_PRO_BASIC_TYPE_CODES),
                )
            ).subquery()

            primary_result = await db.execute(
                select(
                    ranked_primary.c.user_id,
                    ranked_primary.c.engagement_id,
                    ranked_primary.c.assessment_instance_id,
                    ranked_primary.c.package_id,
                ).where(ranked_primary.c.rn == 1)
            )
            primary_by_key: dict[tuple[int, int], tuple[int, int]] = {}
            instance_ids: list[int] = []
            package_ids: set[int] = set()
            for user_id, engagement_id, instance_id, package_id in primary_result.all():
                key = (int(user_id), int(engagement_id))
                if key not in enrollment_keys:
                    continue
                iid = int(instance_id)
                pid = int(package_id)
                primary_by_key[key] = (iid, pid)
                instance_ids.append(iid)
                package_ids.add(pid)

            if instance_ids and package_ids:
                pkg_cat_result = await db.execute(
                    select(
                        AssessmentPackageCategory.package_id,
                        QuestionnaireCategory.category_key,
                    )
                    .join(
                        QuestionnaireCategory,
                        QuestionnaireCategory.category_id == AssessmentPackageCategory.category_id,
                    )
                    .where(
                        AssessmentPackageCategory.package_id.in_(list(package_ids)),
                        QuestionnaireCategory.category_of == "metsights",
                    )
                )
                categories_by_package: dict[int, list[str]] = {}
                for package_id, category_key in pkg_cat_result.all():
                    if category_key is None:
                        continue
                    categories_by_package.setdefault(int(package_id), []).append(str(category_key))

                progress_result = await db.execute(
                    select(
                        AssessmentCategoryProgress.assessment_instance_id,
                        QuestionnaireCategory.category_key,
                        AssessmentCategoryProgress.status,
                    )
                    .join(
                        QuestionnaireCategory,
                        QuestionnaireCategory.category_id == AssessmentCategoryProgress.category_id,
                    )
                    .where(
                        AssessmentCategoryProgress.assessment_instance_id.in_(instance_ids),
                        QuestionnaireCategory.category_of == "metsights",
                    )
                )
                progress_by_instance: dict[int, dict[str, bool]] = {}
                for instance_id, category_key, status in progress_result.all():
                    if category_key is None:
                        continue
                    progress_by_instance.setdefault(int(instance_id), {})[
                        str(category_key)
                    ] = (status or "").strip().lower() == "complete"

                for key, participant_id in participant_by_key.items():
                    primary = primary_by_key.get(key)
                    if primary is None:
                        continue
                    instance_id, package_id = primary
                    category_keys = categories_by_package.get(package_id, [])
                    progress = progress_by_instance.get(instance_id, {})
                    by_participant_id[participant_id].questionnaires = {
                        ck: bool(progress.get(ck, False)) for ck in category_keys
                    }

        return by_participant_id

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
                enrolled.c.age.label("user_age"),
                enrolled.c.date_of_birth.label("user_date_of_birth"),
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
                ranked.c.user_age,
                ranked.c.user_date_of_birth,
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
                enrolled.c.age.label("user_age"),
                enrolled.c.date_of_birth.label("user_date_of_birth"),
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
                ranked.c.user_age,
                ranked.c.user_date_of_birth,
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
                enrolled.c.age.label("user_age"),
                enrolled.c.date_of_birth.label("user_date_of_birth"),
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
                ranked.c.user_age,
                ranked.c.user_date_of_birth,
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

    async def get_organization_benchmark_context(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
    ) -> tuple[int, str] | None:
        """Return (organization_id, organization_name) for a camp."""
        result = await db.execute(
            select(
                Organization.organization_id,
                Organization.name,
            )
            .select_from(Engagement)
            .join(Organization, Organization.organization_id == Engagement.organization_id)
            .where(Engagement.camp_no == camp_no)
            .limit(1)
        )
        row = result.one_or_none()
        if row is None:
            return None
        organization_id, name = row
        return int(organization_id), (name or "")

    async def list_distinct_engagement_states_for_camp(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        city: str | None = None,
    ) -> list[str]:
        """Return distinct non-empty engagements.state values for a camp.

        When ``city`` is set, only engagements in that city are considered.
        Dedupes case-insensitively and keeps the first spelling encountered (ordered).
        """
        query = (
            select(Engagement.state)
            .where(
                Engagement.camp_no == camp_no,
                Engagement.state.isnot(None),
                func.trim(Engagement.state) != "",
            )
            .distinct()
            .order_by(Engagement.state.asc())
        )
        if city is not None:
            query = query.where(func.lower(func.trim(Engagement.city)) == city.lower())

        result = await db.execute(query)
        states: list[str] = []
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
            states.append(trimmed)
        return states

    async def list_benchmark_score_maps_for_camp(
        self,
        db: AsyncSession,
        *,
        camp_no: int,
        department: str | None = None,
        city: str | None = None,
    ) -> list[dict[str, float]]:
        """Return per-person benchmark category scores for camp enrollees with Bio AI."""
        from modules.reports.camp_report_section_builders import extract_benchmark_category_scores

        enrolled = self._enrolled_users_ranked_subquery(
            camp_no=camp_no, department=department, city=city
        )
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
        score_maps: list[dict[str, float]] = []
        for (reports,) in reports_result.all():
            reports_dict = _coerce_reports_dict(reports)
            if not reports_dict:
                continue
            scores = extract_benchmark_category_scores(reports_dict)
            if scores:
                score_maps.append(scores)
        return score_maps

    async def list_benchmark_score_maps_by_org_for_state(
        self,
        db: AsyncSession,
        *,
        state: str,
        exclude_organization_id: int,
    ) -> dict[int, list[dict[str, float]]]:
        """Return {organization_id: [score_map, ...]} for peer Bio-AI orgs in ``state``.

        Peers are matched by ``engagements.state`` (not organizations.state). Only score maps
        from engagements in the target state are included. Selected
        ``exclude_organization_id`` is omitted. Only orgs with at least one usable
        Bio-AI score map are included.
        """
        from modules.reports.camp_report_section_builders import extract_benchmark_category_scores

        state_norm = state.strip().lower()
        if not state_norm:
            return {}

        ranked_reports = (
            select(
                Organization.organization_id.label("organization_id"),
                EngagementParticipant.user_id.label("user_id"),
                IndividualHealthReport.reports,
                func.row_number()
                .over(
                    partition_by=EngagementParticipant.user_id,
                    order_by=_latest_report_order(),
                )
                .label("rn"),
            )
            .select_from(Organization)
            .join(Engagement, Engagement.organization_id == Organization.organization_id)
            .join(
                EngagementParticipant,
                EngagementParticipant.engagement_id == Engagement.engagement_id,
            )
            .join(
                AssessmentInstance,
                and_(
                    AssessmentInstance.engagement_id == Engagement.engagement_id,
                    AssessmentInstance.user_id == EngagementParticipant.user_id,
                ),
            )
            .join(AssessmentPackage, AssessmentPackage.package_id == AssessmentInstance.package_id)
            .join(
                IndividualHealthReport,
                IndividualHealthReport.assessment_instance_id
                == AssessmentInstance.assessment_instance_id,
            )
            .where(
                Engagement.state.isnot(None),
                func.lower(func.trim(Engagement.state)) == state_norm,
                Organization.organization_id != exclude_organization_id,
                AssessmentPackage.assessment_type_code.in_(("1", "2")),
            )
        ).subquery()

        reports_result = await db.execute(
            select(
                ranked_reports.c.organization_id,
                ranked_reports.c.reports,
            ).where(ranked_reports.c.rn == 1)
        )

        by_org: dict[int, list[dict[str, float]]] = {}
        for organization_id, reports in reports_result.all():
            if organization_id is None:
                continue
            reports_dict = _coerce_reports_dict(reports)
            if not reports_dict:
                continue
            scores = extract_benchmark_category_scores(reports_dict)
            if not scores:
                continue
            org_id = int(organization_id)
            by_org.setdefault(org_id, []).append(scores)
        return by_org
