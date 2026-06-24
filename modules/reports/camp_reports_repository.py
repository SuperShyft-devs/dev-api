"""Database access for camp reports."""

from __future__ import annotations

from datetime import date

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.engagements.models import Engagement, EngagementParticipant
from modules.organizations.models import Organization
from modules.reports.models import CampReport
from modules.users.models import User


class CampReportsRepository:
    """CRUD queries for camp_reports."""

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
        ranked_rows = (
            select(
                User.user_id,
                User.date_of_birth,
                User.age,
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

        ranked_rows = ranked_rows.subquery()
        query = select(
            ranked_rows.c.user_id,
            ranked_rows.c.date_of_birth,
            ranked_rows.c.age,
        ).where(ranked_rows.c.rn == 1)

        result = await db.execute(query)
        return [(int(r[0]), r[1], int(r[2])) for r in result.all()]

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
