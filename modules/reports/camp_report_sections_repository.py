"""Database access for camp report sections."""

from __future__ import annotations

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.reports.models import CampReportSection


class CampReportSectionsRepository:
    """CRUD queries for camp_report_sections."""

    async def list_sections(
        self,
        db: AsyncSession,
        *,
        page: int,
        limit: int,
    ) -> list[CampReportSection]:
        offset = (page - 1) * limit
        result = await db.execute(
            select(CampReportSection)
            .order_by(CampReportSection.report_sections.asc())
            .offset(offset)
            .limit(limit)
        )
        return list(result.scalars().all())

    async def count_sections(self, db: AsyncSession) -> int:
        result = await db.execute(select(func.count()).select_from(CampReportSection))
        return int(result.scalar_one())

    async def get_by_id(self, db: AsyncSession, *, report_sections: int) -> CampReportSection | None:
        result = await db.execute(
            select(CampReportSection).where(CampReportSection.report_sections == report_sections)
        )
        return result.scalar_one_or_none()

    async def get_by_section_key(self, db: AsyncSession, *, section_key: str) -> CampReportSection | None:
        result = await db.execute(
            select(CampReportSection).where(CampReportSection.section_key == section_key)
        )
        return result.scalar_one_or_none()

    async def create(self, db: AsyncSession, row: CampReportSection) -> CampReportSection:
        db.add(row)
        await db.flush()
        return row

    async def delete_by_id(self, db: AsyncSession, *, report_sections: int) -> int:
        result = await db.execute(
            delete(CampReportSection).where(CampReportSection.report_sections == report_sections)
        )
        return int(result.rowcount or 0)
