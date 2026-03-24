"""Reports repository.

Only database queries belong here.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.reports.models import IndividualHealthReport


class ReportsRepository:
    """Database access for reports."""

    async def get_individual_report_by_assessment(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
    ) -> IndividualHealthReport | None:
        result = await db.execute(
            select(IndividualHealthReport).where(
                IndividualHealthReport.assessment_instance_id == assessment_instance_id
            )
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
