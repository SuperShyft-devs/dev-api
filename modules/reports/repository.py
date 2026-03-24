"""Reports repository.

Only database queries belong here.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.assessments.models import AssessmentInstance
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
