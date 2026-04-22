"""Reports repository.

Only database queries belong here.
"""

from __future__ import annotations

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.reports.models import IndividualHealthReport, ReportsUserSyncState


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
