"""Reports repository.

Only database queries belong here.
"""

from __future__ import annotations

from sqlalchemy import and_, delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.reports.models import IndividualHealthReport, ReportsUserSyncState

# MetSights Basic / Pro (excludes FitPrint and other types).
_METSIGHTS_PRO_BASIC_TYPE_CODES = ("1", "2")


class ReportsRepository:
    """Database access for reports."""

    async def get_individual_report_by_engagement(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        engagement_id: int,
    ) -> IndividualHealthReport | None:
        result = await db.execute(
            select(IndividualHealthReport)
            .where(IndividualHealthReport.user_id == user_id)
            .where(IndividualHealthReport.engagement_id == engagement_id)
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def get_individual_report_by_assessment(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
    ) -> IndividualHealthReport | None:
        """Legacy fallback: prefer engagement lookup when assessment context is available."""
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
        """Clear assessment-scoped fields; keep engagement-scoped blood data.

        Nulls ``assessment_instance_id``, ``reports``, and ``report_url`` on matching rows.
        Deletes the row only when no blood/diagnostic data remains.
        """
        result = await db.execute(
            select(IndividualHealthReport).where(
                IndividualHealthReport.assessment_instance_id == assessment_instance_id
            )
        )
        rows = list(result.scalars().all())
        deleted = 0
        for row in rows:
            row.assessment_instance_id = None
            row.reports = None
            row.report_url = None
            has_blood = row.blood_parameters is not None or row.blood_report_raw is not None
            has_diag = row.diagnostic_report_url is not None
            if not has_blood and not has_diag:
                await db.delete(row)
                deleted += 1
            else:
                db.add(row)
        await db.flush()
        return deleted

    async def delete_individual_reports_for_engagement(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        engagement_id: int,
    ) -> int:
        result = await db.execute(
            delete(IndividualHealthReport).where(
                IndividualHealthReport.user_id == user_id,
                IndividualHealthReport.engagement_id == engagement_id,
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
                and_(
                    AssessmentInstance.user_id == IndividualHealthReport.user_id,
                    AssessmentInstance.engagement_id == IndividualHealthReport.engagement_id,
                ),
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
