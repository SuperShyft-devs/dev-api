"""Reports repository.

Only database queries belong here.
"""

from __future__ import annotations

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.assessments.models import AssessmentInstance, AssessmentPackage
from modules.engagements.models import Engagement
from modules.reports.models import IndividualHealthReport, ReportsUserSyncState

# MetSights Basic / Pro (excludes FitPrint and other types).
_METSIGHTS_PRO_BASIC_TYPE_CODES = ("1", "2")
_FITPRINT_TYPE_CODE = "7"


class ReportsRepository:
    """Database access for reports."""

    async def get_individual_report_by_engagement(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        engagement_id: int,
    ) -> IndividualHealthReport | None:
        """Engagement-scoped lookup; prefer non-FitPrint rows with blood or diagnostic data."""
        result = await db.execute(
            select(IndividualHealthReport)
            .outerjoin(
                AssessmentInstance,
                AssessmentInstance.assessment_instance_id
                == IndividualHealthReport.assessment_instance_id,
            )
            .outerjoin(
                AssessmentPackage,
                AssessmentPackage.package_id == AssessmentInstance.package_id,
            )
            .where(IndividualHealthReport.user_id == user_id)
            .where(IndividualHealthReport.engagement_id == engagement_id)
            .order_by(
                IndividualHealthReport.blood_parameters.isnot(None).desc(),
                (func.coalesce(AssessmentPackage.assessment_type_code, "") == _FITPRINT_TYPE_CODE).asc(),
                IndividualHealthReport.diagnostic_report_url.isnot(None).desc(),
                IndividualHealthReport.report_id.desc(),
            )
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def get_individual_report_by_assessment(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
    ) -> IndividualHealthReport | None:
        """Assessment-scoped lookup (primary key for Metsights reports / PDFs)."""
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

    async def get_primary_assessment_instance_for_user_engagement(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        engagement_id: int,
    ) -> AssessmentInstance | None:
        """Participant primary assessment: package matches engagement.assessment_package_id."""
        result = await db.execute(
            select(AssessmentInstance)
            .join(Engagement, Engagement.engagement_id == AssessmentInstance.engagement_id)
            .where(AssessmentInstance.user_id == user_id)
            .where(AssessmentInstance.engagement_id == engagement_id)
            .where(Engagement.assessment_package_id.isnot(None))
            .where(AssessmentInstance.package_id == Engagement.assessment_package_id)
            .order_by(AssessmentInstance.assessment_instance_id.asc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def get_assessment_type_code_for_instance(
        self,
        db: AsyncSession,
        *,
        assessment_instance_id: int,
    ) -> str | None:
        result = await db.execute(
            select(AssessmentPackage.assessment_type_code)
            .join(
                AssessmentInstance,
                AssessmentInstance.package_id == AssessmentPackage.package_id,
            )
            .where(AssessmentInstance.assessment_instance_id == assessment_instance_id)
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def resolve_blood_storage_assessment_instance_id(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        engagement_id: int,
        caller_assessment_instance_id: int,
    ) -> int:
        """Assessment instance that should own persisted blood for this engagement."""
        primary = await self.get_primary_assessment_instance_for_user_engagement(
            db,
            user_id=user_id,
            engagement_id=engagement_id,
        )
        if primary is not None:
            return int(primary.assessment_instance_id)

        caller_type = await self.get_assessment_type_code_for_instance(
            db,
            assessment_instance_id=caller_assessment_instance_id,
        )
        if (caller_type or "").strip() == _FITPRINT_TYPE_CODE:
            raise AppError(
                status_code=422,
                error_code="INVALID_STATE",
                message="Blood cannot be stored on a FitPrint assessment",
            )
        return int(caller_assessment_instance_id)

    async def clear_fitprint_blood_fields_for_engagement(
        self,
        db: AsyncSession,
        *,
        user_id: int,
        engagement_id: int,
    ) -> int:
        """Null blood fields on FitPrint IHR rows for the same user+engagement."""
        result = await db.execute(
            select(IndividualHealthReport, AssessmentPackage.assessment_type_code)
            .outerjoin(
                AssessmentInstance,
                AssessmentInstance.assessment_instance_id
                == IndividualHealthReport.assessment_instance_id,
            )
            .outerjoin(
                AssessmentPackage,
                AssessmentPackage.package_id == AssessmentInstance.package_id,
            )
            .where(IndividualHealthReport.user_id == user_id)
            .where(IndividualHealthReport.engagement_id == engagement_id)
        )
        cleared = 0
        for ihr, type_code in result.all():
            if (type_code or "").strip() != _FITPRINT_TYPE_CODE:
                continue
            if ihr.blood_parameters is None and ihr.blood_report_raw is None:
                continue
            ihr.blood_parameters = None
            ihr.blood_report_raw = None
            db.add(ihr)
            cleared += 1
        if cleared:
            await db.flush()
        return cleared

    async def list_individual_reports_for_user_with_assessment(
        self,
        db: AsyncSession,
        *,
        user_id: int,
    ) -> list[tuple[IndividualHealthReport, AssessmentInstance, AssessmentPackage | None]]:
        result = await db.execute(
            select(IndividualHealthReport, AssessmentInstance, AssessmentPackage)
            .join(
                AssessmentInstance,
                AssessmentInstance.assessment_instance_id
                == IndividualHealthReport.assessment_instance_id,
            )
            .outerjoin(
                AssessmentPackage,
                AssessmentPackage.package_id == AssessmentInstance.package_id,
            )
            .where(IndividualHealthReport.user_id == user_id)
            .where(IndividualHealthReport.assessment_instance_id.isnot(None))
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
