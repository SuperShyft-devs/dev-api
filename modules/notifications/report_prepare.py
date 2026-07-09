"""Prepare blood and BioAI report URLs for admin notification flows."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.assessments.repository import AssessmentsRepository
from modules.metsights.service import MetsightsService
from modules.reports.bio_ai_report_resolver import resolve_bio_ai_report_url
from modules.reports.blood_report_resolver import resolve_blood_report_url
from modules.reports.repository import ReportsRepository

_BIO_AI_TYPE_CODES = {"1", "2"}


async def prepare_user_report_urls(
    db: AsyncSession,
    *,
    user_id: int,
    require_blood: bool,
    require_bioai: bool,
    metsights_service: MetsightsService,
) -> list[dict]:
    """Attempt to load missing report URLs for all user assessments."""
    if not require_blood and not require_bioai:
        return []

    assessments_repo = AssessmentsRepository()
    reports_repo = ReportsRepository()
    rows = await assessments_repo.list_instances_for_user_with_engagement(
        db,
        user_id=user_id,
        page=1,
        limit=100,
    )

    details: list[dict] = []
    for instance, package, _engagement in rows:
        record_id = (instance.metsights_record_id or "").strip()
        engagement_id = instance.engagement_id
        if engagement_id is None:
            continue

        type_code = (package.assessment_type_code if package is not None else "") or ""
        ihr = await reports_repo.get_individual_report_by_assessment(
            db, assessment_instance_id=instance.assessment_instance_id
        )

        item: dict = {
            "assessment_instance_id": instance.assessment_instance_id,
            "engagement_id": int(engagement_id),
            "assessment_type_code": type_code,
        }

        if require_blood:
            if not record_id:
                item["blood"] = {"status": "skipped", "message": "No Metsights record id"}
            else:
                try:
                    await resolve_blood_report_url(
                        db,
                        user_id=user_id,
                        engagement_id=int(engagement_id),
                        assessment_instance_id=instance.assessment_instance_id,
                        metsights_record_id=record_id,
                        metsights_service=metsights_service,
                        existing_ihr=ihr,
                        reports_repository=reports_repo,
                    )
                    item["blood"] = {"status": "ok"}
                except AppError as exc:
                    item["blood"] = {"status": "failed", "message": exc.message}
                except Exception as exc:
                    item["blood"] = {"status": "failed", "message": str(exc)}

        if require_bioai:
            if type_code not in _BIO_AI_TYPE_CODES:
                item["bio_ai"] = {"status": "skipped", "message": "Not a BioAI assessment type"}
            elif not record_id:
                item["bio_ai"] = {"status": "skipped", "message": "No Metsights record id"}
            else:
                ihr = await reports_repo.get_individual_report_by_assessment(
                    db, assessment_instance_id=instance.assessment_instance_id
                )
                try:
                    await resolve_bio_ai_report_url(
                        db,
                        user_id=user_id,
                        engagement_id=int(engagement_id),
                        assessment_instance_id=instance.assessment_instance_id,
                        metsights_record_id=record_id,
                        assessment_type_code=type_code,
                        metsights_service=metsights_service,
                        existing_ihr=ihr,
                        reports_repository=reports_repo,
                    )
                    item["bio_ai"] = {"status": "ok"}
                except AppError as exc:
                    item["bio_ai"] = {"status": "failed", "message": exc.message}
                except Exception as exc:
                    item["bio_ai"] = {"status": "failed", "message": str(exc)}

        if require_blood or (require_bioai and type_code in _BIO_AI_TYPE_CODES):
            details.append(item)

    return details
