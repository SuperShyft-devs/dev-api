"""Resolve and cache MetSights BioAI report URLs for Basic/Pro assessments."""

from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.metsights.service import MetsightsService
from modules.reports.models import IndividualHealthReport
from modules.reports.repository import ReportsRepository

_BIO_AI_TYPE_CODES = {"1", "2"}

_BIO_AI_METSIGHTS_REPORT_URL_OVERRIDES: dict[str, str] = {
    "https://storages.metsights.com/reports/D6E1178CCA4F488C_Deepa_Gupta_MHR.pdf": (
        "https://api.supershyft.com/media/bio-ai/141a9b846e254200995dcbdcc1596ea5.pdf"
    ),
    "https://storages.metsights.com/reports/4334065C1F6F4027_Ms_Manali_Bhojwani_MHR.pdf": (
        "https://api.supershyft.com/media/bio-ai/f2d73e35010d4eadbf38c2ca5c4e34c9.pdf"
    ),
    "https://storages.metsights.com/reports/5890D12E4C024F2D_Apoorv_Jain_MHR.pdf": (
        "https://api.supershyft.com/media/bio-ai/91fd557d121f4768b8ad03d2e57ef0d3.pdf"
    ),
    "https://storages.metsights.com/reports/46D0D007925941B7_Kuldeep_Chobey_MHR.pdf": (
        "https://api.supershyft.com/media/bio-ai/c7e12326b7ca4dbfa5b58dce6af5b3cd.pdf"
    ),
    "https://storages.metsights.com/reports/A45996FC284642C5_Akash_Gupta_MHR.pdf": (
        "https://api.supershyft.com/media/bio-ai/9e529ee782984068813c511f7b944e26.pdf"
    ),
}


def _normalize_report_url(url: str) -> str:
    stripped = url.strip()
    return _BIO_AI_METSIGHTS_REPORT_URL_OVERRIDES.get(stripped, stripped)


def _extract_file_url(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    data = payload.get("data")
    if isinstance(data, dict):
        file_url = data.get("file") or data.get("url")
        if file_url:
            return str(file_url).strip() or None
    file_url = payload.get("file") or payload.get("url")
    if file_url:
        return str(file_url).strip() or None
    return None


async def resolve_bio_ai_report_url(
    db: AsyncSession,
    *,
    user_id: int,
    engagement_id: int,
    assessment_instance_id: int,
    metsights_record_id: str,
    assessment_type_code: str,
    metsights_service: MetsightsService,
    existing_ihr: IndividualHealthReport | None = None,
    reports_repository: ReportsRepository | None = None,
) -> str:
    """Return a BioAI report URL, fetching from MetSights and caching when needed."""
    type_code = (assessment_type_code or "").strip()
    if type_code not in _BIO_AI_TYPE_CODES:
        raise AppError(
            status_code=422,
            error_code="INVALID_STATE",
            message=(
                "BioAI reports are only available for MetSights Basic or MetSights Pro assessments"
            ),
        )

    repo = reports_repository or ReportsRepository()

    assessment_report = await repo.get_individual_report_by_assessment(
        db, assessment_instance_id=assessment_instance_id
    )
    engagement_report = await repo.get_individual_report_by_engagement(
        db,
        user_id=user_id,
        engagement_id=engagement_id,
    )

    cached_ihr: IndividualHealthReport | None = None
    for candidate in (assessment_report, engagement_report, existing_ihr):
        if candidate is None:
            continue
        cached_url = (candidate.report_url or "").strip()
        if cached_url:
            return _normalize_report_url(cached_url)
        if cached_ihr is None:
            cached_ihr = candidate

    if cached_ihr is not None and cached_ihr.reports is not None:
        url_from_reports = _extract_file_url(cached_ihr.reports)
        if url_from_reports:
            report_url = _normalize_report_url(url_from_reports)
            cached_ihr.report_url = report_url
            await repo.update_individual_report(db, cached_ihr)
            return report_url

    record_id = (metsights_record_id or "").strip()
    if not record_id:
        raise AppError(
            status_code=422,
            error_code="INVALID_STATE",
            message="Metsights record id is missing for this assessment",
        )

    report_payload = await metsights_service.get_report(
        record_id=record_id,
        assessment_type_code=type_code,
    )
    fetched_reports = report_payload if isinstance(report_payload, dict) else None
    fetched_url = _extract_file_url(report_payload)

    if not fetched_url:
        pdf_payload = await metsights_service.get_report_pdf(
            record_id=record_id,
            assessment_type_code=type_code,
        )
        fetched_url = _extract_file_url(pdf_payload)

    if not fetched_url:
        raise AppError(
            status_code=422,
            error_code="INVALID_STATE",
            message="Metsights did not return a BioAI report URL for this record",
        )

    report_url = _normalize_report_url(fetched_url)

    target = assessment_report or engagement_report or existing_ihr or cached_ihr
    if target is None:
        report = IndividualHealthReport(
            user_id=user_id,
            engagement_id=engagement_id,
            assessment_instance_id=assessment_instance_id,
            reports=fetched_reports,
            report_url=report_url,
        )
        await repo.create_individual_report(db, report)
    else:
        if fetched_reports is not None:
            target.reports = fetched_reports
        target.report_url = report_url
        if target.assessment_instance_id is None:
            target.assessment_instance_id = assessment_instance_id
        await repo.update_individual_report(db, target)

    return report_url
