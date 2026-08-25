"""Register permanent Bio-AI PDF links via the bio-ai-reports service."""

from __future__ import annotations

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from core.exceptions import AppError
from modules.audit.cron_sync_logging import tracked_integration_call
from modules.bioai_report.client import BioAiReportsClient
from modules.bioai_report.report_engine.services.report_service import BioReportService

_PROVIDER = "bio_ai_reports"
_BIO_AI_TYPE_CODES = frozenset({"1", "2"})


def is_bio_ai_assessment_type(assessment_type_code: str | None) -> bool:
    return (assessment_type_code or "").strip() in _BIO_AI_TYPE_CODES


def bioreport_generate_endpoint(assessment_instance_id: int) -> str:
    return f"internal://bioai-report/{assessment_instance_id}"


def bioreport_register_endpoint() -> str:
    base = (settings.BIO_AI_REPORTS_BASE_URL or "").strip().rstrip("/")
    return f"{base}/api/reports"


def summarize_bioreport_payload(payload: dict[str, Any]) -> dict[str, Any]:
    metadata = payload.get("report_metadata") if isinstance(payload.get("report_metadata"), dict) else {}
    patient = payload.get("patient") if isinstance(payload.get("patient"), dict) else {}
    return {
        "record_id": metadata.get("record_id"),
        "patient_name": patient.get("name"),
        "disease_count": metadata.get("disease_count"),
        "engine_version": metadata.get("engine_version"),
        "truncated": True,
    }


def extract_registered_report_url(response: dict[str, Any]) -> str:
    url = response.get("url")
    if not isinstance(url, str) or not url.strip():
        raise AppError(
            status_code=502,
            error_code="BIO_AI_REPORTS_ERROR",
            message="bio-ai-reports did not return a report url",
        )
    return url.strip()


async def register_permanent_bio_ai_report_url(
    db: AsyncSession,
    *,
    assessment_instance_id: int,
    engagement_id: int | None = None,
    user_id: int | None = None,
    bio_report_service: BioReportService | None = None,
    bio_ai_reports_client: BioAiReportsClient | None = None,
) -> str:
    """Generate BioReport JSON and register a permanent PDF link."""
    service = bio_report_service
    if service is None:
        from modules.bioai_report.report_engine.api.dependencies import get_bioreport_service

        service = get_bioreport_service()
    client = bio_ai_reports_client or BioAiReportsClient()
    instance_id = int(assessment_instance_id)

    bioreport_payload = await tracked_integration_call(
        db,
        provider=_PROVIDER,
        api_url=bioreport_generate_endpoint(instance_id),
        engagement_id=engagement_id,
        user_id=user_id,
        request_payload={"assessment_instance_id": instance_id},
        operation=lambda: _generate_bioreport_payload(
            service,
            assessment_instance_id=instance_id,
            db=db,
        ),
        reraise=True,
    )
    if not isinstance(bioreport_payload, dict):
        raise AppError(
            status_code=500,
            error_code="INTERNAL_ERROR",
            message="BioReport generation returned an invalid payload",
        )

    registration_response = await tracked_integration_call(
        db,
        provider=_PROVIDER,
        api_url=bioreport_register_endpoint(),
        engagement_id=engagement_id,
        user_id=user_id,
        request_payload=summarize_bioreport_payload(bioreport_payload),
        operation=lambda: client.register_report(bioreport_payload),
        reraise=True,
    )
    if not isinstance(registration_response, dict):
        raise AppError(
            status_code=502,
            error_code="BIO_AI_REPORTS_ERROR",
            message="bio-ai-reports returned an invalid response",
        )
    return extract_registered_report_url(registration_response)


async def _generate_bioreport_payload(
    service: BioReportService,
    *,
    assessment_instance_id: int,
    db: AsyncSession,
) -> dict[str, Any]:
    report = await service.generate_for_assessment_instance(
        assessment_instance_id=assessment_instance_id,
        db=db,
    )
    return report.to_dict()
