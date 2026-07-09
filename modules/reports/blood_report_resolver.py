"""Resolve and cache blood diagnostic report URLs from Healthians."""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from core.exceptions import AppError
from modules.diagnostics.healthians import client as healthians_client
from modules.diagnostics.healthians.sync_log import finalize_healthians_sync_log, log_healthians_call
from modules.metsights.service import MetsightsService
from modules.reports.healthians_booking_resolver import (
    HealthiansBookingSource,
    resolve_healthians_booking_id,
)
from modules.reports.models import IndividualHealthReport
from modules.reports.repository import ReportsRepository
from modules.users.models import User


def _healthians_url(path: str) -> str:
    return f"{settings.HEALTHIANS_BASE_URL.rstrip('/')}/{path.lstrip('/')}"


def _match_customer_by_name(
    data_list: list[Any],
    first_name: str,
    last_name: str,
) -> dict[str, Any] | None:
    target_full = f"{first_name} {last_name}".strip().lower()
    target_tokens = set(target_full.split())

    best: dict[str, Any] | None = None
    best_score = 0

    for entry in data_list:
        if not isinstance(entry, dict):
            continue
        customer_name = str(entry.get("customer_name") or "").strip().lower()
        if not customer_name:
            continue
        if customer_name == target_full:
            return entry
        entry_tokens = set(customer_name.split())
        overlap = len(target_tokens & entry_tokens)
        if overlap > best_score:
            best_score = overlap
            best = entry

    if best is not None and best_score >= 1:
        return best
    return data_list[0] if data_list else None


def _extract_report_url_from_healthians_payload(
    report_data: dict[str, Any],
    *,
    first_name: str,
    last_name: str,
) -> str:
    report_list = report_data.get("data")
    if not isinstance(report_list, list) or not report_list:
        raise AppError(
            status_code=422,
            error_code="INVALID_STATE",
            message="Diagnostic report PDF is not available for this record",
        )
    matched_report = _match_customer_by_name(report_list, first_name, last_name)
    if matched_report is None:
        raise AppError(
            status_code=422,
            error_code="INVALID_STATE",
            message="Diagnostic report PDF is not available for this record",
        )
    raw_url = matched_report.get("report_url") or matched_report.get("url")
    if not isinstance(raw_url, str) or not raw_url.strip():
        raise AppError(
            status_code=422,
            error_code="INVALID_STATE",
            message="Diagnostic report PDF is not available for this record",
        )
    return raw_url.strip()


async def _fetch_healthians_report_url(
    db: AsyncSession,
    *,
    booking_id: str,
    engagement_id: int,
    user_id: int,
    first_name: str,
    last_name: str,
) -> str:
    sync_log = await log_healthians_call(
        db,
        engagement_id=engagement_id,
        user_id=user_id,
        provider="healthians",
        api_url=_healthians_url("toast4health/getBookingReport"),
        request_payload={"booking_id": booking_id, "action": "getBookingReport"},
        status="pending",
    )
    try:
        access_token = await healthians_client.get_access_token()
        report_data = await healthians_client.get_booking_report(access_token, booking_id)
        await finalize_healthians_sync_log(
            db,
            sync_log_id=sync_log.sync_log_id,
            status="success",
            response_payload=report_data if isinstance(report_data, dict) else None,
        )
    except Exception as exc:
        await finalize_healthians_sync_log(
            db,
            sync_log_id=sync_log.sync_log_id,
            status="failed",
            error_message=str(exc)[:2000],
        )
        raise AppError(
            status_code=503,
            error_code="EXTERNAL_SERVICE_UNAVAILABLE",
            message="Healthians booking report request failed",
        ) from exc

    if not isinstance(report_data, dict):
        raise AppError(
            status_code=422,
            error_code="INVALID_STATE",
            message="Diagnostic report PDF is not available for this record",
        )
    return _extract_report_url_from_healthians_payload(
        report_data,
        first_name=first_name,
        last_name=last_name,
    )


async def resolve_blood_report_url(
    db: AsyncSession,
    *,
    user_id: int,
    engagement_id: int,
    assessment_instance_id: int,
    metsights_record_id: str,
    metsights_service: MetsightsService,
    existing_ihr: IndividualHealthReport | None = None,
    reports_repository: ReportsRepository | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
) -> str:
    """Return a blood diagnostic report URL, fetching from Healthians and caching when needed."""
    repo = reports_repository or ReportsRepository()

    assessment_report = await repo.get_individual_report_by_assessment(
        db, assessment_instance_id=assessment_instance_id
    )
    engagement_report = await repo.get_individual_report_by_engagement(
        db,
        user_id=user_id,
        engagement_id=engagement_id,
    )
    for candidate in (assessment_report, engagement_report, existing_ihr):
        cached = (candidate.diagnostic_report_url if candidate is not None else None) or ""
        if cached.strip():
            return cached.strip()

    record_id = (metsights_record_id or "").strip()
    if not record_id:
        raise AppError(
            status_code=422,
            error_code="INVALID_STATE",
            message="Metsights record id is missing for this assessment",
        )

    if first_name is None or last_name is None:
        user_row = await db.execute(
            select(User.first_name, User.last_name).where(User.user_id == user_id)
        )
        user_names = user_row.one_or_none()
        first_name = (user_names[0] if user_names else "") or ""
        last_name = (user_names[1] if user_names else "") or ""

    resolved = await resolve_healthians_booking_id(
        db,
        user_id=user_id,
        engagement_id=engagement_id,
        record_id=record_id,
        metsights_service=metsights_service,
    )

    report_url: str | None = None
    if resolved.source == HealthiansBookingSource.PARTICIPANT:
        report_url = await _fetch_healthians_report_url(
            db,
            booking_id=resolved.booking_id,
            engagement_id=engagement_id,
            user_id=user_id,
            first_name=first_name or "",
            last_name=last_name or "",
        )
    else:
        collection_data = resolved.collection_data or {}
        file_url = collection_data.get("file")
        if isinstance(file_url, str) and file_url.strip():
            report_url = file_url.strip()
        else:
            report_url = await _fetch_healthians_report_url(
                db,
                booking_id=resolved.booking_id,
                engagement_id=engagement_id,
                user_id=user_id,
                first_name=first_name or "",
                last_name=last_name or "",
            )

    target = assessment_report or engagement_report or existing_ihr
    if target is None:
        target = IndividualHealthReport(
            user_id=user_id,
            engagement_id=engagement_id,
            assessment_instance_id=assessment_instance_id,
            diagnostic_report_url=report_url,
        )
        await repo.create_individual_report(db, target)
    else:
        target.diagnostic_report_url = report_url
        if target.assessment_instance_id is None:
            target.assessment_instance_id = assessment_instance_id
        await repo.update_individual_report(db, target)

    return report_url
