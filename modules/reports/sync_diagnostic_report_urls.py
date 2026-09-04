"""Sync diagnostic_report_url from Healthians for participants with booking_id.

URL-only backfill: calls getBookingReport, archives PDFs when needed, and updates
``individual_health_report.diagnostic_report_url`` (plus full_report / verified_at).
No digital values, Metsights push, or notifications.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from db.transaction import release_request_transaction
from modules.assessments.models import AssessmentInstance
from modules.audit.cron_sync_logging import tracked_integration_call
from modules.diagnostics.healthians import client as healthians_client
from modules.diagnostics.models import DiagnosticPackage
from modules.engagements.models import Engagement, EngagementParticipant
from modules.reports.blood_report_archival import (
    diagnostic_report_url_to_persist,
    is_archived_blood_report_url,
    resolve_persistable_diagnostic_report_url,
)
from modules.reports.healthians_report_fields import (
    customer_display_name,
    parse_booking_report_entry,
)
from modules.reports.models import IndividualHealthReport
from modules.reports.repository import ReportsRepository
from modules.users.models import User

logger = logging.getLogger(__name__)

ProgressCallback = Callable[[int, int, int, int, int], None]


def _healthians_url(path: str) -> str:
    return f"{settings.HEALTHIANS_BASE_URL.rstrip('/')}/{path.lstrip('/')}"


def _match_customer_by_name(
    data_list: list[Any],
    first_name: str,
    last_name: str,
) -> dict[str, Any] | None:
    """Find the customer entry whose name matches the user (case-insensitive, tokenised)."""
    target_full = f"{first_name} {last_name}".strip().lower()
    target_tokens = set(target_full.split())

    best: dict[str, Any] | None = None
    best_score = 0

    for entry in data_list:
        if not isinstance(entry, dict):
            continue
        customer_name = customer_display_name(entry).lower()
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


async def _get_eligible_participants(
    db: AsyncSession,
    *,
    engagement_id: int | None = None,
) -> list[tuple]:
    """Return participants with a non-empty booking_id (all engagement statuses).

    Returns tuples of:
    (user_id, engagement_id, first_name, last_name, booking_id, diagnostic_provider)
    """
    query = (
        select(
            EngagementParticipant.user_id,
            Engagement.engagement_id,
            User.first_name,
            User.last_name,
            EngagementParticipant.booking_id,
            DiagnosticPackage.diagnostic_provider,
        )
        .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
        .join(User, User.user_id == EngagementParticipant.user_id)
        .outerjoin(
            DiagnosticPackage,
            DiagnosticPackage.diagnostic_package_id == Engagement.diagnostic_package_id,
        )
        .where(EngagementParticipant.booking_id.isnot(None))
        .where(EngagementParticipant.booking_id != "")
    )
    if engagement_id is not None:
        query = query.where(Engagement.engagement_id == engagement_id)
    result = await db.execute(query)
    return result.all()


async def _resolve_assessment_instance_id(
    db: AsyncSession,
    *,
    user_id: int,
    engagement_id: int,
) -> int | None:
    repo = ReportsRepository()
    primary = await repo.get_primary_assessment_instance_for_user_engagement(
        db,
        user_id=user_id,
        engagement_id=engagement_id,
    )
    if primary is not None:
        return int(primary.assessment_instance_id)
    fallback = await db.execute(
        select(AssessmentInstance.assessment_instance_id)
        .where(AssessmentInstance.user_id == user_id)
        .where(AssessmentInstance.engagement_id == engagement_id)
        .order_by(AssessmentInstance.assessment_instance_id.asc())
        .limit(1)
    )
    value = fallback.scalar_one_or_none()
    return int(value) if value is not None else None


async def _get_or_create_ihr(
    db: AsyncSession,
    *,
    repo: ReportsRepository,
    user_id: int,
    engagement_id: int,
    instance_id: int | None,
) -> IndividualHealthReport:
    ihr = await repo.get_individual_report_by_engagement(
        db,
        user_id=user_id,
        engagement_id=engagement_id,
    )

    if ihr is None:
        ihr = IndividualHealthReport(
            user_id=user_id,
            engagement_id=engagement_id,
            assessment_instance_id=instance_id,
        )
        db.add(ihr)
    elif ihr.assessment_instance_id is None and instance_id is not None:
        ihr.assessment_instance_id = instance_id
    return ihr


async def sync_diagnostic_report_urls(
    db: AsyncSession,
    *,
    dry_run: bool = False,
    force: bool = False,
    engagement_id: int | None = None,
    on_progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    """Fetch Healthians booking reports and persist diagnostic_report_url only."""
    participants = await _get_eligible_participants(db, engagement_id=engagement_id)
    await release_request_transaction(db)
    matched = len(participants)
    updated = 0
    skipped = 0
    failed = 0
    details: list[dict[str, Any]] = []
    repo = ReportsRepository()

    def _report(done: int) -> None:
        if on_progress is not None:
            on_progress(done, matched, updated, skipped, failed)

    _report(0)

    for index, row in enumerate(participants, start=1):
        try:
            (
                user_id,
                row_engagement_id,
                first_name,
                last_name,
                booking_id,
                diagnostic_provider,
            ) = row

            booking_id = (booking_id or "").strip()
            if not booking_id:
                skipped += 1
                details.append({
                    "user_id": user_id,
                    "engagement_id": row_engagement_id,
                    "action": "skipped",
                    "reason": "empty booking_id",
                })
                continue

            existing_ihr = await repo.get_individual_report_by_engagement(
                db,
                user_id=user_id,
                engagement_id=row_engagement_id,
            )
            diag_url = existing_ihr.diagnostic_report_url if existing_ihr else None
            stored_full_report = (
                existing_ihr.blood_parameters_full_report if existing_ihr else None
            )
            stored_verified_at = (
                existing_ihr.blood_parameters_verified_at if existing_ihr else None
            )

            existing_url = (diag_url or "").strip()
            if not force and existing_url and is_archived_blood_report_url(existing_url):
                skipped += 1
                details.append({
                    "user_id": user_id,
                    "engagement_id": row_engagement_id,
                    "action": "skipped",
                    "reason": "diagnostic_report_url already archived",
                })
                continue

            instance_id = await _resolve_assessment_instance_id(
                db,
                user_id=user_id,
                engagement_id=row_engagement_id,
            )

            if dry_run:
                details.append({
                    "user_id": user_id,
                    "engagement_id": row_engagement_id,
                    "action": "dry_run",
                    "reason": (
                        f"would_fetch_getBookingReport for booking_id={booking_id}"
                        + (f" (provider={diagnostic_provider})" if diagnostic_provider else "")
                    ),
                })
                continue

            await release_request_transaction(db)

            access_token = await tracked_integration_call(
                db,
                provider="healthians",
                api_url=_healthians_url("toast4health/getAccessToken"),
                engagement_id=row_engagement_id,
                user_id=user_id,
                request_payload=None,
                operation=healthians_client.get_access_token,
            )
            if access_token is None:
                skipped += 1
                details.append({
                    "user_id": user_id,
                    "engagement_id": row_engagement_id,
                    "action": "skipped",
                    "reason": "Healthians authentication failed",
                })
                continue

            report_data = await tracked_integration_call(
                db,
                provider="healthians",
                api_url=_healthians_url("toast4health/getBookingReport"),
                engagement_id=row_engagement_id,
                user_id=user_id,
                request_payload={"booking_id": booking_id},
                operation=lambda: healthians_client.get_booking_report(
                    access_token, booking_id
                ),
                reraise=False,
            )
            if report_data is None:
                skipped += 1
                details.append({
                    "user_id": user_id,
                    "engagement_id": row_engagement_id,
                    "action": "skipped",
                    "reason": "Healthians getBookingReport failed",
                })
                continue

            report_list = report_data.get("data")
            if not isinstance(report_list, list) or not report_list:
                skipped += 1
                details.append({
                    "user_id": user_id,
                    "engagement_id": row_engagement_id,
                    "action": "skipped",
                    "reason": "Healthians getBookingReport returned no report data",
                })
                continue

            matched_report = _match_customer_by_name(
                report_list, first_name or "", last_name or ""
            )
            if matched_report is None:
                skipped += 1
                details.append({
                    "user_id": user_id,
                    "engagement_id": row_engagement_id,
                    "action": "skipped",
                    "reason": "Healthians getBookingReport: no matching customer report",
                })
                continue

            fetched_diag_url, api_full_report, api_verified_at = parse_booking_report_entry(
                matched_report
            )
            if fetched_diag_url is None:
                skipped += 1
                details.append({
                    "user_id": user_id,
                    "engagement_id": row_engagement_id,
                    "action": "skipped",
                    "reason": "Healthians getBookingReport: no report_url in matched entry",
                })
                continue

            is_full_report = (
                bool(api_full_report)
                if api_full_report is not None
                else bool(stored_full_report) if stored_full_report is not None else False
            )

            storage_instance_id = instance_id if instance_id is not None else 0
            persistable_url = await resolve_persistable_diagnostic_report_url(
                fetched_diag_url or "",
                is_full_report=is_full_report,
                existing_url=diag_url,
                assessment_instance_id=storage_instance_id,
            )
            if is_full_report and persistable_url is None and not is_archived_blood_report_url(diag_url):
                details.append({
                    "user_id": user_id,
                    "engagement_id": row_engagement_id,
                    "action": "skipped",
                    "reason": "blood report PDF archival failed",
                })
                # Still fall through to clear any leftover Healthians/S3 URL.

            stored_diag_url = existing_url
            url_to_store = diagnostic_report_url_to_persist(
                is_full_report=is_full_report,
                persistable_url=persistable_url,
                existing_url=stored_diag_url,
            )
            metadata_unchanged = (
                not force
                and (url_to_store or "") == (stored_diag_url or "")
                and (api_full_report is None or bool(stored_full_report) == api_full_report)
                and (
                    api_verified_at is None
                    or (
                        stored_verified_at is not None
                        and stored_verified_at.replace(microsecond=0)
                        == api_verified_at.replace(microsecond=0)
                    )
                )
            )
            if metadata_unchanged:
                skipped += 1
                details.append({
                    "user_id": user_id,
                    "engagement_id": row_engagement_id,
                    "action": "skipped",
                    "reason": "diagnostic_report_url already up to date",
                })
                continue

            ihr = await _get_or_create_ihr(
                db,
                repo=repo,
                user_id=user_id,
                engagement_id=row_engagement_id,
                instance_id=instance_id,
            )
            ihr.diagnostic_report_url = url_to_store
            if api_full_report is not None:
                ihr.blood_parameters_full_report = api_full_report
            if api_verified_at is not None:
                ihr.blood_parameters_verified_at = api_verified_at
            await db.flush()
            await db.commit()
            updated += 1
            details.append({
                "user_id": user_id,
                "engagement_id": row_engagement_id,
                "action": "updated",
                "reason": f"diagnostic_report_url synced from booking_id={booking_id}",
            })
        except Exception as exc:
            failed += 1
            logger.exception(
                "sync_diagnostic_report_urls failed for user_id=%s engagement_id=%s",
                row[0] if row else "?",
                row[1] if row else "?",
            )
            details.append({
                "user_id": row[0] if row else 0,
                "engagement_id": row[1] if row else 0,
                "action": "failed",
                "reason": str(exc)[:500],
            })
            try:
                await db.rollback()
            except Exception:
                pass
        finally:
            _report(index)

    return {
        "dry_run": dry_run,
        "force": force,
        "engagement_id": engagement_id,
        "matched": matched,
        "updated": updated,
        "skipped": skipped,
        "failed": failed,
        "details": details,
    }
