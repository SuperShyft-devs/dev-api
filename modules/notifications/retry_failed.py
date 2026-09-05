"""Retry failed notification dispatches (cron job helper)."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.audit.models import IntegrationSyncLog
from modules.notifications.dedup import should_skip_notification
from modules.notifications.models import Notification, NotificationService
from modules.notifications.repository import NotificationsRepository
from modules.notifications.schemas import DispatchRequest, SessionDetails
from modules.notifications.service import NotificationsService

logger = logging.getLogger(__name__)

DEFAULT_HOURS = 24
DEFAULT_DELAY_SECONDS = 4
DEFAULT_LIMIT = 400
DEFAULT_MAX_CONSECUTIVE_FAILURES = 5

_WEBHOOK_FAILURE_SNIPPET = "webhook call failed"
_EMAIL_FAILURE_SNIPPET = "email sending failed"


@dataclass(frozen=True)
class RetryCandidate:
    """A single user-level retry derived from a failed notification row."""

    notification_id: int
    service_key: str
    user_id: int
    engagement_id: int | None
    assessment_instance_id: int | None
    dispatched_at: datetime | None


def _parse_user_ids(raw_user: Any) -> list[int]:
    if not isinstance(raw_user, dict):
        return []
    user_ids = raw_user.get("user_ids") or []
    return [int(uid) for uid in user_ids if isinstance(uid, int)]


def dedupe_failed_notifications(notifications: list[Notification]) -> list[RetryCandidate]:
    """Keep the latest failed row per (service_key, user_id, engagement_id)."""
    best: dict[tuple[str, int, int | None], RetryCandidate] = {}
    for notification in notifications:
        for user_id in _parse_user_ids(notification.user):
            key = (notification.service_key, user_id, notification.engagement_id)
            candidate = RetryCandidate(
                notification_id=notification.notification_id,
                service_key=notification.service_key,
                user_id=user_id,
                engagement_id=notification.engagement_id,
                assessment_instance_id=notification.assessment_instance_id,
                dispatched_at=notification.dispatched_at,
            )
            existing = best.get(key)
            if existing is None or notification.notification_id > existing.notification_id:
                best[key] = candidate
    return sorted(best.values(), key=lambda c: c.notification_id)


async def list_failed_notifications(
    db: AsyncSession,
    *,
    hours: int,
    channel: str,
    service_key: str | None = None,
) -> list[Notification]:
    """Return failed notifications in the lookback window, excluding OTP services."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    query = (
        select(Notification)
        .join(NotificationService, Notification.service_key == NotificationService.service_key)
        .where(Notification.status == "failed")
        .where(Notification.channel == channel)
        .where(Notification.dispatched_at.isnot(None))
        .where(Notification.dispatched_at >= cutoff)
        .where(NotificationService.is_active.is_(True))
        .where(NotificationService.require_otp.is_(False))
        .order_by(Notification.notification_id.desc())
    )
    if service_key is not None:
        query = query.where(Notification.service_key == service_key)
    result = await db.execute(query)
    return list(result.scalars().all())


async def load_n8n_request_payload(
    db: AsyncSession,
    *,
    notification_id: int,
) -> dict[str, Any] | None:
    """Load the original n8n webhook body for a notification dispatch."""
    result = await db.execute(
        select(IntegrationSyncLog.request_payload)
        .where(IntegrationSyncLog.provider == "n8n")
        .where(IntegrationSyncLog.request_payload.isnot(None))
        .where(IntegrationSyncLog.request_payload["notification_id"].as_integer() == notification_id)
        .order_by(IntegrationSyncLog.sync_log_id.desc())
        .limit(1)
    )
    payload = result.scalar_one_or_none()
    return payload if isinstance(payload, dict) else None


def _member_for_user(payload: dict[str, Any], user_id: int) -> dict[str, Any] | None:
    members = payload.get("members")
    if not isinstance(members, list) or not members:
        return None
    for member in members:
        if not isinstance(member, dict):
            continue
        # Single-user payloads have one member; multi-user may omit user_id on member.
        if len(members) == 1:
            return member
    return members[0] if isinstance(members[0], dict) else None


def build_dispatch_request(
    candidate: RetryCandidate,
    *,
    service: NotificationService,
    sync_payload: dict[str, Any] | None,
) -> tuple[DispatchRequest | None, str | None]:
    """Build a dispatch payload for retry; return skip reason when required fields are missing."""
    kwargs: dict[str, Any] = {
        "service_key": candidate.service_key,
        "user_ids": [candidate.user_id],
        "engagement_id": candidate.engagement_id,
        "assessment_instance_id": candidate.assessment_instance_id,
    }

    if sync_payload:
        participant_details = sync_payload.get("participant_details")
        if participant_details is not None:
            kwargs["participant_details"] = participant_details

        member = _member_for_user(sync_payload, candidate.user_id)
        if member:
            external_link = (member.get("external_link") or "").strip()
            if external_link:
                kwargs["external_link"] = external_link
            session_details = member.get("session_details")
            if isinstance(session_details, dict) and session_details:
                kwargs["session_details_by_user_id"] = {
                    candidate.user_id: SessionDetails.model_validate(session_details),
                }

    if service.require_participant_detail and not kwargs.get("participant_details"):
        return None, "missing participant_details in sync log"
    if service.require_session_details and not kwargs.get("session_details_by_user_id"):
        return None, "missing session_details in sync log"
    if service.require_external_link and not kwargs.get("external_link"):
        return None, "missing external_link in sync log"

    return DispatchRequest(**kwargs), None


def is_dispatch_failure(result: dict[str, Any]) -> bool:
    """True when dispatch indicates an immediate send/webhook failure worth circuit-breaking."""
    status = (result.get("status") or "").lower()
    message = (result.get("message") or "").lower()
    if status == "failed":
        return True
    if _WEBHOOK_FAILURE_SNIPPET in message:
        return True
    if _EMAIL_FAILURE_SNIPPET in message:
        return True
    return False


async def retry_failed_notifications(
    session_factory,
    *,
    notifications_service: NotificationsService,
    repository: NotificationsRepository | None = None,
    hours: int = DEFAULT_HOURS,
    channel: str = "email",
    service_key: str | None = None,
    limit: int = DEFAULT_LIMIT,
    delay_seconds: float = DEFAULT_DELAY_SECONDS,
    max_consecutive_failures: int = DEFAULT_MAX_CONSECUTIVE_FAILURES,
    dry_run: bool = False,
    sleep_fn: Callable[[float], Awaitable[None]] | None = None,
) -> dict[str, Any]:
    """Select failed notifications and retry them with throttling."""
    repo = repository or NotificationsRepository()
    sleep = sleep_fn or asyncio.sleep

    async with session_factory() as session:
        failed_rows = await list_failed_notifications(
            session,
            hours=hours,
            channel=channel,
            service_key=service_key,
        )
        candidates = dedupe_failed_notifications(failed_rows)[:limit]

    matched = len(candidates)
    retried = 0
    skipped = 0
    failed = 0
    stopped_early = False
    consecutive_failures = 0
    details: list[dict[str, Any]] = []

    for index, candidate in enumerate(candidates):
        async with session_factory() as session:
            skip_reason = None
            if candidate.engagement_id is not None:
                skip_reason = await should_skip_notification(
                    session,
                    service_key=candidate.service_key,
                    user_id=candidate.user_id,
                    engagement_id=candidate.engagement_id,
                    repository=repo,
                )

            if skip_reason:
                skipped += 1
                details.append(
                    {
                        "notification_id": candidate.notification_id,
                        "user_id": candidate.user_id,
                        "service_key": candidate.service_key,
                        "action": "skipped",
                        "reason": skip_reason,
                    }
                )
            else:
                service = await repo.get_service_by_key(session, service_key=candidate.service_key)
                if service is None or not service.is_active:
                    skipped += 1
                    details.append(
                        {
                            "notification_id": candidate.notification_id,
                            "user_id": candidate.user_id,
                            "service_key": candidate.service_key,
                            "action": "skipped",
                            "reason": "service inactive or missing",
                        }
                    )
                elif dry_run:
                    details.append(
                        {
                            "notification_id": candidate.notification_id,
                            "user_id": candidate.user_id,
                            "service_key": candidate.service_key,
                            "action": "would_retry",
                            "reason": "dry-run",
                        }
                    )
                else:
                    sync_payload = await load_n8n_request_payload(
                        session,
                        notification_id=candidate.notification_id,
                    )
                    dispatch_request, build_skip = build_dispatch_request(
                        candidate,
                        service=service,
                        sync_payload=sync_payload,
                    )
                    if build_skip:
                        skipped += 1
                        details.append(
                            {
                                "notification_id": candidate.notification_id,
                                "user_id": candidate.user_id,
                                "service_key": candidate.service_key,
                                "action": "skipped",
                                "reason": build_skip,
                            }
                        )
                    else:
                        try:
                            result = await notifications_service.dispatch(
                                session,
                                payload=dispatch_request,
                                triggered_by_user_id=None,
                            )
                            await session.commit()
                            retried += 1
                            dispatch_failed = is_dispatch_failure(result)
                            if dispatch_failed:
                                failed += 1
                                consecutive_failures += 1
                            else:
                                consecutive_failures = 0
                            details.append(
                                {
                                    "notification_id": candidate.notification_id,
                                    "user_id": candidate.user_id,
                                    "service_key": candidate.service_key,
                                    "action": "retried",
                                    "new_notification_id": result.get("notification_id"),
                                    "status": result.get("status"),
                                    "message": result.get("message"),
                                }
                            )
                        except AppError as exc:
                            await session.rollback()
                            failed += 1
                            consecutive_failures += 1
                            details.append(
                                {
                                    "notification_id": candidate.notification_id,
                                    "user_id": candidate.user_id,
                                    "service_key": candidate.service_key,
                                    "action": "failed",
                                    "reason": exc.message,
                                }
                            )
                        except Exception as exc:
                            await session.rollback()
                            failed += 1
                            consecutive_failures += 1
                            logger.exception(
                                "Retry failed for notification_id=%s user_id=%s",
                                candidate.notification_id,
                                candidate.user_id,
                            )
                            details.append(
                                {
                                    "notification_id": candidate.notification_id,
                                    "user_id": candidate.user_id,
                                    "service_key": candidate.service_key,
                                    "action": "failed",
                                    "reason": str(exc),
                                }
                            )

        if (
            not dry_run
            and consecutive_failures >= max_consecutive_failures
            and index < len(candidates) - 1
        ):
            stopped_early = True
            details.append(
                {
                    "action": "stopped",
                    "reason": (
                        f"circuit breaker: {consecutive_failures} consecutive failures "
                        f"(max {max_consecutive_failures})"
                    ),
                }
            )
            break

        if not dry_run and index < len(candidates) - 1 and delay_seconds > 0:
            await sleep(delay_seconds)

    would_retry = sum(1 for d in details if d.get("action") == "would_retry")
    return {
        "dry_run": dry_run,
        "hours": hours,
        "channel": channel,
        "service_key": service_key,
        "limit": limit,
        "delay_seconds": delay_seconds,
        "max_consecutive_failures": max_consecutive_failures,
        "matched": matched,
        "retried": retried if not dry_run else would_retry,
        "skipped": skipped,
        "failed": failed,
        "stopped_early": stopped_early,
        "details": details,
    }
