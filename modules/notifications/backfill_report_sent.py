"""DB-only backfill: mark report notifications as sent for multiple engagements.

Cohorts (embedded data, no Excel at runtime):
- engagement_id 16 — CBTW Pvt ltd
- engagement_id 72 — Celebal Technologies Male
- engagement_id 73 — Celebal Technologies Female

Rule: for each row, resolve/create user by phone and enroll in engagement_id.
Only channels with an Excel tick (embedded True) get notifications.status=sent
(INSERT if missing or existing is not sent; skip if already sent — never UPDATE).
Blank / False channels are ignored. Never dispatches / n8n / email / WhatsApp.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import cast, select, type_coerce
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from common.phone import phone_lookup_candidates
from core.exceptions import AppError
from modules.engagements.models import EngagementNotification, EngagementParticipant
from modules.engagements.repository import EngagementsRepository
from modules.notifications.models import Notification, NotificationService
from modules.notifications.report_sent_backfill_data import BACKFILL_COHORTS
from modules.notifications.repository import NotificationsRepository
from modules.users.models import User
from modules.users.repository import UsersRepository
from modules.users.service import UsersService

logger = logging.getLogger(__name__)

BACKFILL_MESSAGE = "Backfilled from registrations embed (no dispatch)"

CHANNEL_FLAG_TO_SERVICE_KEY: dict[str, str] = {
    "email_blood": "send-blood-report-email-v2",
    "email_bioai": "send-bioai-report-email-v2",
    "whatsapp_blood": "send-blood-report-whatsapp",
    "whatsapp_bioai": "send-bioai-whatsapp",
}

# Concept headers used when remapping engagement_notifications overrides
CONCEPT_TO_FALLBACK_KEY: dict[str, str] = {
    "email blood report sent": "send-blood-report-email-v2",
    "email bio-ai report sent": "send-bioai-report-email-v2",
    "whatsapp blood report sent": "send-blood-report-whatsapp",
    "whatsapp bio-ai report sent": "send-bioai-whatsapp",
}


@dataclass
class SourceRow:
    row_number: int
    first_name: str | None
    last_name: str | None
    phone: str
    email: str | None
    channels_to_mark: dict[str, bool]


@dataclass
class CohortResult:
    dry_run: bool
    engagement_id: int
    label: str
    rows_read: int = 0
    users_found: int = 0
    users_created: int = 0
    enrollments_created: int = 0
    notifications_inserted: int = 0
    notifications_updated: int = 0
    notifications_skipped: int = 0
    errors: list[dict[str, Any]] = field(default_factory=list)
    actions: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dry_run": self.dry_run,
            "engagement_id": self.engagement_id,
            "label": self.label,
            "rows_read": self.rows_read,
            "users_found": self.users_found,
            "users_created": self.users_created,
            "enrollments_created": self.enrollments_created,
            "notifications_inserted": self.notifications_inserted,
            "notifications_updated": self.notifications_updated,
            "notifications_skipped": self.notifications_skipped,
            "errors": self.errors,
            "actions": self.actions,
        }


@dataclass
class BackfillResult:
    dry_run: bool
    cohorts: list[CohortResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dry_run": self.dry_run,
            "source": "embedded",
            "cohorts": [c.to_dict() for c in self.cohorts],
            "totals": {
                "rows_read": sum(c.rows_read for c in self.cohorts),
                "users_found": sum(c.users_found for c in self.cohorts),
                "users_created": sum(c.users_created for c in self.cohorts),
                "enrollments_created": sum(c.enrollments_created for c in self.cohorts),
                "notifications_inserted": sum(
                    c.notifications_inserted for c in self.cohorts
                ),
                "notifications_updated": sum(
                    c.notifications_updated for c in self.cohorts
                ),
                "notifications_skipped": sum(
                    c.notifications_skipped for c in self.cohorts
                ),
                "errors": sum(len(c.errors) for c in self.cohorts),
            },
        }


def _cell_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_phone_for_store(raw: str) -> str:
    digits = "".join(ch for ch in (raw or "") if ch.isdigit())
    if len(digits) == 10:
        return digits
    if len(digits) == 12 and digits.startswith("91"):
        return digits[2:]
    if len(digits) == 11 and digits.startswith("0"):
        return digits[1:]
    return (raw or "").strip().replace(" ", "").replace("-", "")


def _rows_from_cohort(cohort: dict) -> list[SourceRow]:
    rows: list[SourceRow] = []
    for i, raw in enumerate(cohort.get("rows") or [], start=1):
        channels: dict[str, bool] = {}
        for flag, service_key in CHANNEL_FLAG_TO_SERVICE_KEY.items():
            # Tick only (True). Blank / False / missing = ignore that channel.
            if raw.get(flag) is True:
                channels[service_key] = True
        if not channels:
            continue
        rows.append(
            SourceRow(
                row_number=i,
                first_name=_cell_str(raw.get("first_name")),
                last_name=_cell_str(raw.get("last_name")),
                phone=str(raw.get("phone") or "").strip(),
                email=_cell_str(raw.get("email")),
                channels_to_mark=channels,
            )
        )
    return rows


async def _resolve_service_keys_for_engagement(
    db: AsyncSession,
    *,
    engagement_id: int,
) -> dict[str, str]:
    mapping = dict(CONCEPT_TO_FALLBACK_KEY)
    from modules.engagements.models import EngagementNotification

    result = await db.execute(
        select(EngagementNotification.event_code, EngagementNotification.notification_services)
        .where(EngagementNotification.engagement_id == engagement_id)
        .where(
            EngagementNotification.event_code.in_(
                ("reports.blood_ready", "reports.bioai_ready")
            )
        )
    )
    configured: dict[str, list[str]] = {"blood": [], "bioai": []}
    for event_code, services in result.all():
        keys: list[str] = []
        if isinstance(services, list):
            for item in services:
                if isinstance(item, dict) and item.get("service_key"):
                    keys.append(str(item["service_key"]))
                elif isinstance(item, str):
                    keys.append(item)
        bucket = "blood" if event_code == "reports.blood_ready" else "bioai"
        configured[bucket].extend(keys)

    async def _channel_of(service_key: str) -> str | None:
        row = await db.execute(
            select(NotificationService.channel).where(
                NotificationService.service_key == service_key
            )
        )
        return row.scalar_one_or_none()

    async def _pick(keys: list[str], *, channel: str, fallback: str) -> str:
        for key in keys:
            if await _channel_of(key) == channel:
                return key
        return fallback

    blood_keys = configured["blood"]
    bio_keys = configured["bioai"]
    if blood_keys or bio_keys:
        mapping = {
            "email blood report sent": await _pick(
                blood_keys, channel="email", fallback=mapping["email blood report sent"]
            ),
            "email bio-ai report sent": await _pick(
                bio_keys, channel="email", fallback=mapping["email bio-ai report sent"]
            ),
            "whatsapp blood report sent": await _pick(
                blood_keys,
                channel="whatsapp",
                fallback=mapping["whatsapp blood report sent"],
            ),
            "whatsapp bio-ai report sent": await _pick(
                bio_keys,
                channel="whatsapp",
                fallback=mapping["whatsapp bio-ai report sent"],
            ),
        }
    return mapping


def _remap_row_channels(
    row: SourceRow,
    concept_to_key: dict[str, str],
) -> dict[str, bool]:
    remapped: dict[str, bool] = {}
    inverse = {v: k for k, v in CONCEPT_TO_FALLBACK_KEY.items()}
    for service_key in row.channels_to_mark:
        concept = inverse.get(service_key)
        if concept is None:
            remapped[service_key] = True
            continue
        remapped[concept_to_key.get(concept, service_key)] = True
    return remapped


async def _find_notification(
    db: AsyncSession,
    *,
    service_key: str,
    engagement_id: int,
    user_id: int,
) -> Notification | None:
    result = await db.execute(
        select(Notification)
        .where(Notification.service_key == service_key)
        .where(Notification.engagement_id == engagement_id)
        .where(
            cast(Notification.user, JSONB)["user_ids"].contains(
                type_coerce([user_id], JSONB)
            )
        )
        .order_by(Notification.notification_id.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def _backfill_cohort(
    db: AsyncSession,
    *,
    cohort: dict,
    dry_run: bool,
) -> CohortResult:
    engagement_id = int(cohort["engagement_id"])
    label = str(cohort.get("label") or f"engagement_{engagement_id}")
    result = CohortResult(dry_run=dry_run, engagement_id=engagement_id, label=label)

    users_repo = UsersRepository()
    users_service = UsersService(users_repo)
    engagements_repo = EngagementsRepository()
    notifications_repo = NotificationsRepository()

    engagement = await engagements_repo.get_engagement_by_id(db, engagement_id)
    if engagement is None:
        result.errors.append({"error": f"engagement_id={engagement_id} not found"})
        return result

    concept_to_key = await _resolve_service_keys_for_engagement(
        db, engagement_id=engagement_id
    )
    service_keys = list(dict.fromkeys(concept_to_key.values()))
    channel_by_key: dict[str, str] = {}
    for sk in service_keys:
        svc = await notifications_repo.get_service_by_key(db, service_key=sk)
        if svc is None:
            result.errors.append({"error": f"unknown notification service_key={sk}"})
            continue
        channel_by_key[sk] = svc.channel

    if result.errors and not channel_by_key:
        return result

    source_rows = _rows_from_cohort(cohort)
    result.rows_read = len(source_rows)
    now = datetime.now(timezone.utc)
    referred_by = (getattr(engagement, "engagement_code", None) or "").strip() or None

    for row in source_rows:
        action: dict[str, Any] = {
            "engagement_id": engagement_id,
            "row": row.row_number,
            "first_name": row.first_name,
            "last_name": row.last_name,
            "phone": row.phone,
            "email": row.email,
        }
        if not row.phone or not phone_lookup_candidates(row.phone):
            result.errors.append({**action, "error": "invalid or missing phone"})
            continue

        try:
            user = await users_service.resolve_user_by_phone(db, row.phone)
        except AppError as exc:
            result.errors.append({**action, "error": str(exc.message)})
            continue

        created_user = False
        email_val = (row.email or "").strip() or None
        if user is None and email_val:
            email_owner = await users_repo.get_user_by_email(db, email_val)
            if email_owner is not None:
                user = email_owner
                action["resolved_by"] = "email"

        if user is None:
            phone_store = _normalize_phone_for_store(row.phone)
            if dry_run:
                result.users_created += 1
                action["user"] = "would_create"
                action["channels"] = list(_remap_row_channels(row, concept_to_key).keys())
                action["notifications"] = "would_insert_after_create"
                result.actions.append(action)
                continue

            user = User(
                first_name=row.first_name,
                last_name=row.last_name,
                phone=phone_store,
                email=email_val,
                referred_by=referred_by,
                is_participant=True,
                status="active",
            )
            try:
                user = await users_repo.create_user(db, user)
            except IntegrityError as exc:
                result.errors.append(
                    {
                        **action,
                        "error": (
                            f"create user failed: "
                            f"{exc.orig if getattr(exc, 'orig', None) else exc}"
                        ),
                    }
                )
                continue
            created_user = True
            result.users_created += 1
            action["user_created"] = True
            action["user_id"] = int(user.user_id)
        else:
            result.users_found += 1
            action["user_id"] = int(user.user_id)
            action["user_created"] = False

        participant = await engagements_repo.get_participant_for_user_engagement(
            db,
            user_id=int(user.user_id),
            engagement_id=int(engagement.engagement_id),
        )
        if participant is None:
            if dry_run:
                action["enroll"] = "would_enroll"
                result.enrollments_created += 1
            else:
                camp_no = engagement.camp_no
                if camp_no is not None:
                    same_camp = await engagements_repo.get_participant_for_user_camp_no(
                        db,
                        user_id=int(user.user_id),
                        camp_no=int(camp_no),
                        exclude_engagement_id=int(engagement.engagement_id),
                    )
                    if same_camp is not None:
                        result.errors.append(
                            {
                                **action,
                                "error": "already enrolled in another engagement for this camp",
                            }
                        )
                        continue
                status = (engagement.status or "").lower()
                if status not in ("scheduled", "running", "draft"):
                    result.errors.append(
                        {
                            **action,
                            "error": (
                                f"engagement status {engagement.status!r} "
                                "not accepting enrollments"
                            ),
                        }
                    )
                    continue
                participant = EngagementParticipant(
                    engagement_id=int(engagement.engagement_id),
                    user_id=int(user.user_id),
                    booked_by_user_id=int(user.user_id)
                )
                await engagements_repo.create_participant(db, participant)
                result.enrollments_created += 1
                action["enrolled"] = True
        else:
            action["enrolled"] = False

        channels = _remap_row_channels(row, concept_to_key)
        channel_actions: list[dict[str, Any]] = []
        for service_key in channels:
            channel = channel_by_key.get(service_key)
            if channel is None:
                channel_actions.append(
                    {"service_key": service_key, "error": "unknown service_key"}
                )
                continue

            existing = await _find_notification(
                db,
                service_key=service_key,
                engagement_id=int(engagement.engagement_id),
                user_id=int(user.user_id),
            )
            existing_status = (
                (existing.status or "").lower() if existing is not None else None
            )
            if existing is not None and existing_status == "sent":
                result.notifications_skipped += 1
                channel_actions.append(
                    {
                        "service_key": service_key,
                        "action": "skipped_already_sent",
                        "status": existing.status,
                        "notification_id": int(existing.notification_id),
                    }
                )
                continue

            # Missing or not-sent: INSERT a new sent row (never UPDATE).
            insert_meta: dict[str, Any] = {"service_key": service_key}
            if existing is not None:
                insert_meta["existing_notification_id"] = int(existing.notification_id)
                insert_meta["existing_status"] = existing.status

            if dry_run:
                result.notifications_inserted += 1
                channel_actions.append({**insert_meta, "action": "would_insert"})
            else:
                notification = Notification(
                    service_key=service_key,
                    status="sent",
                    channel=channel,
                    user={"user_ids": [int(user.user_id)]},
                    engagement_id=int(engagement.engagement_id),
                    assessment_instance_id=None,
                    message=BACKFILL_MESSAGE,
                    triggered_by_user_id=None,
                    dispatched_at=now,
                    completed_at=now,
                )
                await notifications_repo.create_notification(db, notification)
                result.notifications_inserted += 1
                channel_actions.append(
                    {
                        **insert_meta,
                        "action": "inserted",
                        "notification_id": int(notification.notification_id),
                    }
                )

        action["notifications"] = channel_actions
        action["created_user"] = created_user
        result.actions.append(action)

    return result


async def backfill_report_sent(
    db: AsyncSession,
    *,
    engagement_ids: list[int] | None = None,
    dry_run: bool = True,
) -> BackfillResult:
    """Run DB-only sent backfill for one or more engagement cohorts."""
    wanted = set(engagement_ids) if engagement_ids else None
    result = BackfillResult(dry_run=dry_run)

    for cohort in BACKFILL_COHORTS:
        eid = int(cohort["engagement_id"])
        if wanted is not None and eid not in wanted:
            continue
        cohort_result = await _backfill_cohort(db, cohort=cohort, dry_run=dry_run)
        result.cohorts.append(cohort_result)

    if wanted is not None:
        found = {c.engagement_id for c in result.cohorts}
        for missing in sorted(wanted - found):
            result.cohorts.append(
                CohortResult(
                    dry_run=dry_run,
                    engagement_id=missing,
                    label=f"engagement_{missing}",
                    errors=[{"error": f"no embedded cohort for engagement_id={missing}"}],
                )
            )

    return result
