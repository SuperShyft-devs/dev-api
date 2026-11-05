"""Command Center bootstrap + drill-downs for admin dashboard.

Canonical B2B/B2C: engagements.organization_id IS NOT NULL / IS NULL.
See docs/dashboard-kpi-reference.md.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal

from sqlalchemy import and_, cast, func, or_, select, String, true
from sqlalchemy.ext.asyncio import AsyncSession

from common.masking import mask_email, mask_phone
from modules.assessments.models import AssessmentInstance
from modules.audit.repository import AuditRepository
from modules.diagnostics.models import DiagnosticPackage
from modules.discounts.models import DiscountCode, DiscountUsage
from modules.engagements.blood_booking_enums import BloodBookingStatus
from modules.engagements.enums import EngagementPaymentType
from modules.engagements.models import Engagement, EngagementParticipant, ParticipantBloodBooking
from modules.experts.models import ConsultationBooking
from modules.notifications.models import Notification
from modules.notifications.repository import NotificationsRepository
from modules.notifications.service import NotificationsService
from modules.organizations.models import Organization
from modules.payments.models import Booking
from modules.reports.models import IndividualHealthReport
from modules.server_health.state_repository import ServerHealthStateRepository
from modules.support.models import SupportTicket
from modules.users.models import User

logger = logging.getLogger(__name__)

Audience = Literal["all", "b2b", "b2c"]
Lifecycle = Literal["all", "ongoing", "upcoming", "completed"]

DRILL_IDS = frozenset(
    {
        "people_registered",
        "tests_completed",
        "lab_reports",
        "reports_delivered",
        "bio_ai_reports",
        "bio_ai_pending",
        "consultations",
        "consultations_pending",
        "questionnaire_done",
        "questionnaire_pending",
        "upcoming_collections",
        "overdue_collections",
        "blood_reports_pending",
        "collections_tomorrow",
        "tickets_open",
        "tickets_solved",
        "tickets_all",
        "payment_failed",
        "unserviceable",
        "booking_issues",
        "failed_notifications",
        "total_bookings",
        "open_engagements",
        "b2b_camps",
        "package_performance",
        "coupon_usage",
    }
)

_SENT_STATUSES = ("sent", "delivered", "read")
_PENDING_STATUSES = ("pending", "not_sent", "queued", "dispatching")
_ACTIVE_BLOOD = BloodBookingStatus.active.value
_OPEN_BOOKING = ("pending", "confirmed")
_B2C_BOOKING_STATUSES = ("pending", "confirmed", "failed")

_LIFECYCLE_STATUS = {
    "ongoing": "running",
    "upcoming": "scheduled",
    "completed": "completed",
}


def _parse_date(value: str | date | None, *, default: date | None = None) -> date | None:
    if value is None:
        return default
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return default
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return default


def _audience_engagement_filter(audience: Audience):
    if audience == "b2b":
        return Engagement.organization_id.is_not(None)
    if audience == "b2c":
        return Engagement.organization_id.is_(None)
    return true()


def _date_in_range(column, date_from: date | None, date_to: date | None):
    clauses = []
    if date_from is not None:
        clauses.append(func.date(column) >= date_from)
    if date_to is not None:
        clauses.append(func.date(column) <= date_to)
    if not clauses:
        return true()
    return and_(*clauses)


def _booking_meta_engagement_id():
    return Booking.metadata_.op("->>")("engagement_id")


def _b2b_engagement_id_texts():
    return select(cast(Engagement.engagement_id, String)).where(
        Engagement.organization_id.is_not(None)
    )


def _exclude_b2b_payment_bookings():
    meta = _booking_meta_engagement_id()
    return or_(meta.is_(None), meta == "", ~meta.in_(_b2b_engagement_id_texts()))


def _user_display_name(first: str | None, last: str | None) -> str:
    parts = [p for p in ((first or "").strip(), (last or "").strip()) if p]
    return " ".join(parts) if parts else "—"


async def _attach_journey_statuses(
    db: AsyncSession, items: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Attach questionnaire / bio-AI / consult statuses for journey-style drill columns."""
    if not items:
        return items
    user_ids = {int(i["user_id"]) for i in items if i.get("user_id") is not None}
    eng_ids = {int(i["engagement_id"]) for i in items if i.get("engagement_id") is not None}
    q_done: set[tuple[int, int]] = set()
    bio_done: set[tuple[int, int]] = set()
    consult_done: set[tuple[int, int]] = set()
    consult_booked: set[tuple[int, int]] = set()
    if user_ids and eng_ids:
        q_rows = (
            await db.execute(
                select(AssessmentInstance.user_id, AssessmentInstance.engagement_id)
                .where(AssessmentInstance.user_id.in_(user_ids))
                .where(AssessmentInstance.engagement_id.in_(eng_ids))
                .where(
                    or_(
                        AssessmentInstance.completed_at.is_not(None),
                        AssessmentInstance.metsights_record_id.is_not(None),
                    )
                )
            )
        ).all()
        q_done = {(int(r.user_id), int(r.engagement_id)) for r in q_rows if r.user_id and r.engagement_id}

        bio_rows = (
            await db.execute(
                select(IndividualHealthReport.user_id, IndividualHealthReport.engagement_id)
                .where(IndividualHealthReport.user_id.in_(user_ids))
                .where(IndividualHealthReport.engagement_id.in_(eng_ids))
                .where(
                    or_(
                        and_(
                            IndividualHealthReport.report_url.is_not(None),
                            func.trim(IndividualHealthReport.report_url) != "",
                        ),
                        IndividualHealthReport.reports.is_not(None),
                    )
                )
            )
        ).all()
        bio_done = {
            (int(r.user_id), int(r.engagement_id)) for r in bio_rows if r.user_id and r.engagement_id
        }

        c_rows = (
            await db.execute(
                select(
                    EngagementParticipant.user_id,
                    EngagementParticipant.engagement_id,
                    ConsultationBooking.done,
                    ConsultationBooking.want,
                )
                .select_from(ConsultationBooking)
                .join(
                    EngagementParticipant,
                    EngagementParticipant.engagement_participant_id
                    == ConsultationBooking.engagement_participant_id,
                )
                .where(EngagementParticipant.user_id.in_(user_ids))
                .where(EngagementParticipant.engagement_id.in_(eng_ids))
            )
        ).all()
        for r in c_rows:
            key = (int(r.user_id), int(r.engagement_id))
            if r.done:
                consult_done.add(key)
            elif r.want:
                consult_booked.add(key)

    for item in items:
        uid = item.get("user_id")
        eid = item.get("engagement_id")
        key = (int(uid), int(eid)) if uid is not None and eid is not None else None
        if key and key in q_done:
            item["questionnaire_status"] = "completed"
        else:
            item["questionnaire_status"] = "pending"
        if key and key in bio_done:
            item["bio_ai_status"] = "generated"
        elif item.get("lab_report_status") == "delivered":
            item["bio_ai_status"] = "pending"
        else:
            item["bio_ai_status"] = "not_started"
        if key and key in consult_done:
            item["consultation_status"] = "attended"
        elif key and key in consult_booked:
            item["consultation_status"] = "booked"
        else:
            item["consultation_status"] = "not_booked"
        if "collection_status" not in item:
            if item.get("collected_at"):
                item["collection_status"] = "collected"
            elif item.get("collection_date"):
                item["collection_status"] = "scheduled"
            else:
                item["collection_status"] = "not_scheduled"
        if "lab_report_status" not in item:
            item["lab_report_status"] = "not_started"
    return items


async def _bookings_for_package(
    db: AsyncSession,
    *,
    package_id: int,
    date_from: date | None,
    date_to: date | None,
    limit: int,
    offset: int,
) -> tuple[list[dict[str, Any]], int]:
    base = (
        select(
            Booking.booking_id,
            Booking.user_id,
            Booking.entity_name,
            Booking.amount_paise,
            Booking.status,
            Booking.booked_at,
            Booking.metadata_,
            User.first_name,
            User.last_name,
            User.phone,
            User.email,
            User.city,
        )
        .select_from(Booking)
        .outerjoin(User, User.user_id == Booking.user_id)
        .where(Booking.status.in_(_B2C_BOOKING_STATUSES))
        .where(Booking.entity_type == "diagnostic_package")
        .where(Booking.entity_id == package_id)
        .where(_exclude_b2b_payment_bookings())
        .where(_date_in_range(Booking.booked_at, date_from, date_to))
    )
    total = await _scalar_count(db, select(func.count()).select_from(base.order_by(None).subquery()))
    rows = (await db.execute(base.order_by(Booking.booked_at.desc()).offset(offset).limit(limit))).all()
    items: list[dict[str, Any]] = []
    for r in rows:
        meta = r.metadata_ if isinstance(r.metadata_, dict) else {}
        raw_eng = meta.get("engagement_id")
        try:
            engagement_id = int(raw_eng) if raw_eng not in (None, "") else None
        except (TypeError, ValueError):
            engagement_id = None
        items.append(
            {
                "booking_id": r.booking_id,
                "user_id": r.user_id,
                "user_name": _user_display_name(r.first_name, r.last_name),
                "phone": mask_phone(r.phone),
                "email": mask_email(r.email),
                "entity_name": r.entity_name,
                "package_name": r.entity_name,
                "amount_paise": r.amount_paise,
                "status": str(r.status),
                "booked_at": r.booked_at.isoformat() if r.booked_at else None,
                "engagement_id": engagement_id,
                "city": r.city,
                "audience": "b2c",
            }
        )
    await _attach_booking_collection(db, items)
    await _attach_journey_statuses(db, items)
    return items, total


async def _usages_for_coupon(
    db: AsyncSession,
    *,
    discount_code_id: int,
    date_from: date | None,
    date_to: date | None,
    limit: int,
    offset: int,
) -> tuple[list[dict[str, Any]], int]:
    base = (
        select(
            DiscountUsage.usage_id,
            DiscountUsage.user_id,
            DiscountUsage.engagement_id,
            DiscountUsage.discount_paise,
            DiscountUsage.created_at,
            DiscountUsage.status,
            DiscountCode.code,
            DiscountCode.name,
            User.first_name,
            User.last_name,
            User.phone,
            User.email,
            User.city,
        )
        .select_from(DiscountUsage)
        .join(DiscountCode, DiscountCode.discount_code_id == DiscountUsage.discount_code_id)
        .outerjoin(User, User.user_id == DiscountUsage.user_id)
        .where(DiscountUsage.discount_code_id == discount_code_id)
        .where(DiscountUsage.status == "committed")
        .where(_date_in_range(DiscountUsage.created_at, date_from, date_to))
    )
    total = await _scalar_count(db, select(func.count()).select_from(base.order_by(None).subquery()))
    rows = (
        await db.execute(base.order_by(DiscountUsage.created_at.desc()).offset(offset).limit(limit))
    ).all()
    items = [
        {
            "usage_id": r.usage_id,
            "user_id": r.user_id,
            "user_name": _user_display_name(r.first_name, r.last_name),
            "phone": mask_phone(r.phone),
            "email": mask_email(r.email),
            "city": r.city,
            "code": r.code,
            "name": r.name,
            "discount_paise": int(r.discount_paise or 0),
            "status": str(r.status),
            "booked_at": r.created_at.isoformat() if r.created_at else None,
            "engagement_id": r.engagement_id,
            "audience": "b2c",
        }
        for r in rows
    ]
    await _attach_booking_collection(db, items)
    await _attach_journey_statuses(db, items)
    return items, total


async def _attach_booking_collection(db: AsyncSession, items: list[dict[str, Any]]) -> None:
    """Fill collection / lab fields from PBB when engagement_id is known."""
    eng_ids = {int(i["engagement_id"]) for i in items if i.get("engagement_id") is not None}
    user_ids = {int(i["user_id"]) for i in items if i.get("user_id") is not None}
    if not eng_ids or not user_ids:
        return
    rows = (
        await db.execute(
            select(
                EngagementParticipant.user_id,
                EngagementParticipant.engagement_id,
                EngagementParticipant.address,
                EngagementParticipant.city,
                Engagement.engagement_name,
                Engagement.blood_collection_type,
                Engagement.city.label("eng_city"),
                ParticipantBloodBooking.collection_date,
                ParticipantBloodBooking.collection_time,
                ParticipantBloodBooking.collected_at,
                ParticipantBloodBooking.diagnostic_report_url,
                ParticipantBloodBooking.diagnostic_provider,
            )
            .select_from(EngagementParticipant)
            .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
            .outerjoin(
                ParticipantBloodBooking,
                and_(
                    ParticipantBloodBooking.engagement_participant_id
                    == EngagementParticipant.engagement_participant_id,
                    ParticipantBloodBooking.status == _ACTIVE_BLOOD,
                ),
            )
            .where(EngagementParticipant.user_id.in_(user_ids))
            .where(EngagementParticipant.engagement_id.in_(eng_ids))
        )
    ).all()
    by_key: dict[tuple[int, int], Any] = {}
    for r in rows:
        key = (int(r.user_id), int(r.engagement_id))
        # Prefer a row that has collection info
        prev = by_key.get(key)
        if prev is None or (r.collection_date and not prev.collection_date):
            by_key[key] = r
    for item in items:
        uid, eid = item.get("user_id"), item.get("engagement_id")
        if uid is None or eid is None:
            continue
        r = by_key.get((int(uid), int(eid)))
        if not r:
            continue
        item["engagement_name"] = r.engagement_name
        item["collection_date"] = r.collection_date.isoformat() if r.collection_date else None
        item["collection_time"] = r.collection_time.isoformat() if r.collection_time else None
        item["collected_at"] = r.collected_at.isoformat() if r.collected_at else None
        item["collection_status"] = (
            "collected"
            if r.collected_at is not None
            else "scheduled"
            if r.collection_date is not None
            else "not_scheduled"
        )
        item["lab_report_status"] = _lab_report_status(
            collected_at=r.collected_at,
            diagnostic_report_url=r.diagnostic_report_url,
        )
        item["lab_partner"] = str(r.diagnostic_provider) if r.diagnostic_provider else None
        mode = str(r.blood_collection_type) if r.blood_collection_type else None
        item["collection_mode"] = mode
        loc_bits = [x for x in (r.address, r.city or r.eng_city) if x]
        item["collection_location"] = ", ".join(loc_bits) if loc_bits else (r.eng_city or item.get("city"))
        if not item.get("city"):
            item["city"] = r.city or r.eng_city


def _package_id_for_gender(row: Any, gender: str | None) -> int | None:
    text = (gender or "").strip().lower()
    if "female" in text and row.diagnostic_package_id_female:
        return int(row.diagnostic_package_id_female)
    if "male" in text and "female" not in text and row.diagnostic_package_id_male:
        return int(row.diagnostic_package_id_male)
    if row.diagnostic_package_id:
        return int(row.diagnostic_package_id)
    if row.diagnostic_package_id_male:
        return int(row.diagnostic_package_id_male)
    if row.diagnostic_package_id_female:
        return int(row.diagnostic_package_id_female)
    return None


async def _attach_package_names(db: AsyncSession, items: list[dict[str, Any]]) -> None:
    """Fill package_name from the engagement diagnostic package when the row has none."""
    needing = [i for i in items if i.get("engagement_id") is not None and not i.get("package_name")]
    if not needing:
        return
    eng_ids = {int(i["engagement_id"]) for i in needing}
    user_ids = {int(i["user_id"]) for i in needing if i.get("user_id") is not None}
    eng_rows = (
        await db.execute(
            select(
                Engagement.engagement_id,
                Engagement.diagnostic_package_id,
                Engagement.diagnostic_package_id_male,
                Engagement.diagnostic_package_id_female,
            ).where(Engagement.engagement_id.in_(eng_ids))
        )
    ).all()
    by_eng = {int(r.engagement_id): r for r in eng_rows}
    genders: dict[int, str] = {}
    if user_ids:
        user_rows = (
            await db.execute(select(User.user_id, User.gender).where(User.user_id.in_(user_ids)))
        ).all()
        genders = {int(r.user_id): str(r.gender or "") for r in user_rows}
    pkg_ids = {
        pid
        for row in eng_rows
        for pid in (
            row.diagnostic_package_id,
            row.diagnostic_package_id_male,
            row.diagnostic_package_id_female,
        )
        if pid is not None
    }
    names: dict[int, str] = {}
    if pkg_ids:
        pkg_rows = (
            await db.execute(
                select(DiagnosticPackage.diagnostic_package_id, DiagnosticPackage.package_name).where(
                    DiagnosticPackage.diagnostic_package_id.in_(pkg_ids)
                )
            )
        ).all()
        names = {int(r.diagnostic_package_id): r.package_name for r in pkg_rows if r.package_name}
    for item in needing:
        row = by_eng.get(int(item["engagement_id"]))
        if row is None:
            continue
        uid = item.get("user_id")
        pid = _package_id_for_gender(row, genders.get(int(uid)) if uid is not None else None)
        if pid is not None and names.get(pid):
            item["package_name"] = names[pid]


def _lab_report_status(*, collected_at, diagnostic_report_url: str | None) -> str:
    """Derive Not started / Pending / Delivered from DB fields (no Delayed)."""
    url = (diagnostic_report_url or "").strip()
    if url:
        return "delivered"
    if collected_at is not None:
        return "pending"
    return "not_started"


def _is_nutritionist(expert_type: str | None) -> bool:
    text = (expert_type or "").strip().lower()
    return "nutrition" in text or "diet" in text


def _split_kpi(total: int, b2b: int | None, b2c: int | None) -> dict[str, Any]:
    return {"total": total, "b2b": b2b, "b2c": b2c}


async def _scalar_count(db: AsyncSession, stmt) -> int:
    return int((await db.execute(stmt)).scalar_one() or 0)


# ---------------------------------------------------------------------------
# Health / issues
# ---------------------------------------------------------------------------


async def _server_health_snapshot(db: AsyncSession) -> dict[str, Any]:
    row = await ServerHealthStateRepository().get_latest(db)
    if row is not None:
        return {
            "cpu_usage": float(row.cpu_usage or 0),
            "memory_usage": float(row.memory_usage or 0),
            "storage_usage": float(row.storage_usage or 0),
            "reported_at": row.reported_at.isoformat() if row.reported_at else None,
            "source": "ingested",
        }
    try:
        import psutil

        disk = psutil.disk_usage("/")
        mem = psutil.virtual_memory()
        return {
            "cpu_usage": float(psutil.cpu_percent(interval=0.05)),
            "memory_usage": float(mem.percent),
            "storage_usage": float(disk.percent),
            "reported_at": datetime.now(timezone.utc).isoformat(),
            "source": "live",
        }
    except Exception:
        logger.exception("command server_health live snapshot failed")
        return {
            "cpu_usage": 0.0,
            "memory_usage": 0.0,
            "storage_usage": 0.0,
            "reported_at": None,
            "source": "unavailable",
        }


async def _notification_health_counts(
    db: AsyncSession,
    *,
    date_from: date | None,
    date_to: date | None,
) -> dict[str, int]:
    # Status column is label-mapped; compare with label strings.
    base = select(Notification.status, func.count()).select_from(Notification)
    if date_from is not None or date_to is not None:
        base = base.where(_date_in_range(Notification.dispatched_at, date_from, date_to))
    rows = (await db.execute(base.group_by(Notification.status))).all()
    sent = failed = pending = 0
    for status, count in rows:
        label = str(status or "").strip().lower()
        n = int(count or 0)
        if label in _SENT_STATUSES or label == "sent":
            sent += n
        elif label == "failed":
            failed += n
        elif label in _PENDING_STATUSES or label == "pending":
            pending += n
        else:
            pending += n
    return {"sent": sent, "failed": failed, "pending": pending, "total": sent + failed + pending}


async def _unserviceable_count(db: AsyncSession) -> int:
    return await AuditRepository().count_sync_logs(
        db,
        provider="healthians",
        statuses=["failed"],
        search="checkServiceabilityByLocation",
    )


async def _payment_failed_count(
    db: AsyncSession,
    *,
    date_from: date | None,
    date_to: date | None,
) -> int:
    stmt = (
        select(func.count())
        .select_from(Booking)
        .where(Booking.status == "failed")
        .where(_date_in_range(Booking.booked_at, date_from, date_to))
    )
    return await _scalar_count(db, stmt)


async def _ticket_counts(db: AsyncSession) -> dict[str, int]:
    from modules.dashboard.service import ticket_status_counts

    counts = await ticket_status_counts(db)
    solved = counts.get("resolved", 0)
    return {
        "open": counts.get("open", 0),
        "resolved": counts.get("resolved", 0),
        "closed": counts.get("closed", 0),
        "solved": solved,
    }


# ---------------------------------------------------------------------------
# Overview KPI counters
# ---------------------------------------------------------------------------


async def _count_users_in_range(
    db: AsyncSession,
    *,
    date_from: date | None,
    date_to: date | None,
) -> int:
    stmt = select(func.count()).select_from(User).where(
        _date_in_range(User.created_at, date_from, date_to)
    )
    return await _scalar_count(db, stmt)


async def _count_tests_completed(
    db: AsyncSession,
    *,
    audience: Audience,
    date_from: date | None,
    date_to: date | None,
) -> int:
    stmt = (
        select(func.count())
        .select_from(ParticipantBloodBooking)
        .join(
            EngagementParticipant,
            EngagementParticipant.engagement_participant_id
            == ParticipantBloodBooking.engagement_participant_id,
        )
        .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
        .where(ParticipantBloodBooking.status == _ACTIVE_BLOOD)
        .where(ParticipantBloodBooking.collected_at.is_not(None))
        .where(_audience_engagement_filter(audience))
        .where(_date_in_range(ParticipantBloodBooking.collected_at, date_from, date_to))
    )
    return await _scalar_count(db, stmt)


async def _count_questionnaire_done(
    db: AsyncSession,
    *,
    audience: Audience,
    date_from: date | None,
    date_to: date | None,
) -> int:
    done = or_(
        AssessmentInstance.completed_at.is_not(None),
        AssessmentInstance.metsights_record_id.is_not(None),
    )
    # Prefer completed_at for dating; fall back to assigned_at when only metsights id set.
    date_col = func.coalesce(AssessmentInstance.completed_at, AssessmentInstance.assigned_at)
    stmt = (
        select(func.count())
        .select_from(AssessmentInstance)
        .join(Engagement, Engagement.engagement_id == AssessmentInstance.engagement_id)
        .where(done)
        .where(_audience_engagement_filter(audience))
        .where(_date_in_range(date_col, date_from, date_to))
    )
    return await _scalar_count(db, stmt)


async def _count_lab_reports(
    db: AsyncSession,
    *,
    audience: Audience,
    date_from: date | None,
    date_to: date | None,
) -> int:
    url = func.trim(func.coalesce(ParticipantBloodBooking.diagnostic_report_url, ""))
    date_col = func.coalesce(
        func.date(ParticipantBloodBooking.collected_at),
        ParticipantBloodBooking.collection_date,
    )
    stmt = (
        select(func.count())
        .select_from(ParticipantBloodBooking)
        .join(
            EngagementParticipant,
            EngagementParticipant.engagement_participant_id
            == ParticipantBloodBooking.engagement_participant_id,
        )
        .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
        .where(ParticipantBloodBooking.status == _ACTIVE_BLOOD)
        .where(url != "")
        .where(_audience_engagement_filter(audience))
        .where(_date_in_range(date_col, date_from, date_to))
    )
    return await _scalar_count(db, stmt)


async def _count_bio_ai_reports(
    db: AsyncSession,
    *,
    audience: Audience,
    date_from: date | None,
    date_to: date | None,
) -> int:
    usable = or_(
        and_(
            IndividualHealthReport.report_url.is_not(None),
            func.trim(IndividualHealthReport.report_url) != "",
        ),
        IndividualHealthReport.reports.is_not(None),
    )
    stmt = (
        select(func.count(func.distinct(IndividualHealthReport.report_id)))
        .select_from(IndividualHealthReport)
        .join(Engagement, Engagement.engagement_id == IndividualHealthReport.engagement_id)
        .where(usable)
        .where(_audience_engagement_filter(audience))
        .where(_date_in_range(IndividualHealthReport.created_at, date_from, date_to))
    )
    return await _scalar_count(db, stmt)


async def _consultations_stats(
    db: AsyncSession,
    *,
    audience: Audience,
    date_from: date | None,
    date_to: date | None,
    done_only: bool = True,
) -> dict[str, Any]:
    clauses = [
        _audience_engagement_filter(audience),
        _date_in_range(ConsultationBooking.consultation_date, date_from, date_to),
    ]
    if done_only:
        clauses.append(ConsultationBooking.done.is_(True))
    stmt = (
        select(ConsultationBooking.expert_type, func.count())
        .select_from(ConsultationBooking)
        .join(
            EngagementParticipant,
            EngagementParticipant.engagement_participant_id
            == ConsultationBooking.engagement_participant_id,
        )
        .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
        .where(and_(*clauses))
        .group_by(ConsultationBooking.expert_type)
    )
    rows = (await db.execute(stmt)).all()
    total = doctor = nutritionist = 0
    for expert_type, count in rows:
        n = int(count or 0)
        total += n
        if _is_nutritionist(str(expert_type) if expert_type is not None else None):
            nutritionist += n
        else:
            doctor += n
    return {
        "total": total,
        "doctor": doctor,
        "nutritionist": nutritionist,
    }


async def _b2b_lifecycle_counts(db: AsyncSession) -> dict[str, int]:
    stmt = (
        select(Engagement.status, func.count())
        .select_from(Engagement)
        .where(Engagement.organization_id.is_not(None))
        .where(Engagement.status.in_(("running", "scheduled", "completed")))
        .group_by(Engagement.status)
    )
    rows = (await db.execute(stmt)).all()
    out = {"ongoing": 0, "upcoming": 0, "completed": 0}
    for status, count in rows:
        label = str(status or "").strip().lower()
        n = int(count or 0)
        if label == "running":
            out["ongoing"] = n
        elif label == "scheduled":
            out["upcoming"] = n
        elif label == "completed":
            out["completed"] = n
    return out


async def _count_open_b2c_bookings(db: AsyncSession) -> int:
    stmt = (
        select(func.count())
        .select_from(Booking)
        .where(Booking.status.in_(_OPEN_BOOKING))
    )
    return await _scalar_count(db, stmt)


async def _overview_kpis(
    db: AsyncSession,
    *,
    audience: Audience,
    date_from: date | None,
    date_to: date | None,
) -> dict[str, Any]:
    people = await _count_users_in_range(db, date_from=date_from, date_to=date_to)
    tests_all = await _count_tests_completed(db, audience="all", date_from=date_from, date_to=date_to)
    tests_b2b = await _count_tests_completed(db, audience="b2b", date_from=date_from, date_to=date_to)
    tests_b2c = await _count_tests_completed(db, audience="b2c", date_from=date_from, date_to=date_to)
    q_all = await _count_questionnaire_done(db, audience="all", date_from=date_from, date_to=date_to)
    q_b2b = await _count_questionnaire_done(db, audience="b2b", date_from=date_from, date_to=date_to)
    q_b2c = await _count_questionnaire_done(db, audience="b2c", date_from=date_from, date_to=date_to)
    lab_all = await _count_lab_reports(db, audience="all", date_from=date_from, date_to=date_to)
    lab_b2b = await _count_lab_reports(db, audience="b2b", date_from=date_from, date_to=date_to)
    lab_b2c = await _count_lab_reports(db, audience="b2c", date_from=date_from, date_to=date_to)
    bio_all = await _count_bio_ai_reports(db, audience="all", date_from=date_from, date_to=date_to)
    bio_b2b = await _count_bio_ai_reports(db, audience="b2b", date_from=date_from, date_to=date_to)
    bio_b2c = await _count_bio_ai_reports(db, audience="b2c", date_from=date_from, date_to=date_to)
    c_all = await _consultations_stats(db, audience="all", date_from=date_from, date_to=date_to)
    c_b2b = await _consultations_stats(db, audience="b2b", date_from=date_from, date_to=date_to)
    c_b2c = await _consultations_stats(db, audience="b2c", date_from=date_from, date_to=date_to)
    lifecycle = await _b2b_lifecycle_counts(db)
    open_b2b = lifecycle["ongoing"] + lifecycle["upcoming"]
    open_b2c = await _count_open_b2c_bookings(db)

    # Audience query only filters audience-aware action items elsewhere;
    # overview cards always expose total + split.
    del audience

    return {
        "people_registered": _split_kpi(people, None, None),
        "tests_completed": _split_kpi(tests_all, tests_b2b, tests_b2c),
        "questionnaire_done": _split_kpi(q_all, q_b2b, q_b2c),
        "reports_delivered": {
            "total": lab_all + bio_all,
            "lab": lab_all,
            "bio_ai": bio_all,
            "b2b": lab_b2b + bio_b2b,
            "b2c": lab_b2c + bio_b2c,
        },
        "consultations": {
            "total": c_all["total"],
            "b2b": c_b2b["total"],
            "b2c": c_b2c["total"],
            "doctor": c_all["doctor"],
            "nutritionist": c_all["nutritionist"],
        },
        "open_engagements": _split_kpi(open_b2b + open_b2c, open_b2b, open_b2c),
    }


# ---------------------------------------------------------------------------
# Action center
# ---------------------------------------------------------------------------


async def _action_center_counts(
    db: AsyncSession,
    *,
    audience: Audience,
    date_from: date | None = None,
    date_to: date | None = None,
) -> dict[str, int]:
    today = date.today()
    tomorrow = today + timedelta(days=1)

    async def pbb_count(*extra):
        stmt = (
            select(func.count())
            .select_from(ParticipantBloodBooking)
            .join(
                EngagementParticipant,
                EngagementParticipant.engagement_participant_id
                == ParticipantBloodBooking.engagement_participant_id,
            )
            .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
            .where(ParticipantBloodBooking.status == _ACTIVE_BLOOD)
            .where(_audience_engagement_filter(audience))
        )
        for clause in extra:
            stmt = stmt.where(clause)
        return await _scalar_count(db, stmt)

    upcoming = await pbb_count(
        ParticipantBloodBooking.collection_date >= tomorrow,
        ParticipantBloodBooking.collected_at.is_(None),
        _date_in_range(ParticipantBloodBooking.collection_date, date_from, date_to),
    )
    overdue = await pbb_count(
        ParticipantBloodBooking.collection_date < today,
        ParticipantBloodBooking.collected_at.is_(None),
        _date_in_range(ParticipantBloodBooking.collection_date, date_from, date_to),
    )
    blood_pending = await pbb_count(
        ParticipantBloodBooking.collected_at.is_not(None),
        or_(
            ParticipantBloodBooking.diagnostic_report_url.is_(None),
            func.trim(ParticipantBloodBooking.diagnostic_report_url) == "",
        ),
        _date_in_range(ParticipantBloodBooking.collected_at, date_from, date_to),
    )

    # Proxy: participants on running/scheduled with no active blood booking.
    active_pbb = (
        select(ParticipantBloodBooking.engagement_participant_id)
        .where(ParticipantBloodBooking.status == _ACTIVE_BLOOD)
        .correlate(EngagementParticipant)
    )
    q_pending_stmt = (
        select(func.count())
        .select_from(EngagementParticipant)
        .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
        .where(Engagement.status.in_(("running", "scheduled")))
        .where(_audience_engagement_filter(audience))
        .where(~EngagementParticipant.engagement_participant_id.in_(active_pbb))
    )
    # Date window on engagement schedule overlap when provided.
    if date_from is not None:
        q_pending_stmt = q_pending_stmt.where(
            or_(Engagement.end_date.is_(None), Engagement.end_date >= date_from)
        )
    if date_to is not None:
        q_pending_stmt = q_pending_stmt.where(
            or_(Engagement.start_date.is_(None), Engagement.start_date <= date_to)
        )
    questionnaire_pending = await _scalar_count(db, q_pending_stmt)

    usable_ihr = or_(
        and_(
            IndividualHealthReport.report_url.is_not(None),
            func.trim(IndividualHealthReport.report_url) != "",
        ),
        IndividualHealthReport.reports.is_not(None),
    )
    bio_stmt = (
        select(func.count())
        .select_from(EngagementParticipant)
        .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
        .join(
            ParticipantBloodBooking,
            and_(
                ParticipantBloodBooking.engagement_participant_id
                == EngagementParticipant.engagement_participant_id,
                ParticipantBloodBooking.status == _ACTIVE_BLOOD,
                ParticipantBloodBooking.collected_at.is_not(None),
            ),
        )
        .outerjoin(
            IndividualHealthReport,
            and_(
                IndividualHealthReport.user_id == EngagementParticipant.user_id,
                IndividualHealthReport.engagement_id == Engagement.engagement_id,
            ),
        )
        .where(_audience_engagement_filter(audience))
        .where(
            or_(
                IndividualHealthReport.report_id.is_(None),
                ~usable_ihr,
            )
        )
        .where(_date_in_range(ParticipantBloodBooking.collected_at, date_from, date_to))
    )
    bio_pending = await _scalar_count(db, bio_stmt)

    consult_stmt = (
        select(func.count())
        .select_from(ConsultationBooking)
        .join(
            EngagementParticipant,
            EngagementParticipant.engagement_participant_id
            == ConsultationBooking.engagement_participant_id,
        )
        .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
        .where(ConsultationBooking.done.is_(False))
        .where(ConsultationBooking.want.is_(True))
        .where(_audience_engagement_filter(audience))
        .where(_date_in_range(ConsultationBooking.consultation_date, date_from, date_to))
    )
    consultations_pending = await _scalar_count(db, consult_stmt)

    return {
        "upcoming_collections": upcoming,
        "overdue_collections": overdue,
        "blood_reports_pending": blood_pending,
        "questionnaire_pending": questionnaire_pending,
        "bio_ai_pending": bio_pending,
        "consultations_pending": consultations_pending,
    }


# ---------------------------------------------------------------------------
# B2B / B2C blocks
# ---------------------------------------------------------------------------


async def _list_b2b_camps(
    db: AsyncSession,
    *,
    lifecycle: Lifecycle,
    date_from: date | None,
    date_to: date | None,
    limit: int = 50,
    offset: int = 0,
    q: str | None = None,
    city: str | None = None,
) -> tuple[list[dict[str, Any]], int]:
    filters = [Engagement.organization_id.is_not(None)]
    if lifecycle != "all":
        filters.append(Engagement.status == _LIFECYCLE_STATUS[lifecycle])
    else:
        filters.append(Engagement.status.in_(("running", "scheduled", "completed", "cancelled")))
    if date_from is not None:
        filters.append(or_(Engagement.end_date.is_(None), Engagement.end_date >= date_from))
    if date_to is not None:
        filters.append(or_(Engagement.start_date.is_(None), Engagement.start_date <= date_to))
    if city:
        filters.append(func.lower(Engagement.city) == city.strip().lower())
    if q:
        like = f"%{q.strip().lower()}%"
        filters.append(
            or_(
                func.lower(Engagement.engagement_name).like(like),
                func.lower(Engagement.engagement_code).like(like),
                func.lower(Organization.name).like(like),
                func.lower(func.coalesce(Engagement.city, "")).like(like),
            )
        )

    base = (
        select(
            Engagement.engagement_id,
            Engagement.engagement_code,
            Engagement.engagement_name,
            Engagement.camp_no,
            Engagement.city,
            Engagement.start_date,
            Engagement.end_date,
            Engagement.status,
            Engagement.organization_id,
            Engagement.payment_type,
            Engagement.blood_collection_type,
            Engagement.consultation_mode,
            Organization.name.label("organization_name"),
        )
        .select_from(Engagement)
        .outerjoin(Organization, Organization.organization_id == Engagement.organization_id)
        .where(and_(*filters))
    )
    count_stmt = (
        select(func.count())
        .select_from(Engagement)
        .outerjoin(Organization, Organization.organization_id == Engagement.organization_id)
        .where(and_(*filters))
    )
    total = await _scalar_count(db, count_stmt)
    rows = (
        await db.execute(
            base.order_by(Engagement.start_date.desc().nullslast(), Engagement.engagement_id.desc())
            .offset(offset)
            .limit(limit)
        )
    ).all()
    camps = []
    for row in rows:
        status = str(row.status or "").strip().lower()
        camps.append(
            {
                "engagement_id": row.engagement_id,
                "engagement_code": row.engagement_code,
                "engagement_name": row.engagement_name,
                "camp_no": row.camp_no,
                "city": row.city,
                "start_date": row.start_date.isoformat() if row.start_date else None,
                "end_date": row.end_date.isoformat() if row.end_date else None,
                "status": status,
                "lifecycle": (
                    "ongoing"
                    if status == "running"
                    else "upcoming"
                    if status == "scheduled"
                    else "completed"
                    if status == "completed"
                    else status
                ),
                "organization_id": row.organization_id,
                "organization_name": row.organization_name,
                "payment_type": str(row.payment_type) if row.payment_type is not None else None,
                "blood_collection_type": str(row.blood_collection_type)
                if row.blood_collection_type is not None
                else None,
                "consultation_mode": str(row.consultation_mode)
                if row.consultation_mode is not None
                else None,
                "audience": "b2b",
            }
        )
    return camps, total


async def _b2c_bookings_count_simple(
    db: AsyncSession,
    *,
    date_from: date | None,
    date_to: date | None,
) -> int:
    stmt = (
        select(func.count())
        .select_from(Booking)
        .where(Booking.status.in_(_B2C_BOOKING_STATUSES))
        .where(_exclude_b2b_payment_bookings())
        .where(_date_in_range(Booking.booked_at, date_from, date_to))
    )
    return await _scalar_count(db, stmt)


async def _collections_tomorrow_count(db: AsyncSession) -> int:
    tomorrow = date.today() + timedelta(days=1)
    paid = or_(
        EngagementParticipant.paid.is_(True),
        Engagement.payment_type == EngagementPaymentType.paid_by_group.value,
    )
    stmt = (
        select(func.count())
        .select_from(ParticipantBloodBooking)
        .join(
            EngagementParticipant,
            EngagementParticipant.engagement_participant_id
            == ParticipantBloodBooking.engagement_participant_id,
        )
        .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
        .where(ParticipantBloodBooking.status == _ACTIVE_BLOOD)
        .where(Engagement.organization_id.is_(None))
        .where(ParticipantBloodBooking.collection_date == tomorrow)
        .where(paid)
    )
    return await _scalar_count(db, stmt)


async def _package_performance(
    db: AsyncSession,
    *,
    date_from: date | None,
    date_to: date | None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    stmt = (
        select(
            Booking.entity_id,
            Booking.entity_name,
            func.count().label("bookings"),
            func.coalesce(func.sum(Booking.amount_paise), 0).label("revenue_paise"),
        )
        .select_from(Booking)
        .where(Booking.status == "confirmed")
        .where(Booking.entity_type == "diagnostic_package")
        .where(_exclude_b2b_payment_bookings())
        .where(_date_in_range(Booking.booked_at, date_from, date_to))
        .group_by(Booking.entity_id, Booking.entity_name)
        .order_by(func.count().desc())
        .limit(limit)
    )
    rows = (await db.execute(stmt)).all()
    return [
        {
            "package_id": row.entity_id,
            "package_name": row.entity_name,
            "bookings": int(row.bookings or 0),
            "revenue_paise": int(row.revenue_paise or 0),
        }
        for row in rows
    ]


async def _coupon_usage(
    db: AsyncSession,
    *,
    date_from: date | None,
    date_to: date | None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    stmt = (
        select(
            DiscountCode.discount_code_id,
            DiscountCode.code,
            DiscountCode.name,
            func.count().label("uses"),
            func.coalesce(func.sum(DiscountUsage.discount_paise), 0).label("discount_paise"),
            func.count(func.distinct(DiscountUsage.user_id)).label("unique_users"),
        )
        .select_from(DiscountUsage)
        .join(DiscountCode, DiscountCode.discount_code_id == DiscountUsage.discount_code_id)
        .where(DiscountUsage.status == "committed")
        .where(or_(DiscountUsage.organization_id.is_(None), DiscountUsage.organization_id == 0))
        .where(_date_in_range(DiscountUsage.created_at, date_from, date_to))
        .group_by(DiscountCode.discount_code_id, DiscountCode.code, DiscountCode.name)
        .order_by(func.count().desc())
        .limit(limit)
    )
    rows = (await db.execute(stmt)).all()
    return [
        {
            "discount_code_id": row.discount_code_id,
            "code": row.code,
            "name": row.name,
            "uses": int(row.uses or 0),
            "discount_paise": int(row.discount_paise or 0),
            "unique_users": int(row.unique_users or 0),
        }
        for row in rows
    ]


async def _drill_collections_tomorrow(
    db: AsyncSession,
    *,
    limit: int = 20,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], int]:
    tomorrow = date.today() + timedelta(days=1)
    paid = or_(
        EngagementParticipant.paid.is_(True),
        Engagement.payment_type == EngagementPaymentType.paid_by_group.value,
    )
    filters = [
        ParticipantBloodBooking.status == _ACTIVE_BLOOD,
        Engagement.organization_id.is_(None),
        ParticipantBloodBooking.collection_date == tomorrow,
        paid,
    ]
    count_stmt = (
        select(func.count())
        .select_from(ParticipantBloodBooking)
        .join(
            EngagementParticipant,
            EngagementParticipant.engagement_participant_id
            == ParticipantBloodBooking.engagement_participant_id,
        )
        .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
        .where(and_(*filters))
    )
    total = await _scalar_count(db, count_stmt)
    stmt = (
        select(
            ParticipantBloodBooking.id,
            ParticipantBloodBooking.collection_date,
            ParticipantBloodBooking.collection_time,
            ParticipantBloodBooking.booking_id,
            EngagementParticipant.paid,
            Engagement.engagement_id,
            Engagement.engagement_name,
            Engagement.city,
            User.user_id,
            User.first_name,
            User.last_name,
            User.phone,
            User.email,
        )
        .select_from(ParticipantBloodBooking)
        .join(
            EngagementParticipant,
            EngagementParticipant.engagement_participant_id
            == ParticipantBloodBooking.engagement_participant_id,
        )
        .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
        .join(User, User.user_id == EngagementParticipant.user_id)
        .where(and_(*filters))
        .order_by(ParticipantBloodBooking.collection_time.asc().nullslast())
        .offset(offset)
        .limit(limit)
    )
    rows = (await db.execute(stmt)).all()
    items = [
        {
            "blood_booking_id": row.id,
            "user_id": row.user_id,
            "user_name": _user_display_name(row.first_name, row.last_name),
            "phone": mask_phone(row.phone),
            "email": mask_email(row.email),
            "collection_date": row.collection_date.isoformat() if row.collection_date else None,
            "collection_time": row.collection_time.isoformat() if row.collection_time else None,
            "booking_id": row.booking_id,
            "paid": bool(row.paid),
            "engagement_id": row.engagement_id,
            "engagement_name": row.engagement_name,
            "city": row.city,
            "audience": "b2c",
        }
        for row in rows
    ]
    return items, total


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------


async def build_command_bootstrap(
    db: AsyncSession,
    *,
    audience: Audience = "all",
    date_from: date | None = None,
    date_to: date | None = None,
    lifecycle: Lifecycle = "all",
) -> dict[str, Any]:
    if audience not in ("all", "b2b", "b2c"):
        audience = "all"
    if lifecycle not in ("all", "ongoing", "upcoming", "completed"):
        lifecycle = "all"

    server_health = await _server_health_snapshot(db)
    notification_health = await _notification_health_counts(
        db, date_from=date_from, date_to=date_to
    )
    unserviceable = await _unserviceable_count(db)
    payment_failed = await _payment_failed_count(db, date_from=date_from, date_to=date_to)
    tickets_raw = await _ticket_counts(db)
    tickets = {
        "open": tickets_raw["open"],
        "resolved": tickets_raw["resolved"],
        "closed": tickets_raw["closed"],
        "solved": tickets_raw["solved"],
    }
    overview_kpis = await _overview_kpis(
        db, audience=audience, date_from=date_from, date_to=date_to
    )
    action_audience: Audience = audience if audience in ("b2b", "b2c") else "all"
    action_center = await _action_center_counts(
        db,
        audience=action_audience,
        date_from=date_from,
        date_to=date_to,
    )
    lifecycle_counts = await _b2b_lifecycle_counts(db)
    camps, camps_total = await _list_b2b_camps(
        db, lifecycle=lifecycle, date_from=date_from, date_to=date_to, limit=50
    )

    b2c_consults = await _consultations_stats(
        db, audience="b2c", date_from=date_from, date_to=date_to
    )
    tomorrow_preview, _ = await _drill_collections_tomorrow(db, limit=20)

    return {
        "server_health": server_health,
        "notification_health": notification_health,
        "booking_issues": {
            "unserviceable": unserviceable,
            "payment_failed": payment_failed,
            "total": unserviceable + payment_failed,
        },
        "tickets": tickets,
        "overview_kpis": overview_kpis,
        "action_center": action_center,
        "b2b": {
            "lifecycle_counts": lifecycle_counts,
            "camps": camps,
            "camps_total": camps_total,
        },
        "b2c": {
            "total_bookings": await _b2c_bookings_count_simple(
                db, date_from=date_from, date_to=date_to
            ),
            "collections_tomorrow": await _collections_tomorrow_count(db),
            "tests_completed": await _count_tests_completed(
                db, audience="b2c", date_from=date_from, date_to=date_to
            ),
            "lab_reports_delivered": await _count_lab_reports(
                db, audience="b2c", date_from=date_from, date_to=date_to
            ),
            "bio_ai_reports_generated": await _count_bio_ai_reports(
                db, audience="b2c", date_from=date_from, date_to=date_to
            ),
            "consultations": b2c_consults,
            "package_performance": await _package_performance(
                db, date_from=date_from, date_to=date_to
            ),
            "coupon_usage": await _coupon_usage(db, date_from=date_from, date_to=date_to),
            "tomorrows_collections": tomorrow_preview,
        },
        "filters": {
            "audience": audience,
            "from": date_from.isoformat() if date_from else None,
            "to": date_to.isoformat() if date_to else None,
            "lifecycle": lifecycle,
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Drills
# ---------------------------------------------------------------------------


def _normalize_drill_id(drill: str) -> tuple[str, int | None, int | None]:
    raw = (drill or "").strip()
    package_id = discount_code_id = None
    if raw.startswith("package_"):
        try:
            package_id = int(raw.split("_", 1)[1])
        except ValueError:
            package_id = None
        return "package_performance", package_id, None
    if raw.startswith("coupon_"):
        try:
            discount_code_id = int(raw.split("_", 1)[1])
        except ValueError:
            discount_code_id = None
        return "coupon_usage", None, discount_code_id
    if raw == "reports_delivered":
        return "lab_reports", None, None
    if raw == "bio_ai_pending":
        return "blood_reports_pending", None, None
    if raw == "questionnaire_pending":
        return "upcoming_collections", None, None
    return raw, None, None


def _recipient_label(recipients: list[dict] | None) -> str | None:
    names: list[str] = []
    for rec in recipients or []:
        if not isinstance(rec, dict):
            continue
        first = str(rec.get("first_name") or "").strip()
        last = str(rec.get("last_name") or "").strip()
        name = " ".join(part for part in (first, last) if part)
        if not name:
            stored = str(rec.get("name") or "").strip()
            name = stored
        if not name and isinstance(rec.get("user_id"), int):
            name = f"User #{rec['user_id']}"
        if name:
            names.append(name)
    return ", ".join(names) if names else None


async def _fill_engagement_names(db: AsyncSession, items: list[dict[str, Any]]) -> None:
    missing = [
        item
        for item in items
        if item.get("engagement_id") and not item.get("engagement_name")
    ]
    if not missing:
        return
    ids = {int(item["engagement_id"]) for item in missing}
    rows = (
        await db.execute(
            select(
                Engagement.engagement_id,
                Engagement.engagement_name,
                Engagement.engagement_code,
            ).where(Engagement.engagement_id.in_(ids))
        )
    ).all()
    by_id = {row.engagement_id: row for row in rows}
    for item in missing:
        row = by_id.get(int(item["engagement_id"]))
        if row is None:
            continue
        item["engagement_name"] = row.engagement_name
        item["engagement_code"] = row.engagement_code


def _apply_user_search(stmt, *, q: str | None, city: str | None):
    if q:
        like = f"%{q.strip().lower()}%"
        stmt = stmt.where(
            or_(
                func.lower(func.coalesce(User.first_name, "")).like(like),
                func.lower(func.coalesce(User.last_name, "")).like(like),
                func.lower(func.coalesce(User.phone, "")).like(like),
                func.lower(func.coalesce(User.email, "")).like(like),
            )
        )
    if city and city.strip():
        like = f"%{city.strip().lower()}%"
        stmt = stmt.where(func.lower(func.coalesce(User.city, "")).like(like))
    return stmt


async def build_command_drill(
    db: AsyncSession,
    *,
    drill: str,
    audience: Audience = "all",
    date_from: date | None = None,
    date_to: date | None = None,
    lifecycle: Lifecycle = "all",
    page: int = 1,
    limit: int = 25,
    package_id: int | None = None,
    discount_code_id: int | None = None,
    q: str | None = None,
    city: str | None = None,
) -> dict[str, Any]:
    if audience not in ("all", "b2b", "b2c"):
        audience = "all"
    drill_id, pkg_from_prefix, coupon_from_prefix = _normalize_drill_id(drill)
    if package_id is None:
        package_id = pkg_from_prefix
    if discount_code_id is None:
        discount_code_id = coupon_from_prefix

    if drill_id not in DRILL_IDS and drill_id not in {
        "lab_reports",
        "blood_reports_pending",
        "upcoming_collections",
    }:
        return {
            "drill": drill_id,
            "items": [],
            "error": "unknown_drill",
        }

    page = max(1, int(page or 1))
    limit = min(max(1, int(limit or 25)), 100)
    offset = (page - 1) * limit
    items: list[dict[str, Any]] = []
    total = 0

    if drill_id == "people_registered":
        stmt = select(
            User.user_id,
            User.first_name,
            User.last_name,
            User.phone,
            User.email,
            User.city,
            User.created_at,
            User.status,
        ).where(_date_in_range(User.created_at, date_from, date_to))
        stmt = _apply_user_search(stmt, q=q, city=city)
        count_stmt = select(func.count()).select_from(stmt.order_by(None).subquery())
        total = await _scalar_count(db, count_stmt)
        rows = (
            await db.execute(
                stmt.order_by(User.created_at.desc()).offset(offset).limit(limit)
            )
        ).all()
        items = [
            {
                "user_id": r.user_id,
                "user_name": _user_display_name(r.first_name, r.last_name),
                "phone": mask_phone(r.phone),
                "email": mask_email(r.email),
                "city": r.city,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "status": str(r.status) if r.status is not None else None,
            }
            for r in rows
        ]

    elif drill_id in {
        "tests_completed",
        "lab_reports",
        "upcoming_collections",
        "overdue_collections",
        "blood_reports_pending",
        "collections_tomorrow",
    }:
        today = date.today()
        tomorrow = today + timedelta(days=1)
        filters = [
            ParticipantBloodBooking.status == _ACTIVE_BLOOD,
            _audience_engagement_filter(audience),
        ]
        if drill_id == "tests_completed":
            filters.append(ParticipantBloodBooking.collected_at.is_not(None))
            filters.append(_date_in_range(ParticipantBloodBooking.collected_at, date_from, date_to))
        elif drill_id == "lab_reports":
            filters.append(
                and_(
                    ParticipantBloodBooking.diagnostic_report_url.is_not(None),
                    func.trim(ParticipantBloodBooking.diagnostic_report_url) != "",
                )
            )
            date_col = func.coalesce(
                func.date(ParticipantBloodBooking.collected_at),
                ParticipantBloodBooking.collection_date,
            )
            filters.append(_date_in_range(date_col, date_from, date_to))
        elif drill_id == "upcoming_collections":
            filters.append(ParticipantBloodBooking.collection_date >= tomorrow)
            filters.append(ParticipantBloodBooking.collected_at.is_(None))
        elif drill_id == "overdue_collections":
            filters.append(ParticipantBloodBooking.collection_date < today)
            filters.append(ParticipantBloodBooking.collected_at.is_(None))
        elif drill_id == "blood_reports_pending":
            filters.append(ParticipantBloodBooking.collected_at.is_not(None))
            filters.append(
                or_(
                    ParticipantBloodBooking.diagnostic_report_url.is_(None),
                    func.trim(ParticipantBloodBooking.diagnostic_report_url) == "",
                )
            )
        elif drill_id == "collections_tomorrow":
            filters = [
                ParticipantBloodBooking.status == _ACTIVE_BLOOD,
                Engagement.organization_id.is_(None),
                ParticipantBloodBooking.collection_date == tomorrow,
                or_(
                    EngagementParticipant.paid.is_(True),
                    Engagement.payment_type == EngagementPaymentType.paid_by_group.value,
                ),
            ]

        base = (
            select(
                ParticipantBloodBooking.id,
                ParticipantBloodBooking.collection_date,
                ParticipantBloodBooking.collection_time,
                ParticipantBloodBooking.collected_at,
                ParticipantBloodBooking.diagnostic_report_url,
                ParticipantBloodBooking.diagnostic_provider,
                ParticipantBloodBooking.booking_id,
                Engagement.engagement_id,
                Engagement.engagement_code,
                Engagement.engagement_name,
                Engagement.organization_id,
                Engagement.city,
                Engagement.blood_collection_type,
                EngagementParticipant.address,
                EngagementParticipant.city.label("participant_city"),
                Organization.name.label("organization_name"),
                User.user_id,
                User.first_name,
                User.last_name,
                User.phone,
                User.email,
            )
            .select_from(ParticipantBloodBooking)
            .join(
                EngagementParticipant,
                EngagementParticipant.engagement_participant_id
                == ParticipantBloodBooking.engagement_participant_id,
            )
            .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
            .outerjoin(Organization, Organization.organization_id == Engagement.organization_id)
            .join(User, User.user_id == EngagementParticipant.user_id)
            .where(and_(*filters))
        )
        base = _apply_user_search(base, q=q, city=city)
        count_stmt = select(func.count()).select_from(base.order_by(None).subquery())
        total = await _scalar_count(db, count_stmt)
        rows = (
            await db.execute(
                base.order_by(ParticipantBloodBooking.id.desc()).offset(offset).limit(limit)
            )
        ).all()
        items = [
            {
                "blood_booking_id": r.id,
                "user_id": r.user_id,
                "user_name": _user_display_name(r.first_name, r.last_name),
                "phone": mask_phone(r.phone),
                "email": mask_email(r.email),
                "collection_date": r.collection_date.isoformat() if r.collection_date else None,
                "collection_time": r.collection_time.isoformat() if r.collection_time else None,
                "collected_at": r.collected_at.isoformat() if r.collected_at else None,
                "collection_status": (
                    "collected"
                    if r.collected_at is not None
                    else "scheduled"
                    if r.collection_date is not None
                    else "not_scheduled"
                ),
                "lab_report_status": _lab_report_status(
                    collected_at=r.collected_at,
                    diagnostic_report_url=r.diagnostic_report_url,
                ),
                "lab_partner": str(r.diagnostic_provider) if r.diagnostic_provider else None,
                "collection_mode": str(r.blood_collection_type) if r.blood_collection_type else None,
                "collection_location": ", ".join(
                    x for x in (r.address, r.participant_city or r.city) if x
                )
                or r.city,
                "diagnostic_report_url": r.diagnostic_report_url,
                "booking_id": r.booking_id,
                "engagement_id": r.engagement_id,
                "engagement_code": r.engagement_code,
                "engagement_name": r.engagement_name,
                "organization_name": r.organization_name,
                "city": r.city,
                "audience": "b2b" if r.organization_id else "b2c",
            }
            for r in rows
        ]
        await _attach_journey_statuses(db, items)

    elif drill_id == "bio_ai_reports":
        usable = or_(
            and_(
                IndividualHealthReport.report_url.is_not(None),
                func.trim(IndividualHealthReport.report_url) != "",
            ),
            IndividualHealthReport.reports.is_not(None),
        )
        base = (
            select(
                IndividualHealthReport.report_id,
                IndividualHealthReport.report_url,
                IndividualHealthReport.created_at,
                Engagement.engagement_id,
                Engagement.engagement_name,
                Engagement.organization_id,
                User.user_id,
                User.first_name,
                User.last_name,
                User.phone,
                User.email,
            )
            .select_from(IndividualHealthReport)
            .join(Engagement, Engagement.engagement_id == IndividualHealthReport.engagement_id)
            .join(User, User.user_id == IndividualHealthReport.user_id)
            .where(usable)
            .where(_audience_engagement_filter(audience))
            .where(_date_in_range(IndividualHealthReport.created_at, date_from, date_to))
        )
        base = _apply_user_search(base, q=q, city=city)
        total = await _scalar_count(db, select(func.count()).select_from(base.order_by(None).subquery()))
        rows = (
            await db.execute(
                base.order_by(IndividualHealthReport.created_at.desc()).offset(offset).limit(limit)
            )
        ).all()
        items = [
            {
                "report_id": r.report_id,
                "report_url": r.report_url,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "user_id": r.user_id,
                "user_name": _user_display_name(r.first_name, r.last_name),
                "phone": mask_phone(r.phone),
                "email": mask_email(r.email),
                "engagement_id": r.engagement_id,
                "engagement_name": r.engagement_name,
                "audience": "b2b" if r.organization_id else "b2c",
            }
            for r in rows
        ]

    elif drill_id in {"consultations", "consultations_pending"}:
        filters = [_audience_engagement_filter(audience)]
        if drill_id == "consultations":
            filters.append(ConsultationBooking.done.is_(True))
            filters.append(
                _date_in_range(ConsultationBooking.consultation_date, date_from, date_to)
            )
        else:
            filters.append(ConsultationBooking.done.is_(False))
            filters.append(ConsultationBooking.want.is_(True))
        base = (
            select(
                ConsultationBooking.consultation_id,
                ConsultationBooking.expert_type,
                ConsultationBooking.consultation_date,
                ConsultationBooking.done,
                ConsultationBooking.want,
                Engagement.engagement_id,
                Engagement.engagement_name,
                Engagement.organization_id,
                User.user_id,
                User.first_name,
                User.last_name,
                User.phone,
                User.email,
            )
            .select_from(ConsultationBooking)
            .join(
                EngagementParticipant,
                EngagementParticipant.engagement_participant_id
                == ConsultationBooking.engagement_participant_id,
            )
            .join(Engagement, Engagement.engagement_id == EngagementParticipant.engagement_id)
            .join(User, User.user_id == EngagementParticipant.user_id)
            .where(and_(*filters))
        )
        base = _apply_user_search(base, q=q, city=city)
        total = await _scalar_count(db, select(func.count()).select_from(base.order_by(None).subquery()))
        rows = (
            await db.execute(
                base.order_by(ConsultationBooking.consultation_id.desc()).offset(offset).limit(limit)
            )
        ).all()
        items = [
            {
                "consultation_id": r.consultation_id,
                "expert_type": r.expert_type,
                "consultation_date": r.consultation_date.isoformat()
                if r.consultation_date
                else None,
                "done": bool(r.done),
                "want": bool(r.want),
                "user_id": r.user_id,
                "user_name": _user_display_name(r.first_name, r.last_name),
                "phone": mask_phone(r.phone),
                "email": mask_email(r.email),
                "engagement_id": r.engagement_id,
                "engagement_name": r.engagement_name,
                "audience": "b2b" if r.organization_id else "b2c",
            }
            for r in rows
        ]

    elif drill_id == "questionnaire_done":
        done = or_(
            AssessmentInstance.completed_at.is_not(None),
            AssessmentInstance.metsights_record_id.is_not(None),
        )
        date_col = func.coalesce(AssessmentInstance.completed_at, AssessmentInstance.assigned_at)
        base = (
            select(
                AssessmentInstance.assessment_instance_id,
                AssessmentInstance.completed_at,
                AssessmentInstance.metsights_record_id,
                AssessmentInstance.status,
                Engagement.engagement_id,
                Engagement.engagement_name,
                Engagement.organization_id,
                User.user_id,
                User.first_name,
                User.last_name,
                User.phone,
                User.email,
            )
            .select_from(AssessmentInstance)
            .join(Engagement, Engagement.engagement_id == AssessmentInstance.engagement_id)
            .join(User, User.user_id == AssessmentInstance.user_id)
            .where(done)
            .where(_audience_engagement_filter(audience))
            .where(_date_in_range(date_col, date_from, date_to))
        )
        base = _apply_user_search(base, q=q, city=city)
        total = await _scalar_count(db, select(func.count()).select_from(base.order_by(None).subquery()))
        rows = (
            await db.execute(
                base.order_by(AssessmentInstance.assessment_instance_id.desc())
                .offset(offset)
                .limit(limit)
            )
        ).all()
        items = [
            {
                "assessment_instance_id": r.assessment_instance_id,
                "completed_at": r.completed_at.isoformat() if r.completed_at else None,
                "metsights_record_id": r.metsights_record_id,
                "status": str(r.status) if r.status is not None else None,
                "user_id": r.user_id,
                "user_name": _user_display_name(r.first_name, r.last_name),
                "phone": mask_phone(r.phone),
                "email": mask_email(r.email),
                "engagement_id": r.engagement_id,
                "engagement_name": r.engagement_name,
                "audience": "b2b" if r.organization_id else "b2c",
            }
            for r in rows
        ]

    elif drill_id in {"tickets_open", "tickets_solved", "tickets_all"}:
        from db.column_types import STATUS_SUPPORT_TICKET

        code_to_label = {code: label for label, code in STATUS_SUPPORT_TICKET.items()}
        status_filter = None
        if drill_id == "tickets_open":
            status_filter = "open"
        elif drill_id == "tickets_solved":
            status_filter = "resolved"
        base = select(
            SupportTicket.ticket_id,
            SupportTicket.user_id,
            SupportTicket.contact_input,
            SupportTicket.query_text,
            SupportTicket.status,
            SupportTicket.created_at,
        )
        if status_filter:
            # Status column accepts label comparison via TypeDecorator
            base = base.where(SupportTicket.status == status_filter)
        total = await _scalar_count(db, select(func.count()).select_from(base.order_by(None).subquery()))
        rows = (
            await db.execute(
                base.order_by(SupportTicket.created_at.desc()).offset(offset).limit(limit)
            )
        ).all()
        items = []
        for r in rows:
            status = r.status
            if isinstance(status, int):
                status = code_to_label.get(status, str(status))
            else:
                status = str(status or "").strip().lower()
            items.append(
                {
                    "ticket_id": r.ticket_id,
                    "user_id": r.user_id,
                    "contact_input": r.contact_input,
                    "query_text": r.query_text,
                    "status": "solved" if status == "resolved" else status,
                    "created_at": r.created_at.isoformat() if r.created_at else None,
                }
            )

    elif drill_id in {"payment_failed", "unserviceable", "booking_issues"}:
        items = []
        total = 0
        if drill_id in {"payment_failed", "booking_issues"}:
            pay_base = (
                select(
                    Booking.booking_id,
                    Booking.user_id,
                    Booking.entity_name,
                    Booking.amount_paise,
                    Booking.status,
                    Booking.booked_at,
                    User.first_name,
                    User.last_name,
                    User.phone,
                )
                .select_from(Booking)
                .outerjoin(User, User.user_id == Booking.user_id)
                .where(Booking.status == "failed")
                .where(_date_in_range(Booking.booked_at, date_from, date_to))
            )
            pay_total = await _scalar_count(
                db, select(func.count()).select_from(pay_base.order_by(None).subquery())
            )
            if drill_id == "payment_failed":
                total = pay_total
                rows = (
                    await db.execute(
                        pay_base.order_by(Booking.booked_at.desc()).offset(offset).limit(limit)
                    )
                ).all()
                items = [
                    {
                        "kind": "payment_failed",
                        "booking_id": r.booking_id,
                        "user_id": r.user_id,
                        "user_name": _user_display_name(r.first_name, r.last_name),
                        "phone": mask_phone(r.phone),
                        "entity_name": r.entity_name,
                        "amount_paise": r.amount_paise,
                        "status": str(r.status),
                        "booked_at": r.booked_at.isoformat() if r.booked_at else None,
                    }
                    for r in rows
                ]
            else:
                # Merge payment + unserviceable for booking_issues (simple concat page 1 style)
                pay_rows = (
                    await db.execute(pay_base.order_by(Booking.booked_at.desc()).limit(limit))
                ).all()
                items.extend(
                    {
                        "kind": "payment_failed",
                        "booking_id": r.booking_id,
                        "user_id": r.user_id,
                        "user_name": _user_display_name(r.first_name, r.last_name),
                        "phone": mask_phone(r.phone),
                        "entity_name": r.entity_name,
                        "amount_paise": r.amount_paise,
                        "status": str(r.status),
                        "booked_at": r.booked_at.isoformat() if r.booked_at else None,
                    }
                    for r in pay_rows
                )
                total += pay_total

        if drill_id in {"unserviceable", "booking_issues"}:
            audit = AuditRepository()
            log_total = await audit.count_sync_logs(
                db,
                provider="healthians",
                statuses=["failed"],
                search="checkServiceabilityByLocation",
            )
            logs = await audit.list_sync_logs(
                db,
                provider="healthians",
                statuses=["failed"],
                search="checkServiceabilityByLocation",
                page=page if drill_id == "unserviceable" else 1,
                limit=limit,
            )
            if drill_id == "unserviceable":
                total = log_total
                items = [
                    {
                        "kind": "unserviceable",
                        "sync_log_id": log.sync_log_id,
                        "user_id": log.user_id,
                        "engagement_id": log.engagement_id,
                        "status": str(log.status),
                        "created_at": log.created_at.isoformat() if log.created_at else None,
                        "api_endpoint_url": log.api_endpoint_url,
                    }
                    for log in logs
                ]
            else:
                total += log_total
                items.extend(
                    {
                        "kind": "unserviceable",
                        "sync_log_id": log.sync_log_id,
                        "user_id": log.user_id,
                        "engagement_id": log.engagement_id,
                        "status": str(log.status),
                        "created_at": log.created_at.isoformat() if log.created_at else None,
                        "api_endpoint_url": log.api_endpoint_url,
                    }
                    for log in logs[: max(0, limit - len(items))]
                )

    elif drill_id == "failed_notifications":
        filters = [Notification.status == "failed"]
        if date_from is not None or date_to is not None:
            filters.append(_date_in_range(Notification.dispatched_at, date_from, date_to))
        if q and q.strip():
            like = f"%{q.strip().lower()}%"
            filters.append(func.lower(func.coalesce(Notification.message, "")).like(like))
        base = select(Notification).where(*filters)
        total = await _scalar_count(db, select(func.count()).select_from(base.order_by(None).subquery()))
        notes = (
            await db.execute(
                base.order_by(Notification.dispatched_at.desc().nullslast()).offset(offset).limit(limit)
            )
        ).scalars().all()
        enriched = await NotificationsService(NotificationsRepository())._enrich_notification_list_items(
            db, list(notes)
        )
        items = []
        for row in enriched:
            recipients = row.get("recipients") if isinstance(row.get("recipients"), list) else []
            user_id = next(
                (
                    rec.get("user_id")
                    for rec in recipients
                    if isinstance(rec, dict) and isinstance(rec.get("user_id"), int)
                ),
                None,
            )
            items.append(
                {
                    "notification_id": row.get("notification_id"),
                    "recipient_name": _recipient_label(recipients),
                    "engagement_id": row.get("engagement_id"),
                    "engagement_name": row.get("engagement_name"),
                    "engagement_code": row.get("engagement_code"),
                    "message": row.get("message"),
                    "user_id": user_id,
                }
            )

    elif drill_id == "total_bookings":
        base = (
            select(
                Booking.booking_id,
                Booking.user_id,
                Booking.entity_type,
                Booking.entity_id,
                Booking.entity_name,
                Booking.amount_paise,
                Booking.status,
                Booking.booked_at,
                Booking.metadata_,
                User.first_name,
                User.last_name,
                User.phone,
                User.email,
            )
            .select_from(Booking)
            .outerjoin(User, User.user_id == Booking.user_id)
            .where(Booking.status.in_(_B2C_BOOKING_STATUSES))
            .where(_exclude_b2b_payment_bookings())
            .where(_date_in_range(Booking.booked_at, date_from, date_to))
        )
        if q and q.strip():
            like = f"%{q.strip().lower()}%"
            base = base.where(
                or_(
                    func.lower(func.coalesce(User.first_name, "")).like(like),
                    func.lower(func.coalesce(User.last_name, "")).like(like),
                    func.lower(func.coalesce(User.phone, "")).like(like),
                    func.lower(func.coalesce(User.email, "")).like(like),
                    func.lower(func.coalesce(Booking.entity_name, "")).like(like),
                )
            )
        total = await _scalar_count(db, select(func.count()).select_from(base.order_by(None).subquery()))
        rows = (
            await db.execute(base.order_by(Booking.booked_at.desc()).offset(offset).limit(limit))
        ).all()
        items = []
        for r in rows:
            meta = r.metadata_ if isinstance(r.metadata_, dict) else {}
            raw_eng = meta.get("engagement_id")
            try:
                engagement_id = int(raw_eng) if raw_eng not in (None, "") else None
            except (TypeError, ValueError):
                engagement_id = None
            items.append(
                {
                    "booking_id": r.booking_id,
                    "user_id": r.user_id,
                    "user_name": _user_display_name(r.first_name, r.last_name),
                    "phone": mask_phone(r.phone),
                    "email": mask_email(r.email),
                    "entity_type": str(r.entity_type),
                    "entity_id": r.entity_id,
                    "entity_name": r.entity_name,
                    "package_name": r.entity_name,
                    "amount_paise": r.amount_paise,
                    "status": str(r.status),
                    "booked_at": r.booked_at.isoformat() if r.booked_at else None,
                    "engagement_id": engagement_id,
                    "audience": "b2c",
                    "metadata": {"engagement_id": engagement_id} if engagement_id else {},
                }
            )
        await _attach_booking_collection(db, items)
        await _attach_journey_statuses(db, items)

    elif drill_id == "open_engagements":
        camps, camps_total = await _list_b2b_camps(
            db,
            lifecycle="all",
            date_from=None,
            date_to=None,
            limit=limit,
            offset=offset,
            q=q,
            city=city,
        )
        # Only open camps for this drill
        open_camps = [c for c in camps if c["status"] in ("running", "scheduled")]
        book_stmt = (
            select(func.count())
            .select_from(Booking)
            .where(Booking.status.in_(_OPEN_BOOKING))
        )
        open_bookings = await _scalar_count(db, book_stmt)
        items = [
            {**c, "kind": "b2b_camp"} for c in open_camps
        ]
        # Append booking summary row counts via separate page when offset==0
        if page == 1:
            book_rows = (
                await db.execute(
                    select(
                        Booking.booking_id,
                        Booking.user_id,
                        Booking.entity_name,
                        Booking.status,
                        Booking.booked_at,
                        User.first_name,
                        User.last_name,
                        User.phone,
                    )
                    .select_from(Booking)
                    .outerjoin(User, User.user_id == Booking.user_id)
                    .where(Booking.status.in_(_OPEN_BOOKING))
                    .order_by(Booking.booked_at.desc())
                    .limit(max(0, limit - len(items)))
                )
            ).all()
            items.extend(
                {
                    "kind": "b2c_booking",
                    "booking_id": r.booking_id,
                    "user_id": r.user_id,
                    "user_name": _user_display_name(r.first_name, r.last_name),
                    "phone": mask_phone(r.phone),
                    "entity_name": r.entity_name,
                    "status": str(r.status),
                    "booked_at": r.booked_at.isoformat() if r.booked_at else None,
                    "audience": "b2c",
                }
                for r in book_rows
            )
        total = camps_total + open_bookings

    elif drill_id == "b2b_camps":
        items, total = await _list_b2b_camps(
            db,
            lifecycle=lifecycle,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
            offset=offset,
            q=q,
            city=city,
        )

    elif drill_id == "package_performance":
        if package_id is not None:
            items, total = await _bookings_for_package(
                db,
                package_id=package_id,
                date_from=date_from,
                date_to=date_to,
                limit=limit,
                offset=offset,
            )
        else:
            packages = await _package_performance(db, date_from=date_from, date_to=date_to, limit=100)
            total = len(packages)
            items = packages[offset : offset + limit]

    elif drill_id == "coupon_usage":
        if discount_code_id is not None:
            items, total = await _usages_for_coupon(
                db,
                discount_code_id=discount_code_id,
                date_from=date_from,
                date_to=date_to,
                limit=limit,
                offset=offset,
            )
        else:
            coupons = await _coupon_usage(db, date_from=date_from, date_to=date_to, limit=100)
            total = len(coupons)
            items = coupons[offset : offset + limit]

    await _attach_booking_collection(db, items)
    await _attach_package_names(db, items)
    await _attach_journey_statuses(db, items)
    await _fill_engagement_names(db, items)
    return {
        "drill": drill_id,
        "items": items,
        "filters": {
            "audience": audience,
            "from": date_from.isoformat() if date_from else None,
            "to": date_to.isoformat() if date_to else None,
            "lifecycle": lifecycle,
            "q": q,
            "city": city,
            "package_id": package_id,
            "discount_code_id": discount_code_id,
        },
        "page": page,
        "limit": limit,
        "total": total,
    }
