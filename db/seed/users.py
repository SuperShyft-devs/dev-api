"""Seed extra users, admin employees, and B2C data from ``db/seed/users.txt`` (multi-JSON).

Each blob yields **one** B2C engagement per user, named like auto onboarding
(``{first_name}-{slot_date}``). Only the **first** ``engagement_participants`` row sets the slot
(and the name date); extra participant rows are ignored. Every ``assessment_instances`` entry is
attached to that engagement (MetSights before FitPrint). ``start_date`` / ``end_date`` match
auto-created B2C (both the slot day).
"""

from __future__ import annotations

import json
import secrets
import string
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.assessments.models import AssessmentCategoryProgress, AssessmentInstance
from modules.assessments.repository import AssessmentsRepository
from modules.employee.models import Employee
from modules.engagements.models import Engagement, EngagementKind, EngagementParticipant
from modules.engagements.repository import EngagementsRepository
from modules.platform_settings.models import PlatformSettings
from modules.users.models import User

USERS_SEED_FILE = Path(__file__).resolve().parent / "users.txt"

_MET_PACKAGE_IDS = frozenset({1, 2})
_FITPRINT_PACKAGE_ID = 3


def _generate_engagement_code(length: int = 8) -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def _parse_json_objects(raw: str) -> list[dict[str, Any]]:
    decoder = json.JSONDecoder()
    s = raw.strip()
    out: list[dict[str, Any]] = []
    while s:
        s = s.lstrip()
        if not s:
            break
        obj, idx = decoder.raw_decode(s)
        if not isinstance(obj, dict):
            raise ValueError(f"Expected JSON object, got {type(obj).__name__}")
        out.append(obj)
        s = s[idx:].lstrip()
    return out


def _display_name_to_package_id(display_name: str) -> int:
    n = (display_name or "").strip().lower()
    if "fitprint" in n or "fit print" in n:
        return _FITPRINT_PACKAGE_ID
    if "pro" in n:
        return 2
    if "basic" in n or "metsight" in n:
        return 1
    raise ValueError(f"Unknown assessment package display_name: {display_name!r}")


def _normalize_gender(raw: str | None) -> str:
    g = (raw or "").strip().lower()
    if g in ("male", "m", "1"):
        return "male"
    if g in ("female", "f", "2"):
        return "female"
    return g or "unknown"


def _dob_from_age(age: int, *, ref_year: int = 2026) -> date:
    y = max(ref_year - int(age), 1900)
    return date(y, 1, 1)


def _parse_time_slot(raw: str) -> time:
    parts = (raw or "10:00").strip().split(":")
    h = int(parts[0]) if parts else 10
    m = int(parts[1]) if len(parts) > 1 else 0
    return time(h, m)


def _assessment_rows_from_json(
    assessment_instances: list[dict[str, Any]],
) -> list[tuple[int, str | None, date]]:
    """Each JSON assessment → (package_id, metsights_record_id, assigned_date)."""

    rows: list[tuple[int, str | None, date]] = []
    for inst in assessment_instances:
        raw_date = inst.get("assigned_at")
        if not raw_date:
            continue
        ad = date.fromisoformat(str(raw_date)[:10])
        pkg = inst.get("assessment_package") or {}
        dn = str(pkg.get("display_name") or "")
        pid = _display_name_to_package_id(dn)
        mid = inst.get("metsights_record_id")
        mid_s = str(mid).strip() if mid is not None else None
        rows.append((pid, mid_s or None, ad))
    return rows


def _order_assessment_rows(
    rows: list[tuple[int, str | None, date]],
) -> list[tuple[int, str | None, date]]:
    """MetSights (1/2) first, then FitPrint (3), then any other — stable within each group."""

    met = [r for r in rows if r[0] in _MET_PACKAGE_IDS]
    fit = [r for r in rows if r[0] == _FITPRINT_PACKAGE_ID]
    unknown = [r for r in rows if r[0] not in _MET_PACKAGE_IDS and r[0] != _FITPRINT_PACKAGE_ID]
    return met + fit + unknown


def _b2c_engagement_name(user_first_name: str | None, engagement_date: date) -> str:
    """Match ``EngagementsService.create_b2c_engagement`` (`{first}-{YYYY-MM-DD}`)."""

    name_part = (user_first_name or "user").strip() or "user"
    return f"{name_part}-{engagement_date.isoformat()}"


async def _next_user_id(session: AsyncSession) -> int:
    r = await session.execute(select(func.coalesce(func.max(User.user_id), 0)))
    return int(r.scalar_one()) + 1


async def _next_employee_id(session: AsyncSession) -> int:
    r = await session.execute(select(func.coalesce(func.max(Employee.employee_id), 0)))
    return int(r.scalar_one()) + 1


async def _resolve_user(session: AsyncSession, blob: dict[str, Any]) -> User:
    u = blob.get("users") or {}
    email = (u.get("email") or "").strip() or None
    phone = str(u.get("phone") or "").strip()
    if not email and not phone:
        raise ValueError("User needs email or phone")

    stmt = select(User)
    if email:
        stmt = stmt.where(User.email == email)
    else:
        stmt = stmt.where(User.phone == phone)

    existing = (await session.execute(stmt.limit(1))).scalar_one_or_none()

    first_name = str(u.get("first_name") or "").strip() or "User"
    last_name = str(u.get("last_name") or "").strip()
    age_val = max(int(u.get("age") or 0), 1)
    gender_val = _normalize_gender(u.get("gender"))
    is_participant = bool(u.get("is_participant", True))
    status_val = str(u.get("status") or "active").strip().lower() or "active"
    relationship = str(u.get("relationship") or "self").strip() or "self"
    dob = _dob_from_age(age_val)

    if existing is None:
        if not phone:
            raise ValueError("New users require a non-empty phone (NOT NULL in DB)")
        uid = await _next_user_id(session)
        existing = User(
            user_id=uid,
            first_name=first_name,
            last_name=last_name,
            age=age_val,
            phone=phone,
            email=email,
            gender=gender_val,
            is_participant=is_participant,
            status=status_val,
            relationship=relationship,
            date_of_birth=dob,
        )
        session.add(existing)
        await session.flush()
    else:
        existing.first_name = first_name or existing.first_name
        existing.last_name = last_name or existing.last_name
        existing.age = int(u.get("age") or existing.age or 1)
        if email:
            existing.email = email
        if phone:
            existing.phone = phone
        existing.relationship = relationship
        existing.gender = gender_val
        existing.is_participant = is_participant
        existing.status = status_val
        if existing.date_of_birth is None:
            existing.date_of_birth = _dob_from_age(int(existing.age or 1))

    for key, col in (
        ("address", "address"),
        ("pin_code", "pin_code"),
        ("city", "city"),
        ("state", "state"),
        ("country", "country"),
    ):
        if u.get(key) is not None:
            setattr(existing, col, str(u.get(key) or ""))
    return existing


async def _ensure_admin_employee(session: AsyncSession, user_id: int) -> None:
    row = (await session.execute(select(Employee).where(Employee.user_id == user_id).limit(1))).scalar_one_or_none()
    if row is None:
        eid = await _next_employee_id(session)
        session.add(
            Employee(
                employee_id=eid,
                user_id=user_id,
                role="admin",
                status="active",
            )
        )
    else:
        row.role = "admin"
        row.status = "active"
    await session.flush()


async def _resolve_b2c_diagnostic_id(session: AsyncSession) -> int:
    row = await session.get(PlatformSettings, 1)
    if row is not None and row.b2c_default_diagnostic_package_id is not None:
        return int(row.b2c_default_diagnostic_package_id)
    return 6


async def _unique_engagement_code(session: AsyncSession, er: EngagementsRepository) -> str:
    for _ in range(50):
        code = _generate_engagement_code()
        if await er.get_engagement_by_code(session, code) is None:
            return code
    raise RuntimeError("Could not allocate unique engagement_code")


async def _profile_seed_engagement_exists(
    session: AsyncSession, *, user_id: int, engagement_name: str
) -> bool:
    q = (
        select(Engagement.engagement_id)
        .join(EngagementParticipant, EngagementParticipant.engagement_id == Engagement.engagement_id)
        .where(EngagementParticipant.user_id == user_id)
        .where(Engagement.engagement_name == engagement_name)
        .where(Engagement.organization_id.is_(None))
        .where(Engagement.engagement_type == EngagementKind.bio_ai)
        .limit(1)
    )
    return (await session.execute(q)).scalar_one_or_none() is not None


async def _ensure_instance(
    session: AsyncSession,
    ar: AssessmentsRepository,
    *,
    user_id: int,
    engagement_id: int,
    package_id: int,
    metsights_record_id: str | None,
    assigned_at: datetime,
) -> None:
    existing = await ar.get_instance_by_user_engagement_package(
        session,
        user_id=user_id,
        engagement_id=engagement_id,
        package_id=package_id,
    )
    mid = (metsights_record_id or "").strip() or None
    if existing is not None:
        if mid and not (existing.metsights_record_id or "").strip():
            await ar.set_metsights_record_id(
                session,
                assessment_instance_id=int(existing.assessment_instance_id),
                metsights_record_id=mid,
            )
        if existing.assigned_at is None:
            existing.assigned_at = assigned_at
        return

    instance = AssessmentInstance(
        user_id=user_id,
        engagement_id=engagement_id,
        package_id=package_id,
        status="active",
        metsights_record_id=mid,
        assigned_at=assigned_at,
        completed_at=None,
    )
    await ar.create_instance(session, instance)
    for link in await ar.list_package_categories(session, package_id=package_id):
        cp = await ar.get_category_progress(
            session,
            assessment_instance_id=int(instance.assessment_instance_id),
            category_id=int(link.category_id),
        )
        if cp is not None:
            continue
        await ar.create_category_progress(
            session,
            AssessmentCategoryProgress(
                assessment_instance_id=int(instance.assessment_instance_id),
                category_id=int(link.category_id),
                status="incomplete",
                completed_at=None,
            ),
        )


async def _seed_one_engagement_per_user(
    session: AsyncSession,
    *,
    user: User,
    participants: list[dict[str, Any]],
    instances: list[dict[str, Any]],
    diagnostic_package_id: int,
) -> None:
    """One B2C engagement per user; first ``engagement_participants`` row drives enrollment slot.

    Every ``assessment_instances`` entry is attached to that engagement (MetSights before FitPrint).
    Extra ``engagement_participants`` rows are ignored for enrollment count.
    """

    uid = int(user.user_id)

    rows = _assessment_rows_from_json(instances)
    ordered = _order_assessment_rows(rows)
    first_slot = participants[0] if participants else None

    if first_slot:
        slot_date = date.fromisoformat(str(first_slot["engagement_date"]))
        slot_start = _parse_time_slot(str(first_slot.get("slot_start_time") or "10:00"))
    elif ordered:
        slot_date = ordered[0][2]
        slot_start = time(10, 0)
    else:
        slot_date = date.today()
        slot_start = time(10, 0)

    if not ordered:
        ordered = [(1, None, slot_date)]

    engagement_name = _b2c_engagement_name(user.first_name, slot_date)
    if await _profile_seed_engagement_exists(session, user_id=uid, engagement_name=engagement_name):
        return

    primary_pid = ordered[0][0]

    er = EngagementsRepository()
    ar = AssessmentsRepository()
    code = await _unique_engagement_code(session, er)

    engagement = Engagement(
        engagement_name=engagement_name,
        metsights_engagement_id=None,
        organization_id=None,
        engagement_code=code,
        engagement_type=EngagementKind.bio_ai,
        assessment_package_id=primary_pid,
        diagnostic_package_id=diagnostic_package_id,
        city=(user.city or "").strip() or "",
        address=None,
        pincode=None,
        slot_duration=20,
        start_date=slot_date,
        end_date=slot_date,
        status="active",
        participant_count=0,
    )
    await er.create_engagement(session, engagement)
    eid = int(engagement.engagement_id)

    participant = EngagementParticipant(
        engagement_id=eid,
        user_id=uid,
        engagement_date=slot_date,
        slot_start_time=slot_start,
        participants_employee_id=None,
        participant_department=None,
        participant_blood_group=None,
        want_doctor_consultation=None,
        want_nutritionist_consultation=None,
        want_doctor_and_nutritionist_consultation=None,
        is_metsights_profile_created=False,
    )
    await er.create_participant(session, participant)

    seen_package: set[int] = set()
    for pkg_id, mid, ad in ordered:
        if pkg_id in seen_package:
            continue
        seen_package.add(pkg_id)
        assigned_at = datetime.combine(ad, time.min, tzinfo=timezone.utc)
        await _ensure_instance(
            session,
            ar,
            user_id=uid,
            engagement_id=eid,
            package_id=pkg_id,
            metsights_record_id=mid,
            assigned_at=assigned_at,
        )


async def seed_users(session: AsyncSession) -> int:
    """Upsert users, admin employees, and B2C engagements from ``USERS_SEED_FILE``. Returns blob count."""

    if not USERS_SEED_FILE.is_file():
        raise SystemExit(
            f"Seed data missing: {USERS_SEED_FILE}. "
            "Add multi-JSON users.txt next to db/seed/users.py (committed with the repo)."
        )

    raw = USERS_SEED_FILE.read_text(encoding="utf-8")
    blobs = _parse_json_objects(raw)
    if not blobs:
        raise SystemExit(f"No JSON user blobs in {USERS_SEED_FILE}")

    diagnostic_id = await _resolve_b2c_diagnostic_id(session)
    for blob in blobs:
        user = await _resolve_user(session, blob)
        await _ensure_admin_employee(session, int(user.user_id))
        participants = blob.get("engagement_participants") or []
        instances = blob.get("assessment_instances") or []
        await _seed_one_engagement_per_user(
            session,
            user=user,
            participants=participants,
            instances=instances,
            diagnostic_package_id=diagnostic_id,
        )
    return len(blobs)
