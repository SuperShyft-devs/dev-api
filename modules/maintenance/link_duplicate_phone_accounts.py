"""Link duplicate primary users that share a phone (10-digit / +91 / 91 variants).

OTP and booking lookup treat ``9769746493`` and ``+919769746493`` as the same
number. The unique index on ``users.phone`` is the raw string, so both can exist
as primaries (``parent_id IS NULL``). Auth then returns AMBIGUOUS_PHONE.

This job keeps the primary with the most engagement enrollments as the main
account and sets ``parent_id`` on the others (existing family/sub-profile model).
An employee in the group always stays primary so staff OTP/login is not stolen.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.employee.models import Employee
from modules.engagements.models import EngagementParticipant
from modules.users.models import User
from modules.users.repository import UsersRepository

_MIN_PHONE_DIGITS = 10


def phone_key(phone: str | None) -> str | None:
    digits = "".join(ch for ch in (phone or "") if ch.isdigit())
    if len(digits) < _MIN_PHONE_DIGITS:
        return None
    return digits[-10:]


def _name_tokens(user: User) -> set[str]:
    raw = f"{user.first_name or ''} {user.last_name or ''}".lower()
    return {part for part in raw.split() if part}


def names_similar(left: User, right: User) -> bool:
    left_tokens = _name_tokens(left)
    right_tokens = _name_tokens(right)
    if not left_tokens or not right_tokens:
        return False
    if left_tokens == right_tokens:
        return True
    overlap = len(left_tokens & right_tokens)
    union = len(left_tokens | right_tokens)
    return (overlap / union) >= 0.5


def _pick_main(
    primaries: list[User],
    *,
    engagement_counts: dict[int, int],
    employee_ids: set[int],
) -> User | None:
    employees = [user for user in primaries if int(user.user_id) in employee_ids]
    if len(employees) > 1:
        return None
    if len(employees) == 1:
        return employees[0]
    return max(
        primaries,
        key=lambda user: (
            engagement_counts.get(int(user.user_id), 0),
            1 if user.is_participant else 0,
            -int(user.user_id),
        ),
    )


async def _engagement_counts(db: AsyncSession, user_ids: list[int]) -> dict[int, int]:
    if not user_ids:
        return {}
    result = await db.execute(
        select(EngagementParticipant.user_id, func.count())
        .where(EngagementParticipant.user_id.in_(user_ids))
        .group_by(EngagementParticipant.user_id)
    )
    counts = {int(user_id): 0 for user_id in user_ids}
    for user_id, count in result.all():
        counts[int(user_id)] = int(count)
    return counts


async def _employee_user_ids(db: AsyncSession, user_ids: list[int]) -> set[int]:
    if not user_ids:
        return set()
    result = await db.execute(select(Employee.user_id).where(Employee.user_id.in_(user_ids)))
    return {int(row[0]) for row in result.all()}


async def _reparent_children(db: AsyncSession, *, from_parent_ids: list[int], to_parent_id: int) -> int:
    if not from_parent_ids:
        return 0
    result = await db.execute(select(User).where(User.parent_id.in_(from_parent_ids)))
    children = list(result.scalars().all())
    now = datetime.now(timezone.utc)
    moved = 0
    for child in children:
        if int(child.user_id) == int(to_parent_id):
            continue
        child.parent_id = to_parent_id
        child.updated_at = now
        db.add(child)
        moved += 1
    return moved


def _user_snapshot(user: User, *, engagement_count: int, is_employee: bool) -> dict[str, Any]:
    return {
        "user_id": int(user.user_id),
        "first_name": user.first_name,
        "last_name": user.last_name,
        "phone": user.phone,
        "email": user.email,
        "parent_id": user.parent_id,
        "is_participant": bool(user.is_participant),
        "engagement_count": engagement_count,
        "is_employee": is_employee,
    }


async def link_duplicate_phone_accounts(
    db: AsyncSession,
    *,
    dry_run: bool = True,
    require_similar_name: bool = False,
    phone: str | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """Link extra primary accounts that share a last-10-digit phone key.

    Returns a JSON-serializable report. Callers commit when ``dry_run`` is False.
    """
    wanted_key = phone_key(phone) if phone else None
    groups = await UsersRepository().list_duplicate_phone_groups(db)

    scanned = 0
    linked: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    for group in groups:
        key = phone_key(group[0].phone)
        if wanted_key is not None and key != wanted_key:
            continue
        primaries = [user for user in group if user.parent_id is None]
        if len(primaries) < 2:
            continue
        if limit is not None and scanned >= limit:
            break
        scanned += 1

        primary_ids = [int(user.user_id) for user in primaries]
        engagement_counts = await _engagement_counts(db, primary_ids)
        employee_ids = await _employee_user_ids(db, primary_ids)
        main = _pick_main(primaries, engagement_counts=engagement_counts, employee_ids=employee_ids)
        snapshots = [
            _user_snapshot(
                user,
                engagement_count=engagement_counts.get(int(user.user_id), 0),
                is_employee=int(user.user_id) in employee_ids,
            )
            for user in sorted(primaries, key=lambda row: int(row.user_id))
        ]

        if main is None:
            skipped.append(
                {
                    "phone_key": key,
                    "reason": "multiple_employees",
                    "users": snapshots,
                }
            )
            continue

        if require_similar_name:
            dissimilar = [
                user
                for user in primaries
                if int(user.user_id) != int(main.user_id) and not names_similar(main, user)
            ]
            if dissimilar:
                skipped.append(
                    {
                        "phone_key": key,
                        "reason": "dissimilar_names",
                        "users": snapshots,
                    }
                )
                continue

        subs = [user for user in primaries if int(user.user_id) != int(main.user_id)]
        sub_ids = [int(user.user_id) for user in subs]
        child_result = await db.execute(select(func.count()).select_from(User).where(User.parent_id.in_(sub_ids)))
        reparented_children = int(child_result.scalar_one() or 0)
        if not dry_run:
            now = datetime.now(timezone.utc)
            await _reparent_children(
                db,
                from_parent_ids=sub_ids,
                to_parent_id=int(main.user_id),
            )
            for sub in subs:
                sub.parent_id = int(main.user_id)
                if (sub.relationship or "self") == "self":
                    sub.relationship = "other"
                sub.updated_at = now
                db.add(sub)
            await db.flush()
            for sub in subs:
                sub.phone = main.phone
                sub.updated_at = now
                db.add(sub)
            await db.flush()

        linked.append(
            {
                "phone_key": key,
                "main_user_id": int(main.user_id),
                "sub_user_ids": [int(user.user_id) for user in subs],
                "reparented_children": reparented_children,
                "users": snapshots,
            }
        )

    return {
        "dry_run": dry_run,
        "require_similar_name": require_similar_name,
        "phone_key": wanted_key,
        "scanned_groups": scanned,
        "linked_groups": len(linked),
        "skipped_groups": len(skipped),
        "linked": linked,
        "skipped": skipped,
    }
