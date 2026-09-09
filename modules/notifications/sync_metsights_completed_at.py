"""Backfill assessment_instances.completed_at from MetSights record detail.

For every assessment instance with a metsights_record_id (all engagement statuses):
1. GET /records/{record_id}/
2. Read data.completed_at
3. Overwrite local completed_at to match (including NULL)
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from modules.assessments.repository import AssessmentsRepository
from modules.metsights.service import MetsightsService

logger = logging.getLogger(__name__)

ProgressCallback = Callable[[int, int, int, int, int, int], None]


def _parse_metsights_completed_at(raw: Any) -> datetime | None:
    if raw is None:
        return None
    value = str(raw).strip()
    if not value:
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _normalize_dt(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _same_completed_at(local: datetime | None, remote: datetime | None) -> bool:
    return _normalize_dt(local) == _normalize_dt(remote)


async def sync_metsights_completed_at(
    db: AsyncSession,
    *,
    metsights_service: MetsightsService,
    assessments_repo: AssessmentsRepository | None = None,
    engagement_id: int | None = None,
    dry_run: bool = False,
    on_progress: ProgressCallback | None = None,
) -> dict[str, Any]:
    """Sync local completed_at from MetSights for all linked assessment instances."""

    repo = assessments_repo or AssessmentsRepository()
    instances = await repo.list_instances_with_metsights_record_id(
        db,
        engagement_id=engagement_id,
    )

    matched = len(instances)
    updated = 0
    cleared = 0
    skipped = 0
    failed = 0
    details: list[dict[str, Any]] = []

    def _report(done: int) -> None:
        if on_progress is not None:
            on_progress(done, matched, updated, cleared, skipped, failed)

    _report(0)

    for index, instance in enumerate(instances, start=1):
        ai_id = int(instance.assessment_instance_id)
        user_id = int(instance.user_id)
        record_id = (instance.metsights_record_id or "").strip()

        if not record_id:
            skipped += 1
            details.append(
                {
                    "assessment_instance_id": ai_id,
                    "user_id": user_id,
                    "action": "skipped",
                    "reason": "missing metsights_record_id",
                }
            )
            _report(index)
            continue

        try:
            record_data = await metsights_service.get_record_detail(record_id=record_id)
            if not isinstance(record_data, dict):
                raise ValueError(f"unexpected record detail type: {type(record_data)!r}")

            remote_completed_at = _parse_metsights_completed_at(record_data.get("completed_at"))
            local_completed_at = instance.completed_at

            if _same_completed_at(local_completed_at, remote_completed_at):
                skipped += 1
                details.append(
                    {
                        "assessment_instance_id": ai_id,
                        "user_id": user_id,
                        "action": "skipped",
                        "reason": "already matches MetSights completed_at",
                    }
                )
                _report(index)
                continue

            action = "cleared" if remote_completed_at is None else "updated"
            reason = (
                "set completed_at=NULL from MetSights"
                if remote_completed_at is None
                else f"set completed_at={remote_completed_at.isoformat()}"
            )

            if dry_run:
                if action == "cleared":
                    cleared += 1
                else:
                    updated += 1
                details.append(
                    {
                        "assessment_instance_id": ai_id,
                        "user_id": user_id,
                        "action": action,
                        "reason": f"dry-run: {reason}",
                    }
                )
                _report(index)
                continue

            instance.completed_at = remote_completed_at
            await db.commit()

            if action == "cleared":
                cleared += 1
            else:
                updated += 1
            details.append(
                {
                    "assessment_instance_id": ai_id,
                    "user_id": user_id,
                    "action": action,
                    "reason": reason,
                }
            )
        except Exception as exc:
            if not dry_run:
                await db.rollback()
            failed += 1
            details.append(
                {
                    "assessment_instance_id": ai_id,
                    "user_id": user_id,
                    "action": "failed",
                    "reason": str(exc),
                }
            )
            logger.warning(
                "sync_metsights_completed_at failed: instance=%s user=%s record=%s: %s",
                ai_id,
                user_id,
                record_id,
                exc,
                exc_info=True,
            )

        _report(index)

    return {
        "matched": matched,
        "updated": updated,
        "cleared": cleared,
        "skipped": skipped,
        "failed": failed,
        "dry_run": dry_run,
        "engagement_id": engagement_id,
        "details": details,
    }
