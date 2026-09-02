"""Resolve blood test completion flags via live Healthians getBookingReport."""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from modules.diagnostics.healthians import client as healthians_client
from modules.reports.healthians_report_fields import has_full_booking_report_in_payload

logger = logging.getLogger(__name__)

_LIVE_CONCURRENCY = 8


def _participant_key(row: tuple) -> tuple[int, int]:
    return (int(row[2]), int(row[1]))


def _participant_booking_id(row: tuple) -> str:
    return str(row[25] or "").strip()


async def _live_healthians_full_report(
    *,
    booking_id: str,
    first_name: str,
    last_name: str,
) -> bool:
    access_token = await healthians_client.get_access_token()
    report_data = await healthians_client.get_booking_report(access_token, booking_id)
    return has_full_booking_report_in_payload(
        report_data,
        first_name=first_name,
        last_name=last_name,
    )


async def resolve_blood_test_complete_flags(
    db: AsyncSession,
    rows: list[tuple],
) -> dict[tuple[int, int], bool]:
    """Map (user_id, engagement_id) to Healthians full_report via live API calls."""
    _ = db
    if not rows:
        return {}

    row_by_key = {_participant_key(row): row for row in rows}
    flags: dict[tuple[int, int], bool] = dict.fromkeys(row_by_key, False)

    if not settings.HEALTHIANS_API_KEY or not settings.HEALTHIANS_SECRET_KEY:
        return flags

    pending: list[tuple[tuple[int, int], tuple]] = [
        (key, row)
        for key, row in row_by_key.items()
        if _participant_booking_id(row)
    ]
    if not pending:
        return flags

    semaphore = asyncio.Semaphore(_LIVE_CONCURRENCY)

    async def _check_one(key: tuple[int, int], row: tuple) -> tuple[tuple[int, int], bool]:
        async with semaphore:
            try:
                ready = await _live_healthians_full_report(
                    booking_id=_participant_booking_id(row),
                    first_name=str(row[3] or ""),
                    last_name=str(row[4] or ""),
                )
            except Exception:
                logger.exception(
                    "Live Healthians getBookingReport failed for user_id=%s engagement_id=%s booking_id=%s",
                    key[0],
                    key[1],
                    _participant_booking_id(row),
                )
                return key, False
            return key, ready

    results = await asyncio.gather(*(_check_one(key, row) for key, row in pending))
    for key, ready in results:
        if ready:
            flags[key] = True
    return flags
