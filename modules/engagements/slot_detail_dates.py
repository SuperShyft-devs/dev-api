"""Helpers for matching blood collection dates in engagement slot_detail JSON."""

from __future__ import annotations

from datetime import date
from typing import Any

from modules.engagements.slot_availability import is_slot_detail_date_key, section_is_enabled


def _blood_collection_section(slot_detail: dict[str, Any] | None) -> dict[str, Any] | None:
    if not slot_detail:
        return None
    blood = slot_detail.get("blood_collection")
    if not isinstance(blood, dict) or not blood:
        return None
    if not section_is_enabled(blood):
        return None
    return blood


def enabled_blood_collection_dates(slot_detail: dict[str, Any] | None) -> list[date]:
    """Sorted on-site blood dates from an enabled blood_collection section.

    Skips non-date keys and dates whose own ``is_enable`` is false. A disabled
    section yields no dates.
    """
    blood = _blood_collection_section(slot_detail)
    if blood is None:
        return []

    found: list[date] = []
    for date_key, cfg in blood.items():
        key = str(date_key)
        if not is_slot_detail_date_key(key):
            continue
        if isinstance(cfg, dict) and cfg.get("is_enable") is False:
            continue
        found.append(date.fromisoformat(key))
    found.sort()
    return found


def blood_collection_dates_include(
    slot_detail: dict[str, Any] | None,
    target: date,
) -> bool:
    """Return True when an enabled blood_collection date key equals target."""
    return target in enabled_blood_collection_dates(slot_detail)


def engagement_collection_date_matches(
    *,
    start_date: date | None,
    slot_detail: dict[str, Any] | None,
    collection_date: date,
    has_participant_booking_on_date: bool = False,
) -> bool:
    """Return True when collection_date is a blood-collection day for the engagement.

    Priority:
    1. Enabled ``blood_collection`` dates in ``slot_detail`` (when that section exists)
    2. Else active participant blood-booking rows on ``collection_date``
    3. Else ``start_date`` fallback (camps with neither slots nor bookings yet)
    """
    if blood_collection_dates_include(slot_detail, collection_date):
        return True
    if _blood_collection_section(slot_detail) is not None:
        return False
    if has_participant_booking_on_date:
        return True
    return start_date == collection_date
