"""Helpers for matching blood collection dates in engagement slot_detail JSON."""

from __future__ import annotations

from datetime import date
from typing import Any


def _blood_collection_section(slot_detail: dict[str, Any] | None) -> dict[str, Any] | None:
    if not slot_detail:
        return None
    blood = slot_detail.get("blood_collection")
    if not isinstance(blood, dict) or not blood:
        return None
    return blood


def blood_collection_dates_include(
    slot_detail: dict[str, Any] | None,
    target: date,
) -> bool:
    """Return True when an enabled blood_collection date key equals target."""
    blood = _blood_collection_section(slot_detail)
    if blood is None:
        return False

    target_str = target.isoformat()
    for date_key, cfg in blood.items():
        if date_key != target_str:
            continue
        if isinstance(cfg, dict) and cfg.get("is_enable") is False:
            continue
        return True
    return False


def engagement_collection_date_matches(
    *,
    start_date: date | None,
    slot_detail: dict[str, Any] | None,
    collection_date: date,
) -> bool:
    """Return True when collection_date matches blood_collection or start_date fallback."""
    if blood_collection_dates_include(slot_detail, collection_date):
        return True
    if _blood_collection_section(slot_detail) is not None:
        return False
    return start_date == collection_date
