"""Query filters for engagement participant list endpoints."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Literal

ReportsReadyFilter = Literal["bio_ai", "blood", "both", "missing"]

_REPORTS_READY_VALUES: frozenset[str] = frozenset({"bio_ai", "blood", "both", "missing"})
# Backward-compatible aliases from the initial yes/no filter.
_REPORTS_READY_ALIASES: dict[str, ReportsReadyFilter] = {
    "yes": "both",
    "no": "missing",
}


@dataclass(frozen=True)
class ParticipantListFilters:
    search: str | None = None
    engagement_date: date | None = None
    booking_date: date | None = None
    department: str | None = None
    has_booking_id: bool | None = None
    reports_ready: ReportsReadyFilter | None = None
    booking_date_user_ids: set[int] | None = None
    consultation_filters: dict[str, str] = field(default_factory=dict)


def parse_participant_list_filters(
    *,
    search: str | None = None,
    engagement_date: date | None = None,
    booking_date: date | None = None,
    department: str | None = None,
    has_booking_id: str | None = None,
    reports_ready: str | None = None,
    consultation_filters: dict[str, str] | None = None,
) -> ParticipantListFilters:
    parsed_has_booking_id: bool | None = None
    if has_booking_id == "yes":
        parsed_has_booking_id = True
    elif has_booking_id == "no":
        parsed_has_booking_id = False

    parsed_reports_ready: ReportsReadyFilter | None = None
    if reports_ready in _REPORTS_READY_VALUES:
        parsed_reports_ready = reports_ready  # type: ignore[assignment]
    elif reports_ready in _REPORTS_READY_ALIASES:
        parsed_reports_ready = _REPORTS_READY_ALIASES[reports_ready]

    normalized_search = (search or "").strip() or None
    normalized_department = (department or "").strip() or None

    return ParticipantListFilters(
        search=normalized_search,
        engagement_date=engagement_date,
        booking_date=booking_date,
        department=normalized_department,
        has_booking_id=parsed_has_booking_id,
        reports_ready=parsed_reports_ready,
        consultation_filters={
            key: value
            for key, value in (consultation_filters or {}).items()
            if value in {"yes", "no"}
        },
    )


def filters_are_active(filters: ParticipantListFilters) -> bool:
    return bool(
        filters.search
        or filters.engagement_date
        or filters.booking_date
        or filters.department
        or filters.has_booking_id is not None
        or filters.reports_ready is not None
        or filters.consultation_filters
    )


def filters_to_meta(filters: ParticipantListFilters) -> dict[str, Any]:
    return {
        "search": filters.search,
        "engagement_date": filters.engagement_date.isoformat() if filters.engagement_date else None,
        "booking_date": filters.booking_date.isoformat() if filters.booking_date else None,
        "department": filters.department,
        "has_booking_id": (
            "yes"
            if filters.has_booking_id is True
            else "no"
            if filters.has_booking_id is False
            else None
        ),
        "reports_ready": filters.reports_ready,
        "consultation_filters": filters.consultation_filters or None,
    }
