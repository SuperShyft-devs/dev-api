"""Camp number helpers for B2B engagements."""

from __future__ import annotations

import calendar
from datetime import date


def compute_camp_no(organization_id: int | None, start_date: date | None) -> int | None:
    """Derive camp_no from organization_id and start_date, or None for B2C."""
    if organization_id is None or organization_id <= 0 or start_date is None:
        return None
    day = f"{start_date.day:02d}"
    month = f"{start_date.month:02d}"
    year = f"{start_date.year % 100:02d}"
    return int(f"{organization_id}{day}{month}{year}")


def format_camp_name(organization_name: str, start_date: date) -> str:
    """Human-readable camp label, e.g. 'Acme Corp 23 June 2026'."""
    name = (organization_name or "").strip() or "Organization"
    month_word = calendar.month_name[start_date.month]
    return f"{name} {start_date.day} {month_word} {start_date.year}"
