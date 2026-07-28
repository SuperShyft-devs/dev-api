"""Helpers for parsing Healthians getBookingReport payload fields."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def customer_display_name(entry: dict[str, Any]) -> str:
    """Return customer name from either digital-value or report payload shapes."""
    return str(entry.get("customer_name") or entry.get("cust_name") or "").strip()


def parse_healthians_verified_at(value: Any) -> datetime | None:
    """Parse Healthians ``verified_at`` (``YYYY-MM-DD HH:MM:SS``) as UTC-aware datetime."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def parse_healthians_full_report(value: Any) -> bool | None:
    """Parse Healthians ``full_report`` (0/1 or bool) into a nullable bool."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(int(value))
    text = str(value).strip().lower()
    if text in {"1", "true", "yes"}:
        return True
    if text in {"0", "false", "no"}:
        return False
    return None


def extract_report_url(entry: dict[str, Any]) -> str | None:
    """Extract signed PDF URL from a Healthians report entry."""
    raw = entry.get("report_url") or entry.get("url")
    if not isinstance(raw, str):
        return None
    stripped = raw.strip()
    return stripped or None


def verified_at_unchanged(
    stored: datetime | None,
    incoming: datetime | None,
) -> bool:
    """True when both timestamps exist and match at second precision."""
    if stored is None or incoming is None:
        return False
    return stored.replace(microsecond=0) == incoming.replace(microsecond=0)


def parse_booking_report_entry(
    entry: dict[str, Any],
) -> tuple[str | None, bool | None, datetime | None]:
    """Return ``(report_url, full_report, verified_at)`` from a matched report entry."""
    return (
        extract_report_url(entry),
        parse_healthians_full_report(entry.get("full_report")),
        parse_healthians_verified_at(entry.get("verified_at")),
    )
