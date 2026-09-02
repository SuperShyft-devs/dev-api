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


def match_booking_report_entry(
    report_list: list[Any],
    *,
    first_name: str = "",
    last_name: str = "",
) -> dict[str, Any] | None:
    """Pick the report row for a participant, with a single-entry fallback."""
    if not report_list:
        return None

    entries = [entry for entry in report_list if isinstance(entry, dict)]
    if not entries:
        return None
    if len(entries) == 1:
        return entries[0]

    target_full = f"{first_name} {last_name}".strip().lower()
    target_tokens = set(target_full.split()) if target_full else set()

    best: dict[str, Any] | None = None
    best_score = 0
    for entry in entries:
        customer_name = customer_display_name(entry).lower()
        if not customer_name:
            continue
        if customer_name == target_full:
            return entry
        entry_tokens = set(customer_name.split())
        overlap = len(target_tokens & entry_tokens)
        if overlap > best_score:
            best_score = overlap
            best = entry

    if best is not None and best_score >= 1:
        return best
    return None


def has_full_booking_report_in_payload(
    response_payload: dict[str, Any] | list[Any] | None,
    *,
    first_name: str = "",
    last_name: str = "",
) -> bool:
    """True when a Healthians getBookingReport payload indicates ``full_report``."""
    if not isinstance(response_payload, dict) or response_payload.get("status") is not True:
        return False

    report_list = response_payload.get("data")
    if not isinstance(report_list, list):
        return False

    matched = match_booking_report_entry(
        report_list,
        first_name=first_name,
        last_name=last_name,
    )
    if matched is None:
        return False

    _, full_report, _ = parse_booking_report_entry(matched)
    return full_report is True
