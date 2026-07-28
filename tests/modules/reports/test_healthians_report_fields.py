"""Tests for Healthians getBookingReport field helpers."""

from datetime import datetime, timezone

from modules.reports.healthians_report_fields import (
    customer_display_name,
    parse_booking_report_entry,
    parse_healthians_full_report,
    parse_healthians_verified_at,
    verified_at_unchanged,
)


def test_customer_display_name_prefers_customer_name():
    assert customer_display_name({"customer_name": "A", "cust_name": "B"}) == "A"
    assert customer_display_name({"cust_name": "Harshili Gada"}) == "Harshili Gada"


def test_parse_full_report_and_verified_at():
    assert parse_healthians_full_report(1) is True
    assert parse_healthians_full_report(0) is False
    assert parse_healthians_verified_at("2025-12-04 19:55:56") == datetime(
        2025, 12, 4, 19, 55, 56, tzinfo=timezone.utc
    )


def test_parse_booking_report_entry_and_verified_at_unchanged():
    url, full, verified = parse_booking_report_entry(
        {
            "report_url": "https://example.com/a.pdf",
            "full_report": 1,
            "verified_at": "2025-12-04 19:55:56",
        }
    )
    assert url == "https://example.com/a.pdf"
    assert full is True
    assert verified_at_unchanged(verified, verified) is True
    assert verified_at_unchanged(None, verified) is False
