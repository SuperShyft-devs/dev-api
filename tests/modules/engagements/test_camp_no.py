"""Unit tests for camp_no helpers."""

from __future__ import annotations

from datetime import date

from modules.engagements.camp_no import compute_camp_no, format_camp_name, format_department_camp_name


def test_compute_camp_no_b2b_example():
    assert compute_camp_no(8, date(2026, 6, 23)) == 8230626


def test_compute_camp_no_multi_digit_org():
    assert compute_camp_no(12, date(2026, 6, 23)) == 12230626


def test_compute_camp_no_b2c_returns_none():
    assert compute_camp_no(None, date(2026, 6, 23)) is None
    assert compute_camp_no(0, date(2026, 6, 23)) is None


def test_compute_camp_no_missing_date_returns_none():
    assert compute_camp_no(8, None) is None


def test_format_camp_name():
    assert format_camp_name("Acme Corp", date(2026, 6, 23)) == "Acme Corp 23 June 2026"


def test_format_department_camp_name():
    assert format_department_camp_name("Acme Corp", "sales", date(2026, 6, 23)) == "Acme Corp sales 23 June 2026"
