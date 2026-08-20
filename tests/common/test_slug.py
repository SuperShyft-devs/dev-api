"""Tests for department slug helper."""

from __future__ import annotations

from common.slug import slugify_department


def test_slugify_department_basic():
    assert slugify_department("Sales") == "sales"


def test_slugify_department_spaces_and_special_chars():
    assert slugify_department("Sales & Marketing") == "sales_marketing"
    assert slugify_department("  HR  ") == "hr"


def test_slugify_department_collapses_underscores():
    assert slugify_department("R & D") == "r_d"


def test_slugify_cabin_key_matches_department_slug():
    from common.slug import slugify_cabin_key

    assert slugify_cabin_key("Room 1") == "room_1"
    assert slugify_cabin_key("Room 2") == "room_2"


def test_sanitize_cabin_key_converts_legacy_keys():
    from common.slug import sanitize_cabin_key

    assert sanitize_cabin_key("btc-001") == "btc_001"
    assert sanitize_cabin_key("room2_2") == "room2_2"
