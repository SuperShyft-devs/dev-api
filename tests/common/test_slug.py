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
