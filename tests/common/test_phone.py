"""Tests for shared phone normalization."""

from __future__ import annotations

import pytest

from common.phone import phone_lookup_candidates
from core.exceptions import AppError


def test_phone_lookup_candidates_india_ten_digit():
    assert phone_lookup_candidates("8103946120") == [
        "8103946120",
        "+918103946120",
        "918103946120",
    ]


def test_phone_lookup_candidates_thailand_e164():
    candidates = phone_lookup_candidates("+66961275268")
    assert "+66961275268" in candidates
    assert "66961275268" in candidates


def test_phone_lookup_candidates_strict_rejects_too_short():
    with pytest.raises(AppError) as exc:
        phone_lookup_candidates("12345", strict=True)
    assert exc.value.error_code == "INVALID_INPUT"


def test_phone_lookup_candidates_non_strict_empty_for_invalid_chars():
    assert phone_lookup_candidates("abc") == []
