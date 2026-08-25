"""Tests for shared phone normalization."""

from __future__ import annotations

import pytest

from common.phone import phone_lookup_candidates, to_healthians_mobile
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


def test_to_healthians_mobile_plus91():
    assert to_healthians_mobile("+918882025050") == "8882025050"


def test_to_healthians_mobile_91_prefix():
    assert to_healthians_mobile("918882025050") == "8882025050"


def test_to_healthians_mobile_ten_digit():
    assert to_healthians_mobile("8882025050") == "8882025050"


def test_to_healthians_mobile_too_short():
    assert to_healthians_mobile("12345") == "12345"


def test_to_healthians_mobile_empty():
    assert to_healthians_mobile("") == ""
    assert to_healthians_mobile(None) == ""
