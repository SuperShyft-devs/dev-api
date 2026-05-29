"""Tests for BYPASS_OTP_BY_PHONE config parsing."""

from __future__ import annotations

from core.config import settings


def test_get_bypass_otp_by_phone_expands_india_local_form():
    previous = settings.BYPASS_OTP_BY_PHONE
    settings.BYPASS_OTP_BY_PHONE = "8103946120:111111"
    settings._bypass_otp_cache_key = None
    settings._bypass_otp_by_phone_index = {}
    try:
        index = settings.get_bypass_otp_by_phone()
        assert index.get("8103946120") == "111111"
        assert index.get("+918103946120") == "111111"
    finally:
        settings.BYPASS_OTP_BY_PHONE = previous
        settings._bypass_otp_cache_key = None
        settings._bypass_otp_by_phone_index = {}


def test_get_bypass_otp_by_phone_skips_invalid_entries():
    previous = settings.BYPASS_OTP_BY_PHONE
    settings.BYPASS_OTP_BY_PHONE = "badentry,+66961275268:654321"
    settings._bypass_otp_cache_key = None
    settings._bypass_otp_by_phone_index = {}
    try:
        index = settings.get_bypass_otp_by_phone()
        assert "+66961275268" in index
        assert index["+66961275268"] == "654321"
        assert len([k for k in index if k == "badentry"]) == 0
    finally:
        settings.BYPASS_OTP_BY_PHONE = previous
        settings._bypass_otp_cache_key = None
        settings._bypass_otp_by_phone_index = {}
