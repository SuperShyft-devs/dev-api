"""Tests for legacy DB data sanitization helpers."""

from __future__ import annotations

import pytest

from common.data_sanitize import (
    SanitizeKind,
    SanitizeStatus,
    sanitize_nested_json,
    sanitize_value,
)
from common.slug import migrate_slot_detail_cabin_keys, sanitize_cabin_key
from common.validation import validate_nested_strings
from modules.maintenance.special_handlers import (
    clamp_scale_answer,
    sanitize_questionnaire_answer,
    sanitize_slot_detail,
)


class TestSanitizeValue:
    def test_person_name_strips_digits(self):
        out = sanitize_value("Rahul2 Kumar", kind=SanitizeKind.PERSON_NAME)
        assert out.status == SanitizeStatus.OK
        assert out.value == "Rahul Kumar"

    def test_person_name_null_when_empty_after_repair(self):
        out = sanitize_value("123", kind=SanitizeKind.PERSON_NAME, required=False)
        assert out.status == SanitizeStatus.NULL

    def test_person_name_skip_when_required(self):
        out = sanitize_value("123", kind=SanitizeKind.PERSON_NAME, required=True)
        assert out.status == SanitizeStatus.SKIP

    def test_phone_optional_too_short_becomes_null(self):
        out = sanitize_value("1234567", kind=SanitizeKind.PHONE, required=False)
        assert out.status == SanitizeStatus.NULL

    def test_phone_required_too_short_skips(self):
        out = sanitize_value("1234567", kind=SanitizeKind.PHONE, required=True)
        assert out.status == SanitizeStatus.SKIP

    def test_phone_valid(self):
        out = sanitize_value("9876543210", kind=SanitizeKind.PHONE, required=True)
        assert out.status in {SanitizeStatus.OK, SanitizeStatus.UNCHANGED}
        assert out.value == "9876543210"

    def test_pin_code_invalid_becomes_null(self):
        out = sanitize_value("5600", kind=SanitizeKind.PIN_CODE, required=False)
        assert out.status == SanitizeStatus.NULL

    def test_pin_code_valid(self):
        out = sanitize_value("560001", kind=SanitizeKind.PIN_CODE, required=False)
        assert out.status in {SanitizeStatus.OK, SanitizeStatus.UNCHANGED}
        assert out.value == "560001"

    def test_slug_key_hyphen_normalized(self):
        out = sanitize_value("btc-001", kind=SanitizeKind.SLUG_KEY, required=False)
        assert out.status == SanitizeStatus.OK
        assert out.value == "btc_001"

    def test_service_key_allows_hyphens(self):
        out = sanitize_value("whatapi-otp", kind=SanitizeKind.SERVICE_KEY, required=True)
        assert out.status in {SanitizeStatus.OK, SanitizeStatus.UNCHANGED}
        assert out.value == "whatapi-otp"

    def test_safe_display_name_strips_html(self):
        out = sanitize_value("Acme <script>alert(1)</script> Corp", kind=SanitizeKind.SAFE_DISPLAY_NAME)
        assert out.status == SanitizeStatus.OK
        assert "<" not in out.value
        assert ">" not in out.value


class TestNestedJson:
    def test_strips_script_from_nested(self):
        payload = {"note": "hello <script>x</script> world"}
        out = sanitize_nested_json(payload, required=False)
        assert out.status == SanitizeStatus.OK
        assert "<script>" not in str(out.value)

    def test_validate_nested_strings_accepts_clean(self):
        cleaned = validate_nested_strings({"a": "hello"})
        assert cleaned == {"a": "hello"}


class TestCabinKeys:
    def test_sanitize_cabin_key(self):
        assert sanitize_cabin_key("btc-001") == "btc_001"

    def test_migrate_slot_detail_rewrites_keys(self):
        slot_detail = {
            "blood_collection": {
                "2026-01-01": [
                    {"cabin_name": "Room A", "cabin_key": "btc-001"},
                ]
            }
        }
        updated, mapping = migrate_slot_detail_cabin_keys(slot_detail)
        assert mapping.get("btc-001") == "room_a"
        assert updated["blood_collection"]["2026-01-01"][0]["cabin_key"] == "room_a"

    def test_sanitize_slot_detail(self):
        out = sanitize_slot_detail(
            {
                "blood_collection": {
                    "2026-01-01": [{"cabin_name": "Room 1", "cabin_key": "room-1"}]
                }
            }
        )
        assert out.status == SanitizeStatus.OK
        assert out.value["blood_collection"]["2026-01-01"][0]["cabin_key"] == "room_1"


class TestQuestionnaireAnswer:
    def test_health_priorities_string_coerced(self):
        out = sanitize_questionnaire_answer(
            "2",
            question_key="health_priorities",
            question_type="multiple_choice",
            allowed_option_values={"0", "1", "2", "3", "4", "5"},
        )
        assert out.status == SanitizeStatus.OK
        assert out.value == ["2"]

    def test_health_priorities_truncates_to_two(self):
        out = sanitize_questionnaire_answer(
            ["1", "2", "3"],
            question_key="health_priorities",
            question_type="multiple_choice",
            allowed_option_values={"1", "2", "3"},
        )
        assert out.status == SanitizeStatus.OK
        assert out.value == ["1", "2"]

    def test_anthropometry_weight_clamped(self):
        clamped = clamp_scale_answer("weight", {"value": 10, "unit": "0"})
        assert clamped["value"] == 20.0

    def test_scale_answer_clamped_in_sanitize(self):
        out = sanitize_questionnaire_answer(
            {"value": 10, "unit": "0"},
            question_key="weight",
            question_type="scale",
        )
        assert out.status == SanitizeStatus.OK
        assert out.value["value"] == 20.0

    def test_invalid_single_choice_nulled(self):
        out = sanitize_questionnaire_answer(
            "bad",
            question_key="diet",
            question_type="single_choice",
            allowed_option_values={"veg", "nonveg"},
        )
        assert out.status == SanitizeStatus.NULL
