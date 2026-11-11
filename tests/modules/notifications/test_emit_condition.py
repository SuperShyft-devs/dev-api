"""Tests for binding emit conditions."""

from modules.notifications.emit_condition import (
    derive_condition_key,
    format_emit_condition_label,
    matches_condition,
    validate_emit_condition,
)


def test_matches_discount_equals_case_insensitive():
    ctx = {"payment": {"discount_code": "camp50"}}
    cond = {"path": "payment.discount_code", "op": "equals", "value": "CAMP50"}
    assert matches_condition(ctx, cond) is True
    assert matches_condition(ctx, {"path": "payment.discount_code", "op": "equals", "value": "OTHER"}) is False


def test_matches_discount_empty_and_one_of():
    ctx = {"payment": {"discount_code": ""}}
    assert matches_condition(ctx, {"path": "payment.discount_code", "op": "is_empty"}) is True
    ctx2 = {"payment": {"discount_code": "VIP"}}
    assert matches_condition(ctx2, {"path": "payment.discount_code", "op": "is_not_empty"}) is True
    assert matches_condition(
        ctx2,
        {"path": "payment.discount_code", "op": "one_of", "value": "STAFF, vip"},
    ) is True


def test_missing_condition_always_matches():
    assert matches_condition({"payment": {}}, None) is True


def test_validate_emit_condition_payments_only():
    validated = validate_emit_condition(
        {"path": "payment.discount_code", "op": "equals", "value": "X"},
        event_code="payments.completed",
    )
    assert validated is not None
    assert validated["op"] == "equals"


def test_derive_condition_key_and_label():
    key = derive_condition_key({"path": "payment.discount_code", "op": "equals", "value": "VIP"})
    assert key == "dc-eq-vip"
    assert format_emit_condition_label({"op": "is_empty"}) == "No discount code"
