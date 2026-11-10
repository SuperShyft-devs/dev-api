"""Tests for WhatsApp template slot resolution."""

from __future__ import annotations

from modules.notifications.template_variables import (
    build_legacy_message_csv,
    resolve_slots,
)


def test_resolve_slots_otp():
    context = {"otp": "654321", "user": {"first_name": "A"}}
    tv = {"slots": [{"index": 1, "source": "otp"}]}
    assert resolve_slots(tv, context) == ["654321"]


def test_legacy_csv_otp():
    assert build_legacy_message_csv("otp", ["123456"]) == "otp,123456"


def test_resolve_slots_legacy_component_list():
    context = {"otp": "654321", "user": {"first_name": "Pratheek"}}
    tv = [
        {
            "component": "body",
            "parameters": [
                {"ref": "member.first_name", "type": "text"},
                {"ref": "member.otp", "type": "text"},
            ],
        }
    ]
    assert resolve_slots(tv, context) == ["Pratheek", "654321"]
    assert build_legacy_message_csv("otpmsg", resolve_slots(tv, context)) == (
        "otpmsg,Pratheek,654321"
    )
