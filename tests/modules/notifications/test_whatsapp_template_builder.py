"""Tests for WhatsApp template slot resolution and send payload building."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from modules.notifications.models import NotificationService
from modules.notifications.template_variables import (
    build_legacy_message_csv,
    resolve_slots,
)
from modules.notifications.whatsapp_template_builder import build_whatsapp_send_payload


def test_resolve_slots_otp():
    context = {"otp": {"otp": "654321"}, "user": {"first_name": "A"}}
    tv = {"slots": [{"index": 1, "source": "otp.otp"}]}
    assert resolve_slots(tv, context) == ["654321"]


def test_legacy_csv_otp():
    assert build_legacy_message_csv("otp", ["123456"]) == "otp,123456"


def test_resolve_slots_legacy_component_list():
    context = {"otp": {"otp": "654321"}, "user": {"first_name": "Pratheek"}}
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


@pytest.mark.asyncio
async def test_build_whatsapp_send_payload_uses_meta_canonical_template_name():
    client = MagicMock()
    client.fetch_message_template = AsyncMock(
        return_value={
            "data": [
                {
                    "name": "otpmsg__",
                    "language": "en",
                    "components": [
                        {
                            "type": "BODY",
                            "text": "Hello {{1}}, code {{2}}",
                        }
                    ],
                }
            ]
        }
    )
    svc = NotificationService(
        service_key="whatapi-otp",
        display_name="OTP",
        channel="whatsapp",
        webhook_path="/test",
        template_name="otpmsg",
        template_variables={
            "slots": [
                {"index": 1, "source": "user.first_name"},
                {"index": 2, "source": "otp.otp"},
            ]
        },
    )
    member = {"first_name": "A", "otp": "123456"}

    name, components, language = await build_whatsapp_send_payload(
        client, svc, member=member
    )

    assert name == "otpmsg__"
    assert language == "en"
    assert components
    client.fetch_message_template.assert_awaited_once_with(template_name="otpmsg")
