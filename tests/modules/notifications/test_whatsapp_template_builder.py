"""Tests for WhatsApp template slot resolution and send payload building."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from modules.notifications.models import NotificationService
from modules.notifications.template_variables import (
    build_legacy_message_csv,
    resolve_header_filename,
    resolve_header_link,
    resolve_slots,
)
from modules.notifications.whatsapp_template_builder import build_whatsapp_send_payload


def test_resolve_slots_otp():
    context = {"otp": {"otp": "654321"}, "user": {"first_name": "A"}}
    tv = {"slots": [{"index": 1, "source": "otp.otp"}]}
    assert resolve_slots(tv, context) == ["654321"]


def test_resolve_header_filename_literal_and_variable():
    context = {"report": {"filename": "blood-report.pdf"}}
    assert (
        resolve_header_filename(
            {"header": {"filename_source": "literal", "filename_value": "document.pdf"}},
            context,
        )
        == "document.pdf"
    )
    assert (
        resolve_header_filename(
            {"header": {"filename_source": "report.filename"}},
            context,
        )
        == "blood-report.pdf"
    )
    assert resolve_header_filename({"header": {"source": "literal", "value": "https://x"}}, context) is None


def test_resolve_header_link_literal_and_variable():
    context = {"user": {"first_name": "A"}, "gift": {"gift_card_url": "https://x/y.pdf"}}
    assert (
        resolve_header_link(
            {"header": {"source": "literal", "value": "https://static/pretest.pdf"}},
            context,
        )
        == "https://static/pretest.pdf"
    )
    assert (
        resolve_header_link({"header": {"source": "gift.gift_card_url"}}, context)
        == "https://x/y.pdf"
    )


def test_resolve_slots_literal_mustache_interpolates():
    context = {"user": {"first_name": "Pratheek"}}
    tv = {
        "slots": [
            {"index": 1, "source": "literal", "value": "Hello {{user.first_name}}"},
            {"index": 2, "source": "{{user.first_name}}"},
        ]
    }
    assert resolve_slots(tv, context) == ["Hello Pratheek", "Pratheek"]


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
async def test_build_whatsapp_send_payload_infers_otp_slots_when_unconfigured():
    client = MagicMock()
    client.fetch_message_template = AsyncMock(
        return_value={
            "data": [
                {
                    "name": "otpmsg__",
                    "language": "en",
                    "components": [{"type": "BODY", "text": "Hi {{1}}, code {{2}}"}],
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
        template_variables=None,
        require_otp=True,
    )
    member = {"first_name": "Pratheek", "phone": "8762830757", "otp": "482913"}

    name, components, _ = await build_whatsapp_send_payload(client, svc, member=member)

    assert name == "otpmsg__"
    body = next(c for c in components if c.get("type") == "body")
    texts = [p["text"] for p in body["parameters"]]
    assert texts == ["Pratheek", "482913"]


@pytest.mark.asyncio
async def test_build_whatsapp_send_payload_prefers_english_language():
    client = MagicMock()
    client.fetch_message_template = AsyncMock(
        return_value={
            "data": [
                {
                    "name": "otpmsg__",
                    "language": "sk",
                    "components": [{"type": "BODY", "text": "{{1}}"}],
                },
                {
                    "name": "otpmsg__",
                    "language": "en",
                    "components": [{"type": "BODY", "text": "{{1}}"}],
                },
            ]
        }
    )
    svc = NotificationService(
        service_key="whatapi-otp",
        display_name="OTP",
        channel="whatsapp",
        webhook_path="/test",
        template_name="otpmsg",
        template_variables={"slots": [{"index": 1, "source": "otp.otp"}]},
    )
    member = {"first_name": "A", "otp": "123456"}

    _, _, language = await build_whatsapp_send_payload(client, svc, member=member)

    assert language == "en"


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


@pytest.mark.asyncio
async def test_build_whatsapp_send_payload_document_header_filename_from_config():
    client = MagicMock()
    client.fetch_message_template = AsyncMock(
        return_value={
            "data": [
                {
                    "name": "camp15",
                    "language": "en",
                    "components": [
                        {"type": "HEADER", "format": "DOCUMENT"},
                        {"type": "BODY", "text": "Hi {{1}}"},
                    ],
                }
            ]
        }
    )
    svc = NotificationService(
        service_key="pretest-whatsapp-session-v1",
        display_name="Pretest PDF",
        channel="whatsapp",
        webhook_path="/test",
        template_name="camp15",
        template_variables={
            "slots": [{"index": 1, "source": "literal", "value": "Hello"}],
            "header": {
                "source": "literal",
                "value": "https://supershyft.com/reports/tCO8xWspk1Mdxsn5.pdf",
                "filename_source": "literal",
                "filename_value": "pretest-guidelines.pdf",
            },
        },
    )
    member = {"first_name": "Pratheek"}

    _, components, _ = await build_whatsapp_send_payload(client, svc, member=member)

    doc = components[0]["parameters"][0]["document"]
    assert doc["link"] == "https://supershyft.com/reports/tCO8xWspk1Mdxsn5.pdf"
    assert doc["filename"] == "pretest-guidelines.pdf"
