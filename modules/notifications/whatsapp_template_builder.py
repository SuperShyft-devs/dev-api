"""Build WhatAPI Meta template POST components from service config."""

from __future__ import annotations

from typing import Any

from modules.notifications.context_builder import build_member_context
from modules.notifications.models import NotificationService
from modules.notifications.template_variables import (
    resolve_header_link,
    resolve_slots,
)
from modules.notifications.whatapi_client import WhatApiClient


def _count_body_params(components: list[dict[str, Any]]) -> int:
    for component in components:
        if not isinstance(component, dict):
            continue
        if str(component.get("type", "")).upper() == "BODY":
            text = str(component.get("text") or "")
            if "{{" in text:
                return text.count("{{")
    return 0


def _header_is_document(components: list[dict[str, Any]]) -> bool:
    for component in components:
        if not isinstance(component, dict):
            continue
        if str(component.get("type", "")).upper() != "HEADER":
            continue
        return str(component.get("format", "")).upper() in {"DOCUMENT", "IMAGE"}
    return False


async def build_whatsapp_send_payload(
    client: WhatApiClient,
    svc: NotificationService,
    *,
    member: dict[str, Any],
    participant_details: dict[str, Any] | None = None,
    template_body: dict[str, Any] | None = None,
) -> tuple[str, list[dict[str, Any]], str]:
    template_name = (svc.template_name or "").strip()
    context = build_member_context(member, participant_details=participant_details)
    slot_values = resolve_slots(svc.template_variables, context)

    if template_body is None:
        template_body = await client.fetch_message_template(template_name=template_name)
    data = template_body.get("data")
    components_meta: list[dict[str, Any]] = []
    language_code = "en"
    canonical_name = template_name
    if isinstance(data, list) and data and isinstance(data[0], dict):
        entry = data[0]
        meta_name = entry.get("name")
        if isinstance(meta_name, str) and meta_name.strip():
            canonical_name = meta_name.strip()
        raw_components = entry.get("components")
        if isinstance(raw_components, list):
            components_meta = raw_components
        language = entry.get("language")
        if isinstance(language, str) and language.strip():
            language_code = language.strip()

    expected = _count_body_params(components_meta)
    if expected > 0 and len(slot_values) > expected:
        slot_values = slot_values[:expected]
    elif expected > len(slot_values):
        slot_values = slot_values + [""] * (expected - len(slot_values))

    components: list[dict[str, Any]] = []
    header_link = resolve_header_link(svc.template_variables, context)
    if header_link and _header_is_document(components_meta):
        components.append(
            {
                "type": "header",
                "parameters": [
                    {
                        "type": "document",
                        "document": {"link": header_link, "filename": "document.pdf"},
                    }
                ],
            }
        )

    if slot_values:
        components.append(
            {
                "type": "body",
                "parameters": [{"type": "text", "text": value} for value in slot_values],
            }
        )

    return canonical_name, components, language_code
