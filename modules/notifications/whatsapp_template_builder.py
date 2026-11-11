"""Build WhatAPI Meta template POST components from service config."""

from __future__ import annotations

from typing import Any

from core.exceptions import AppError
from modules.notifications.context_builder import build_member_context
from modules.notifications.models import NotificationService
from modules.notifications.template_variables import (
    _get_nested,
    resolve_header_filename,
    resolve_header_link,
    resolve_slots,
)
from modules.notifications.whatapi_client import WhatApiClient

_OTP_WHATSAPP_SERVICE_KEYS = frozenset({"whatapi-otp", "send-otp-whatsapp"})


def _is_otp_whatsapp_service(svc: NotificationService) -> bool:
    return bool(svc.require_otp) or (svc.service_key or "").strip() in _OTP_WHATSAPP_SERVICE_KEYS


def infer_otp_whatsapp_slot_values(expected: int, context: dict[str, Any]) -> list[str]:
    """Default slot fill when DB template_variables is missing for OTP services."""
    if expected <= 0:
        return []
    otp = _get_nested(context, "otp.otp")
    if expected == 1:
        return [otp]
    if expected == 2:
        name = (
            _get_nested(context, "user.first_name")
            or _get_nested(context, "user.name")
            or _get_nested(context, "staff.name")
            or "there"
        )
        return [name, otp]
    name = (
        _get_nested(context, "user.first_name")
        or _get_nested(context, "user.name")
        or _get_nested(context, "staff.name")
        or "there"
    )
    leading = [name] * max(0, expected - 1)
    leading.append(otp)
    return leading[:expected]


def _count_body_params(components: list[dict[str, Any]]) -> int:
    for component in components:
        if not isinstance(component, dict):
            continue
        if str(component.get("type", "")).upper() == "BODY":
            text = str(component.get("text") or "")
            if "{{" in text:
                return text.count("{{")
    return 0


def _pick_template_entry(
    data: list[Any], *, preferred_language: str = "en"
) -> dict[str, Any] | None:
    if not data:
        return None
    pref = preferred_language.strip().lower()
    for item in data:
        if not isinstance(item, dict):
            continue
        lang = str(item.get("language") or "").strip().lower()
        if lang == pref:
            return item
    for item in data:
        if isinstance(item, dict):
            return item
    return None


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
    entry = _pick_template_entry(data, preferred_language="en") if isinstance(data, list) else None
    if entry is not None:
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
    if expected > 0:
        if len(slot_values) > expected:
            slot_values = slot_values[:expected]
        if expected > len(slot_values):
            if not slot_values and _is_otp_whatsapp_service(svc):
                slot_values = infer_otp_whatsapp_slot_values(expected, context)
            if expected > len(slot_values):
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message=(
                        f"WhatsApp template '{canonical_name}' expects {expected} body parameter(s) "
                        f"but notification service has {len(slot_values)} slot(s) configured"
                    ),
                )

    components: list[dict[str, Any]] = []
    header_link = resolve_header_link(svc.template_variables, context)
    if header_link and _header_is_document(components_meta):
        filename = resolve_header_filename(svc.template_variables, context) or "document.pdf"
        components.append(
            {
                "type": "header",
                "parameters": [
                    {
                        "type": "document",
                        "document": {"link": header_link, "filename": filename},
                    }
                ],
            }
        )

    if slot_values:
        body_params: list[dict[str, str]] = []
        for index, value in enumerate(slot_values, start=1):
            text = str(value or "").strip()
            if not text:
                raise AppError(
                    status_code=400,
                    error_code="INVALID_INPUT",
                    message=(
                        f"WhatsApp template body slot {index} is empty for template "
                        f"'{canonical_name}' — check service template_variables mapping"
                    ),
                )
            body_params.append({"type": "text", "text": text})
        components.append({"type": "body", "parameters": body_params})

    return canonical_name, components, language_code
