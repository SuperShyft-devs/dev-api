"""Normalize WhatAPI message template metadata for admin UI."""

from __future__ import annotations

import re
from typing import Any

from modules.notifications.whatapi_client import WhatApiClient, get_whatapi_client
from modules.notifications.whatapi_sync_logging import log_whatapi_get_message_templates

_PLACEHOLDER = re.compile(r"\{\{(\d+)\}\}")


def _count_body_positional_params(components: list[dict[str, Any]]) -> int:
    max_index = 0
    for comp in components:
        if str(comp.get("type") or "").upper() != "BODY":
            continue
        text = str(comp.get("text") or "")
        for match in _PLACEHOLDER.findall(text):
            max_index = max(max_index, int(match))
    return max_index


def _header_format(components: list[dict[str, Any]]) -> str | None:
    for comp in components:
        if str(comp.get("type") or "").upper() != "HEADER":
            continue
        fmt = str(comp.get("format") or "").strip().upper()
        if fmt:
            return fmt
        example = comp.get("example")
        if isinstance(example, dict):
            handles = example.get("header_handle")
            if isinstance(handles, list) and handles:
                return "DOCUMENT"
            header_text = example.get("header_text")
            if isinstance(header_text, list) and header_text:
                return "TEXT"
        return "DOCUMENT"
    return None


def template_requires_header_media(components: list[dict[str, Any]], header_format: str | None) -> bool:
    fmt = (header_format or "").upper()
    if fmt in {"DOCUMENT", "IMAGE", "VIDEO"}:
        return True
    for comp in components:
        if str(comp.get("type") or "").upper() != "HEADER":
            continue
        comp_fmt = str(comp.get("format") or "").strip().upper()
        if comp_fmt in {"DOCUMENT", "IMAGE", "VIDEO"}:
            return True
        example = comp.get("example")
        if isinstance(example, dict) and example.get("header_handle"):
            return True
    return False


def _component_types(components: list[dict[str, Any]]) -> list[str]:
    types: list[str] = []
    for comp in components:
        t = str(comp.get("type") or "").strip()
        if t and t not in types:
            types.append(t)
    return types


def normalize_template_response(raw: dict[str, Any]) -> dict[str, Any] | None:
    data = raw.get("data")
    if not isinstance(data, list) or not data:
        return None
    item = data[0]
    if not isinstance(item, dict):
        return None
    components = item.get("components") if isinstance(item.get("components"), list) else []
    body_text = ""
    for comp in components:
        if str(comp.get("type") or "").upper() == "BODY":
            body_text = str(comp.get("text") or "")
            break
    header_format = _header_format(components)
    return {
        "name": str(item.get("name") or ""),
        "language": str(item.get("language") or ""),
        "category": str(item.get("category") or ""),
        "status": str(item.get("status") or ""),
        "parameter_format": str(item.get("parameter_format") or ""),
        "body_positional_count": _count_body_positional_params(components),
        "header_format": header_format,
        "requires_header_document": template_requires_header_media(components, header_format),
        "component_types": _component_types(components),
        "body_text": body_text,
    }


class WhatsappTemplateProxy:
    def __init__(self, client: WhatApiClient | None = None) -> None:
        self._client = client or get_whatapi_client()

    async def lookup(self, *, template_name: str) -> dict[str, Any]:
        name = template_name.strip()
        raw = await log_whatapi_get_message_templates(
            template_name=name,
            engagement_id=None,
            user_id=None,
            notification_id=None,
            operation=lambda: self._client.fetch_message_template(template_name=name),
        )
        normalized = normalize_template_response(raw)
        if normalized is None:
            return {"found": False, "name": template_name.strip()}
        return {"found": True, **normalized}
