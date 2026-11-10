"""Normalize WhatAPI message template metadata for admin UI."""

from __future__ import annotations

import re
from typing import Any

from core.exceptions import AppError
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
    return {
        "name": str(item.get("name") or ""),
        "language": str(item.get("language") or ""),
        "category": str(item.get("category") or ""),
        "status": str(item.get("status") or ""),
        "parameter_format": str(item.get("parameter_format") or ""),
        "body_positional_count": _count_body_positional_params(components),
        "component_types": _component_types(components),
        "body_text": body_text,
    }


class WhatsappTemplateProxy:
    def __init__(self, client: WhatApiClient | None = None) -> None:
        self._client = client or get_whatapi_client()

    async def lookup(self, *, template_name: str) -> dict[str, Any]:
        name = template_name.strip()
        try:
            raw = await log_whatapi_get_message_templates(
                template_name=name,
                engagement_id=None,
                user_id=None,
                notification_id=None,
                operation=lambda: self._client.fetch_message_template(template_name=name),
            )
        except AppError as exc:
            if exc.error_code in ("WHATAPI_ERROR", "CONFIG_ERROR"):
                return {
                    "found": False,
                    "name": template_name.strip(),
                    "lookup_error": exc.message,
                }
            raise
        normalized = normalize_template_response(raw)
        if normalized is None:
            return {"found": False, "name": template_name.strip()}
        return {"found": True, **normalized}
