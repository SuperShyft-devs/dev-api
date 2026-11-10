"""Resolve template_variables slots into positional WhatsApp values."""

from __future__ import annotations

from typing import Any


def _get_nested(context: dict[str, Any], path: str) -> str:
    if path == "participant_details":
        raw = context.get("participant_details")
        if isinstance(raw, dict):
            return ""
        return str(raw or "")
    if path == "external_link":
        return str(context.get("external_link") or "")
    if path == "otp":
        return str(context.get("otp") or "")

    parts = path.split(".")
    node: Any = context
    for part in parts:
        if not isinstance(node, dict):
            return ""
        node = node.get(part)
    if node is None:
        return ""
    return str(node).strip()


def resolve_slot(slot: dict[str, Any], context: dict[str, Any]) -> str:
    source = str(slot.get("source") or "").strip()
    if source == "literal":
        return str(slot.get("value") or "")
    if source.startswith("expression:"):
        return source.removeprefix("expression:")
    return _get_nested(context, source)


def resolve_slots(
    template_variables: dict[str, Any] | None,
    context: dict[str, Any],
) -> list[str]:
    if not isinstance(template_variables, dict):
        return []
    slots = template_variables.get("slots") or []
    if not isinstance(slots, list):
        return []
    ordered = sorted(
        (s for s in slots if isinstance(s, dict)),
        key=lambda s: int(s.get("index") or 0),
    )
    return [resolve_slot(slot, context) for slot in ordered]


def resolve_header_link(
    template_variables: dict[str, Any] | None,
    context: dict[str, Any],
) -> str | None:
    if not isinstance(template_variables, dict):
        return None
    header = template_variables.get("header")
    if isinstance(header, dict):
        source = str(header.get("source") or "").strip()
        if source:
            value = _get_nested(context, source)
            if value:
                return value
    attachments = template_variables.get("attachments")
    if isinstance(attachments, dict):
        for key in ("pdflink", "medialink"):
            ref = attachments.get(key)
            if isinstance(ref, str) and "{{" in ref:
                source = ref.replace("{{", "").replace("}}", "").strip()
                value = _get_nested(context, source) if "." in source else str(context.get(source) or "")
                if value:
                    return value
    return None


def build_legacy_message_csv(
    template_name: str,
    resolved_slots: list[str],
) -> str:
    parts = [template_name.strip(), *resolved_slots]
    return ",".join(parts)


def interpolate_email_template(text: str, context: dict[str, Any]) -> str:
    user = context.get("user") if isinstance(context.get("user"), dict) else {}
    replacements = {
        "{{first_name}}": str(user.get("first_name") or ""),
        "{{otp}}": str(context.get("otp") or ""),
        "{{email}}": str(user.get("email") or ""),
    }
    out = text
    for key, value in replacements.items():
        out = out.replace(key, value)
    return out
