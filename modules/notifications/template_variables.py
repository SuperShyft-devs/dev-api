"""Resolve template_variables slots into positional WhatsApp values."""

from __future__ import annotations

import re
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
        raw = context.get("otp")
        if isinstance(raw, dict):
            return str(raw.get("otp") or "").strip()
        return str(raw or "").strip()

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


def _resolve_member_ref(ref: str, context: dict[str, Any]) -> str:
    """Map n8n-era `member.*` refs onto dispatch context."""
    key = ref.strip()
    if key.startswith("member."):
        field = key.removeprefix("member.")
        aliases = {
            "first_name": "user.first_name",
            "last_name": "user.last_name",
            "phone": "user.phone",
            "email": "user.email",
            "otp": "otp",
        }
        path = aliases.get(field, field)
        return _get_nested(context, path)
    return _get_nested(context, key)


def _resolve_legacy_component_slots(
    template_variables: list[Any],
    context: dict[str, Any],
) -> list[str]:
    """Legacy DB rows store Meta-style component lists with parameter refs."""
    values: list[str] = []
    for item in template_variables:
        if not isinstance(item, dict):
            continue
        params = item.get("parameters") or []
        if not isinstance(params, list):
            continue
        for param in params:
            if not isinstance(param, dict):
                continue
            ref = str(param.get("ref") or param.get("source") or "").strip()
            if ref:
                values.append(_resolve_member_ref(ref, context))
    return values


def resolve_slots(
    template_variables: dict[str, Any] | list[Any] | None,
    context: dict[str, Any],
) -> list[str]:
    if isinstance(template_variables, list):
        return _resolve_legacy_component_slots(template_variables, context)
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


def _expand_email_placeholder(path: str) -> str:
    key = path.strip()
    if key == "first_name":
        return "user.first_name"
    if key == "email":
        return "user.email"
    return key


def render_otp_digit_boxes_html(context: dict[str, Any]) -> str:
    """Six high-contrast digit cells for OTP emails (email client friendly)."""
    otp = str(_get_nested(context, "otp.otp") or "").strip()
    digits = [c for c in otp if c.isdigit()][:6]
    if not digits:
        return (
            '<span style="font-size:32px;font-weight:700;color:#ffffff;'
            'letter-spacing:6px;font-family:Consolas,Monaco,monospace;">'
            f"{otp}</span>"
        )
    cell = (
        'display:inline-block;min-width:40px;max-width:48px;padding:14px 6px;'
        "margin:0 3px;background-color:rgba(255,255,255,0.14);border-radius:10px;"
        "font-size:28px;font-weight:700;color:#ffffff;line-height:1;"
        'font-family:Consolas,Monaco,"Courier New",monospace;text-align:center;'
        "vertical-align:middle;"
    )
    return "".join(f'<span style="{cell}">{d}</span>' for d in digits)


def interpolate_email_template(text: str, context: dict[str, Any]) -> str:
    if not text:
        return text

    def replacer(match: re.Match[str]) -> str:
        raw = match.group(1).strip()
        if raw == "otp.digit_boxes":
            return render_otp_digit_boxes_html(context)
        inner = _expand_email_placeholder(raw)
        return _get_nested(context, inner)

    return re.sub(r"\{\{([^}]+)\}\}", replacer, text)
