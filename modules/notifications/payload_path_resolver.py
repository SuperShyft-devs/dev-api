"""Resolve phone/email contacts from emit context using a configured JSON path."""

from __future__ import annotations

from typing import Any


def _split_name(name: str) -> tuple[str, str]:
    parts = name.strip().split(None, 1)
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[1]


def _contact_from_object(
    obj: dict[str, Any],
    *,
    value_type: str,
) -> dict[str, str] | None:
    if value_type == "phone":
        raw = str(obj.get("phone") or "").strip()
        if not raw:
            return None
        contact: dict[str, str] = {"phone": raw}
    elif value_type == "email":
        raw = str(obj.get("email") or "").strip().lower()
        if not raw:
            return None
        contact = {"email": raw}
    else:
        return None

    name = str(obj.get("name") or "").strip()
    if name:
        first, last = _split_name(name)
        if first:
            contact["first_name"] = first
        if last:
            contact["last_name"] = last
    elif obj.get("first_name"):
        contact["first_name"] = str(obj["first_name"]).strip()
        if obj.get("last_name"):
            contact["last_name"] = str(obj["last_name"]).strip()
    return contact


def _walk_segments(context: dict[str, Any], segments: list[str]) -> list[tuple[Any, dict[str, Any] | None]]:
    """Return (leaf value, parent dict for scalar leaves) pairs."""
    current: list[Any] = [context]
    parents: list[dict[str, Any] | None] = [None]
    for idx, segment in enumerate(segments):
        is_array = segment.endswith("[]")
        key = segment[:-2] if is_array else segment
        is_last = idx == len(segments) - 1
        next_level: list[Any] = []
        next_parents: list[dict[str, Any] | None] = []
        for node, parent in zip(current, parents, strict=False):
            if not isinstance(node, dict):
                continue
            value = node.get(key)
            if is_array:
                if isinstance(value, list):
                    for item in value:
                        next_level.append(item)
                        next_parents.append(None)
                continue
            if value is None:
                continue
            if is_last and not isinstance(value, dict):
                next_level.append(value)
                next_parents.append(node)
            else:
                next_level.append(value)
                next_parents.append(parent if not is_last else None)
        current = next_level
        parents = next_parents
        if not current:
            break
    return list(zip(current, parents, strict=False))


def resolve_payload_contacts(
    context: dict[str, Any],
    *,
    payload_path: str,
    value_type: str,
) -> list[dict[str, str]]:
    path = (payload_path or "").strip()
    if not path or not isinstance(context, dict):
        return []

    segments = [s for s in path.split(".") if s]
    if not segments:
        return []

    finals = _walk_segments(context, segments)
    contacts: list[dict[str, str]] = []
    seen: set[str] = set()

    for item, parent in finals:
        if isinstance(item, dict):
            contact = _contact_from_object(item, value_type=value_type)
        elif value_type == "phone":
            raw = str(item or "").strip()
            contact = {"phone": raw} if raw else None
            if contact and isinstance(parent, dict):
                enriched = _contact_from_object(parent, value_type=value_type)
                if enriched:
                    for k, v in enriched.items():
                        if k != "phone" and k not in contact:
                            contact[k] = v
        elif value_type == "email":
            raw = str(item or "").strip().lower()
            contact = {"email": raw} if raw else None
            if contact and isinstance(parent, dict):
                enriched = _contact_from_object(parent, value_type=value_type)
                if enriched:
                    for k, v in enriched.items():
                        if k != "email" and k not in contact:
                            contact[k] = v
        else:
            contact = None

        if contact is None:
            continue
        dedupe_key = contact.get("phone") or contact.get("email") or ""
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        contacts.append(contact)

    return contacts
