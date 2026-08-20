"""Slug helpers for human-readable labels."""

from __future__ import annotations

import re


_NON_ALNUM = re.compile(r"[^a-z0-9]+")
_MULTI_UNDERSCORE = re.compile(r"_+")


def slugify_department(name: str) -> str:
    """Convert a department display name to a stable slug.

    Examples:
        "Sales" -> "sales"
        "Sales & Marketing" -> "sales_marketing"
    """
    normalized = (name or "").strip().lower()
    if not normalized:
        return ""
    slug = _NON_ALNUM.sub("_", normalized)
    slug = _MULTI_UNDERSCORE.sub("_", slug).strip("_")
    return slug


def slugify_cabin_key(name: str) -> str:
    """Convert a cabin display name to a stable slug key (SlugKey format)."""
    return slugify_department(name)


def sanitize_cabin_key(value: str) -> str:
    """Normalize an existing cabin key to SlugKey format (e.g. btc-001 -> btc_001)."""
    normalized = (value or "").strip().lower()
    if not normalized:
        return ""
    slug = _NON_ALNUM.sub("_", normalized)
    slug = _MULTI_UNDERSCORE.sub("_", slug).strip("_")
    return slug


def sanitize_service_key(value: str) -> str:
    """Normalize a notification service key (allows hyphens)."""
    normalized = (value or "").strip().lower()
    if not normalized:
        return ""
    slug = re.sub(r"[^a-z0-9_-]+", "-", normalized)
    slug = re.sub(r"-+", "-", slug).strip("-_")
    slug = re.sub(r"_+", "_", slug)
    return slug


def derive_cabin_base_key(cabin_name: str, cabin_key: str) -> str:
    """Derive a slug base from cabin_name, falling back to cabin_key."""
    from_name = slugify_cabin_key(cabin_name)
    if from_name:
        return from_name
    return sanitize_cabin_key(cabin_key)


def migrate_slot_detail_cabin_keys(slot_detail: dict) -> tuple[dict, dict[str, str]]:
    """Normalize cabin keys in slot_detail JSON; return updated doc and old->new mapping."""
    if not isinstance(slot_detail, dict):
        return slot_detail, {}

    mapping: dict[str, str] = {}
    used: set[str] = set()
    entries: list[tuple[dict, str, str]] = []

    for section_name in ("blood_collection", "consultation"):
        section = slot_detail.get(section_name)
        if not isinstance(section, dict):
            continue
        for cabins in section.values():
            if not isinstance(cabins, list):
                continue
            for cabin in cabins:
                if not isinstance(cabin, dict):
                    continue
                old_key = (cabin.get("cabin_key") or "").strip()
                cabin_name = (cabin.get("cabin_name") or "").strip()
                entries.append((cabin, old_key, cabin_name))

    for cabin, old_key, cabin_name in entries:
        base = derive_cabin_base_key(cabin_name, old_key)
        if not base:
            if old_key:
                used.add(sanitize_cabin_key(old_key) or old_key)
            continue

        candidate = base
        suffix = 2
        while candidate in used:
            candidate = f"{base}_{suffix}"
            suffix += 1
        used.add(candidate)
        cabin["cabin_key"] = candidate
        if old_key and old_key != candidate:
            mapping[old_key] = candidate

    return slot_detail, mapping


def collect_cabin_keys_from_slot_detail(slot_detail: dict | None) -> set[str]:
    """Return all cabin_key values defined in a slot_detail document."""
    keys: set[str] = set()
    if not isinstance(slot_detail, dict):
        return keys
    for section_name in ("blood_collection", "consultation"):
        section = slot_detail.get(section_name)
        if not isinstance(section, dict):
            continue
        for cabins in section.values():
            if not isinstance(cabins, list):
                continue
            for cabin in cabins:
                if not isinstance(cabin, dict):
                    continue
                key = sanitize_cabin_key(str(cabin.get("cabin_key") or ""))
                if key:
                    keys.add(key)
    return keys
