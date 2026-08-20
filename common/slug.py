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
