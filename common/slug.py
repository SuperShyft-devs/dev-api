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
