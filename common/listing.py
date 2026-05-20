"""Shared helpers for paginated admin list endpoints."""

from __future__ import annotations

from sqlalchemy import asc, desc
from sqlalchemy.sql import ColumnElement

SORT_ASC = "asc"
SORT_DESC = "desc"


def normalize_sort_dir(sort_dir: str | None) -> str:
    if (sort_dir or "").lower() == SORT_ASC:
        return SORT_ASC
    return SORT_DESC


def ilike_pattern(search: str) -> str:
    escaped = (
        search.strip()
        .replace("\\", "\\\\")
        .replace("%", "\\%")
        .replace("_", "\\_")
    )
    return f"%{escaped}%"


def apply_sort(
    query,
    *,
    sort_by: str | None,
    sort_dir: str | None,
    columns: dict[str, ColumnElement],
    default_column: ColumnElement,
):
    direction = asc if normalize_sort_dir(sort_dir) == SORT_ASC else desc
    key = (sort_by or "").strip()
    col = columns.get(key) if key else None
    if col is None:
        return query.order_by(direction(default_column))
    return query.order_by(direction(col), direction(default_column))
