"""Truncate content lists to configured report limits."""

from __future__ import annotations

from collections.abc import Sequence


def limit_items(items: Sequence[str] | None, limit: int) -> list[str]:
    """Return the first ``limit`` non-empty string items (deterministic order)."""
    if not items or limit <= 0:
        return []
    result: list[str] = []
    for raw in items:
        if not isinstance(raw, str):
            continue
        text = raw.strip()
        if not text:
            continue
        result.append(text)
        if len(result) >= limit:
            break
    return result
