"""Deduplicate actionable insights across disease sections."""

from __future__ import annotations

import re


_WHITESPACE_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[^\w\s]", re.UNICODE)


def normalize_insight(text: str) -> str:
    """Normalize insight text for equality comparison."""
    cleaned = text.strip().lower()
    cleaned = _PUNCT_RE.sub(" ", cleaned)
    cleaned = _WHITESPACE_RE.sub(" ", cleaned).strip()
    return cleaned


def dedupe_insights(insights: list[str], *, limit: int | None = None) -> list[str]:
    """Return insights in order, dropping near-duplicates (case/punctuation-insensitive)."""
    seen: set[str] = set()
    result: list[str] = []
    for raw in insights:
        if not isinstance(raw, str):
            continue
        text = raw.strip()
        if not text:
            continue
        key = normalize_insight(text)
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(text)
        if limit is not None and len(result) >= limit:
            break
    return result
