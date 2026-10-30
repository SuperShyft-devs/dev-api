"""Static Healthians profile catalog for admin test group mapping."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

_PROFILES_PATH = Path(__file__).resolve().parent / "profiles.json"


@lru_cache(maxsize=1)
def load_profiles() -> list[dict[str, str]]:
    with _PROFILES_PATH.open(encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, list):
        raise ValueError("profiles.json must be a JSON array")
    return data


def search_profiles(query: str | None, *, limit: int = 50) -> list[dict[str, str]]:
    items = load_profiles()
    q = (query or "").strip().lower()
    if not q:
        return items[:limit]
    matched: list[dict[str, str]] = []
    for item in items:
        name = str(item.get("name", ""))
        item_id = str(item.get("id", ""))
        if q in name.lower() or q in item_id:
            matched.append({"id": item_id, "name": name})
            if len(matched) >= limit:
                break
    return matched
