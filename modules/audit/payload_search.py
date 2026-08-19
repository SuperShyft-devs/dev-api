"""Parse partial / incomplete JSON for request-payload log search."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

_KEY_PATTERN = re.compile(r'"([A-Za-z_][A-Za-z0-9_]*)"\s*:')
_VALUE_PATTERN = re.compile(
    r'"([A-Za-z_][A-Za-z0-9_]*)"\s*:\s*'
    r"(-?\d+(?:\.\d+)?|true|false|null|\"[^\"]*\")"
    r"(?=\s*(?:,|\}|$))",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class PayloadSearchCriteria:
    """Criteria for matching integration log request payloads."""

    contains: dict[str, Any] | None = None
    keys: tuple[str, ...] = ()

    @property
    def is_active(self) -> bool:
        return bool(self.contains) or bool(self.keys)


def _coerce_json_value(raw: str) -> Any:
    value = raw.strip()
    lowered = value.lower()
    if lowered == "null":
        return None
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if value.startswith('"') and value.endswith('"'):
        return value[1:-1]
    if "." in value:
        return float(value)
    return int(value)


def _parse_loose_object(raw: str) -> PayloadSearchCriteria:
    contains: dict[str, Any] = {}
    valued_keys: set[str] = set()

    for match in _VALUE_PATTERN.finditer(raw):
        key = match.group(1)
        contains[key] = _coerce_json_value(match.group(2))
        valued_keys.add(key)

    keys = tuple(key for key in dict.fromkeys(_KEY_PATTERN.findall(raw)) if key not in valued_keys)
    return PayloadSearchCriteria(contains=contains or None, keys=keys)


def parse_payload_search(raw: str | None) -> PayloadSearchCriteria | None:
    """Parse admin payload search text into request-payload match criteria."""
    if raw is None:
        return None
    trimmed = raw.strip()
    if not trimmed:
        return None

    try:
        parsed = json.loads(trimmed)
    except json.JSONDecodeError:
        criteria = _parse_loose_object(trimmed)
        return criteria if criteria.is_active else None

    if not isinstance(parsed, dict):
        return None
    if not parsed:
        return None
    return PayloadSearchCriteria(contains=parsed, keys=())
