"""Shared API response helpers.

All success responses must follow:
{
  "data": {},
  "meta": {}
}
"""

from __future__ import annotations

from typing import Any, Dict, Optional


def success_response(data: Any, meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Return a standardized success response."""
    return {
        "data": data,
        "meta": meta or {},
    }
