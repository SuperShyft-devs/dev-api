"""HMAC-SHA256 checksum generation for Healthians booking API."""

from __future__ import annotations

import hashlib
import hmac


def generate_checksum(data: str, key: str) -> str:
    """Generate a checksum using HMAC-SHA256."""
    hmac_obj = hmac.new(key.encode(), data.encode(), hashlib.sha256)
    return hmac_obj.hexdigest()
