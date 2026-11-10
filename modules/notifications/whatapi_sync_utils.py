"""WhatAPI template validation helpers for migration jobs."""

from __future__ import annotations

import logging

from modules.notifications.whatapi_client import WhatApiClient

logger = logging.getLogger(__name__)


async def validate_template_approved(
    client: WhatApiClient, *, template_name: str
) -> tuple[bool, str]:
    """Return (ok, note) after fetching template metadata from WhatAPI."""
    name = (template_name or "").strip()
    if not name:
        return False, "missing_template_name"

    try:
        body = await client.fetch_message_template(template_name=name)
    except Exception as exc:  # noqa: BLE001 — migration warnings only
        logger.warning("WhatAPI template fetch failed for %s: %s", name, exc)
        return False, f"fetch_failed:{exc}"

    data = body.get("data")
    if not isinstance(data, list) or not data:
        return False, "no_template_data"

    first = data[0]
    if not isinstance(first, dict):
        return False, "invalid_template_data"

    status = str(first.get("status") or "").upper()
    if status == "APPROVED":
        return True, "approved"
    return False, f"status_{status or 'unknown'}"
