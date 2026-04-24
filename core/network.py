"""Network helpers shared across routers."""

from __future__ import annotations

from fastapi import Request

from core.config import settings


def get_client_ip(request: Request) -> str:
    """Return the most trustworthy client IP.

    X-Forwarded-For is only trusted when the TCP peer (request.client.host)
    is in the TRUSTED_PROXIES list.  Otherwise the raw TCP peer is returned.
    """
    peer = request.client.host if request.client else "unknown"

    if not settings.TRUSTED_PROXIES:
        return peer

    if peer not in settings.TRUSTED_PROXIES:
        return peer

    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()

    return peer
