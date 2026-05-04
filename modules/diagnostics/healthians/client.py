"""HTTP client for the Healthians Bridge API."""

from __future__ import annotations

import logging

import httpx

from core.config import settings

logger = logging.getLogger(__name__)

_cached_token: str | None = None


async def get_access_token() -> str:
    """Authenticate with Healthians and return an access token."""
    global _cached_token

    url = f"{settings.HEALTHIANS_BASE_URL}/toast4health/getAccessToken"
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            url,
            auth=(settings.HEALTHIANS_API_KEY, settings.HEALTHIANS_SECRET_KEY),
        )
        if resp.status_code == 403:
            logger.error(
                "Healthians returned 403 Forbidden for %s – "
                "the server IP may be blocked by their CloudFront/WAF. "
                "Contact Healthians to whitelist this IP. "
                "Response body: %s",
                url,
                resp.text[:300],
            )
            raise RuntimeError(
                "Healthians blocked the request (403 Forbidden). "
                "The server IP is likely not whitelisted in their WAF/CDN. "
                "Contact Healthians to whitelist your server IP."
            )
        resp.raise_for_status()
        data = resp.json()
    if not data.get("success"):
        raise RuntimeError(f"Healthians auth failed: {data}")
    _cached_token = data["access_token"]
    return _cached_token


async def get_product_details(access_token: str, deal_type_id: int) -> dict:
    """Fetch product (package) details including constituents."""
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            f"{settings.HEALTHIANS_BASE_URL}/toast4health/getProductDetails",
            headers={"Authorization": f"Bearer {access_token}"},
            json={"deal_type": "package", "deal_type_id": deal_type_id},
        )
        resp.raise_for_status()
        data = resp.json()
    if not data.get("status"):
        raise RuntimeError(f"Healthians product details failed: {data}")
    return data["data"]
