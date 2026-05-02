"""HTTP client for the Healthians Bridge API."""

from __future__ import annotations

import httpx

from core.config import settings


async def get_access_token() -> str:
    """Authenticate with Healthians and return an access token."""
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            f"{settings.HEALTHIANS_BASE_URL}/toast4health/getAccessToken",
            auth=(settings.HEALTHIANS_API_KEY, settings.HEALTHIANS_SECRET_KEY),
        )
        resp.raise_for_status()
        data = resp.json()
    if not data.get("success"):
        raise RuntimeError(f"Healthians auth failed: {data}")
    return data["access_token"]


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
