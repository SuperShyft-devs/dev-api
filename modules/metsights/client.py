"""HTTP client for Metsights Records APIs."""

from __future__ import annotations

from typing import Any

import httpx

from core.config import settings


class MetsightsClient:
    """Thin HTTP client for Metsights records resources."""

    async def get_record_resource(self, *, record_id: str, resource: str) -> dict[str, Any]:
        base_url = settings.METSIGHTS_BASE_URL.rstrip("/")
        url = f"{base_url}/records/{record_id}/{resource.strip('/')}/"
        headers = {"X-API-KEY": settings.METSIGHTS_API_KEY}
        timeout = settings.METSIGHTS_TIMEOUT_SECONDS

        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                return {"detail": "Unexpected response", "data": None}
            return payload
