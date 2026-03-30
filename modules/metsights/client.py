"""HTTP client for Metsights APIs."""

from __future__ import annotations

from typing import Any

import httpx

from core.config import settings


class MetsightsClient:
    """Thin HTTP client for Metsights resources."""

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

    async def create_profile(self, *, data: dict[str, Any]) -> dict[str, Any]:
        base_url = settings.METSIGHTS_BASE_URL.rstrip("/")
        url = f"{base_url}/profiles/"
        headers = {"X-API-KEY": settings.METSIGHTS_API_KEY}
        timeout = settings.METSIGHTS_TIMEOUT_SECONDS

        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(url, headers=headers, json=data)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                return {"detail": "Unexpected response", "data": None}
            return payload

    async def list_profiles(self, *, search: str | None) -> dict[str, Any]:
        base_url = settings.METSIGHTS_BASE_URL.rstrip("/")
        url = f"{base_url}/profiles/"
        headers = {"X-API-KEY": settings.METSIGHTS_API_KEY}
        timeout = settings.METSIGHTS_TIMEOUT_SECONDS

        params: dict[str, Any] = {}
        if search is not None and str(search).strip() != "":
            params["search"] = str(search).strip()

        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(url, headers=headers, params=params)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                return {"detail": "Unexpected response", "data": None}
            return payload

    async def create_profile_record(self, *, profile_id: str, data: dict[str, Any]) -> dict[str, Any]:
        base_url = settings.METSIGHTS_BASE_URL.rstrip("/")
        url = f"{base_url}/profiles/{profile_id}/records/"
        headers = {"X-API-KEY": settings.METSIGHTS_API_KEY}
        timeout = settings.METSIGHTS_TIMEOUT_SECONDS

        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(url, headers=headers, json=data)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                return {"detail": "Unexpected response", "data": None}
            return payload
