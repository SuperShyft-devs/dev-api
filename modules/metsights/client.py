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

    async def options_record_resource(self, *, record_id: str, resource: str) -> dict[str, Any]:
        base_url = settings.METSIGHTS_BASE_URL.rstrip("/")
        url = f"{base_url}/records/{record_id}/{resource.strip('/')}/"
        headers = {"X-API-KEY": settings.METSIGHTS_API_KEY}
        timeout = settings.METSIGHTS_TIMEOUT_SECONDS

        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.request("OPTIONS", url, headers=headers)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                return {"detail": "Unexpected response", "data": None}
            return payload

    async def get_report(self, *, record_id: str, assessment_type_code: str | None) -> dict[str, Any]:
        base_url = settings.METSIGHTS_BASE_URL.rstrip("/")
        type_code = (assessment_type_code or "").strip()
        if type_code == "7":
            url = f"{base_url}/reports/fitness-reports/{record_id}/"
        else:
            url = f"{base_url}/reports/{record_id}/"
        headers = {"X-API-KEY": settings.METSIGHTS_API_KEY}
        timeout = settings.METSIGHTS_TIMEOUT_SECONDS

        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                return {"detail": "Unexpected response", "data": None}
            return payload

    async def get_report_pdf(self, *, record_id: str, assessment_type_code: str | None) -> dict[str, Any]:
        base_url = settings.METSIGHTS_BASE_URL.rstrip("/")
        type_code = (assessment_type_code or "").strip()
        if type_code == "7":
            url = f"{base_url}/reports/fitness-reports/{record_id}/pdf/"
        else:
            url = f"{base_url}/reports/{record_id}/pdf/"
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

    async def list_profile_records(
        self,
        *,
        profile_id: str,
        completed: str | None = None,
        code: str | None = None,
        search: str | None = None,
    ) -> dict[str, Any]:
        base_url = settings.METSIGHTS_BASE_URL.rstrip("/")
        safe_pid = (profile_id or "").strip().strip("/")
        url = f"{base_url}/profiles/{safe_pid}/records/"
        headers = {"X-API-KEY": settings.METSIGHTS_API_KEY}
        timeout = settings.METSIGHTS_TIMEOUT_SECONDS
        params: dict[str, Any] = {}
        if completed is not None and str(completed).strip() != "":
            params["completed"] = str(completed).strip()
        if code is not None and str(code).strip() != "":
            params["code"] = str(code).strip()
        if search is not None and str(search).strip() != "":
            params["search"] = str(search).strip()

        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(url, headers=headers, params=params or None)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                return {"detail": "Unexpected response", "data": None}
            return payload

    async def get_record_detail(self, *, record_id: str) -> dict[str, Any]:
        base_url = settings.METSIGHTS_BASE_URL.rstrip("/")
        safe_rid = (record_id or "").strip().strip("/")
        url = f"{base_url}/records/{safe_rid}/"
        headers = {"X-API-KEY": settings.METSIGHTS_API_KEY}
        timeout = settings.METSIGHTS_TIMEOUT_SECONDS

        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                return {"detail": "Unexpected response", "data": None}
            return payload
