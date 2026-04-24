"""HTTP client for Metsights APIs."""

from __future__ import annotations

import re
from typing import Any

import httpx

from core.config import settings

_SAFE_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")

_ALLOWED_RESOURCES = frozenset({
    "anthropometrics",
    "assessments",
    "blood-biomarkers",
    "blood-pressure",
    "body-composition",
    "cardiac-health",
    "cardio-metabolic-risk",
    "clinical-chemistry",
    "complete-blood-count",
    "diabetes",
    "diet-plan",
    "endocrinology",
    "exercise-plan",
    "fitness-assessment",
    "glucose-tolerance",
    "haematology",
    "health-risk",
    "hematology",
    "hepatic",
    "immunology",
    "kidney",
    "lifestyle",
    "lipid-profile",
    "liver-function",
    "metabolic-health",
    "musculo-skeletal",
    "nutrition",
    "obesity",
    "overall-health",
    "physical-activity",
    "pulmonary-function",
    "questionnaire",
    "renal-function",
    "serology",
    "sleep",
    "stress",
    "thyroid",
    "thyroid-function",
    "urinalysis",
    "vitals",
    "well-being",
})


def _validate_record_id(record_id: str) -> str:
    rid = (record_id or "").strip().strip("/")
    if not rid or not _SAFE_ID_PATTERN.match(rid):
        raise ValueError(f"Invalid record_id: {record_id!r}")
    return rid


def _validate_resource(resource: str) -> str:
    res = (resource or "").strip().strip("/")
    if not res or not _SAFE_ID_PATTERN.match(res):
        raise ValueError(f"Invalid resource: {resource!r}")
    return res


class MetsightsClient:
    """Thin HTTP client for Metsights resources."""

    async def get_record_resource(self, *, record_id: str, resource: str) -> dict[str, Any]:
        rid = _validate_record_id(record_id)
        res = _validate_resource(resource)
        base_url = settings.METSIGHTS_BASE_URL.rstrip("/")
        url = f"{base_url}/records/{rid}/{res}/"
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
        rid = _validate_record_id(record_id)
        res = _validate_resource(resource)
        base_url = settings.METSIGHTS_BASE_URL.rstrip("/")
        url = f"{base_url}/records/{rid}/{res}/"
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
        rid = _validate_record_id(record_id)
        base_url = settings.METSIGHTS_BASE_URL.rstrip("/")
        type_code = (assessment_type_code or "").strip()
        if type_code == "7":
            url = f"{base_url}/reports/fitness-reports/{rid}/"
        else:
            url = f"{base_url}/reports/{rid}/"
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
        rid = _validate_record_id(record_id)
        base_url = settings.METSIGHTS_BASE_URL.rstrip("/")
        type_code = (assessment_type_code or "").strip()
        if type_code == "7":
            url = f"{base_url}/reports/fitness-reports/{rid}/pdf/"
        else:
            url = f"{base_url}/reports/{rid}/pdf/"
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
        pid = _validate_record_id(profile_id)
        base_url = settings.METSIGHTS_BASE_URL.rstrip("/")
        url = f"{base_url}/profiles/{pid}/records/"
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
        pid = _validate_record_id(profile_id)
        base_url = settings.METSIGHTS_BASE_URL.rstrip("/")
        url = f"{base_url}/profiles/{pid}/records/"
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

    async def patch_record_resource(self, *, record_id: str, resource: str, data: dict[str, Any]) -> dict[str, Any]:
        rid = _validate_record_id(record_id)
        res = _validate_resource(resource)
        base_url = settings.METSIGHTS_BASE_URL.rstrip("/")
        url = f"{base_url}/records/{rid}/{res}/"
        headers = {"X-API-KEY": settings.METSIGHTS_API_KEY}
        timeout = settings.METSIGHTS_TIMEOUT_SECONDS

        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.patch(url, headers=headers, json=data)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                return {"detail": "Unexpected response", "data": None}
            return payload

    async def post_record_resource(self, *, record_id: str, resource: str, data: dict[str, Any]) -> dict[str, Any]:
        """POST creates a sub-resource (first questionnaire submission per Metsights Records API)."""

        rid = _validate_record_id(record_id)
        res = _validate_resource(resource)
        base_url = settings.METSIGHTS_BASE_URL.rstrip("/")
        url = f"{base_url}/records/{rid}/{res}/"
        headers = {"X-API-KEY": settings.METSIGHTS_API_KEY}
        timeout = settings.METSIGHTS_TIMEOUT_SECONDS

        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(url, headers=headers, json=data)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                return {"detail": "Unexpected response", "data": None}
            return payload

    async def get_record_detail(self, *, record_id: str) -> dict[str, Any]:
        rid = _validate_record_id(record_id)
        base_url = settings.METSIGHTS_BASE_URL.rstrip("/")
        url = f"{base_url}/records/{rid}/"
        headers = {"X-API-KEY": settings.METSIGHTS_API_KEY}
        timeout = settings.METSIGHTS_TIMEOUT_SECONDS

        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                return {"detail": "Unexpected response", "data": None}
            return payload
