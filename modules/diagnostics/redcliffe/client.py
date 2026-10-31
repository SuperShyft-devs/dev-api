"""Redcliffe Labs Partner API HTTP client."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from core.config import settings

logger = logging.getLogger(__name__)


class RedcliffeApiError(Exception):
    """Redcliffe API returned an error response."""

    def __init__(self, message: str, *, status_code: int | None = None, body: Any = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body = body


def _base_url() -> str:
    return (settings.REDCLIFFE_BASE_URL or "").strip().rstrip("/")


def _timeout() -> float:
    return float(settings.REDCLIFFE_TIMEOUT_SECONDS or 15)


def _headers() -> dict[str, str]:
    return {"key": settings.REDCLIFFE_API_KEY or ""}


def search_area_api_url() -> str:
    return f"{_base_url()}/api/partner/v2/get-partner-location-2-eloc/"


def coordinates_api_url() -> str:
    return f"{_base_url()}/api/partner/v2/get-partner-loc-2-eloc/"


def serviceability_api_url() -> str:
    return f"{_base_url()}/api/center/v2/is-location-serviceable/"


def _coord(value: float) -> float:
    return round(float(value), 6)


def _failure_message(data: Any, fallback: str) -> str:
    if isinstance(data, dict):
        msg = data.get("message")
        if isinstance(msg, str) and msg.strip():
            return msg.strip()
        status = data.get("status")
        if isinstance(status, str) and status.strip():
            return status.strip()
    return fallback


async def search_area(*, place_query: str) -> dict[str, Any]:
    """GET area search → eloc suggestions."""
    url = search_area_api_url()
    params = {"place_query": place_query.strip()}
    async with httpx.AsyncClient(timeout=_timeout()) as client:
        resp = await client.get(url, params=params, headers=_headers())
    if resp.status_code == 401:
        raise RedcliffeApiError("Unauthorized", status_code=401, body=resp.text)
    try:
        data = resp.json()
    except Exception:
        resp.raise_for_status()
        raise RedcliffeApiError("Invalid JSON response", status_code=resp.status_code, body=resp.text)
    if resp.status_code >= 400:
        raise RedcliffeApiError(
            _failure_message(data, "Area search failed"),
            status_code=resp.status_code,
            body=data,
        )
    if isinstance(data, dict) and str(data.get("status", "")).lower() == "failure":
        raise RedcliffeApiError(
            _failure_message(data, "No record(s) found"),
            status_code=404,
            body=data,
        )
    return data if isinstance(data, dict) else {"data": data}


async def get_coordinates(*, eloc: str) -> dict[str, Any]:
    """GET eloc → latitude/longitude."""
    url = coordinates_api_url()
    params = {"eloc": eloc.strip()}
    async with httpx.AsyncClient(timeout=_timeout()) as client:
        resp = await client.get(url, params=params, headers=_headers())
    if resp.status_code == 401:
        raise RedcliffeApiError("Unauthorized", status_code=401, body=resp.text)
    try:
        data = resp.json()
    except Exception:
        resp.raise_for_status()
        raise RedcliffeApiError("Invalid JSON response", status_code=resp.status_code, body=resp.text)
    if resp.status_code >= 400:
        raise RedcliffeApiError(
            _failure_message(data, "Coordinate lookup failed"),
            status_code=resp.status_code,
            body=data,
        )
    if isinstance(data, dict) and str(data.get("status", "")).lower() == "failure":
        raise RedcliffeApiError(
            _failure_message(data, "eloc is not valid"),
            status_code=422,
            body=data,
        )
    lat = data.get("latitude") if isinstance(data, dict) else None
    lng = data.get("longitude") if isinstance(data, dict) else None
    if lat in ("", None) or lng in ("", None):
        raise RedcliffeApiError("Invalid eloc coordinates", status_code=422, body=data)
    return data


async def check_location_serviceable(*, latitude: float, longitude: float) -> dict[str, Any]:
    """GET lat/lng serviceability."""
    url = serviceability_api_url()
    params = {
        "latitude": str(_coord(latitude)),
        "longitude": str(_coord(longitude)),
    }
    async with httpx.AsyncClient(timeout=_timeout()) as client:
        resp = await client.get(url, params=params, headers=_headers())
    if resp.status_code == 401:
        raise RedcliffeApiError("Unauthorized", status_code=401, body=resp.text)
    try:
        data = resp.json()
    except Exception:
        resp.raise_for_status()
        raise RedcliffeApiError("Invalid JSON response", status_code=resp.status_code, body=resp.text)
    if resp.status_code >= 400:
        raise RedcliffeApiError(
            _failure_message(data, "Serviceability check failed"),
            status_code=resp.status_code,
            body=data,
        )
    return data if isinstance(data, dict) else {"data": data}


def is_serviceable_response(body: dict[str, Any]) -> bool:
    return str(body.get("status", "")).lower() == "success"
