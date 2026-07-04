"""Nominatim geocoding client."""

from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

NOMINATIM_SEARCH_URL = "https://nominatim.openstreetmap.org/search"
USER_AGENT = "SuperShyft"
DEFAULT_TIMEOUT_SECONDS = 8.0

LOCATION_FIELD_KEYS = (
    "address",
    "sub_locality",
    "landmark",
    "city",
    "pincode",
    "state",
    "country",
    "latitude",
    "longitude",
)


def _first_non_empty(*values: Any) -> str | None:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _parse_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def map_nominatim_result(item: dict[str, Any]) -> dict[str, Any]:
    """Map a Nominatim search result to engagement location fields."""
    address = item.get("address") if isinstance(item.get("address"), dict) else {}
    return {
        "display_name": _first_non_empty(item.get("display_name")),
        "address": _first_non_empty(item.get("display_name")),
        "sub_locality": _first_non_empty(address.get("suburb"), address.get("neighbourhood")),
        "landmark": _first_non_empty(item.get("name")),
        "city": _first_non_empty(
            address.get("city"),
            address.get("town"),
            address.get("village"),
        ),
        "pincode": _first_non_empty(address.get("postcode")),
        "state": _first_non_empty(address.get("state")),
        "country": _first_non_empty(address.get("country")),
        "latitude": _parse_float(item.get("lat")),
        "longitude": _parse_float(item.get("lon")),
    }


async def search_places(query: str, *, limit: int = 3) -> list[dict[str, Any]]:
    """Search Nominatim and return mapped place suggestions.

    Never raises — returns an empty list on failure.
    """
    q = (query or "").strip()
    if not q:
        return []

    limit = max(1, min(int(limit), 10))
    params = {
        "q": q,
        "format": "json",
        "addressdetails": 1,
        "limit": limit,
    }
    headers = {"User-Agent": USER_AGENT}

    try:
        async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT_SECONDS) as client:
            response = await client.get(NOMINATIM_SEARCH_URL, params=params, headers=headers)
            response.raise_for_status()
            payload = response.json()
    except Exception:
        logger.warning("Nominatim search failed for query=%r", q, exc_info=True)
        return []

    if not isinstance(payload, list):
        return []

    results: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        results.append(map_nominatim_result(item))
    return results


def _is_present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def location_fields_complete(
    *,
    address: str | None = None,
    sub_locality: str | None = None,
    landmark: str | None = None,
    city: str | None = None,
    pincode: str | None = None,
    state: str | None = None,
    country: str | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
) -> bool:
    return all(
        _is_present(value)
        for value in (
            address,
            sub_locality,
            landmark,
            city,
            pincode,
            state,
            country,
            latitude,
            longitude,
        )
    )


def merge_location_fields(
    existing: dict[str, Any],
    geocoded: dict[str, Any],
) -> dict[str, Any]:
    """Fill only missing location fields from geocoded data."""
    merged = dict(existing)
    for key in LOCATION_FIELD_KEYS:
        if not _is_present(merged.get(key)) and _is_present(geocoded.get(key)):
            merged[key] = geocoded[key]
    return merged


async def enrich_location_fields(
    *,
    address: str | None = None,
    sub_locality: str | None = None,
    landmark: str | None = None,
    city: str | None = None,
    pincode: str | None = None,
    state: str | None = None,
    country: str | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
) -> dict[str, Any]:
    """Return location fields, geocoding missing ones from address when needed."""
    fields = {
        "address": (address or "").strip() or None,
        "sub_locality": (sub_locality or "").strip() or None if isinstance(sub_locality, str) else sub_locality,
        "landmark": (landmark or "").strip() or None if isinstance(landmark, str) else landmark,
        "city": (city or "").strip() or None if isinstance(city, str) else city,
        "pincode": (pincode or "").strip() or None if isinstance(pincode, str) else pincode,
        "state": (state or "").strip() or None if isinstance(state, str) else state,
        "country": (country or "").strip() or None if isinstance(country, str) else country,
        "latitude": _parse_float(latitude),
        "longitude": _parse_float(longitude),
    }

    if location_fields_complete(**fields):
        return fields

    query_parts = [
        fields["address"],
        fields["sub_locality"],
        fields["landmark"],
        fields["city"],
        fields["pincode"],
        fields["state"],
        fields["country"],
    ]
    query = ", ".join(part for part in query_parts if part)
    if not query:
        return fields

    results = await search_places(query, limit=1)
    if not results:
        return fields

    # Prefer keeping the caller-provided address string over display_name.
    geocoded = dict(results[0])
    if fields["address"]:
        geocoded["address"] = fields["address"]

    return merge_location_fields(fields, geocoded)
