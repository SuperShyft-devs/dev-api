"""HTTP client for the Healthians Bridge API."""

from __future__ import annotations

import json
import logging

import httpx

from core.config import settings
from modules.diagnostics.healthians.checksum import generate_checksum

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


async def get_booking_digital_value(access_token: str, booking_id: str) -> dict:
    """Fetch digital blood report values for a Healthians booking."""
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{settings.HEALTHIANS_BASE_URL}/toast4health/getBookingDigitalValue",
            headers={"Authorization": f"Bearer {access_token}"},
            json={"booking_id": str(booking_id)},
        )
        resp.raise_for_status()
        data = resp.json()
    if not data.get("status"):
        raise RuntimeError(f"Healthians getBookingDigitalValue failed: {data}")
    return data


async def get_booking_report(access_token: str, booking_id: str) -> dict:
    """Fetch signed PDF report URLs for a Healthians booking."""
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{settings.HEALTHIANS_BASE_URL}/toast4health/getBookingReport",
            headers={"Authorization": f"Bearer {access_token}"},
            json={"booking_id": str(booking_id)},
        )
        resp.raise_for_status()
        data = resp.json()
    if not data.get("status"):
        raise RuntimeError(f"Healthians getBookingReport failed: {data}")
    return data


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


async def check_serviceability_by_location_v2(
    access_token: str,
    *,
    lat: str,
    long: str,
    zipcode: str,
    is_ppmc_booking: int = 0,
) -> dict:
    """Check whether a location is serviceable for Healthians home collection."""
    payload = {
        "lat": lat,
        "long": long,
        "zipcode": zipcode,
        "is_ppmc_booking": is_ppmc_booking,
    }
    url = f"{settings.HEALTHIANS_BASE_URL}/toast4health/checkServiceabilityByLocation_v2"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            url,
            headers={"Authorization": f"Bearer {access_token}"},
            json=payload,
        )
        resp.raise_for_status()
        return resp.json()


async def get_slots_by_location(
    access_token: str,
    payload: dict,
) -> dict:
    """Fetch available slots by location from Healthians."""
    url = f"{settings.HEALTHIANS_BASE_URL}/toast4health/getSlotsByLocation"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            url,
            headers={"Authorization": f"Bearer {access_token}"},
            json=payload,
        )
        resp.raise_for_status()
        return resp.json()


async def freeze_slot_v1(
    access_token: str,
    *,
    slot_id: str,
    vendor_billing_user_id: str,
) -> dict:
    """Freeze (lock) a slot on Healthians."""
    payload = {
        "slot_id": slot_id,
        "vendor_billing_user_id": vendor_billing_user_id,
    }
    url = f"{settings.HEALTHIANS_BASE_URL}/toast4health/freezeSlot_v1"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            url,
            headers={"Authorization": f"Bearer {access_token}"},
            json=payload,
        )
        resp.raise_for_status()
        return resp.json()


async def create_booking_v3(
    access_token: str,
    payload: dict,
    *,
    checksum_key: str,
) -> dict:
    """Create a Healthians booking with HMAC checksum header."""
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    checksum = generate_checksum(body, checksum_key)
    url = f"{settings.HEALTHIANS_BASE_URL}/toast4health/createBooking_v3"
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "X-Checksum": checksum,
                "Content-Type": "application/json",
            },
            content=body.encode("utf-8"),
        )
        resp.raise_for_status()
        return resp.json()


async def cancel_booking(
    access_token: str,
    *,
    booking_id: str,
    vendor_billing_user_id: str,
    vendor_customer_id: str,
    remarks: str,
) -> dict:
    """Cancel a Healthians booking."""
    payload = {
        "booking_id": booking_id,
        "vendor_billing_user_id": vendor_billing_user_id,
        "vendor_customer_id": vendor_customer_id,
        "remarks": remarks,
    }
    url = f"{settings.HEALTHIANS_BASE_URL}/toast4health/cancelBooking"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            url,
            headers={"Authorization": f"Bearer {access_token}"},
            json=payload,
        )
        resp.raise_for_status()
        return resp.json()
