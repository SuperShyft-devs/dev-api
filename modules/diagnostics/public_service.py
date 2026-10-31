"""Public diagnostic provider helpers (no engagement rows)."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.bookings.service import (
    _get_diagnostic_package,
    _get_healthians_token,
    _healthians_zone_id,
    _is_healthians,
    _slim_healthians_slot,
)
from modules.bookings import orange_health_flow
from modules.diagnostics.external_codes import healthians_package_deal_id
from modules.diagnostics.healthians import client as healthians_client
from modules.diagnostics.orange_health import client as orange_health_client
from modules.diagnostics.orange_health.booking_helpers import is_orange_health
from modules.diagnostics.orange_health.client import OrangeHealthApiError
from modules.diagnostics.redcliffe import service as redcliffe_service
from modules.diagnostics.redcliffe.client import RedcliffeApiError, is_serviceable_response
from modules.diagnostics.models import DiagnosticPackage
from modules.geocoding.client import enrich_location_fields
from modules.platform_settings.repository import PlatformSettingsRepository

logger = logging.getLogger(__name__)

_ALL_PUBLIC_PROVIDERS = ("healthians", "orange_health", "redcliffe")
_PINCODE_PUBLIC_PROVIDERS = ("healthians", "orange_health")


def _normalize_provider(provider: str | None) -> str | None:
    if provider is None:
        return None
    key = (provider or "").strip().lower()
    return key or None


def _provider_targets(provider: str | None) -> list[str]:
    prov = _normalize_provider(provider)
    if prov is not None:
        if prov not in _ALL_PUBLIC_PROVIDERS:
            raise AppError(status_code=422, error_code="INVALID_PROVIDER", message="Unsupported diagnostic provider")
        return [prov]
    return list(_ALL_PUBLIC_PROVIDERS)


def _pincode_provider_targets(provider: str | None) -> list[str]:
    prov = _normalize_provider(provider)
    if prov is not None:
        if prov not in _PINCODE_PUBLIC_PROVIDERS:
            raise AppError(status_code=422, error_code="INVALID_PROVIDER", message="Unsupported diagnostic provider")
        return [prov]
    return list(_PINCODE_PUBLIC_PROVIDERS)


async def _resolve_coordinates(
    db: AsyncSession,
    *,
    address_line: str,
    landmark: str | None,
    city: str,
    pincode: str,
    latitude: float | None,
    longitude: float | None,
) -> tuple[float, float]:
    lat, lng = latitude, longitude
    if lat is None or lng is None:
        primary = await PlatformSettingsRepository().resolve_geocoding_provider(db)
        location = await enrich_location_fields(
            address=address_line,
            landmark=landmark,
            city=city,
            pincode=pincode,
            latitude=latitude,
            longitude=longitude,
            primary=primary,
        )
        lat = location.get("latitude")
        lng = location.get("longitude")
    if lat is None or lng is None:
        raise AppError(status_code=422, error_code="INVALID_INPUT", message="Could not resolve location coordinates")
    return float(lat), float(lng)


def _package_mismatch_message(pkg: DiagnosticPackage, expected: str) -> str:
    actual = (pkg.diagnostic_provider or "").strip().lower()
    return f"Package is not a {expected} package (provider={actual})"


async def _check_orange_health_serviceability(
    db: AsyncSession,
    *,
    latitude: float,
    longitude: float,
    pkg: DiagnosticPackage | None,
    single_provider: bool,
) -> dict[str, Any]:
    if pkg is not None and not is_orange_health(pkg):
        message = _package_mismatch_message(pkg, "orange_health")
        if single_provider:
            raise AppError(status_code=422, error_code="INVALID_PROVIDER", message=message)
        return {
            "provider": "orange_health",
            "serviceable": False,
            "message": message,
            "data": None,
        }
    try:
        result = await orange_health_flow.check_serviceability_at_location(
            db,
            latitude=latitude,
            longitude=longitude,
            engagement_id=None,
        )
        return {
            "provider": "orange_health",
            "serviceable": result.get("status") == "success",
            "message": result.get("message"),
            "data": result,
        }
    except Exception as exc:
        logger.exception("Public Orange Health serviceability failed")
        return {
            "provider": "orange_health",
            "serviceable": False,
            "message": str(exc),
            "data": None,
        }


async def _check_redcliffe_serviceability(
    *,
    latitude: float,
    longitude: float,
    pkg: DiagnosticPackage | None,
    single_provider: bool,
) -> dict[str, Any]:
    if pkg is not None:
        actual = (pkg.diagnostic_provider or "").strip().lower()
        if actual != "redcliffe":
            message = _package_mismatch_message(pkg, "redcliffe")
            if single_provider:
                raise AppError(status_code=422, error_code="INVALID_PROVIDER", message=message)
            return {
                "provider": "redcliffe",
                "serviceable": False,
                "message": message,
                "data": None,
            }
    try:
        resp = await redcliffe_service.check_location_serviceable(
            latitude=latitude,
            longitude=longitude,
        )
        serviceable = is_serviceable_response(resp)
        message = resp.get("message") if isinstance(resp.get("message"), str) else None
        return {
            "provider": "redcliffe",
            "serviceable": serviceable,
            "message": message or (
                "Location is serviceable" if serviceable else "Location is non-serviceable"
            ),
            "latitude": latitude,
            "longitude": longitude,
            "data": resp,
        }
    except RedcliffeApiError as exc:
        body = exc.body if isinstance(exc.body, dict) else None
        return {
            "provider": "redcliffe",
            "serviceable": False,
            "message": str(exc),
            "latitude": latitude,
            "longitude": longitude,
            "data": body,
        }
    except Exception as exc:
        logger.exception("Public Redcliffe serviceability failed")
        return {
            "provider": "redcliffe",
            "serviceable": False,
            "message": str(exc),
            "latitude": latitude,
            "longitude": longitude,
            "data": None,
        }


async def _check_healthians_serviceability(
    db: AsyncSession,
    *,
    latitude: float,
    longitude: float,
    pincode: str,
    pkg: DiagnosticPackage | None,
    single_provider: bool,
) -> dict[str, Any]:
    if pkg is not None and not _is_healthians(pkg):
        message = _package_mismatch_message(pkg, "healthians")
        if single_provider:
            raise AppError(status_code=422, error_code="INVALID_PROVIDER", message=message)
        return {
            "provider": "healthians",
            "serviceable": False,
            "message": message,
            "zone_id": None,
            "latitude": latitude,
            "longitude": longitude,
            "data": None,
        }
    access_token = await _get_healthians_token()
    try:
        resp = await healthians_client.check_serviceability_by_location_v2(
            access_token,
            lat=str(latitude),
            long=str(longitude),
            zipcode=pincode,
        )
    except Exception as exc:
        logger.exception("Public Healthians serviceability failed")
        return {
            "provider": "healthians",
            "serviceable": False,
            "message": str(exc),
            "zone_id": None,
            "latitude": latitude,
            "longitude": longitude,
            "data": None,
        }

    zone_id = _healthians_zone_id(resp)
    return {
        "provider": "healthians",
        "serviceable": bool(resp.get("status")),
        "message": resp.get("message"),
        "zone_id": str(zone_id) if zone_id else None,
        "latitude": latitude,
        "longitude": longitude,
        "data": resp.get("data"),
    }


async def public_check_serviceability(
    db: AsyncSession,
    *,
    provider: str | None,
    address_line: str,
    landmark: str | None,
    city: str,
    pincode: str,
    latitude: float | None,
    longitude: float | None,
    diagnostic_package_id: int | None,
) -> dict[str, Any]:
    targets = _provider_targets(provider)
    single_provider = len(targets) == 1

    pkg: DiagnosticPackage | None = None
    if diagnostic_package_id is not None:
        pkg = await _get_diagnostic_package(db, diagnostic_package_id)

    lat, lng = await _resolve_coordinates(
        db,
        address_line=address_line,
        landmark=landmark,
        city=city,
        pincode=pincode,
        latitude=latitude,
        longitude=longitude,
    )

    providers_out: list[dict[str, Any]] = []
    for prov in targets:
        if prov == "orange_health":
            providers_out.append(
                await _check_orange_health_serviceability(
                    db,
                    latitude=lat,
                    longitude=lng,
                    pkg=pkg,
                    single_provider=single_provider,
                )
            )
        elif prov == "healthians":
            providers_out.append(
                await _check_healthians_serviceability(
                    db,
                    latitude=lat,
                    longitude=lng,
                    pincode=pincode,
                    pkg=pkg,
                    single_provider=single_provider,
                )
            )
        elif prov == "redcliffe":
            providers_out.append(
                await _check_redcliffe_serviceability(
                    latitude=lat,
                    longitude=lng,
                    pkg=pkg,
                    single_provider=single_provider,
                )
            )

    return {"providers": providers_out}


def _orange_pincode_serviceable_status(body: Any) -> bool:
    if not isinstance(body, dict):
        return False
    status = body.get("status")
    if not isinstance(status, str):
        return False
    normalized = status.strip().lower()
    return "serviceable" in normalized and "unserviceable" not in normalized


async def _check_healthians_pincode_serviceability(
    db: AsyncSession,
    *,
    pincode: str,
) -> dict[str, Any]:
    access_token = await _get_healthians_token()
    try:
        active = await healthians_client.get_active_zipcodes(access_token)
    except Exception as exc:
        logger.exception("Public Healthians getActiveZipcodes failed")
        return {
            "provider": "healthians",
            "serviceable": False,
            "message": str(exc),
            "data": None,
        }

    serviceable = pincode in active
    message = (
        "Pincode is serviceable for Healthians home collection"
        if serviceable
        else "Pincode is not in Healthians active zipcode list"
    )
    return {
        "provider": "healthians",
        "serviceable": serviceable,
        "message": message,
        "data": {"pincode": pincode, "active_zipcode_count": len(active)},
    }


async def _check_orange_health_pincode_serviceability(
    *,
    pincode: str,
) -> dict[str, Any]:
    req_date = (date.today() + timedelta(days=1)).isoformat()
    try:
        resp = await orange_health_client.check_serviceability_by_pincode(
            pincode=pincode,
            request_date=req_date,
        )
        serviceable = _orange_pincode_serviceable_status(resp)
        message = resp.get("status") if isinstance(resp.get("status"), str) else None
        return {
            "provider": "orange_health",
            "serviceable": serviceable,
            "message": message or (
                "Serviceable pincode region" if serviceable else "Unserviceable pincode region"
            ),
            "data": resp,
        }
    except OrangeHealthApiError as exc:
        if exc.status_code == 404:
            body = exc.body if isinstance(exc.body, dict) else {"status": str(exc)}
            return {
                "provider": "orange_health",
                "serviceable": False,
                "message": _format_orange_pincode_message(body),
                "data": body,
            }
        logger.exception("Public Orange Health pincode serviceability failed")
        return {
            "provider": "orange_health",
            "serviceable": False,
            "message": str(exc),
            "data": exc.body if isinstance(exc.body, dict) else None,
        }
    except Exception as exc:
        logger.exception("Public Orange Health pincode serviceability failed")
        return {
            "provider": "orange_health",
            "serviceable": False,
            "message": str(exc),
            "data": None,
        }


def _format_orange_pincode_message(body: dict) -> str:
    status = body.get("status")
    if isinstance(status, str) and status.strip():
        return status.strip()
    return "Unserviceable pincode region"


async def public_check_pincode_serviceability(
    db: AsyncSession,
    *,
    provider: str | None,
    pincode: str,
) -> dict[str, Any]:
    _ = db
    targets = _pincode_provider_targets(provider)
    providers_out: list[dict[str, Any]] = []
    for prov in targets:
        if prov == "healthians":
            providers_out.append(
                await _check_healthians_pincode_serviceability(db, pincode=pincode)
            )
        elif prov == "orange_health":
            providers_out.append(
                await _check_orange_health_pincode_serviceability(pincode=pincode)
            )
    return {"providers": providers_out}


def _assert_package_matches_provider(pkg: DiagnosticPackage, provider: str) -> None:
    actual = (pkg.diagnostic_provider or "").strip().lower()
    if actual != provider:
        raise AppError(
            status_code=422,
            error_code="PROVIDER_PACKAGE_MISMATCH",
            message=f"diagnostic_package_id provider ({actual}) does not match request provider ({provider})",
        )


async def public_available_slots(
    db: AsyncSession,
    *,
    provider: str,
    slot_date: date,
    diagnostic_package_id: int | None,
    zone_id: str | None,
    latitude: float | None,
    longitude: float | None,
    pincode: str | None,
    has_female_patient: bool,
) -> dict[str, Any]:
    prov = _normalize_provider(provider)
    if prov not in _ALL_PUBLIC_PROVIDERS:
        raise AppError(status_code=422, error_code="INVALID_PROVIDER", message="Unsupported diagnostic provider")

    if latitude is None or longitude is None:
        raise AppError(status_code=422, error_code="INVALID_INPUT", message="latitude and longitude are required")

    pkg: DiagnosticPackage | None = None
    if diagnostic_package_id is not None:
        pkg = await _get_diagnostic_package(db, diagnostic_package_id)
        _assert_package_matches_provider(pkg, prov)

    if prov == "orange_health":
        slot_resp = await orange_health_flow.fetch_slots_for_location(
            db,
            blood_collection_date=slot_date,
            latitude=float(latitude),
            longitude=float(longitude),
            engagement_id=None,
        )
        if slot_resp.get("status") != "success":
            raise AppError(
                status_code=422,
                error_code="SLOTS_UNAVAILABLE",
                message=slot_resp.get("message", "Failed to fetch slots"),
            )
        return {"provider": prov, "slots": slot_resp.get("slots") or []}

    if diagnostic_package_id is None or pkg is None:
        raise AppError(
            status_code=422,
            error_code="INVALID_INPUT",
            message="diagnostic_package_id is required for healthians",
        )

    zone = (zone_id or "").strip()
    if not zone:
        raise AppError(status_code=422, error_code="INVALID_INPUT", message="zone_id is required for healthians")

    amount = float(pkg.original_price) if pkg.original_price else 0
    deal_id = healthians_package_deal_id(pkg.external_package_code)
    access_token = await _get_healthians_token()
    payload = {
        "slot_date": slot_date.isoformat(),
        "zone_id": zone,
        "lat": str(latitude),
        "long": str(longitude),
        "zipcode": pincode or "",
        "get_ppmc_slots": 0,
        "has_female_patient": 1 if has_female_patient else 0,
        "amount": amount,
        "package": [{"deal_id": [deal_id]}],
    }
    try:
        resp = await healthians_client.get_slots_by_location(access_token, payload)
    except Exception as exc:
        logger.exception("Public Healthians slots failed")
        raise AppError(status_code=502, error_code="HEALTHIANS_API_ERROR", message=str(exc)) from exc

    if not resp.get("status"):
        raise AppError(
            status_code=422,
            error_code="SLOTS_UNAVAILABLE",
            message=resp.get("message", "Failed to fetch slots"),
        )
    raw_slots = resp.get("data", []) or []
    return {
        "provider": "healthians",
        "slots": [_slim_healthians_slot(slot) for slot in raw_slots if isinstance(slot, dict)],
    }
