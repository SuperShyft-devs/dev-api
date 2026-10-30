"""Healthians integration HTTP routes."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, Query

from common.responses import success_response
from core.exceptions import AppError
from modules.diagnostics.healthians import client as healthians_client
from modules.diagnostics.external_codes import (
    healthians_product_deal_type_id,
    healthians_profile_deal_type_id,
)
from modules.diagnostics.healthians.catalog import search_profiles
from modules.diagnostics.healthians.schemas import (
    HealthiansConstituent,
    HealthiansConstituentsRequest,
    HealthiansConstituentsResponse,
    HealthiansProfileSearchResponse,
)
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext

logger = logging.getLogger(__name__)

router = APIRouter(tags=["diagnostics-healthians"])


def _deal_type_id(deal_type: str, external_code: str) -> int:
    if deal_type == "profile":
        return healthians_profile_deal_type_id(external_code)
    return healthians_product_deal_type_id(external_code)


@router.get("/diagnostics/healthians/profiles")
async def search_healthians_profiles(
    search: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    employee: EmployeeContext = Depends(get_current_employee),
):
    profiles = search_profiles(search, limit=limit)
    resp = HealthiansProfileSearchResponse(
        profiles=[HealthiansConstituent(id=item["id"], name=item["name"]) for item in profiles]
    )
    return success_response(resp.model_dump())


@router.post("/diagnostics/healthians/constituents")
async def get_healthians_constituents(
    payload: HealthiansConstituentsRequest,
    employee: EmployeeContext = Depends(get_current_employee),
):
    deal_type = payload.deal_type
    external_code = payload.external_code
    try:
        token = await healthians_client.get_access_token()
        deal_type_id = _deal_type_id(deal_type, external_code)
        product = await healthians_client.get_product_details(token, deal_type_id, deal_type=deal_type)
    except Exception as exc:
        logger.exception("Healthians API error")
        is_blocked = "403" in str(exc) or "blocked" in str(exc).lower()
        raise AppError(
            status_code=502,
            error_code="HEALTHIANS_IP_BLOCKED" if is_blocked else "HEALTHIANS_API_ERROR",
            message=(
                "Healthians is blocking requests from this server. "
                "The server IP needs to be whitelisted by Healthians."
                if is_blocked
                else f"Failed to fetch data from Healthians: {exc}"
            ),
        )

    constituents = [
        HealthiansConstituent(id=c["id"], name=c["name"])
        for c in product.get("constituents", [])
    ]

    resp = HealthiansConstituentsResponse(
        constituents=constituents,
        package_name=product.get("name"),
    )
    return success_response(resp.model_dump())
