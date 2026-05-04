"""Healthians integration HTTP routes."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends

from common.responses import success_response
from core.exceptions import AppError
from modules.diagnostics.healthians import client as healthians_client
from modules.diagnostics.healthians.schemas import (
    HealthiansConstituent,
    HealthiansConstituentsRequest,
    HealthiansConstituentsResponse,
)
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext

logger = logging.getLogger(__name__)

router = APIRouter(tags=["diagnostics-healthians"])


@router.post("/diagnostics/healthians/constituents")
async def get_healthians_constituents(
    payload: HealthiansConstituentsRequest,
    employee: EmployeeContext = Depends(get_current_employee),
):
    try:
        token = await healthians_client.get_access_token()
        product = await healthians_client.get_product_details(
            token, payload.healthians_camp_id
        )
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
