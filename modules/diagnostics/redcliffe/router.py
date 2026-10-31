"""Public Redcliffe Labs serviceability routes (no authentication)."""

from __future__ import annotations

from fastapi import APIRouter, Query, Request

from common.responses import success_response
from common.validation import ValidationError, sanitize_search_query
from core.exceptions import AppError
from core.rate_limit import limiter
from modules.diagnostics.redcliffe import service as redcliffe_service
from modules.diagnostics.redcliffe.client import RedcliffeApiError, is_serviceable_response

router = APIRouter(prefix="/diagnostics/public/redcliffe", tags=["diagnostics-public-redcliffe"])


@router.get("/search-area")
@limiter.limit("10/minute")
async def redcliffe_search_area(
    request: Request,
    place_query: str = Query(default="", min_length=0, max_length=500),
):
    try:
        query = sanitize_search_query(place_query, max_len=500)
    except ValidationError as exc:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message=str(exc)) from exc
    if len(query) < 3:
        raise AppError(
            status_code=400,
            error_code="INVALID_INPUT",
            message="Query must be at least 3 characters",
        )
    try:
        data = await redcliffe_service.search_area(place_query=query)
    except RedcliffeApiError as exc:
        raise AppError(
            status_code=exc.status_code or 502,
            error_code="REDCLIFFE_API_ERROR",
            message=str(exc),
        ) from exc
    return success_response(data)


@router.get("/coordinates")
@limiter.limit("10/minute")
async def redcliffe_coordinates(
    request: Request,
    eloc: str = Query(min_length=6, max_length=6),
):
    code = eloc.strip()
    if len(code) != 6:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="eloc must be 6 characters")
    try:
        data = await redcliffe_service.get_coordinates(eloc=code)
    except RedcliffeApiError as exc:
        raise AppError(
            status_code=exc.status_code or 502,
            error_code="REDCLIFFE_API_ERROR",
            message=str(exc),
        ) from exc
    return success_response(data)


@router.get("/serviceability")
@limiter.limit("10/minute")
async def redcliffe_serviceability(
    request: Request,
    latitude: float = Query(...),
    longitude: float = Query(...),
):
    try:
        data = await redcliffe_service.check_location_serviceable(latitude=latitude, longitude=longitude)
    except RedcliffeApiError as exc:
        raise AppError(
            status_code=exc.status_code or 502,
            error_code="REDCLIFFE_API_ERROR",
            message=str(exc),
        ) from exc
    serviceable = is_serviceable_response(data)
    return success_response(
        {
            "provider": "redcliffe",
            "serviceable": serviceable,
            "message": data.get("message"),
            "latitude": latitude,
            "longitude": longitude,
            "data": data,
        }
    )
