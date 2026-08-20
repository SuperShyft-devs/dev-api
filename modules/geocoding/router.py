"""Geocoding HTTP routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from common.responses import success_response
from common.validation import ValidationError, sanitize_search_query
from core.exceptions import AppError
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext
from modules.geocoding.client import search_places

router = APIRouter(prefix="/geocode", tags=["geocode"])


@router.get("/search")
async def geocode_search(
    q: str = Query(default="", min_length=0, max_length=500),
    limit: int = Query(default=3, ge=1, le=10),
    employee: EmployeeContext = Depends(get_current_employee),
):
    _ = employee
    try:
        query = sanitize_search_query(q, max_len=500)
    except ValidationError as exc:
        raise AppError(
            status_code=400,
            error_code="INVALID_INPUT",
            message=str(exc),
        ) from exc
    if len(query) < 3:
        raise AppError(
            status_code=400,
            error_code="INVALID_INPUT",
            message="Query must be at least 3 characters",
        )

    results = await search_places(query, limit=limit)
    return success_response(results)
