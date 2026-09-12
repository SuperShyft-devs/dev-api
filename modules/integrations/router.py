"""Integration metadata routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from common.responses import success_response
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext
from modules.integrations.metsights_mapping_service import build_metsights_blood_mapping_payload

router = APIRouter(prefix="/integrations", tags=["integrations"])


@router.get("/metsights-blood-mapping")
async def get_metsights_blood_mapping(
    employee: EmployeeContext = Depends(get_current_employee),
):
    """Read-only Healthians ↔ Metsights blood mapping reference for admin UI."""
    _ = employee
    return success_response(build_metsights_blood_mapping_payload())
