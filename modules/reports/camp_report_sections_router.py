"""Employee HTTP routes for camp report sections."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.exceptions import AppError
from db.session import get_db
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext
from modules.reports.camp_report_sections_service import CampReportSectionsService
from modules.reports.dependencies import get_camp_report_sections_service
from modules.reports.schemas import CampReportSectionCreateRequest, CampReportSectionUpdateRequest

router = APIRouter(prefix="/reports/camp-sections", tags=["reports"])


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client is None:
        return "unknown"
    return request.client.host


@router.get("")
async def list_camp_report_sections(
    page: int = 1,
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportSectionsService = Depends(get_camp_report_sections_service),
):
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    rows, total = await service.list_sections(db, employee=employee, page=page, limit=limit)
    data = [
        {
            "report_sections": row.report_sections,
            "section": row.section,
            "section_key": row.section_key,
            "description": row.description,
        }
        for row in rows
    ]
    return success_response(data, meta={"page": page, "limit": limit, "total": total})


@router.post("", status_code=201)
async def create_camp_report_section(
    payload: CampReportSectionCreateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportSectionsService = Depends(get_camp_report_sections_service),
):
    created = await service.create_section(
        db,
        employee=employee,
        payload=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"report_sections": created.report_sections})


@router.put("/{report_sections}")
async def update_camp_report_section(
    report_sections: int,
    payload: CampReportSectionUpdateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportSectionsService = Depends(get_camp_report_sections_service),
):
    updated = await service.update_section(
        db,
        employee=employee,
        report_sections=report_sections,
        payload=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(
        {
            "report_sections": updated.report_sections,
            "section": updated.section,
            "section_key": updated.section_key,
            "description": updated.description,
        }
    )


@router.delete("/{report_sections}")
async def delete_camp_report_section(
    report_sections: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportSectionsService = Depends(get_camp_report_sections_service),
):
    await service.delete_section(
        db,
        employee=employee,
        report_sections=report_sections,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"deleted": True})
