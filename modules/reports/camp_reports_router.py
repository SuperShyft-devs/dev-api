"""Employee HTTP routes for camp reports."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from db.session import get_db
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext
from modules.reports.camp_reports_service import CampReportsService
from modules.reports.dependencies import get_camp_reports_service

router = APIRouter(prefix="/reports/camps", tags=["reports"])


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client is None:
        return "unknown"
    return request.client.host


@router.post("/{camp_no}/init", status_code=201)
async def init_camp_report(
    camp_no: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    created = await service.init_camp_report(
        db,
        employee=employee,
        camp_no=camp_no,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"report_id": created.report_id})


@router.post("/{camp_no}/department/{slug}/init", status_code=201)
async def init_department_camp_report(
    camp_no: int,
    slug: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    created = await service.init_department_camp_report(
        db,
        employee=employee,
        camp_no=camp_no,
        slug=slug,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"report_id": created.report_id})


@router.delete("/{camp_no}")
async def delete_camp_report(
    camp_no: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    await service.delete_camp_report(
        db,
        employee=employee,
        camp_no=camp_no,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"deleted": True})


@router.delete("/{camp_no}/department/{slug}")
async def delete_department_camp_report(
    camp_no: int,
    slug: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    await service.delete_department_camp_report(
        db,
        employee=employee,
        camp_no=camp_no,
        slug=slug,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"deleted": True})
