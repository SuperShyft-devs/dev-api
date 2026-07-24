"""Employee HTTP routes for camp reports."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.exceptions import AppError
from db.session import get_db
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext
from modules.reports.camp_reports_service import CampReportsService
from modules.reports.dependencies import get_camp_reports_service
from modules.reports.schemas import CampReportRefreshRequest

router = APIRouter(prefix="/reports/camps", tags=["reports"])


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client is None:
        return "unknown"
    return request.client.host


@router.get("/{camp_no}/meta")
async def get_camp_report_meta(
    camp_no: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    result = await service.get_camp_report_meta(db, employee=employee, camp_no=camp_no)
    return success_response(result)


@router.get("/{camp_no}/department/{slug}/meta")
async def get_department_camp_report_meta(
    camp_no: int,
    slug: str,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    result = await service.get_camp_report_meta(
        db,
        employee=employee,
        camp_no=camp_no,
        department=slug,
    )
    return success_response(result)


@router.get("/{camp_no}/sections")
async def list_camp_report_sections(
    camp_no: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    result = await service.list_camp_report_section_keys(db, employee=employee, camp_no=camp_no)
    return success_response(result)


@router.get("/{camp_no}/department/{slug}/sections")
async def list_department_camp_report_sections(
    camp_no: int,
    slug: str,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    result = await service.list_camp_report_section_keys(
        db,
        employee=employee,
        camp_no=camp_no,
        department=slug,
    )
    return success_response(result)


@router.get("/{camp_no}/dashboard")
async def get_camp_report_dashboard(
    camp_no: int,
    section: str,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    result = await service.get_camp_report_dashboard(
        db,
        employee=employee,
        camp_no=camp_no,
        section=section,
    )
    return success_response(result)


@router.get("/{camp_no}/department/{slug}/dashboard")
async def get_department_camp_report_dashboard(
    camp_no: int,
    slug: str,
    section: str,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    result = await service.get_camp_report_dashboard(
        db,
        employee=employee,
        camp_no=camp_no,
        section=section,
        department=slug,
    )
    return success_response(result)


@router.get("/{camp_no}/{city}/dashboard")
async def get_city_camp_report_dashboard(
    camp_no: int,
    city: str,
    section: str,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    result = await service.get_camp_report_dashboard(
        db,
        employee=employee,
        camp_no=camp_no,
        section=section,
        city=city,
    )
    return success_response(result)


@router.get("/{camp_no}/{city}/department/{slug}/dashboard")
async def get_city_department_camp_report_dashboard(
    camp_no: int,
    city: str,
    slug: str,
    section: str,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    result = await service.get_camp_report_dashboard(
        db,
        employee=employee,
        camp_no=camp_no,
        section=section,
        department=slug,
        city=city,
    )
    return success_response(result)


@router.get("/{camp_no}/validate/company-average-scores")
async def validate_company_average_scores(
    camp_no: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    result = await service.validate_company_average_scores(
        db,
        employee=employee,
        camp_no=camp_no,
    )
    return success_response(result)


@router.get("/{camp_no}/department/{slug}/validate/company-average-scores")
async def validate_department_company_average_scores(
    camp_no: int,
    slug: str,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    result = await service.validate_company_average_scores(
        db,
        employee=employee,
        camp_no=camp_no,
        department=slug,
    )
    return success_response(result)


@router.get("/{camp_no}/validate/positive-wins")
async def validate_positive_wins(
    camp_no: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    result = await service.validate_positive_wins(
        db,
        employee=employee,
        camp_no=camp_no,
    )
    return success_response(result)


@router.get("/{camp_no}/department/{slug}/validate/positive-wins")
async def validate_department_positive_wins(
    camp_no: int,
    slug: str,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    result = await service.validate_positive_wins(
        db,
        employee=employee,
        camp_no=camp_no,
        department=slug,
    )
    return success_response(result)


@router.get("/{camp_no}/validate/overall-risk-score")
async def validate_overall_risk_score(
    camp_no: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    result = await service.validate_overall_risk_score(
        db,
        employee=employee,
        camp_no=camp_no,
    )
    return success_response(result)


@router.get("/{camp_no}/department/{slug}/validate/overall-risk-score")
async def validate_department_overall_risk_score(
    camp_no: int,
    slug: str,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    result = await service.validate_overall_risk_score(
        db,
        employee=employee,
        camp_no=camp_no,
        department=slug,
    )
    return success_response(result)


@router.get("/{camp_no}/validate/physical-activity-frequency")
async def validate_physical_activity_frequency(
    camp_no: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    result = await service.validate_questionnaire_distribution(
        db,
        employee=employee,
        camp_no=camp_no,
        question_key="physical_activity_frequency",
    )
    return success_response(result)


@router.get("/{camp_no}/department/{slug}/validate/physical-activity-frequency")
async def validate_department_physical_activity_frequency(
    camp_no: int,
    slug: str,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    result = await service.validate_questionnaire_distribution(
        db,
        employee=employee,
        camp_no=camp_no,
        question_key="physical_activity_frequency",
        department=slug,
    )
    return success_response(result)


@router.get("/{camp_no}/validate/sleeping-hours")
async def validate_sleeping_hours(
    camp_no: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    result = await service.validate_questionnaire_distribution(
        db,
        employee=employee,
        camp_no=camp_no,
        question_key="sleeping_hours",
    )
    return success_response(result)


@router.get("/{camp_no}/department/{slug}/validate/sleeping-hours")
async def validate_department_sleeping_hours(
    camp_no: int,
    slug: str,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    result = await service.validate_questionnaire_distribution(
        db,
        employee=employee,
        camp_no=camp_no,
        question_key="sleeping_hours",
        department=slug,
    )
    return success_response(result)


@router.get("/{camp_no}/participants")
async def list_camp_participants(
    camp_no: int,
    page: int = 1,
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    participants, total = await service.list_camp_participants(
        db,
        employee=employee,
        camp_no=camp_no,
        page=page,
        limit=limit,
    )
    return success_response(participants, meta={"page": page, "limit": limit, "total": total})


@router.get("/{camp_no}/department/{slug}/participants")
async def list_department_camp_participants(
    camp_no: int,
    slug: str,
    page: int = 1,
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    participants, total = await service.list_camp_participants(
        db,
        employee=employee,
        camp_no=camp_no,
        page=page,
        limit=limit,
        department=slug,
    )
    return success_response(participants, meta={"page": page, "limit": limit, "total": total})


@router.get("/{camp_no}/{city}/participants")
async def list_city_camp_participants(
    camp_no: int,
    city: str,
    page: int = 1,
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    participants, total = await service.list_camp_participants(
        db,
        employee=employee,
        camp_no=camp_no,
        page=page,
        limit=limit,
        city=city,
    )
    return success_response(participants, meta={"page": page, "limit": limit, "total": total})


@router.get("/{camp_no}/{city}/department/{slug}/participants")
async def list_city_department_camp_participants(
    camp_no: int,
    city: str,
    slug: str,
    page: int = 1,
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    participants, total = await service.list_camp_participants(
        db,
        employee=employee,
        camp_no=camp_no,
        page=page,
        limit=limit,
        department=slug,
        city=city,
    )
    return success_response(participants, meta={"page": page, "limit": limit, "total": total})


@router.get("/{camp_no}")
async def list_camp_reports(
    camp_no: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    rows = await service.list_camp_reports(db, employee=employee, camp_no=camp_no)
    return success_response(rows)


@router.put("/{camp_no}/refresh")
async def refresh_camp_report(
    camp_no: int,
    payload: CampReportRefreshRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    result = await service.refresh_camp_report_section(
        db,
        employee=employee,
        camp_no=camp_no,
        section=payload.section,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(result)


@router.put("/{camp_no}/department/{slug}/refresh")
async def refresh_department_camp_report(
    camp_no: int,
    slug: str,
    payload: CampReportRefreshRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    result = await service.refresh_camp_report_section(
        db,
        employee=employee,
        camp_no=camp_no,
        section=payload.section,
        department=slug,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(result)


@router.put("/{camp_no}/{city}/refresh")
async def refresh_city_camp_report(
    camp_no: int,
    city: str,
    payload: CampReportRefreshRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    result = await service.refresh_camp_report_section(
        db,
        employee=employee,
        camp_no=camp_no,
        section=payload.section,
        city=city,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(result)


@router.put("/{camp_no}/{city}/department/{slug}/refresh")
async def refresh_city_department_camp_report(
    camp_no: int,
    city: str,
    slug: str,
    payload: CampReportRefreshRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    result = await service.refresh_camp_report_section(
        db,
        employee=employee,
        camp_no=camp_no,
        section=payload.section,
        department=slug,
        city=city,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(result)


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


@router.post("/{camp_no}/{city}/init", status_code=201)
async def init_city_camp_report(
    camp_no: int,
    city: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    created = await service.init_city_camp_report(
        db,
        employee=employee,
        camp_no=camp_no,
        city=city,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"report_id": created.report_id})


@router.post("/{camp_no}/{city}/department/{slug}/init", status_code=201)
async def init_city_department_camp_report(
    camp_no: int,
    city: str,
    slug: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: CampReportsService = Depends(get_camp_reports_service),
):
    created = await service.init_city_department_camp_report(
        db,
        employee=employee,
        camp_no=camp_no,
        city=city,
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
