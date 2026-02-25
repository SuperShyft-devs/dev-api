"""Employee HTTP routes.

These endpoints are employee-only.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.exceptions import AppError
from db.session import get_db
from modules.employee.dependencies import get_current_employee, get_employee_management_service
from modules.employee.schemas import (
    EmployeeCreateRequest,
    EmployeeStatusUpdateRequest,
    EmployeeUpdateRequest,
)
from modules.employee.service import EmployeeContext, EmployeeService


router = APIRouter(prefix="/employees", tags=["employees"])


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()

    if request.client is None:
        return "unknown"

    return request.client.host


@router.post("", status_code=201)
async def create_employee(
    payload: EmployeeCreateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    employee_service: EmployeeService = Depends(get_employee_management_service),
):
    created = await employee_service.create_employee(
        db,
        employee=employee,
        payload=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"employee_id": created.employee_id})


@router.get("")
async def list_employees(
    request: Request,
    page: int = 1,
    limit: int = 20,
    status: str | None = None,
    role: str | None = None,
    user_id: int | None = None,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    employee_service: EmployeeService = Depends(get_employee_management_service),
):
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    employees, total = await employee_service.list_employees(
        db,
        employee=employee,
        page=page,
        limit=limit,
        status=status,
        role=role,
        user_id=user_id,
    )

    data = []
    for row in employees:
        data.append(
            {
                "employee_id": row.employee_id,
                "user_id": row.user_id,
                "role": row.role,
                "status": row.status,
            }
        )

    return success_response(data, meta={"page": page, "limit": limit, "total": total})


@router.get("/{employee_id}")
async def get_employee(
    employee_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    employee_service: EmployeeService = Depends(get_employee_management_service),
):
    row = await employee_service.get_employee_details(db, employee=employee, employee_id=employee_id)

    return success_response(
        {
            "employee_id": row.employee_id,
            "user_id": row.user_id,
            "role": row.role,
            "status": row.status,
            "created_at": row.created_at,
            "updated_at": row.updated_at,
        }
    )


@router.put("/{employee_id}")
async def update_employee(
    employee_id: int,
    payload: EmployeeUpdateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    employee_service: EmployeeService = Depends(get_employee_management_service),
):
    updated = await employee_service.update_employee(
        db,
        employee=employee,
        employee_id=employee_id,
        payload=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()

    return success_response({"employee_id": updated.employee_id})


@router.patch("/{employee_id}/status")
async def update_employee_status(
    employee_id: int,
    payload: EmployeeStatusUpdateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    employee_service: EmployeeService = Depends(get_employee_management_service),
):
    updated = await employee_service.change_employee_status(
        db,
        employee=employee,
        employee_id=employee_id,
        status=payload.status,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()

    return success_response({"employee_id": updated.employee_id, "status": updated.status})
