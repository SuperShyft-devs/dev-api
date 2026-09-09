"""Employee HTTP routes.

These endpoints are employee-only.
"""

from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO

from fastapi import APIRouter, Depends, Request
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from common.excel_db_export import export_public_schema_to_xlsx_bytes
from common.responses import success_response
from core.exceptions import AppError
from core.network import get_client_ip
from db.session import get_db
from modules.employee.dependencies import (
    get_current_employee,
    get_current_employee_bearer_or_query,
    get_employee_management_service,
)
from modules.employee.schemas import (
    EmployeeCreateRequest,
    ReplaceEmployeePermissionsRequest,
    EmployeeStatusUpdateRequest,
    EmployeeUpdateRequest,
)
from modules.employee.permissions import TASK_CATALOG
from modules.employee.service import EmployeeContext, EmployeeService
from modules.employee.access_control import ensure_admin
from modules.users.dependencies import get_users_service
from modules.users.schemas import EmployeeCreateUserRequest
from modules.users.service import UsersService
from modules.audit.dependencies import get_audit_service
from modules.audit.service import AuditService


router = APIRouter(prefix="/employees", tags=["employees"])


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
        ip_address=get_client_ip(request),
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
    search: str | None = None,
    sort_by: str | None = None,
    sort_dir: str | None = None,
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
        search=search,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )

    data = []
    for row, first_name, last_name in employees:
        data.append(
            {
                "employee_id": row.employee_id,
                "user_id": row.user_id,
                "role": row.role,
                "status": row.status,
                "permissions_version": row.permissions_version,
                "first_name": first_name,
                "last_name": last_name,
            }
        )

    return success_response(data, meta={"page": page, "limit": limit, "total": total})


@router.post("/users", status_code=201)
async def create_staff_user(
    payload: EmployeeCreateUserRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    users_service: UsersService = Depends(get_users_service),
):
    """Authenticated dev-admin user creation; public POST /users remains compatible."""
    ensure_admin(employee)
    user = await users_service.create_user_by_employee(
        db,
        employee=employee,
        payload=payload,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"user_id": user.user_id})


@router.get("/permission-categories")
async def list_permission_categories(
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    employee_service: EmployeeService = Depends(get_employee_management_service),
):
    rows = await employee_service.list_permission_categories(db, employee=employee)
    return success_response(
        [
            {
                "category_key": row.category_key,
                "display_name": row.display_name,
                "description": row.description,
                "display_order": row.display_order,
                "tasks": [
                    {
                        "task_key": task_key,
                        "display_name": display_name,
                        "description": description,
                        "display_order": position,
                    }
                    for position, (task_key, display_name, description) in enumerate(
                        TASK_CATALOG[row.category_key], start=1
                    )
                ],
            }
            for row in rows
        ]
    )


@router.get("/database-backup")
async def download_database_backup_excel(
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee_bearer_or_query),
    audit_service: AuditService = Depends(get_audit_service),
):
    """Export all public-schema tables to one .xlsx (one sheet per table). Active employee JWT required."""

    ensure_admin(employee)

    async with db.bind.connect() as conn:
        async with conn.begin():
            payload = await conn.run_sync(export_public_schema_to_xlsx_bytes)
    await audit_service.log_event(
        db,
        action="DATABASE_BACKUP_EXPORT",
        endpoint=str(request.url.path),
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        user_id=employee.user_id,
        session_id=None,
    )
    await db.commit()

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%SZ")
    filename = f"supershyft-db-backup-{stamp}.xlsx"
    return StreamingResponse(
        BytesIO(payload),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/{employee_id}/permissions")
async def get_employee_permissions(
    employee_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    employee_service: EmployeeService = Depends(get_employee_management_service),
):
    target, rows, task_rows = await employee_service.get_employee_permissions(
        db, employee=employee, employee_id=employee_id
    )
    tasks_by_category: dict[str, list] = {}
    for task in task_rows:
        tasks_by_category.setdefault(task.category_key, []).append(task)
    return success_response(
        {
            "employee_id": target.employee_id,
            "role": target.role,
            "version": target.permissions_version,
            "permissions": [
                {
                    "category_key": row.category_key,
                    "can_view": row.can_view,
                    "can_edit": row.can_edit,
                    **(
                        {
                            "tasks": [
                                {
                                    "task_key": task.task_key,
                                    "can_view": task.can_view,
                                    "can_edit": task.can_edit,
                                }
                                for task in tasks_by_category[row.category_key]
                            ]
                        }
                        if row.category_key in tasks_by_category
                        else {}
                    ),
                }
                for row in rows
            ],
        }
    )


@router.put("/{employee_id}/permissions")
async def replace_employee_permissions(
    employee_id: int,
    payload: ReplaceEmployeePermissionsRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    employee_service: EmployeeService = Depends(get_employee_management_service),
):
    target = await employee_service.replace_employee_permissions(
        db,
        employee=employee,
        employee_id=employee_id,
        expected_version=payload.expected_version,
        permissions=payload.permissions,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(
        {"employee_id": target.employee_id, "version": target.permissions_version}
    )


@router.get("/{employee_id}")
async def get_employee(
    employee_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    employee_service: EmployeeService = Depends(get_employee_management_service),
):
    row, first_name, last_name = await employee_service.get_employee_details(
        db, employee=employee, employee_id=employee_id
    )

    return success_response(
        {
            "employee_id": row.employee_id,
            "user_id": row.user_id,
            "role": row.role,
            "status": row.status,
            "permissions_version": row.permissions_version,
            "first_name": first_name,
            "last_name": last_name,
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
        ip_address=get_client_ip(request),
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
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()

    return success_response({"employee_id": updated.employee_id, "status": updated.status})
