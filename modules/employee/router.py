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
from core.rate_limit import limiter
from db.session import get_db
from modules.employee.auth_service import EmployeeAuthService
from modules.employee.dependencies import (
    get_current_employee,
    get_current_employee_bearer_or_query,
    get_employee_auth_service,
    get_employee_management_service,
    get_employee_service,
)
from modules.employee.schemas import (
    EmployeeCreateRequest,
    EmployeeLogoutRequest,
    EmployeeRefreshTokenRequest,
    EmployeeSendOtpRequest,
    EmployeeStatusUpdateRequest,
    EmployeeUpdateRequest,
    EmployeeVerifyOtpRequest,
    ReplaceEmployeePermissionsRequest,
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


def _employee_to_dict(row) -> dict:
    return {
        "employee_id": row.employee_id,
        "name": row.name,
        "phone": row.phone,
        "email": row.email,
        "role": row.role,
        "status": row.status,
        "permissions_version": getattr(row, "permissions_version", None),
        "created_at": getattr(row, "created_at", None),
        "updated_at": getattr(row, "updated_at", None),
    }


def _tokens_payload(tokens) -> dict:
    return {
        "access_token": tokens.access_token,
        "refresh_token": tokens.refresh_token,
        "token_type": "bearer",
    }


# ── Auth (registered before /{employee_id} routes) ───────────────────────


@router.post("/auth/send-otp")
@limiter.limit("5/minute")
async def employee_send_otp(
    payload: EmployeeSendOtpRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    auth_service: EmployeeAuthService = Depends(get_employee_auth_service),
):
    session_id, delivery = await auth_service.send_otp(
        db,
        phone=payload.phone,
        email=str(payload.email) if payload.email else None,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    if delivery is not None:
        await auth_service.deliver_otp(db, delivery=delivery)
        await db.commit()
    return success_response({"session_id": session_id})


def _employee_permissions_payload(employee_ctx: EmployeeContext) -> dict | None:
    from modules.employee.models import EmployeeRole

    if employee_ctx.role != EmployeeRole.inferior_admin:
        return None
    task_permissions: dict[str, dict[str, dict[str, bool]]] = {}
    for composite_key, task_grant in employee_ctx.task_permissions.items():
        category_key, task_key = composite_key.split(".", 1)
        task_permissions.setdefault(category_key, {})[task_key] = {
            "can_view": task_grant.can_view,
            "can_edit": task_grant.can_edit,
        }
    return {
        "version": employee_ctx.permissions_version,
        "categories": {
            key: {
                "can_view": grant.can_view,
                "can_edit": grant.can_edit,
                **({"tasks": task_permissions[key]} if key in task_permissions else {}),
            }
            for key, grant in sorted(employee_ctx.permissions.items())
        },
    }


@router.post("/auth/verify-otp")
@limiter.limit("10/minute")
async def employee_verify_otp(
    payload: EmployeeVerifyOtpRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    auth_service: EmployeeAuthService = Depends(get_employee_auth_service),
    employee_service: EmployeeService = Depends(get_employee_service),
):
    employee, tokens = await auth_service.verify_otp(
        db,
        phone=payload.phone,
        email=str(payload.email) if payload.email else None,
        otp=payload.otp,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    employee_ctx = await employee_service.get_active_employee_by_id(db, employee.employee_id)
    body: dict = {
        "employee_id": employee.employee_id,
        "name": employee.name,
        "role": employee.role,
        "tokens": _tokens_payload(tokens),
    }
    permissions = _employee_permissions_payload(employee_ctx)
    if permissions is not None:
        body["permissions"] = permissions
    return success_response(body)


@router.get("/auth/me")
async def employee_auth_me(
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    employee_service: EmployeeService = Depends(get_employee_service),
):
    row = await employee_service.get_employee_row_for_self(db, employee_id=employee.employee_id)
    body: dict = {
        "employee_id": row.employee_id,
        "name": row.name,
        "phone": row.phone,
        "email": row.email,
        "role": row.role,
        "status": row.status,
    }
    permissions = _employee_permissions_payload(employee)
    if permissions is not None:
        body["permissions"] = permissions
    return success_response(body)


@router.post("/auth/refresh-token")
async def employee_refresh_token(
    payload: EmployeeRefreshTokenRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    auth_service: EmployeeAuthService = Depends(get_employee_auth_service),
):
    employee, tokens = await auth_service.refresh_tokens(
        db,
        refresh_token=payload.refresh_token,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(
        {
            "employee_id": employee.employee_id,
            "role": employee.role,
            "tokens": _tokens_payload(tokens),
        }
    )


@router.post("/auth/logout")
async def employee_logout(
    payload: EmployeeLogoutRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    auth_service: EmployeeAuthService = Depends(get_employee_auth_service),
):
    await auth_service.logout(
        db,
        refresh_token=payload.refresh_token,
        ip_address=get_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"success": True})


# ── CRUD ────────────────────────────────────────────────────────────────


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
    return success_response(_employee_to_dict(created))


@router.get("")
async def list_employees(
    request: Request,
    page: int = 1,
    limit: int = 20,
    status: str | None = None,
    role: str | None = None,
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
        search=search,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )

    data = [
        {
            "employee_id": row.employee_id,
            "name": row.name,
            "phone": row.phone,
            "email": row.email,
            "role": row.role,
            "status": row.status,
            "permissions_version": row.permissions_version,
        }
        for row in employees
    ]

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
        user_id=None,
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
    row = await employee_service.get_employee_details(
        db, employee=employee, employee_id=employee_id
    )

    return success_response(_employee_to_dict(row))


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
