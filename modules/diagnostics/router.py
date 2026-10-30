"""Diagnostics HTTP routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request, Response, status
from fastapi.security import HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession

from common.listing import optional_page_params, sanitize_list_search
from common.responses import success_response
from core.dependencies import _http_bearer, authenticate_bearer_user
from db.session import get_db
from modules.diagnostics.actor import DiagnosticsPackageActor, get_diagnostics_package_actor
from modules.diagnostics.dependencies import get_diagnostics_service
from modules.diagnostics.schemas import (
    AssignGroupsToPackageRequest,
    AssignTestsToGroupRequest,
    DiagnosticPackageCreate,
    DiagnosticPackageStatusUpdate,
    DiagnosticPackageUpdate,
    FilterChipCreate,
    FilterChipForSchema,
    FilterChipUpdate,
    PackageListType,
    PackageFilterChipAssign,
    PreparationCreate,
    PreparationUpdate,
    ReasonCreate,
    ReasonUpdate,
    ReorderGroupTestsRequest,
    ReorderPackageGroupsRequest,
    ReorderPackagesRequest,
    ReorderFilterChipsRequest,
    ReorderPackageReasonsRequest,
    SampleCreate,
    SampleUpdate,
    HealthParameterCreate,
    HealthParameterUpdate,
    ParameterType,
    TagCreate,
    TestGroupCreate,
    TestGroupUpdate,
)
from modules.diagnostics.service import DiagnosticsService
from modules.employee.dependencies import get_current_employee, get_employee_service
from modules.employee.service import EmployeeContext, EmployeeService


router = APIRouter(tags=["diagnostics"])


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client is None:
        return "unknown"
    return request.client.host


@router.get("/diagnostic-packages")
async def list_diagnostic_packages(
    gender: str | None = None,
    tag: str | None = None,
    filter_chip: str | None = None,
    package_list_type: PackageListType = Query(default=PackageListType.PUBLIC_PACKAGE, alias="type"),
    include_inactive: bool = Query(default=False),
    package_for: str | None = Query(default=None),
    page: int | None = Query(default=None, ge=1),
    limit: int | None = Query(default=None, ge=1, le=100),
    search: str | None = None,
    diagnostic_provider: str | None = None,
    status: str | None = None,
    db: AsyncSession = Depends(get_db),
    credentials: HTTPAuthorizationCredentials | None = Depends(_http_bearer),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
    employee_service: EmployeeService = Depends(get_employee_service),
):
    active_only = True
    requesting_user_id = None
    if package_list_type == PackageListType.CUSTOM_PACKAGE:
        auth_user = await authenticate_bearer_user(db, credentials)
        requesting_user_id = auth_user.user_id
    if include_inactive:
        from core.dependencies import get_current_employee_from_token

        await get_current_employee_from_token(db, credentials)
        active_only = False

    pagination = optional_page_params(page=page, limit=limit)
    search_value = sanitize_list_search(search)
    data, total, extra_meta = await diagnostics_service.get_packages(
        db,
        gender=gender,
        tag=tag,
        filter_chip=filter_chip,
        active_only=active_only,
        list_type=package_list_type.value,
        requesting_user_id=requesting_user_id,
        package_for=package_for,
        page=pagination.page if pagination else None,
        limit=pagination.limit if pagination else None,
        search=search_value,
        diagnostic_provider=diagnostic_provider,
        status=status,
    )
    if pagination:
        meta = {
            "page": pagination.page,
            "limit": pagination.limit,
            "total": total,
            **(extra_meta or {}),
        }
        return success_response([item.model_dump() for item in data], meta=meta)
    return success_response([item.model_dump() for item in data])


@router.get("/diagnostic-packages/filters-chips")
async def list_diagnostic_filter_chips(
    chip_scope: FilterChipForSchema = Query(default=FilterChipForSchema.PUBLIC_PACKAGE, alias="for"),
    include_inactive: bool = Query(default=False),
    page: int | None = Query(default=None, ge=1),
    limit: int | None = Query(default=None, ge=1, le=100),
    search: str | None = None,
    db: AsyncSession = Depends(get_db),
    credentials: HTTPAuthorizationCredentials | None = Depends(_http_bearer),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    active_only = True
    if include_inactive:
        from core.dependencies import get_current_employee_from_token

        await get_current_employee_from_token(db, credentials)
        active_only = False

    pagination = optional_page_params(page=page, limit=limit)
    search_value = sanitize_list_search(search)
    data, total = await diagnostics_service.get_filter_chips(
        db,
        chip_for=chip_scope.value,
        active_only=active_only,
        page=pagination.page if pagination else None,
        limit=pagination.limit if pagination else None,
        search=search_value,
    )
    if pagination:
        return success_response(
            [item.model_dump() for item in data],
            meta={"page": pagination.page, "limit": pagination.limit, "total": total},
        )
    return success_response([item.model_dump() for item in data])


@router.get("/diagnostic-packages/groups")
async def list_diagnostic_package_groups(
    gender: str | None = None,
    tag: str | None = None,
    filter_chip: str | None = None,
    package_list_type: PackageListType = Query(default=PackageListType.PUBLIC_PACKAGE, alias="type"),
    include_inactive: bool = Query(default=False),
    package_for: str | None = Query(default=None),
    page: int | None = Query(default=None, ge=1),
    limit: int | None = Query(default=None, ge=1, le=100),
    search: str | None = None,
    diagnostic_provider: str | None = None,
    status: str | None = None,
    db: AsyncSession = Depends(get_db),
    credentials: HTTPAuthorizationCredentials | None = Depends(_http_bearer),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
    employee_service: EmployeeService = Depends(get_employee_service),
):
    active_only = True
    requesting_user_id = None
    if package_list_type == PackageListType.CUSTOM_PACKAGE:
        auth_user = await authenticate_bearer_user(db, credentials)
        requesting_user_id = auth_user.user_id
    if include_inactive:
        from core.dependencies import get_current_employee_from_token

        await get_current_employee_from_token(db, credentials)
        active_only = False

    pagination = optional_page_params(page=page, limit=limit)
    search_value = sanitize_list_search(search)
    data, total, extra_meta = await diagnostics_service.get_package_groups(
        db,
        gender=gender,
        tag=tag,
        filter_chip=filter_chip,
        active_only=active_only,
        list_type=package_list_type.value,
        requesting_user_id=requesting_user_id,
        package_for=package_for,
        page=pagination.page if pagination else None,
        limit=pagination.limit if pagination else None,
        search=search_value,
        diagnostic_provider=diagnostic_provider,
        status=status,
    )
    if pagination:
        meta = {
            "page": pagination.page,
            "limit": pagination.limit,
            "total": total,
            **(extra_meta or {}),
        }
        return success_response([item.model_dump() for item in data], meta=meta)
    return success_response([item.model_dump() for item in data])


@router.get("/diagnostic-packages/{package_id}")
async def get_diagnostic_package_detail(
    package_id: int,
    db: AsyncSession = Depends(get_db),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    data = await diagnostics_service.get_package_detail(db, package_id=package_id)
    return success_response(data.model_dump())


@router.get("/diagnostic-packages/{package_id}/tests")
async def get_diagnostic_package_tests(
    package_id: int,
    db: AsyncSession = Depends(get_db),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    data = await diagnostics_service.get_package_tests(db, package_id=package_id)
    return success_response(data.model_dump())


@router.get("/diagnostic-packages/{package_id}/provider-test-diff")
async def get_diagnostic_package_provider_test_diff(
    package_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    data = await diagnostics_service.get_provider_test_diff(db, package_id=package_id)
    return success_response(data.model_dump())


@router.post("/diagnostic-packages/filters-chips", status_code=201)
async def create_filter_chip(
    payload: FilterChipCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    created = await diagnostics_service.create_filter_chip(
        db,
        employee=employee,
        data=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(created.model_dump())


@router.put("/diagnostic-packages/filters-chips/{filter_chip_id}")
async def update_filter_chip(
    filter_chip_id: int,
    payload: FilterChipUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    updated = await diagnostics_service.update_filter_chip(
        db,
        employee=employee,
        filter_chip_id=filter_chip_id,
        data=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(updated.model_dump())


@router.delete("/diagnostic-packages/filters-chips/{filter_chip_id}")
async def delete_filter_chip(
    filter_chip_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    await diagnostics_service.delete_filter_chip(
        db,
        employee=employee,
        filter_chip_id=filter_chip_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"filter_chip_id": filter_chip_id, "deleted": True})


@router.post("/diagnostic-packages", status_code=201)
async def create_package(
    payload: DiagnosticPackageCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    actor: DiagnosticsPackageActor = Depends(get_diagnostics_package_actor),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    created = await diagnostics_service.create_package(
        db,
        employee=actor.employee,
        current_user_id=actor.current_user_id,
        data=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(created.model_dump())


@router.put("/diagnostic-packages/{package_id}")
async def update_package(
    package_id: int,
    payload: DiagnosticPackageUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    actor: DiagnosticsPackageActor = Depends(get_diagnostics_package_actor),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    updated = await diagnostics_service.update_package(
        db,
        employee=actor.employee,
        current_user_id=actor.current_user_id,
        package_id=package_id,
        data=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(updated.model_dump())


@router.patch("/diagnostic-packages/{package_id}/status")
async def update_package_status(
    package_id: int,
    payload: DiagnosticPackageStatusUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    updated = await diagnostics_service.update_package_status(
        db,
        employee=employee,
        package_id=package_id,
        data=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(updated.model_dump())


@router.post("/diagnostic-packages/{package_id}/duplicate", status_code=201)
async def duplicate_package(
    package_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    duplicated = await diagnostics_service.duplicate_package(
        db,
        employee=employee,
        package_id=package_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(duplicated.model_dump())


@router.patch("/diagnostic-packages/order")
async def reorder_packages(
    payload: ReorderPackagesRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    data = await diagnostics_service.reorder_packages(
        db,
        employee=employee,
        package_ids=payload.package_ids,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)


@router.patch("/diagnostic-packages/filters-chips/order")
async def reorder_filter_chips(
    payload: ReorderFilterChipsRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    data = await diagnostics_service.reorder_filter_chips(
        db,
        employee=employee,
        filter_chip_ids=payload.filter_chip_ids,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)


@router.delete("/diagnostic-packages/{package_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_package(
    package_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    await diagnostics_service.delete_package(
        db,
        employee=employee,
        package_id=package_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/diagnostic-packages/{package_id}/reasons", status_code=201)
async def create_reason(
    package_id: int,
    payload: ReasonCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    created = await diagnostics_service.create_reason(
        db,
        employee=employee,
        package_id=package_id,
        data=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(created.model_dump())


@router.put("/diagnostic-packages/{package_id}/reasons/{reason_id}")
async def update_reason(
    package_id: int,
    reason_id: int,
    payload: ReasonUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    updated = await diagnostics_service.update_reason(
        db,
        employee=employee,
        package_id=package_id,
        reason_id=reason_id,
        data=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(updated.model_dump())


@router.patch("/diagnostic-packages/{package_id}/reasons/order")
async def reorder_package_reasons(
    package_id: int,
    payload: ReorderPackageReasonsRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    data = await diagnostics_service.reorder_package_reasons(
        db,
        employee=employee,
        package_id=package_id,
        reason_ids=payload.reason_ids,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)


@router.delete("/diagnostic-packages/{package_id}/reasons/{reason_id}")
async def delete_reason(
    package_id: int,
    reason_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    await diagnostics_service.delete_reason(
        db,
        employee=employee,
        package_id=package_id,
        reason_id=reason_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"reason_id": reason_id, "deleted": True})


@router.post("/diagnostic-packages/{package_id}/tags", status_code=201)
async def create_tag(
    package_id: int,
    payload: TagCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    created = await diagnostics_service.create_tag(
        db,
        employee=employee,
        package_id=package_id,
        data=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(created.model_dump())


@router.delete("/diagnostic-packages/{package_id}/tags/{tag_id}")
async def delete_tag(
    package_id: int,
    tag_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    await diagnostics_service.delete_tag(
        db,
        employee=employee,
        package_id=package_id,
        tag_id=tag_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"tag_id": tag_id, "deleted": True})


@router.post("/diagnostic-packages/{package_id}/filter-chips", status_code=201)
async def assign_package_filter_chip(
    package_id: int,
    payload: PackageFilterChipAssign,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    created = await diagnostics_service.assign_filter_chip_to_package(
        db,
        employee=employee,
        package_id=package_id,
        data=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(created.model_dump())


@router.delete("/diagnostic-packages/{package_id}/filter-chips/{filter_chip_id}")
async def remove_package_filter_chip(
    package_id: int,
    filter_chip_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    await diagnostics_service.remove_filter_chip_from_package(
        db,
        employee=employee,
        package_id=package_id,
        filter_chip_id=filter_chip_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"filter_chip_id": filter_chip_id, "deleted": True})


@router.post("/diagnostic-packages/{package_id}/samples", status_code=201)
async def create_sample(
    package_id: int,
    payload: SampleCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    created = await diagnostics_service.create_sample(
        db,
        employee=employee,
        package_id=package_id,
        data=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(created.model_dump())


@router.put("/diagnostic-packages/{package_id}/samples/{sample_id}")
async def update_sample(
    package_id: int,
    sample_id: int,
    payload: SampleUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    updated = await diagnostics_service.update_sample(
        db,
        employee=employee,
        package_id=package_id,
        sample_id=sample_id,
        data=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(updated.model_dump())


@router.delete("/diagnostic-packages/{package_id}/samples/{sample_id}")
async def delete_sample(
    package_id: int,
    sample_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    await diagnostics_service.delete_sample(
        db,
        employee=employee,
        package_id=package_id,
        sample_id=sample_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"sample_id": sample_id, "deleted": True})


@router.post("/diagnostic-packages/{package_id}/preparations", status_code=201)
async def create_preparation(
    package_id: int,
    payload: PreparationCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    created = await diagnostics_service.create_preparation(
        db,
        employee=employee,
        package_id=package_id,
        data=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(created.model_dump())


@router.put("/diagnostic-packages/{package_id}/preparations/{preparation_id}")
async def update_preparation(
    package_id: int,
    preparation_id: int,
    payload: PreparationUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    updated = await diagnostics_service.update_preparation(
        db,
        employee=employee,
        package_id=package_id,
        preparation_id=preparation_id,
        data=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(updated.model_dump())


@router.delete("/diagnostic-packages/{package_id}/preparations/{preparation_id}")
async def delete_preparation(
    package_id: int,
    preparation_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    await diagnostics_service.delete_preparation(
        db,
        employee=employee,
        package_id=package_id,
        preparation_id=preparation_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"preparation_id": preparation_id, "deleted": True})


# diagnostics/health-parameters: static routes before dynamic routes
@router.get("/diagnostics/health-parameters")
async def list_health_parameters(
    parameter_type: ParameterType | None = None,
    page: int | None = Query(default=None, ge=1),
    limit: int | None = Query(default=None, ge=1, le=100),
    search: str | None = None,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    pagination = optional_page_params(page=page, limit=limit)
    search_value = sanitize_list_search(search)
    data, total = await diagnostics_service.list_parameters(
        db,
        parameter_type=parameter_type,
        page=pagination.page if pagination else None,
        limit=pagination.limit if pagination else None,
        search=search_value,
    )
    if pagination:
        return success_response(
            [item.model_dump() for item in data],
            meta={"page": pagination.page, "limit": pagination.limit, "total": total},
        )
    return success_response([item.model_dump() for item in data])


@router.post("/diagnostics/health-parameters", status_code=201)
async def create_health_parameter(
    payload: HealthParameterCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    created = await diagnostics_service.create_parameter(
        db,
        employee=employee,
        data=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(created.model_dump())


@router.get("/diagnostics/health-parameters/{test_id}")
async def get_health_parameter(
    test_id: int,
    db: AsyncSession = Depends(get_db),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    data = await diagnostics_service.get_parameter(db, test_id=test_id)
    return success_response(data.model_dump())


@router.put("/diagnostics/health-parameters/{test_id}")
async def update_health_parameter(
    test_id: int,
    payload: HealthParameterUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    updated = await diagnostics_service.update_parameter(
        db,
        employee=employee,
        test_id=test_id,
        data=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(updated.model_dump())


@router.delete("/diagnostics/health-parameters/{test_id}")
async def delete_health_parameter(
    test_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    result = await diagnostics_service.delete_parameter(
        db,
        employee=employee,
        test_id=test_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(result)


# diagnostic-test-groups: static routes before dynamic routes
@router.get("/diagnostic-test-groups")
async def list_diagnostic_test_groups(
    filter_chip: str | None = Query(default=None),
    package_for: str | None = Query(default=None),
    page: int | None = Query(default=None, ge=1),
    limit: int | None = Query(default=None, ge=1, le=100),
    search: str | None = None,
    db: AsyncSession = Depends(get_db),
    _actor: DiagnosticsPackageActor = Depends(get_diagnostics_package_actor),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    pagination = optional_page_params(page=page, limit=limit)
    search_value = sanitize_list_search(search)
    data, total = await diagnostics_service.get_all_groups(
        db,
        filter_chip=filter_chip,
        package_for=package_for,
        page=pagination.page if pagination else None,
        limit=pagination.limit if pagination else None,
        search=search_value,
    )
    if pagination:
        return success_response(
            [item.model_dump() for item in data],
            meta={"page": pagination.page, "limit": pagination.limit, "total": total},
        )
    return success_response([item.model_dump() for item in data])


@router.get("/diagnostic-test-groups/groups")
async def list_diagnostic_test_group_profiles(
    filter_chip: str | None = Query(default=None),
    package_for: str | None = Query(default=None),
    page: int | None = Query(default=None, ge=1),
    limit: int | None = Query(default=None, ge=1, le=100),
    search: str | None = None,
    db: AsyncSession = Depends(get_db),
    _actor: DiagnosticsPackageActor = Depends(get_diagnostics_package_actor),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    pagination = optional_page_params(page=page, limit=limit)
    search_value = sanitize_list_search(search)
    data, total = await diagnostics_service.get_test_group_profiles(
        db,
        filter_chip=filter_chip,
        package_for=package_for,
        page=pagination.page if pagination else None,
        limit=pagination.limit if pagination else None,
        search=search_value,
    )
    if pagination:
        return success_response(
            [item.model_dump() for item in data],
            meta={"page": pagination.page, "limit": pagination.limit, "total": total},
        )
    return success_response([item.model_dump() for item in data])


@router.post("/diagnostic-test-groups", status_code=201)
async def create_diagnostic_test_group(
    payload: TestGroupCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    created = await diagnostics_service.create_group(
        db,
        employee=employee,
        data=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(created.model_dump())


@router.get("/diagnostic-test-groups/{group_id}")
async def get_diagnostic_test_group(
    group_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    data = await diagnostics_service.get_group_detail(db, group_id=group_id)
    return success_response(data.model_dump())


@router.get("/diagnostic-test-groups/{group_id}/packages")
async def list_test_group_packages(
    group_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    data = await diagnostics_service.list_packages_for_test_group(db, group_id=group_id)
    return success_response([item.model_dump() for item in data])


@router.get("/diagnostic-test-groups/{group_id}/provider-test-diff")
async def get_diagnostic_test_group_provider_test_diff(
    group_id: int,
    preview_external_test_group_code: str | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    data = await diagnostics_service.get_test_group_provider_test_diff(
        db,
        group_id=group_id,
        preview_external_test_group_code=preview_external_test_group_code,
    )
    return success_response(data.model_dump())


@router.get("/diagnostic-test-groups/{group_id}/tests")
async def list_group_tests(
    group_id: int,
    db: AsyncSession = Depends(get_db),
    _actor: DiagnosticsPackageActor = Depends(get_diagnostics_package_actor),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    data = await diagnostics_service.get_group_tests(db, group_id=group_id)
    return success_response([item.model_dump() for item in data])


@router.post("/diagnostic-test-groups/{group_id}/filter-chips", status_code=201)
async def assign_test_group_filter_chip(
    group_id: int,
    payload: PackageFilterChipAssign,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    created = await diagnostics_service.assign_filter_chip_to_test_group(
        db,
        employee=employee,
        group_id=group_id,
        data=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(created.model_dump())


@router.delete("/diagnostic-test-groups/{group_id}/filter-chips/{filter_chip_id}")
async def remove_test_group_filter_chip(
    group_id: int,
    filter_chip_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    await diagnostics_service.remove_filter_chip_from_test_group(
        db,
        employee=employee,
        group_id=group_id,
        filter_chip_id=filter_chip_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"filter_chip_id": filter_chip_id, "deleted": True})


@router.post("/diagnostic-test-groups/{group_id}/tests", status_code=201)
async def assign_tests_to_group(
    group_id: int,
    payload: AssignTestsToGroupRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    data = await diagnostics_service.assign_tests_to_group(
        db,
        employee=employee,
        group_id=group_id,
        data=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data.model_dump())


@router.patch("/diagnostic-test-groups/{group_id}/tests/order")
async def reorder_group_tests(
    group_id: int,
    payload: ReorderGroupTestsRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    data = await diagnostics_service.reorder_group_tests(
        db,
        employee=employee,
        group_id=group_id,
        data=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)


@router.put("/diagnostic-test-groups/{group_id}")
async def update_diagnostic_test_group(
    group_id: int,
    payload: TestGroupUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    updated = await diagnostics_service.update_group(
        db,
        employee=employee,
        group_id=group_id,
        data=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(updated.model_dump())


@router.delete("/diagnostic-test-groups/{group_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_diagnostic_test_group(
    group_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    await diagnostics_service.delete_group(
        db,
        employee=employee,
        group_id=group_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.delete("/diagnostic-test-groups/{group_id}/tests/{test_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_test_from_group(
    group_id: int,
    test_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    await diagnostics_service.remove_test_from_group(
        db,
        employee=employee,
        group_id=group_id,
        test_id=test_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/diagnostic-packages/{package_id}/test-groups", status_code=201)
async def assign_groups_to_package(
    package_id: int,
    payload: AssignGroupsToPackageRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    actor: DiagnosticsPackageActor = Depends(get_diagnostics_package_actor),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    data = await diagnostics_service.assign_groups_to_package(
        db,
        employee=actor.employee,
        current_user_id=actor.current_user_id,
        package_id=package_id,
        data=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data.model_dump())


@router.post("/diagnostic-packages/{package_id}/test-groups/{group_id}/fork-for-package", status_code=200)
async def fork_test_group_for_package(
    package_id: int,
    group_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    data = await diagnostics_service.fork_test_group_for_package(
        db,
        employee=employee,
        package_id=package_id,
        group_id=group_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data.model_dump())


@router.patch("/diagnostic-packages/{package_id}/test-groups/order")
async def reorder_package_test_groups(
    package_id: int,
    payload: ReorderPackageGroupsRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    data = await diagnostics_service.reorder_package_groups(
        db,
        employee=employee,
        package_id=package_id,
        data=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)


@router.delete("/diagnostic-packages/{package_id}/test-groups/{group_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_group_from_package(
    package_id: int,
    group_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    actor: DiagnosticsPackageActor = Depends(get_diagnostics_package_actor),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    await diagnostics_service.remove_group_from_package(
        db,
        employee=actor.employee,
        current_user_id=actor.current_user_id,
        package_id=package_id,
        group_id=group_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
