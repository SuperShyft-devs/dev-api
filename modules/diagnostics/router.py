"""Diagnostics HTTP routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from db.session import get_db
from modules.diagnostics.dependencies import get_diagnostics_service
from modules.diagnostics.schemas import (
    DiagnosticPackageCreate,
    DiagnosticPackageStatusUpdate,
    DiagnosticPackageUpdate,
    FilterCreate,
    FilterUpdate,
    PreparationCreate,
    PreparationUpdate,
    ReasonCreate,
    ReasonUpdate,
    SampleCreate,
    SampleUpdate,
    TagCreate,
    TestCreate,
    TestGroupCreate,
    TestGroupUpdate,
    TestUpdate,
)
from modules.diagnostics.service import DiagnosticsService
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext


router = APIRouter(prefix="/diagnostic-packages", tags=["diagnostic-packages"])


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client is None:
        return "unknown"
    return request.client.host


@router.get("")
async def list_diagnostic_packages(
    gender: str | None = None,
    tag: str | None = None,
    db: AsyncSession = Depends(get_db),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    data = await diagnostics_service.get_packages(db, gender=gender, tag=tag)
    return success_response([item.model_dump() for item in data])


@router.get("/filters")
async def list_diagnostic_filters(
    db: AsyncSession = Depends(get_db),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    data = await diagnostics_service.get_filters(db)
    return success_response([item.model_dump() for item in data])


@router.get("/{package_id}")
async def get_diagnostic_package_detail(
    package_id: int,
    db: AsyncSession = Depends(get_db),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    data = await diagnostics_service.get_package_detail(db, package_id=package_id)
    return success_response(data.model_dump())


@router.get("/{package_id}/tests")
async def get_diagnostic_package_tests(
    package_id: int,
    db: AsyncSession = Depends(get_db),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    data = await diagnostics_service.get_package_tests(db, package_id=package_id)
    return success_response([item.model_dump() for item in data])


@router.post("/filters", status_code=201)
async def create_filter(
    payload: FilterCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    created = await diagnostics_service.create_filter(
        db,
        employee=employee,
        data=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(created.model_dump())


@router.put("/filters/{filter_id}")
async def update_filter(
    filter_id: int,
    payload: FilterUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    updated = await diagnostics_service.update_filter(
        db,
        employee=employee,
        filter_id=filter_id,
        data=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(updated.model_dump())


@router.delete("/filters/{filter_id}")
async def delete_filter(
    filter_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    await diagnostics_service.delete_filter(
        db,
        employee=employee,
        filter_id=filter_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"filter_id": filter_id, "deleted": True})


@router.post("", status_code=201)
async def create_package(
    payload: DiagnosticPackageCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    created = await diagnostics_service.create_package(
        db,
        employee=employee,
        data=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(created.model_dump())


@router.put("/{package_id}")
async def update_package(
    package_id: int,
    payload: DiagnosticPackageUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    updated = await diagnostics_service.update_package(
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


@router.patch("/{package_id}/status")
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


@router.post("/{package_id}/reasons", status_code=201)
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


@router.put("/{package_id}/reasons/{reason_id}")
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


@router.delete("/{package_id}/reasons/{reason_id}")
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


@router.post("/{package_id}/tags", status_code=201)
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


@router.delete("/{package_id}/tags/{tag_id}")
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


@router.post("/{package_id}/test-groups", status_code=201)
async def create_test_group(
    package_id: int,
    payload: TestGroupCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    created = await diagnostics_service.create_test_group(
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


@router.put("/{package_id}/test-groups/{group_id}")
async def update_test_group(
    package_id: int,
    group_id: int,
    payload: TestGroupUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    updated = await diagnostics_service.update_test_group(
        db,
        employee=employee,
        package_id=package_id,
        group_id=group_id,
        data=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(updated.model_dump())


@router.delete("/{package_id}/test-groups/{group_id}")
async def delete_test_group(
    package_id: int,
    group_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    await diagnostics_service.delete_test_group(
        db,
        employee=employee,
        package_id=package_id,
        group_id=group_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"group_id": group_id, "deleted": True})


@router.post("/{package_id}/test-groups/{group_id}/tests", status_code=201)
async def create_test(
    package_id: int,
    group_id: int,
    payload: TestCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    created = await diagnostics_service.create_test(
        db,
        employee=employee,
        package_id=package_id,
        group_id=group_id,
        data=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(created.model_dump())


@router.put("/{package_id}/test-groups/{group_id}/tests/{test_id}")
async def update_test(
    package_id: int,
    group_id: int,
    test_id: int,
    payload: TestUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    updated = await diagnostics_service.update_test(
        db,
        employee=employee,
        package_id=package_id,
        group_id=group_id,
        test_id=test_id,
        data=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(updated.model_dump())


@router.delete("/{package_id}/test-groups/{group_id}/tests/{test_id}")
async def delete_test(
    package_id: int,
    group_id: int,
    test_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    await diagnostics_service.delete_test(
        db,
        employee=employee,
        package_id=package_id,
        group_id=group_id,
        test_id=test_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"test_id": test_id, "deleted": True})


@router.post("/{package_id}/samples", status_code=201)
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


@router.put("/{package_id}/samples/{sample_id}")
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


@router.delete("/{package_id}/samples/{sample_id}")
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


@router.post("/{package_id}/preparations", status_code=201)
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


@router.put("/{package_id}/preparations/{preparation_id}")
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


@router.delete("/{package_id}/preparations/{preparation_id}")
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
