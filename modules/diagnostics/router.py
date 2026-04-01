"""Diagnostics HTTP routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from db.session import get_db
from modules.diagnostics.dependencies import get_diagnostics_service
from modules.diagnostics.schemas import (
    AssignGroupsToPackageRequest,
    AssignTestsToGroupRequest,
    DiagnosticPackageCreate,
    DiagnosticPackageStatusUpdate,
    DiagnosticPackageUpdate,
    FilterCreate,
    FilterUpdate,
    PreparationCreate,
    PreparationUpdate,
    ReasonCreate,
    ReasonUpdate,
    ReorderGroupTestsRequest,
    ReorderPackageGroupsRequest,
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
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext


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
    db: AsyncSession = Depends(get_db),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    data = await diagnostics_service.get_packages(db, gender=gender, tag=tag)
    return success_response([item.model_dump() for item in data])


@router.get("/diagnostic-packages/filters")
async def list_diagnostic_filters(
    db: AsyncSession = Depends(get_db),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    data = await diagnostics_service.get_filters(db)
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


@router.post("/diagnostic-packages/filters", status_code=201)
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


@router.put("/diagnostic-packages/filters/{filter_id}")
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


@router.delete("/diagnostic-packages/filters/{filter_id}")
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


@router.post("/diagnostic-packages", status_code=201)
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


@router.put("/diagnostic-packages/{package_id}")
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
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    data = await diagnostics_service.list_parameters(db, parameter_type=parameter_type)
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
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    data = await diagnostics_service.get_all_groups(db)
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


@router.get("/diagnostic-test-groups/{group_id}/tests")
async def list_group_tests(
    group_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    data = await diagnostics_service.get_group_tests(db, group_id=group_id)
    return success_response([item.model_dump() for item in data])


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
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    data = await diagnostics_service.assign_groups_to_package(
        db,
        employee=employee,
        package_id=package_id,
        data=payload,
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
    employee: EmployeeContext = Depends(get_current_employee),
    diagnostics_service: DiagnosticsService = Depends(get_diagnostics_service),
):
    await diagnostics_service.remove_group_from_package(
        db,
        employee=employee,
        package_id=package_id,
        group_id=group_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
