"""Assessment packages HTTP routes.

These endpoints are employee-only.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.exceptions import AppError
from db.session import get_db
from modules.assessments.dependencies import (
    get_assessment_package_categories_service,
    get_assessment_packages_service,
)
from modules.assessments.package_questions_service import AssessmentPackageCategoriesService
from modules.assessments.schemas import (
    AssessmentPackageCategoriesAddRequest,
    AssessmentPackageCreateRequest,
    AssessmentPackageUpdateRequest,
    AssessmentStatusUpdateRequest,
)
from modules.assessments.packages_service import AssessmentPackagesService
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext


router = APIRouter(prefix="/assessment-packages", tags=["assessment-packages"])


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()

    if request.client is None:
        return "unknown"

    return request.client.host


@router.post("", status_code=201)
async def create_assessment_package(
    payload: AssessmentPackageCreateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    packages_service: AssessmentPackagesService = Depends(get_assessment_packages_service),
):
    created = await packages_service.create_package_for_employee(
        db,
        employee=employee,
        package_code=payload.package_code,
        display_name=payload.display_name,
        status=payload.status,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()

    return success_response({"package_id": created.package_id})


@router.get("")
async def list_assessment_packages(
    request: Request,
    page: int = 1,
    limit: int = 20,
    status: str | None = None,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    packages_service: AssessmentPackagesService = Depends(get_assessment_packages_service),
):
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    rows, total = await packages_service.list_packages_for_employee(
        db,
        employee=employee,
        page=page,
        limit=limit,
        status=status,
    )

    data = []
    for package in rows:
        data.append(
            {
                "package_id": package.package_id,
                "package_code": package.package_code,
                "display_name": package.display_name,
                "status": package.status,
            }
        )

    return success_response(data, meta={"page": page, "limit": limit, "total": total})


@router.get("/{package_id}")
async def get_assessment_package_details(
    package_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    packages_service: AssessmentPackagesService = Depends(get_assessment_packages_service),
):
    package = await packages_service.get_package_details_for_employee(db, employee=employee, package_id=package_id)

    return success_response(
        {
            "package_id": package.package_id,
            "package_code": package.package_code,
            "display_name": package.display_name,
            "status": package.status,
        }
    )


@router.put("/{package_id}")
async def update_assessment_package_details(
    package_id: int,
    payload: AssessmentPackageUpdateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    packages_service: AssessmentPackagesService = Depends(get_assessment_packages_service),
):
    updated = await packages_service.update_package_for_employee(
        db,
        employee=employee,
        package_id=package_id,
        package_code=payload.package_code,
        display_name=payload.display_name,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()

    return success_response({"package_id": updated.package_id})


@router.patch("/{package_id}/status")
async def update_assessment_package_status(
    package_id: int,
    payload: AssessmentStatusUpdateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    packages_service: AssessmentPackagesService = Depends(get_assessment_packages_service),
):
    updated = await packages_service.update_package_status_for_employee(
        db,
        employee=employee,
        package_id=package_id,
        status=payload.status,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()

    return success_response({"package_id": updated.package_id, "status": updated.status})


@router.get("/{package_id}/categories")
async def list_package_categories(
    package_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: AssessmentPackageCategoriesService = Depends(get_assessment_package_categories_service),
):
    data = await service.list_categories_for_package(db, employee=employee, package_id=package_id)
    return success_response(data)


@router.post("/{package_id}/categories", status_code=201)
async def add_categories_to_package(
    package_id: int,
    payload: AssessmentPackageCategoriesAddRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: AssessmentPackageCategoriesService = Depends(get_assessment_package_categories_service),
):
    data = await service.add_categories_to_package(
        db,
        employee=employee,
        package_id=package_id,
        category_ids=payload.category_ids,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)


@router.delete("/{package_id}/categories/{category_id}")
async def remove_category_from_package(
    package_id: int,
    category_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: AssessmentPackageCategoriesService = Depends(get_assessment_package_categories_service),
):
    data = await service.remove_category_from_package(
        db,
        employee=employee,
        package_id=package_id,
        category_id=category_id,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(data)
