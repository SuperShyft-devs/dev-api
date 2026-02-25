"""Organizations HTTP routes.

These endpoints are employee-only.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.exceptions import AppError
from db.session import get_db
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext
from modules.organizations.dependencies import get_organizations_service
from modules.organizations.schemas import (
    OrganizationCreateRequest,
    OrganizationStatusUpdateRequest,
    OrganizationUpdateRequest,
)
from modules.organizations.service import OrganizationsService


router = APIRouter(prefix="/organizations", tags=["organizations"])


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()

    if request.client is None:
        return "unknown"

    return request.client.host


@router.post("", status_code=201)
async def create_organization(
    payload: OrganizationCreateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    organizations_service: OrganizationsService = Depends(get_organizations_service),
):
    organization = await organizations_service.create_organization_for_employee(
        db,
        employee=employee,
        payload=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"organization_id": organization.organization_id})


@router.get("")
async def list_organizations(
    request: Request,
    page: int = 1,
    limit: int = 20,
    status: str | None = None,
    organization_type: str | None = None,
    bd_employee_id: int | None = None,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    organizations_service: OrganizationsService = Depends(get_organizations_service),
):
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    organizations, total = await organizations_service.list_organizations_for_employee(
        db,
        employee=employee,
        page=page,
        limit=limit,
        status=status,
        organization_type=organization_type,
        bd_employee_id=bd_employee_id,
    )

    data = []
    for organization in organizations:
        data.append(
            {
                "organization_id": organization.organization_id,
                "name": organization.name,
                "organization_type": organization.organization_type,
                "logo": organization.logo,
                "website_url": organization.website_url,
                "city": organization.city,
                "state": organization.state,
                "country": organization.country,
                "status": organization.status,
            }
        )

    return success_response(data, meta={"page": page, "limit": limit, "total": total})


@router.get("/{organization_id}")
async def get_organization_details(
    organization_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    organizations_service: OrganizationsService = Depends(get_organizations_service),
):
    organization = await organizations_service.get_organization_details_for_employee(
        db,
        employee=employee,
        organization_id=organization_id,
    )

    return success_response(
        {
            "organization_id": organization.organization_id,
            "name": organization.name,
            "organization_type": organization.organization_type,
            "logo": organization.logo,
            "website_url": organization.website_url,
            "address": organization.address,
            "pin_code": organization.pin_code,
            "city": organization.city,
            "state": organization.state,
            "country": organization.country,
            "contact_name": organization.contact_name,
            "contact_email": organization.contact_email,
            "contact_phone": organization.contact_phone,
            "contact_designation": organization.contact_designation,
            "bd_employee_id": organization.bd_employee_id,
            "status": organization.status,
            "created_at": organization.created_at,
            "created_employee_id": organization.created_employee_id,
            "updated_at": organization.updated_at,
            "updated_employee_id": organization.updated_employee_id,
        }
    )


@router.put("/{organization_id}")
async def update_organization(
    organization_id: int,
    payload: OrganizationUpdateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    organizations_service: OrganizationsService = Depends(get_organizations_service),
):
    updated = await organizations_service.update_organization_for_employee(
        db,
        employee=employee,
        organization_id=organization_id,
        payload=payload,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()

    return success_response({"organization_id": updated.organization_id})


@router.patch("/{organization_id}/status")
async def update_organization_status(
    organization_id: int,
    payload: OrganizationStatusUpdateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    organizations_service: OrganizationsService = Depends(get_organizations_service),
):
    updated = await organizations_service.change_organization_status_for_employee(
        db,
        employee=employee,
        organization_id=organization_id,
        status=payload.status,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()

    return success_response({"organization_id": updated.organization_id, "status": updated.status})


@router.get("/{organization_id}/participants")
async def get_organization_participants(
    organization_id: int,
    request: Request,
    page: int = 1,
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    organizations_service: OrganizationsService = Depends(get_organizations_service),
):
    """Get all distinct users enrolled across all engagements for an organization.
    
    This endpoint returns a paginated list of participants (users) who are
    enrolled in any engagement belonging to the specified organization.
    """
    # Validate pagination parameters
    if page < 1 or limit < 1 or limit > 100:
        raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")

    # Fetch participants from service
    participants, total = await organizations_service.list_participants_for_organization(
        db,
        employee=employee,
        organization_id=organization_id,
        page=page,
        limit=limit,
    )

    return success_response(participants, meta={"page": page, "limit": limit, "total": total})
