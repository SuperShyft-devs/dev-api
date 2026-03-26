"""Checklists HTTP routes — no business logic."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request, Response
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.dependencies import get_current_user
from db.session import get_db
from modules.checklists.dependencies import get_checklists_service
from modules.checklists.schemas import (
    ApplyTemplateRequest,
    ChecklistTemplateCreate,
    ChecklistTemplateItemCreate,
    ChecklistTemplateItemUpdate,
    ChecklistTemplateStatusUpdate,
    ChecklistTemplateUpdate,
    TaskAssignRequest,
    TaskStatusUpdate,
    TaskUpdate,
)
from modules.checklists.service import ChecklistsService
from modules.employee.dependencies import get_current_employee, get_optional_employee
from modules.employee.service import EmployeeContext

router = APIRouter(tags=["checklists"])


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client is None:
        return "unknown"
    return request.client.host


@router.get("/checklist/my-tasks")
async def list_my_checklist_tasks(
    status: str | None = None,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: ChecklistsService = Depends(get_checklists_service),
):
    rows = await service.get_my_tasks(db, current_employee=employee, status_filter=status)
    return success_response([r.model_dump(mode="json") for r in rows])


@router.get("/checklist-templates")
async def list_checklist_templates(
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: ChecklistsService = Depends(get_checklists_service),
):
    rows = await service.get_all_templates(db)
    return success_response([r.model_dump(mode="json") for r in rows])


@router.post("/checklist-templates", status_code=201)
async def create_checklist_template(
    payload: ChecklistTemplateCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: ChecklistsService = Depends(get_checklists_service),
):
    row = await service.create_template(
        db,
        data=payload,
        current_employee=employee,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(row.model_dump(mode="json"))


@router.get("/checklist-templates/{template_id}")
async def get_checklist_template_detail(
    template_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: ChecklistsService = Depends(get_checklists_service),
):
    row = await service.get_template_detail(db, template_id)
    return success_response(row.model_dump(mode="json"))


@router.put("/checklist-templates/{template_id}")
async def update_checklist_template(
    template_id: int,
    payload: ChecklistTemplateUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: ChecklistsService = Depends(get_checklists_service),
):
    row = await service.update_template(
        db,
        template_id=template_id,
        data=payload,
        current_employee=employee,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(row.model_dump(mode="json"))


@router.patch("/checklist-templates/{template_id}/status")
async def update_checklist_template_status(
    template_id: int,
    payload: ChecklistTemplateStatusUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: ChecklistsService = Depends(get_checklists_service),
):
    await service.update_template_status(
        db,
        template_id=template_id,
        data=payload,
        current_employee=employee,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response({"template_id": template_id, "status": payload.status})


@router.post("/checklist-templates/{template_id}/items", status_code=201)
async def add_checklist_template_item(
    template_id: int,
    payload: ChecklistTemplateItemCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: ChecklistsService = Depends(get_checklists_service),
):
    row = await service.add_template_item(
        db,
        template_id=template_id,
        data=payload,
        current_employee=employee,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(row.model_dump(mode="json"))


@router.put("/checklist-templates/{template_id}/items/{item_id}")
async def update_checklist_template_item(
    template_id: int,
    item_id: int,
    payload: ChecklistTemplateItemUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: ChecklistsService = Depends(get_checklists_service),
):
    row = await service.update_template_item(
        db,
        template_id=template_id,
        item_id=item_id,
        data=payload,
        current_employee=employee,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(row.model_dump(mode="json"))


@router.delete("/checklist-templates/{template_id}/items/{item_id}", status_code=204)
async def delete_checklist_template_item(
    template_id: int,
    item_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: ChecklistsService = Depends(get_checklists_service),
):
    await service.delete_template_item(
        db,
        template_id=template_id,
        item_id=item_id,
        current_employee=employee,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return Response(status_code=204)


@router.patch("/checklist/tasks/{task_id}/assign")
async def assign_checklist_task(
    task_id: int,
    payload: TaskAssignRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: ChecklistsService = Depends(get_checklists_service),
):
    row = await service.assign_task(
        db,
        task_id=task_id,
        data=payload,
        current_employee=employee,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(row.model_dump(mode="json"))


@router.patch("/checklist/tasks/{task_id}/status")
async def update_checklist_task_status(
    task_id: int,
    payload: TaskStatusUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: ChecklistsService = Depends(get_checklists_service),
):
    row = await service.update_task_status(
        db,
        task_id=task_id,
        data=payload,
        current_employee=employee,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(row.model_dump(mode="json"))


@router.put("/checklist/tasks/{task_id}")
async def update_checklist_task(
    task_id: int,
    payload: TaskUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: ChecklistsService = Depends(get_checklists_service),
):
    row = await service.update_task(
        db,
        task_id=task_id,
        data=payload,
        current_employee=employee,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(row.model_dump(mode="json"))


@router.get("/engagements/{engagement_id}/checklists")
async def list_engagement_checklists(
    engagement_id: int,
    db: AsyncSession = Depends(get_db),
    _current_user=Depends(get_current_user),
    employee: EmployeeContext | None = Depends(get_optional_employee),
    service: ChecklistsService = Depends(get_checklists_service),
):
    if employee is not None:
        rows = await service.get_engagement_checklists(db, engagement_id)
        return success_response([r.model_dump(mode="json") for r in rows])

    rows = await service.get_engagement_user_facing_checklists(db, engagement_id)
    return success_response([r.model_dump(mode="json") for r in rows])


@router.post("/engagements/{engagement_id}/checklists", status_code=201)
async def apply_checklist_template_to_engagement(
    engagement_id: int,
    payload: ApplyTemplateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: ChecklistsService = Depends(get_checklists_service),
):
    row = await service.apply_template_to_engagement(
        db,
        engagement_id=engagement_id,
        data=payload,
        current_employee=employee,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return success_response(row.model_dump(mode="json"))


@router.delete("/engagements/{engagement_id}/checklists/{checklist_id}", status_code=204)
async def remove_engagement_checklist(
    engagement_id: int,
    checklist_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: ChecklistsService = Depends(get_checklists_service),
):
    await service.remove_checklist_from_engagement(
        db,
        engagement_id=engagement_id,
        checklist_id=checklist_id,
        current_employee=employee,
        ip_address=_client_ip(request),
        user_agent=request.headers.get("User-Agent", "unknown"),
        endpoint=str(request.url.path),
    )
    await db.commit()
    return Response(status_code=204)


@router.get("/engagements/{engagement_id}/readiness")
async def get_engagement_checklist_readiness(
    engagement_id: int,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: ChecklistsService = Depends(get_checklists_service),
):
    row = await service.get_engagement_readiness(db, engagement_id)
    return success_response(row.model_dump(mode="json"))
