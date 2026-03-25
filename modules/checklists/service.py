"""Checklists service — business rules."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from core.exceptions import AppError
from modules.audit.service import AuditService
from modules.engagements.service import EngagementsService
from modules.checklists.models import EngagementChecklist, EngagementChecklistTask
from modules.checklists.repository import ChecklistsRepository
from modules.checklists.schemas import (
    ApplyTemplateRequest,
    ChecklistReadiness,
    ChecklistTemplateCreate,
    ChecklistTemplateDetailResponse,
    ChecklistTemplateItemCreate,
    ChecklistTemplateItemResponse,
    ChecklistTemplateItemUpdate,
    ChecklistTemplateResponse,
    ChecklistTemplateStatusUpdate,
    ChecklistTemplateUpdate,
    EngagementChecklistResponse,
    MyTaskResponse,
    TaskAssignRequest,
    TaskResponse,
    TaskStatusUpdate,
    TaskUpdate,
)
from modules.employee.service import EmployeeContext


def _readiness_from_tasks(tasks: list[EngagementChecklistTask]) -> ChecklistReadiness:
    total = len(tasks)
    done = sum(1 for t in tasks if (t.status or "").lower() == "done")
    percent = int(round(done * 100 / total)) if total else 0
    return ChecklistReadiness(done=done, total=total, percent=percent)


def _task_to_response(
    task: EngagementChecklistTask,
    *,
    item_title: str | None = None,
    item_description: str | None = None,
) -> TaskResponse:
    title = item_title
    desc = item_description
    if title is None:
        item = task.item
        title = item.title if item is not None else ""
        desc = item.description if item is not None else None
    return TaskResponse(
        task_id=task.task_id,
        checklist_id=task.checklist_id,
        item_id=task.item_id,
        item_title=title,
        item_description=desc,
        assigned_employee_id=task.assigned_employee_id,
        status=task.status,
        notes=task.notes,
        due_date=task.due_date,
        completed_at=task.completed_at,
        completed_by_employee_id=task.completed_by_employee_id,
    )


def _checklist_to_response(checklist: EngagementChecklist) -> EngagementChecklistResponse:
    tasks = list(checklist.tasks or [])
    readiness = _readiness_from_tasks(tasks)
    template_name = checklist.template.name if checklist.template is not None else ""
    task_responses = [_task_to_response(t) for t in tasks]
    return EngagementChecklistResponse(
        checklist_id=checklist.checklist_id,
        engagement_id=checklist.engagement_id,
        template_id=checklist.template_id,
        template_name=template_name,
        created_at=checklist.created_at,
        readiness=readiness,
        tasks=task_responses,
    )


class ChecklistsService:
    def __init__(
        self,
        repository: ChecklistsRepository,
        audit_service: AuditService | None = None,
        engagements_service: EngagementsService | None = None,
    ):
        self._repository = repository
        self._audit_service = audit_service
        self._engagements_service = engagements_service

    def _require_audit_service(self) -> AuditService:
        if self._audit_service is None:
            raise RuntimeError("Audit service is required")
        return self._audit_service

    def _require_engagements_service(self) -> EngagementsService:
        if self._engagements_service is None:
            raise RuntimeError("Engagements service is required")
        return self._engagements_service

    def _ensure_employee(self, employee: EmployeeContext | None) -> None:
        if employee is None:
            raise AppError(
                status_code=403,
                error_code="FORBIDDEN",
                message="You do not have permission to perform this action",
            )

    async def get_all_templates(self, db: AsyncSession) -> list[ChecklistTemplateResponse]:
        rows = await self._repository.get_all_templates(db)
        return [
            ChecklistTemplateResponse(
                template_id=r.template_id,
                name=r.name,
                description=r.description,
                status=r.status,
                created_at=r.created_at,
                created_employee_id=r.created_employee_id,
            )
            for r in rows
        ]

    async def get_template_detail(self, db: AsyncSession, template_id: int) -> ChecklistTemplateDetailResponse:
        template = await self._repository.get_template_by_id(db, template_id)
        if template is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Template does not exist")
        items = [
            ChecklistTemplateItemResponse(
                item_id=i.item_id,
                template_id=i.template_id,
                title=i.title,
                description=i.description,
                display_order=i.display_order,
            )
            for i in template.items
        ]
        return ChecklistTemplateDetailResponse(
            template_id=template.template_id,
            name=template.name,
            description=template.description,
            status=template.status,
            created_at=template.created_at,
            created_employee_id=template.created_employee_id,
            items=items,
        )

    async def create_template(
        self,
        db: AsyncSession,
        *,
        data: ChecklistTemplateCreate,
        current_employee: EmployeeContext,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> ChecklistTemplateResponse:
        self._ensure_employee(current_employee)
        row = await self._repository.create_template(
            db,
            name=data.name,
            description=data.description,
            created_employee_id=current_employee.employee_id,
        )
        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="CHECKLIST_CREATE_TEMPLATE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=current_employee.user_id,
            session_id=None,
        )
        return ChecklistTemplateResponse(
            template_id=row.template_id,
            name=row.name,
            description=row.description,
            status=row.status,
            created_at=row.created_at,
            created_employee_id=row.created_employee_id,
        )

    async def update_template(
        self,
        db: AsyncSession,
        *,
        template_id: int,
        data: ChecklistTemplateUpdate,
        current_employee: EmployeeContext,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> ChecklistTemplateResponse:
        self._ensure_employee(current_employee)
        exists = await self._repository.get_template_by_id(db, template_id)
        if exists is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Template does not exist")
        payload = data.model_dump(exclude_unset=True)
        if not payload:
            row = exists
        else:
            updated = await self._repository.update_template(db, template_id, payload)
            if updated is None:
                raise AppError(status_code=404, error_code="NOT_FOUND", message="Template does not exist")
            row = updated
        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="CHECKLIST_UPDATE_TEMPLATE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=current_employee.user_id,
            session_id=None,
        )
        return ChecklistTemplateResponse(
            template_id=row.template_id,
            name=row.name,
            description=row.description,
            status=row.status,
            created_at=row.created_at,
            created_employee_id=row.created_employee_id,
        )

    async def update_template_status(
        self,
        db: AsyncSession,
        *,
        template_id: int,
        data: ChecklistTemplateStatusUpdate,
        current_employee: EmployeeContext,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        self._ensure_employee(current_employee)
        if data.status not in ("active", "inactive"):
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        exists = await self._repository.get_template_by_id(db, template_id)
        if exists is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Template does not exist")
        updated = await self._repository.update_template_status(db, template_id, data.status)
        if updated is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Template does not exist")
        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="CHECKLIST_UPDATE_TEMPLATE_STATUS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=current_employee.user_id,
            session_id=None,
        )

    async def add_template_item(
        self,
        db: AsyncSession,
        *,
        template_id: int,
        data: ChecklistTemplateItemCreate,
        current_employee: EmployeeContext,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> ChecklistTemplateItemResponse:
        self._ensure_employee(current_employee)
        template = await self._repository.get_template_by_id(db, template_id)
        if template is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Template does not exist")
        row = await self._repository.create_template_item(
            db,
            template_id=template_id,
            title=data.title,
            description=data.description,
            display_order=data.display_order,
        )
        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="CHECKLIST_ADD_TEMPLATE_ITEM",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=current_employee.user_id,
            session_id=None,
        )
        return ChecklistTemplateItemResponse(
            item_id=row.item_id,
            template_id=row.template_id,
            title=row.title,
            description=row.description,
            display_order=row.display_order,
        )

    async def update_template_item(
        self,
        db: AsyncSession,
        *,
        template_id: int,
        item_id: int,
        data: ChecklistTemplateItemUpdate,
        current_employee: EmployeeContext,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> ChecklistTemplateItemResponse:
        self._ensure_employee(current_employee)
        template = await self._repository.get_template_by_id(db, template_id)
        if template is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Template does not exist")
        item = await self._repository.get_template_item_by_id(db, item_id)
        if item is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Item does not exist")
        if item.template_id != template_id:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        payload = data.model_dump(exclude_unset=True)
        updated = await self._repository.update_template_item(db, item_id, payload)
        if updated is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Item does not exist")
        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="CHECKLIST_UPDATE_TEMPLATE_ITEM",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=current_employee.user_id,
            session_id=None,
        )
        return ChecklistTemplateItemResponse(
            item_id=updated.item_id,
            template_id=updated.template_id,
            title=updated.title,
            description=updated.description,
            display_order=updated.display_order,
        )

    async def delete_template_item(
        self,
        db: AsyncSession,
        *,
        template_id: int,
        item_id: int,
        current_employee: EmployeeContext,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        self._ensure_employee(current_employee)
        template = await self._repository.get_template_by_id(db, template_id)
        if template is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Template does not exist")
        item = await self._repository.get_template_item_by_id(db, item_id)
        if item is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Item does not exist")
        if item.template_id != template_id:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        await self._repository.delete_template_item(db, item_id)
        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="CHECKLIST_DELETE_TEMPLATE_ITEM",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=current_employee.user_id,
            session_id=None,
        )

    async def get_engagement_checklists(self, db: AsyncSession, engagement_id: int) -> list[EngagementChecklistResponse]:
        rows = await self._repository.get_checklists_for_engagement(db, engagement_id)
        return [_checklist_to_response(c) for c in rows]

    async def apply_template_to_engagement(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        data: ApplyTemplateRequest,
        current_employee: EmployeeContext,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> EngagementChecklistResponse:
        self._ensure_employee(current_employee)
        engagements = self._require_engagements_service()
        engagement = await engagements.get_by_id(db, engagement_id)
        if engagement is None:
            raise AppError(status_code=404, error_code="ENGAGEMENT_NOT_FOUND", message="Engagement does not exist")

        template = await self._repository.get_template_by_id(db, data.template_id)
        if template is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Template does not exist")
        if (template.status or "").lower() != "active":
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="Cannot apply inactive template",
            )
        if await self._repository.checklist_exists(db, engagement_id, data.template_id):
            raise AppError(
                status_code=400,
                error_code="INVALID_INPUT",
                message="This template is already applied to this engagement",
            )

        checklist_id = await self._repository.create_checklist(
            db,
            engagement_id=engagement_id,
            template_id=data.template_id,
            created_employee_id=current_employee.employee_id,
        )

        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="CHECKLIST_APPLY_TEMPLATE",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=current_employee.user_id,
            session_id=None,
        )

        created = await self._repository.get_checklist_by_id(db, checklist_id)
        if created is None:
            raise AppError(status_code=500, error_code="INTERNAL_ERROR", message="An unexpected error occurred")
        return _checklist_to_response(created)

    async def remove_checklist_from_engagement(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        checklist_id: int,
        current_employee: EmployeeContext,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> None:
        self._ensure_employee(current_employee)
        checklist = await self._repository.get_checklist_by_id(db, checklist_id)
        if checklist is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Checklist does not exist")
        if checklist.engagement_id != engagement_id:
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        await self._repository.delete_checklist(db, checklist_id)
        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="CHECKLIST_REMOVE_CHECKLIST",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=current_employee.user_id,
            session_id=None,
        )

    async def get_engagement_readiness(self, db: AsyncSession, engagement_id: int) -> ChecklistReadiness:
        return await self._repository.get_engagement_readiness(db, engagement_id)

    async def assign_task(
        self,
        db: AsyncSession,
        *,
        task_id: int,
        data: TaskAssignRequest,
        current_employee: EmployeeContext,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> TaskResponse:
        self._ensure_employee(current_employee)
        task = await self._repository.get_task_by_id(db, task_id)
        if task is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Task does not exist")
        await self._repository.assign_task(db, task_id, data.assigned_employee_id)
        task = await self._repository.get_task_by_id(db, task_id)
        if task is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Task does not exist")
        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="CHECKLIST_ASSIGN_TASK",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=current_employee.user_id,
            session_id=None,
        )
        return _task_to_response(task)

    async def update_task_status(
        self,
        db: AsyncSession,
        *,
        task_id: int,
        data: TaskStatusUpdate,
        current_employee: EmployeeContext,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> TaskResponse:
        self._ensure_employee(current_employee)
        if data.status not in ("pending", "done"):
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        task = await self._repository.get_task_by_id(db, task_id)
        if task is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Task does not exist")
        await self._repository.update_task_status(
            db,
            task_id=task_id,
            status=data.status,
            notes=data.notes,
            completed_by_employee_id=current_employee.employee_id,
        )
        task = await self._repository.get_task_by_id(db, task_id)
        if task is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Task does not exist")
        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="CHECKLIST_UPDATE_TASK_STATUS",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=current_employee.user_id,
            session_id=None,
        )
        return _task_to_response(task)

    async def update_task(
        self,
        db: AsyncSession,
        *,
        task_id: int,
        data: TaskUpdate,
        current_employee: EmployeeContext,
        ip_address: str,
        user_agent: str,
        endpoint: str,
    ) -> TaskResponse:
        self._ensure_employee(current_employee)
        task = await self._repository.get_task_by_id(db, task_id)
        if task is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Task does not exist")
        payload = data.model_dump(exclude_unset=True)
        if payload:
            await self._repository.update_task(db, task_id, payload)
        task = await self._repository.get_task_by_id(db, task_id)
        if task is None:
            raise AppError(status_code=404, error_code="NOT_FOUND", message="Task does not exist")
        audit = self._require_audit_service()
        await audit.log_event(
            db,
            action="CHECKLIST_UPDATE_TASK",
            endpoint=endpoint,
            ip_address=ip_address,
            user_agent=user_agent,
            user_id=current_employee.user_id,
            session_id=None,
        )
        return _task_to_response(task)

    async def get_my_tasks(
        self,
        db: AsyncSession,
        *,
        current_employee: EmployeeContext,
        status_filter: str | None,
    ) -> list[MyTaskResponse]:
        self._ensure_employee(current_employee)
        if status_filter is not None and status_filter not in ("pending", "done"):
            raise AppError(status_code=400, error_code="INVALID_INPUT", message="Invalid request")
        rows = await self._repository.get_my_tasks(
            db,
            employee_id=current_employee.employee_id,
            status_filter=status_filter,
        )
        out: list[MyTaskResponse] = []
        for task, eng_id, eng_name, item_title, item_description in rows:
            base = _task_to_response(task, item_title=item_title, item_description=item_description)
            out.append(
                MyTaskResponse(
                    **base.model_dump(),
                    engagement_id=eng_id,
                    engagement_name=eng_name,
                )
            )
        return out
