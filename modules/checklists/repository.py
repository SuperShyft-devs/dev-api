"""Checklists repository — database queries only."""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import case, func, nullslast, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from modules.checklists.models import (
    ChecklistTemplate,
    ChecklistTemplateItem,
    EngagementChecklist,
    EngagementChecklistTask,
)
from modules.checklists.schemas import ChecklistReadiness
from modules.engagements.models import Engagement


class ChecklistsRepository:
    async def get_all_templates(self, db: AsyncSession) -> list[ChecklistTemplate]:
        stmt = select(ChecklistTemplate).order_by(ChecklistTemplate.created_at.desc())
        result = await db.execute(stmt)
        return list(result.scalars().all())

    async def get_template_by_id(self, db: AsyncSession, template_id: int) -> ChecklistTemplate | None:
        stmt = (
            select(ChecklistTemplate)
            .where(ChecklistTemplate.template_id == template_id)
            .options(joinedload(ChecklistTemplate.items))
        )
        result = await db.execute(stmt)
        template = result.unique().scalar_one_or_none()
        if template is None:
            return None
        template.items.sort(
            key=lambda i: (i.display_order is None, i.display_order if i.display_order is not None else 0, i.item_id),
        )
        return template

    async def create_template(
        self,
        db: AsyncSession,
        *,
        name: str,
        description: str | None,
        created_employee_id: int | None,
    ) -> ChecklistTemplate:
        row = ChecklistTemplate(
            name=name,
            description=description,
            created_employee_id=created_employee_id,
        )
        db.add(row)
        await db.flush()
        return row

    async def update_template(self, db: AsyncSession, template_id: int, data: dict) -> ChecklistTemplate | None:
        stmt = select(ChecklistTemplate).where(ChecklistTemplate.template_id == template_id)
        result = await db.execute(stmt)
        template = result.scalar_one_or_none()
        if template is None:
            return None
        for key, value in data.items():
            setattr(template, key, value)
        await db.flush()
        return template

    async def update_template_status(self, db: AsyncSession, template_id: int, status: str) -> ChecklistTemplate | None:
        stmt = select(ChecklistTemplate).where(ChecklistTemplate.template_id == template_id)
        result = await db.execute(stmt)
        template = result.scalar_one_or_none()
        if template is None:
            return None
        template.status = status
        await db.flush()
        return template

    async def create_template_item(
        self,
        db: AsyncSession,
        *,
        template_id: int,
        title: str,
        description: str | None,
        display_order: int | None,
    ) -> ChecklistTemplateItem:
        row = ChecklistTemplateItem(
            template_id=template_id,
            title=title,
            description=description,
            display_order=display_order,
        )
        db.add(row)
        await db.flush()
        return row

    async def update_template_item(self, db: AsyncSession, item_id: int, data: dict) -> ChecklistTemplateItem | None:
        stmt = select(ChecklistTemplateItem).where(ChecklistTemplateItem.item_id == item_id)
        result = await db.execute(stmt)
        row = result.scalar_one_or_none()
        if row is None:
            return None
        for key, value in data.items():
            setattr(row, key, value)
        await db.flush()
        return row

    async def delete_template_item(self, db: AsyncSession, item_id: int) -> None:
        stmt = select(ChecklistTemplateItem).where(ChecklistTemplateItem.item_id == item_id)
        result = await db.execute(stmt)
        row = result.scalar_one_or_none()
        if row is None:
            return
        await db.delete(row)
        await db.flush()

    async def get_template_item_by_id(self, db: AsyncSession, item_id: int) -> ChecklistTemplateItem | None:
        stmt = select(ChecklistTemplateItem).where(ChecklistTemplateItem.item_id == item_id)
        result = await db.execute(stmt)
        return result.scalar_one_or_none()

    async def get_checklists_for_engagement(
        self,
        db: AsyncSession,
        engagement_id: int,
    ) -> list[EngagementChecklist]:
        stmt = (
            select(EngagementChecklist)
            .where(EngagementChecklist.engagement_id == engagement_id)
            .options(
                selectinload(EngagementChecklist.tasks).selectinload(EngagementChecklistTask.item),
                selectinload(EngagementChecklist.template),
            )
            .order_by(EngagementChecklist.checklist_id.asc())
        )
        result = await db.execute(stmt)
        return list(result.scalars().unique().all())

    async def get_checklist_by_id(self, db: AsyncSession, checklist_id: int) -> EngagementChecklist | None:
        stmt = (
            select(EngagementChecklist)
            .where(EngagementChecklist.checklist_id == checklist_id)
            .options(
                selectinload(EngagementChecklist.tasks).selectinload(EngagementChecklistTask.item),
                selectinload(EngagementChecklist.template),
            )
        )
        result = await db.execute(stmt)
        return result.scalars().unique().one_or_none()

    async def checklist_exists(self, db: AsyncSession, engagement_id: int, template_id: int) -> bool:
        stmt = (
            select(EngagementChecklist.checklist_id)
            .where(
                EngagementChecklist.engagement_id == engagement_id,
                EngagementChecklist.template_id == template_id,
            )
            .limit(1)
        )
        result = await db.execute(stmt)
        return result.scalar_one_or_none() is not None

    async def create_checklist(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        template_id: int,
        created_employee_id: int | None,
    ) -> int:
        checklist = EngagementChecklist(
            engagement_id=engagement_id,
            template_id=template_id,
            created_employee_id=created_employee_id,
        )
        db.add(checklist)
        await db.flush()

        items_stmt = (
            select(ChecklistTemplateItem)
            .where(ChecklistTemplateItem.template_id == template_id)
            .order_by(nullslast(ChecklistTemplateItem.display_order.asc()), ChecklistTemplateItem.item_id.asc())
        )
        items_result = await db.execute(items_stmt)
        items = list(items_result.scalars().all())

        for item in items:
            task = EngagementChecklistTask(
                checklist_id=checklist.checklist_id,
                item_id=item.item_id,
                status="pending",
            )
            db.add(task)
        await db.flush()
        return checklist.checklist_id

    async def delete_checklist(self, db: AsyncSession, checklist_id: int) -> None:
        stmt = select(EngagementChecklist).where(EngagementChecklist.checklist_id == checklist_id)
        result = await db.execute(stmt)
        row = result.scalar_one_or_none()
        if row is None:
            return
        await db.delete(row)
        await db.flush()

    async def get_task_by_id(self, db: AsyncSession, task_id: int) -> EngagementChecklistTask | None:
        stmt = (
            select(EngagementChecklistTask)
            .where(EngagementChecklistTask.task_id == task_id)
            .options(joinedload(EngagementChecklistTask.item))
        )
        result = await db.execute(stmt)
        return result.unique().scalar_one_or_none()

    async def assign_task(self, db: AsyncSession, task_id: int, assigned_employee_id: int | None) -> None:
        stmt = select(EngagementChecklistTask).where(EngagementChecklistTask.task_id == task_id)
        result = await db.execute(stmt)
        task = result.scalar_one_or_none()
        if task is None:
            return
        task.assigned_employee_id = assigned_employee_id
        await db.flush()

    async def update_task_status(
        self,
        db: AsyncSession,
        *,
        task_id: int,
        status: str,
        notes: str | None,
        completed_by_employee_id: int | None,
    ) -> None:
        stmt = select(EngagementChecklistTask).where(EngagementChecklistTask.task_id == task_id)
        result = await db.execute(stmt)
        task = result.scalar_one_or_none()
        if task is None:
            return
        if status == "done":
            task.status = "done"
            task.completed_at = datetime.now(timezone.utc)
            task.completed_by_employee_id = completed_by_employee_id
            if notes is not None:
                task.notes = notes
        else:
            task.status = "pending"
            task.completed_at = None
            task.completed_by_employee_id = None
        await db.flush()

    async def update_task(self, db: AsyncSession, task_id: int, data: dict) -> None:
        stmt = select(EngagementChecklistTask).where(EngagementChecklistTask.task_id == task_id)
        result = await db.execute(stmt)
        task = result.scalar_one_or_none()
        if task is None:
            return
        for key, value in data.items():
            setattr(task, key, value)
        await db.flush()

    async def get_engagement_readiness(self, db: AsyncSession, engagement_id: int) -> ChecklistReadiness:
        done_expr = func.coalesce(
            func.sum(case((EngagementChecklistTask.status == "done", 1), else_=0)),
            0,
        )
        total_expr = func.count(EngagementChecklistTask.task_id)
        stmt = (
            select(done_expr, total_expr)
            .select_from(EngagementChecklistTask)
            .join(EngagementChecklist, EngagementChecklist.checklist_id == EngagementChecklistTask.checklist_id)
            .where(EngagementChecklist.engagement_id == engagement_id)
        )
        result = await db.execute(stmt)
        row = result.one_or_none()
        if row is None:
            return ChecklistReadiness(done=0, total=0, percent=0)
        done = int(row[0] or 0)
        total = int(row[1] or 0)
        percent = int(round(done * 100 / total)) if total else 0
        return ChecklistReadiness(done=done, total=total, percent=percent)

    async def get_my_tasks(
        self,
        db: AsyncSession,
        *,
        employee_id: int,
        status_filter: str | None,
    ) -> list[tuple[EngagementChecklistTask, int, str | None, str, str | None]]:
        stmt = (
            select(
                EngagementChecklistTask,
                EngagementChecklist.engagement_id,
                Engagement.engagement_name,
                ChecklistTemplateItem.title,
                ChecklistTemplateItem.description,
            )
            .join(EngagementChecklist, EngagementChecklist.checklist_id == EngagementChecklistTask.checklist_id)
            .join(Engagement, Engagement.engagement_id == EngagementChecklist.engagement_id)
            .join(ChecklistTemplateItem, ChecklistTemplateItem.item_id == EngagementChecklistTask.item_id)
            .where(EngagementChecklistTask.assigned_employee_id == employee_id)
        )
        if status_filter is not None:
            stmt = stmt.where(EngagementChecklistTask.status == status_filter)
        stmt = stmt.order_by(
            nullslast(EngagementChecklistTask.due_date.asc()),
            EngagementChecklistTask.task_id.asc(),
        )
        result = await db.execute(stmt)
        rows = result.all()
        out: list[tuple[EngagementChecklistTask, int, str | None, str, str | None]] = []
        for task, eng_id, eng_name, item_title, item_description in rows:
            out.append((task, eng_id, eng_name, item_title, item_description))
        return out
