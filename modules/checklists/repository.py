"""Checklists repository.

Only database queries live here.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import and_, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from modules.checklists.models import (
    ChecklistTemplate,
    ChecklistTemplateItem,
    EngagementChecklist,
    EngagementChecklistTask,
)


class ChecklistsRepository:
    async def get_all_templates(self, db: AsyncSession) -> list[ChecklistTemplate]:
        res = await db.execute(select(ChecklistTemplate).order_by(ChecklistTemplate.template_id.desc()))
        return list(res.scalars().all())

    async def get_template_by_id(self, db: AsyncSession, template_id: int) -> ChecklistTemplate | None:
        q = (
            select(ChecklistTemplate)
            .where(ChecklistTemplate.template_id == template_id)
            .options(selectinload(ChecklistTemplate.items))
        )
        res = await db.execute(q)
        return res.scalar_one_or_none()

    async def create_template(
        self,
        db: AsyncSession,
        *,
        name: str,
        description: str | None,
        audience: str,
        created_employee_id: int | None,
    ) -> ChecklistTemplate:
        row = ChecklistTemplate(
            name=name,
            description=description,
            status="active",
            audience=audience,
            created_at=datetime.utcnow(),
            created_employee_id=created_employee_id,
        )
        db.add(row)
        await db.flush()
        return row

    async def update_template(
        self,
        db: AsyncSession,
        template_id: int,
        patch: dict,
    ) -> ChecklistTemplate | None:
        await db.execute(update(ChecklistTemplate).where(ChecklistTemplate.template_id == template_id).values(**patch))
        await db.flush()
        return await self.get_template_by_id(db, template_id)

    async def update_template_status(self, db: AsyncSession, template_id: int, status: str) -> ChecklistTemplate | None:
        await db.execute(
            update(ChecklistTemplate)
            .where(ChecklistTemplate.template_id == template_id)
            .values(status=status)
        )
        await db.flush()
        return await self.get_template_by_id(db, template_id)

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

    async def get_template_item_by_id(self, db: AsyncSession, item_id: int) -> ChecklistTemplateItem | None:
        res = await db.execute(select(ChecklistTemplateItem).where(ChecklistTemplateItem.item_id == item_id))
        return res.scalar_one_or_none()

    async def update_template_item(self, db: AsyncSession, item_id: int, patch: dict) -> ChecklistTemplateItem | None:
        await db.execute(update(ChecklistTemplateItem).where(ChecklistTemplateItem.item_id == item_id).values(**patch))
        await db.flush()
        return await self.get_template_item_by_id(db, item_id)

    async def delete_template_item(self, db: AsyncSession, item_id: int) -> None:
        item = await self.get_template_item_by_id(db, item_id)
        if item is None:
            return
        await db.delete(item)
        await db.flush()

    async def checklist_exists(self, db: AsyncSession, engagement_id: int, template_id: int) -> bool:
        res = await db.execute(
            select(func.count())
            .select_from(EngagementChecklist)
            .where(and_(EngagementChecklist.engagement_id == engagement_id, EngagementChecklist.template_id == template_id))
        )
        return int(res.scalar_one()) > 0

    async def create_checklist(
        self,
        db: AsyncSession,
        *,
        engagement_id: int,
        template_id: int,
        created_employee_id: int | None,
    ) -> int:
        row = EngagementChecklist(
            engagement_id=engagement_id,
            template_id=template_id,
            created_at=datetime.utcnow(),
            created_employee_id=created_employee_id,
        )
        db.add(row)
        await db.flush()
        return int(row.checklist_id)

    async def create_tasks_for_checklist(self, db: AsyncSession, *, checklist_id: int, template_id: int) -> int:
        """Create tasks for all template items. Returns count created."""
        res = await db.execute(select(ChecklistTemplateItem).where(ChecklistTemplateItem.template_id == template_id))
        items = list(res.scalars().all())
        if not items:
            return 0
        for item in items:
            db.add(
                EngagementChecklistTask(
                    checklist_id=checklist_id,
                    item_id=item.item_id,
                    assigned_employee_id=None,
                    status="pending",
                )
            )
        await db.flush()
        return len(items)

    async def get_checklist_by_id(self, db: AsyncSession, checklist_id: int) -> EngagementChecklist | None:
        q = (
            select(EngagementChecklist)
            .where(EngagementChecklist.checklist_id == checklist_id)
            .options(
                selectinload(EngagementChecklist.template),
                selectinload(EngagementChecklist.tasks).selectinload(EngagementChecklistTask.item),
            )
        )
        res = await db.execute(q)
        return res.scalar_one_or_none()

    async def delete_checklist(self, db: AsyncSession, checklist_id: int) -> None:
        row = await self.get_checklist_by_id(db, checklist_id)
        if row is None:
            return
        await db.delete(row)
        await db.flush()

    async def get_checklists_for_engagement(self, db: AsyncSession, engagement_id: int) -> list[EngagementChecklist]:
        q = (
            select(EngagementChecklist)
            .where(EngagementChecklist.engagement_id == engagement_id)
            .order_by(EngagementChecklist.checklist_id.asc())
            .options(
                selectinload(EngagementChecklist.template),
                selectinload(EngagementChecklist.tasks).selectinload(EngagementChecklistTask.item),
            )
        )
        res = await db.execute(q)
        return list(res.scalars().all())

    async def get_user_facing_checklists_for_engagement(
        self,
        db: AsyncSession,
        engagement_id: int,
    ) -> list[EngagementChecklist]:
        q = (
            select(EngagementChecklist)
            .join(ChecklistTemplate, ChecklistTemplate.template_id == EngagementChecklist.template_id)
            .where(EngagementChecklist.engagement_id == engagement_id)
            .where(ChecklistTemplate.audience == "user")
            .order_by(EngagementChecklist.checklist_id.asc())
            .options(selectinload(EngagementChecklist.template).selectinload(ChecklistTemplate.items))
        )
        res = await db.execute(q)
        return list(res.scalars().all())

    async def get_engagement_readiness(self, db: AsyncSession, engagement_id: int):
        res = await db.execute(
            select(EngagementChecklistTask.status)
            .join(EngagementChecklist, EngagementChecklist.checklist_id == EngagementChecklistTask.checklist_id)
            .where(EngagementChecklist.engagement_id == engagement_id)
        )
        statuses = [s for (s,) in res.all()]
        total = len(statuses)
        done = sum(1 for s in statuses if (s or "").lower() == "done")
        percent = int(round(done * 100 / total)) if total else 0
        return {"done": done, "total": total, "percent": percent}

    async def get_task_by_id(self, db: AsyncSession, task_id: int) -> EngagementChecklistTask | None:
        q = (
            select(EngagementChecklistTask)
            .where(EngagementChecklistTask.task_id == task_id)
            .options(selectinload(EngagementChecklistTask.item))
        )
        res = await db.execute(q)
        return res.scalar_one_or_none()

    async def assign_task(self, db: AsyncSession, task_id: int, assigned_employee_id: int | None) -> None:
        await db.execute(
            update(EngagementChecklistTask)
            .where(EngagementChecklistTask.task_id == task_id)
            .values(assigned_employee_id=assigned_employee_id)
        )
        await db.flush()

    async def update_task_status(
        self,
        db: AsyncSession,
        *,
        task_id: int,
        status: str,
        notes: str | None,
        completed_by_employee_id: int,
    ) -> None:
        values = {"status": status, "notes": notes}
        if status.lower() == "done":
            values["completed_at"] = datetime.utcnow()
            values["completed_by_employee_id"] = completed_by_employee_id
        else:
            values["completed_at"] = None
            values["completed_by_employee_id"] = None
        await db.execute(update(EngagementChecklistTask).where(EngagementChecklistTask.task_id == task_id).values(**values))
        await db.flush()

    async def update_task(self, db: AsyncSession, task_id: int, patch: dict) -> None:
        await db.execute(update(EngagementChecklistTask).where(EngagementChecklistTask.task_id == task_id).values(**patch))
        await db.flush()

    async def get_my_tasks(
        self,
        db: AsyncSession,
        *,
        employee_id: int,
        status_filter: str | None,
    ):
        from modules.engagements.models import Engagement

        q = (
            select(
                EngagementChecklistTask,
                Engagement.engagement_id,
                Engagement.engagement_name,
                ChecklistTemplateItem.title,
                ChecklistTemplateItem.description,
            )
            .join(EngagementChecklist, EngagementChecklist.checklist_id == EngagementChecklistTask.checklist_id)
            .join(Engagement, Engagement.engagement_id == EngagementChecklist.engagement_id)
            .join(ChecklistTemplateItem, ChecklistTemplateItem.item_id == EngagementChecklistTask.item_id)
            .where(EngagementChecklistTask.assigned_employee_id == employee_id)
            .order_by(EngagementChecklistTask.task_id.desc())
        )
        if status_filter is not None:
            q = q.where(EngagementChecklistTask.status == status_filter)
        res = await db.execute(q)
        return list(res.all())
