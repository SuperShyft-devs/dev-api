"""Pydantic schemas for checklists APIs."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field


class ChecklistTemplateItemCreate(BaseModel):
    title: str = Field(min_length=1)
    description: str | None = None
    display_order: int | None = None


class ChecklistTemplateItemUpdate(BaseModel):
    title: str | None = Field(default=None, min_length=1)
    description: str | None = None
    display_order: int | None = None


class ChecklistTemplateItemResponse(BaseModel):
    item_id: int
    template_id: int
    title: str
    description: str | None
    display_order: int | None


class ChecklistTemplateCreate(BaseModel):
    name: str = Field(min_length=1)
    description: str | None = None


class ChecklistTemplateUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1)
    description: str | None = None


class ChecklistTemplateStatusUpdate(BaseModel):
    status: Literal["active", "inactive"]


class ChecklistTemplateResponse(BaseModel):
    template_id: int
    name: str
    description: str | None
    status: str
    created_at: datetime
    created_employee_id: int | None


class ChecklistTemplateDetailResponse(ChecklistTemplateResponse):
    items: list[ChecklistTemplateItemResponse]


class TaskAssignRequest(BaseModel):
    assigned_employee_id: int | None = None


class TaskStatusUpdate(BaseModel):
    status: Literal["pending", "done"]
    notes: str | None = None


class TaskUpdate(BaseModel):
    notes: str | None = None
    due_date: date | None = None


class TaskResponse(BaseModel):
    task_id: int
    checklist_id: int
    item_id: int
    item_title: str
    item_description: str | None
    assigned_employee_id: int | None
    status: str
    notes: str | None
    due_date: date | None
    completed_at: datetime | None
    completed_by_employee_id: int | None


class ApplyTemplateRequest(BaseModel):
    template_id: int = Field(gt=0)


class ChecklistReadiness(BaseModel):
    done: int
    total: int
    percent: int


class EngagementChecklistResponse(BaseModel):
    checklist_id: int
    engagement_id: int
    template_id: int
    template_name: str
    created_at: datetime
    readiness: ChecklistReadiness
    tasks: list[TaskResponse]


class MyTaskResponse(TaskResponse):
    engagement_id: int
    engagement_name: str | None
