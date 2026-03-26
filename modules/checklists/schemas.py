"""Pydantic schemas for the checklists module."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field


ChecklistAudience = Literal["internal", "user"]


class ChecklistReadiness(BaseModel):
    done: int
    total: int
    percent: int


class ChecklistTemplateResponse(BaseModel):
    template_id: int
    name: str
    description: Optional[str] = None
    status: str
    audience: ChecklistAudience = "internal"
    created_at: datetime
    created_employee_id: Optional[int] = None


class ChecklistTemplateItemResponse(BaseModel):
    item_id: int
    template_id: int
    title: str
    description: Optional[str] = None
    display_order: Optional[int] = None


class ChecklistTemplateDetailResponse(ChecklistTemplateResponse):
    items: list[ChecklistTemplateItemResponse] = Field(default_factory=list)


class ChecklistTemplateCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    description: Optional[str] = None
    audience: ChecklistAudience = "internal"


class ChecklistTemplateUpdate(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=200)
    description: Optional[str] = None
    audience: Optional[ChecklistAudience] = None


class ChecklistTemplateStatusUpdate(BaseModel):
    status: str = Field(min_length=1, max_length=30)


class ChecklistTemplateItemCreate(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    description: Optional[str] = None
    display_order: Optional[int] = Field(default=None, ge=1)


class ChecklistTemplateItemUpdate(BaseModel):
    title: Optional[str] = Field(default=None, min_length=1, max_length=200)
    description: Optional[str] = None
    display_order: Optional[int] = Field(default=None, ge=1)


class ApplyTemplateRequest(BaseModel):
    template_id: int = Field(gt=0)


class TaskAssignRequest(BaseModel):
    assigned_employee_id: int | None = Field(default=None, gt=0)


class TaskStatusUpdate(BaseModel):
    status: str = Field(min_length=1, max_length=30)
    notes: Optional[str] = None


class TaskUpdate(BaseModel):
    notes: Optional[str] = None
    due_date: Optional[date] = None


class TaskResponse(BaseModel):
    task_id: int
    checklist_id: int
    item_id: int
    item_title: str
    item_description: Optional[str] = None
    assigned_employee_id: Optional[int] = None
    status: str
    notes: Optional[str] = None
    due_date: Optional[date] = None
    completed_at: Optional[datetime] = None
    completed_by_employee_id: Optional[int] = None


class EngagementChecklistResponse(BaseModel):
    checklist_id: int
    engagement_id: int
    template_id: int
    template_name: str
    created_at: datetime
    readiness: ChecklistReadiness
    tasks: list[TaskResponse] = Field(default_factory=list)


class MyTaskResponse(TaskResponse):
    engagement_id: int
    engagement_name: Optional[str] = None


class UserFacingChecklistItem(BaseModel):
    title: str
    description: Optional[str] = None
    display_order: Optional[int] = None


class UserFacingEngagementChecklist(BaseModel):
    checklist_id: int
    engagement_id: int
    template_id: int
    template_name: str
    template_description: Optional[str] = None
    items: list[UserFacingChecklistItem] = Field(default_factory=list)
