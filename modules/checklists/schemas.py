"""Pydantic schemas for the checklists module."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field

from common.validation import (
    ChecklistText,
    OptionalChecklistText,
    OptionalSafeDisplayName,
    PositiveIntId,
    SafeDisplayName,
    StatusStr,
)


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
    name: SafeDisplayName
    description: OptionalChecklistText = None
    audience: ChecklistAudience = "internal"


class ChecklistTemplateUpdate(BaseModel):
    name: OptionalSafeDisplayName = None
    description: OptionalChecklistText = None
    audience: Optional[ChecklistAudience] = None


class ChecklistTemplateStatusUpdate(BaseModel):
    status: StatusStr


class ChecklistTemplateItemCreate(BaseModel):
    title: SafeDisplayName
    description: OptionalChecklistText = None
    display_order: Optional[int] = Field(default=None, ge=1)


class ChecklistTemplateItemUpdate(BaseModel):
    title: OptionalSafeDisplayName = None
    description: OptionalChecklistText = None
    display_order: Optional[int] = Field(default=None, ge=1)


class ApplyTemplateRequest(BaseModel):
    template_id: PositiveIntId


class TaskAssignRequest(BaseModel):
    assigned_employee_id: PositiveIntId | None = None


class TaskStatusUpdate(BaseModel):
    status: StatusStr
    notes: OptionalChecklistText = None


class TaskUpdate(BaseModel):
    notes: OptionalChecklistText = None
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
