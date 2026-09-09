"""Pydantic schemas for employee APIs."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from common.validation import PositiveIntId, StatusStr
from modules.employee.models import EmployeeRole


class TaskGrantRequest(BaseModel):
    task_key: str = Field(..., min_length=1, max_length=80)
    can_view: bool = False
    can_edit: bool = False


class CategoryGrantRequest(BaseModel):
    category_key: str = Field(..., min_length=1, max_length=50)
    can_view: bool = False
    can_edit: bool = False
    tasks: list[TaskGrantRequest] | None = None


class EmployeeCreateRequest(BaseModel):
    user_id: PositiveIntId
    role: EmployeeRole
    status: Optional[StatusStr] = "active"
    permissions: list[CategoryGrantRequest] | None = None


class EmployeeUpdateRequest(BaseModel):
    user_id: PositiveIntId
    role: EmployeeRole
    expected_version: int | None = Field(default=None, ge=1)
    permissions: list[CategoryGrantRequest] | None = None


class EmployeeStatusUpdateRequest(BaseModel):
    status: StatusStr


class EmployeeListItem(BaseModel):
    employee_id: int
    user_id: int
    role: Optional[EmployeeRole] = None
    status: Optional[str] = None


class EmployeeDetailsResponse(EmployeeListItem):
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class ReplaceEmployeePermissionsRequest(BaseModel):
    expected_version: int = Field(..., ge=1)
    permissions: list[CategoryGrantRequest] = Field(default_factory=list)
