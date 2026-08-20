"""Pydantic schemas for employee APIs."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from common.validation import PositiveIntId, StatusStr
from modules.employee.models import EmployeeRole


class EmployeeCreateRequest(BaseModel):
    user_id: PositiveIntId
    role: EmployeeRole
    status: Optional[StatusStr] = "active"


class EmployeeUpdateRequest(BaseModel):
    user_id: PositiveIntId
    role: EmployeeRole


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
