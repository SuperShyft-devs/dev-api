"""Pydantic schemas for employee APIs."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class EmployeeCreateRequest(BaseModel):
    user_id: int = Field(gt=0)
    role: str = Field(min_length=1, max_length=50)
    status: Optional[str] = Field(default="active", max_length=30)


class EmployeeUpdateRequest(BaseModel):
    user_id: int = Field(gt=0)
    role: str = Field(min_length=1, max_length=50)


class EmployeeStatusUpdateRequest(BaseModel):
    status: str = Field(min_length=1, max_length=30)


class EmployeeListItem(BaseModel):
    employee_id: int
    user_id: int
    role: Optional[str] = None
    status: Optional[str] = None


class EmployeeDetailsResponse(EmployeeListItem):
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
