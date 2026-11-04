"""Pydantic schemas for support APIs."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from common.validation import PositiveIntId, SupportQueryText


class SupportTicketCreate(BaseModel):
    user_id: PositiveIntId
    query_text: SupportQueryText


class SupportTicketResponse(BaseModel):
    ticket_id: int
    user_id: int | None = None
    contact_input: str
    query_text: str
    status: str
    created_at: datetime
    resolved_by_employee_id: int | None = None
    resolved_at: datetime | None = None
    resolution_notes: str | None = None
    resolved_by_employee_name: str | None = None


class SupportTicketStatusUpdate(BaseModel):
    status: Literal["open", "resolved"]
    resolution_notes: str | None = Field(default=None, max_length=1000)
