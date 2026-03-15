"""Pydantic schemas for support APIs."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class SupportTicketCreate(BaseModel):
    contact_input: str = Field(min_length=1, max_length=255)
    query_text: str = Field(min_length=1)


class SupportTicketResponse(BaseModel):
    ticket_id: int
    user_id: int | None = None
    contact_input: str
    query_text: str
    status: str
    created_at: datetime


class SupportTicketStatusUpdate(BaseModel):
    status: Literal["open", "resolved", "closed"]
