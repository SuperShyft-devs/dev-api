"""Pydantic schemas for server health monitoring."""

from __future__ import annotations

from pydantic import BaseModel, Field


class HealthRunRead(BaseModel):
    id: int
    run_at: str
    ok_count: int
    warn_count: int
    crit_count: int
    overall_status: str


class HealthCheckRead(BaseModel):
    id: int
    run_id: int
    category: str
    status: str
    message: str


class HealthChecksByCategory(BaseModel):
    category: str
    checks: list[HealthCheckRead] = Field(default_factory=list)


class ServerHealthCurrentRead(BaseModel):
    run: HealthRunRead
    checks_by_category: list[HealthChecksByCategory] = Field(default_factory=list)
