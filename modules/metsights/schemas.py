"""Schemas for Metsights integration payloads."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class MetsightsEnvelope(BaseModel):
    """Response envelope returned by Metsights records APIs."""

    detail: str | None = None
    data: Any = None


class MetsightsProfilesPage(BaseModel):
    """Paginated GET /profiles/ list response."""

    detail: str | None = None
    count: int = 0
    next: str | None = None
    previous: str | None = None
    data: list[dict[str, Any]] = []
