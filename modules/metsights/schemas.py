"""Schemas for Metsights integration payloads."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class MetsightsEnvelope(BaseModel):
    """Response envelope returned by Metsights records APIs."""

    detail: str | None = None
    data: Any = None
