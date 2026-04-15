"""Engagement domain enums."""

from __future__ import annotations

import enum


class EngagementKind(str, enum.Enum):
    """Allowed values for `engagements.engagement_type` (PostgreSQL `engagement_kind`)."""

    bio_ai = "bio_ai"
    diagnostic = "diagnostic"
    doctor = "doctor"
    nutritionist = "nutritionist"
