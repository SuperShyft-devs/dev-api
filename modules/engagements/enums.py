"""Engagement domain enums (no SQLAlchemy / db.base dependency)."""

from __future__ import annotations

import enum


class EngagementKind(str, enum.Enum):
    """PostgreSQL enum `engagement_kind` / column `engagements.engagement_type`."""

    bio_ai = "bio_ai"
    blood_test = "blood_test"
    consultation = "consultation"
    blood_test_with_consultation = "blood_test_with_consultation"
    bio_ai_with_consultation = "bio_ai_with_consultation"


class BloodCollectionType(str, enum.Enum):
    """PostgreSQL enum `blood_collection_type_enum`."""

    home_collection = "home_collection"
    camp_collection = "camp_collection"


class ConsultationMode(str, enum.Enum):
    """PostgreSQL enum `consultation_mode_enum`."""

    online = "online"
    offline = "offline"


class EngagementStatus(str, enum.Enum):
    """Application-level engagement status values."""

    draft = "draft"
    scheduled = "scheduled"
    running = "running"
    completed = "completed"
    cancelled = "cancelled"
