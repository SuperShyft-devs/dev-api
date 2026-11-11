"""Map engagement_notifications rows to API responses."""

from __future__ import annotations

from modules.engagement_notifications.service_config import normalize_notification_services, serialize_for_db
from modules.engagements.models import EngagementNotification


def engagement_binding_to_response(row: EngagementNotification) -> dict:
    services = serialize_for_db(normalize_notification_services(row.notification_services))
    service_key = services[0]["service_key"] if services else None
    return {
        "id": row.id,
        "engagement_id": row.engagement_id,
        "event_code": row.event_code,
        "event_name": row.event_name or row.event_code,
        "recipient_type": row.recipient_type,
        "delivery_channel": row.delivery_channel,
        "notification_services": services,
        "recipient_config": (
            dict(row.recipient_config) if isinstance(row.recipient_config, dict) else None
        ),
        "service_key": service_key,
        "condition_key": row.condition_key or "default",
        "emit_condition": (
            dict(row.emit_condition) if isinstance(row.emit_condition, dict) else None
        ),
    }
