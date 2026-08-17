"""Seed book_expert and consultation_remainder auto notification events.

Revision ID: 0113_book_expert_consultation_remainder_events
Revises: 0112_org_contact_person_user_ids
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0113_book_expert_consultation_remainder_events"
down_revision = "0112_org_contact_person_user_ids"
branch_labels = None
depends_on = None

_EVENT_SEEDS: list[tuple[str, str, str]] = [
    (
        "book_expert",
        "Book Expert",
        "Notification sent the day before camp when a participant has not booked an offered consultation",
    ),
    (
        "consultation_remainder",
        "Consultation Remainder",
        "Reminder sent on the day of a scheduled consultation",
    ),
]


def upgrade() -> None:
    conn = op.get_bind()
    rows = conn.execute(
        sa.text(
            """
            SELECT id FROM engagement_types
            WHERE code IN (
                'consultation',
                'blood_test_with_consultation',
                'bio_ai_with_consultation'
            )
            """
        )
    ).fetchall()
    type_ids = [row.id for row in rows]
    if not type_ids:
        return

    for event_code, display_name, description in _EVENT_SEEDS:
        conn.execute(
            sa.text(
                """
                INSERT INTO auto_notification_events
                    (engagement_type_ids, event_code, display_name, description)
                VALUES (:type_ids, :event_code, :display_name, :description)
                ON CONFLICT (event_code) DO NOTHING
                """
            ),
            {
                "type_ids": type_ids,
                "event_code": event_code,
                "display_name": display_name,
                "description": description,
            },
        )


def downgrade() -> None:
    conn = op.get_bind()
    for event_code, _, _ in _EVENT_SEEDS:
        conn.execute(
            sa.text(
                "DELETE FROM auto_notification_events WHERE event_code = :event_code"
            ),
            {"event_code": event_code},
        )
