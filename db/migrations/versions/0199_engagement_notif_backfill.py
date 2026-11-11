"""Add consultations.booked user binding; engagement backfill via populate_from_defaults."""

from __future__ import annotations

import json

from alembic import op
from sqlalchemy import text

revision = "0199_engagement_notif_backfill"
down_revision = "0198_welcome_camp13_slots"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    conn.execute(
        text(
            """
            INSERT INTO notification_events (
                event_code, event_name, recipient_type, delivery_channel,
                notification_services, engagement_type_id
            )
            SELECT
                'consultations.booked',
                'Consultation booked',
                'user',
                'whatsapp',
                CAST(:services AS jsonb),
                NULL
            WHERE NOT EXISTS (
                SELECT 1 FROM notification_events
                WHERE event_code = 'consultations.booked'
                  AND recipient_type = 'user'
                  AND delivery_channel = 'whatsapp'
                  AND engagement_type_id IS NULL
            )
            """
        ),
        {
            "services": json.dumps(
                [{"service_key": "book-doctor-consultation-whatsapp", "external_link": None}]
            ),
        },
    )

def downgrade() -> None:
    pass
