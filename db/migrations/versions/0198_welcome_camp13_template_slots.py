"""Backfill camp13 WhatsApp services missing template_variables slots."""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

revision = "0198_welcome_camp13_slots"
down_revision = "0197_email_otp_template"
branch_labels = None
depends_on = None

# camp13: {{1}} name, {{2}}–{{6}} body lines (legacy welcome / registration participant messages).
_CAMP13_WELCOME_SLOTS = (
    '{"slots": ['
    '{"index": 1, "source": "user.first_name"}, '
    '{"index": 2, "source": "literal", "value": "Welcome to SuperShyft!"}, '
    '{"index": 3, "source": "booking.collection_date"}, '
    '{"index": 4, "source": "booking.collection_time"}, '
    '{"index": 5, "source": "user.address"}, '
    '{"index": 6, "source": "literal", "value": "https://app.supershyft.com"}'
    "]}"
)


def upgrade() -> None:
    op.execute(
        text(
            f"""
            UPDATE notification_services
            SET template_variables = '{_CAMP13_WELCOME_SLOTS}'::jsonb
            WHERE template_name = 'camp13'
              AND channel = 'whatsapp'
              AND (
                template_variables IS NULL
                OR template_variables::text IN ('null', '[]', '{{}}')
                OR NOT (template_variables ? 'slots')
                OR jsonb_array_length(COALESCE(template_variables->'slots', '[]'::jsonb)) = 0
              )
            """
        )
    )


def downgrade() -> None:
    pass
