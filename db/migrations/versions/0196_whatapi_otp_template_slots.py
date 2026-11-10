"""Backfill whatapi-otp template_variables when missing on staging."""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

revision = "0196_whatapi_otp_slots"
down_revision = "0195_notif_binding_channel"
branch_labels = None
depends_on = None

_DEFAULT_OTP_WA_SLOTS = (
    '{"slots": ['
    '{"index": 1, "source": "user.first_name"}, '
    '{"index": 2, "source": "otp.otp"}'
    "]}"
)


def upgrade() -> None:
    op.execute(
        text(
            f"""
            UPDATE notification_services
            SET template_variables = '{_DEFAULT_OTP_WA_SLOTS}'::jsonb
            WHERE service_key = 'whatapi-otp'
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
