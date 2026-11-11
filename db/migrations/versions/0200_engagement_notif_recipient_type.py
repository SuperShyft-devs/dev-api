"""Add recipient_type (and optional event_name) on engagement_notifications."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import text

revision = "0200_engagement_notif_recipient"
down_revision = "0199_engagement_notif_backfill"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    cols = {
        row[0]
        for row in conn.execute(
            text(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'engagement_notifications'
                """
            )
        ).fetchall()
    }
    if "recipient_type" not in cols:
        op.add_column(
            "engagement_notifications",
            sa.Column("recipient_type", sa.String(), nullable=False, server_default="user"),
        )
    if "event_name" not in cols:
        op.add_column(
            "engagement_notifications",
            sa.Column("event_name", sa.String(), nullable=True),
        )
    conn.execute(
        text(
            """
            UPDATE engagement_notifications
            SET recipient_type = 'user'
            WHERE recipient_type IS NULL OR recipient_type = ''
            """
        )
    )


def downgrade() -> None:
    pass
