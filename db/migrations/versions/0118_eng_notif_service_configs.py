"""Store engagement notification services as JSON configs with external_link.

Revision ID: 0118_eng_notif_service_configs
Revises: 0117_merge_heads
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0118_eng_notif_service_configs"
down_revision = "0117_merge_heads"
branch_labels = None
depends_on = None


def _migrate_array_to_json(table_name: str) -> None:
    conn = op.get_bind()
    conn.execute(
        sa.text(
            f"""
            ALTER TABLE {table_name}
            ADD COLUMN notification_services_json JSONB
            """
        )
    )
    conn.execute(
        sa.text(
            f"""
            UPDATE {table_name}
            SET notification_services_json = COALESCE(
                (
                    SELECT jsonb_agg(
                        jsonb_build_object(
                            'service_key', service_key,
                            'external_link', NULL
                        )
                    )
                    FROM unnest(notification_services) AS service_key
                ),
                '[]'::jsonb
            )
            """
        )
    )
    op.drop_column(table_name, "notification_services")
    op.alter_column(
        table_name,
        "notification_services_json",
        new_column_name="notification_services",
        nullable=False,
    )


def upgrade() -> None:
    _migrate_array_to_json("engagement_notifications")
    _migrate_array_to_json("engagement_notification_defaults")


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported for this migration")
