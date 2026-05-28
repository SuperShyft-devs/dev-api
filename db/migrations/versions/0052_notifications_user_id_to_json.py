"""Rename notifications.user_id (int FK) to notifications.user (JSON).

Stores {"user_ids": [1, 2, ...]} to support multi-user notifications.

Revision ID: 0052_notif_user_json
Revises: 0051_eng_notif_svc_key
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0052_notif_user_json"
down_revision = "0051_eng_notif_svc_key"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("notifications", sa.Column("user", sa.JSON(), nullable=True))

    op.execute(
        """
        UPDATE notifications
        SET "user" = jsonb_build_object('user_ids', jsonb_build_array(user_id))
        WHERE user_id IS NOT NULL
        """
    )

    op.drop_constraint("notifications_user_id_fkey", "notifications", type_="foreignkey")
    op.drop_column("notifications", "user_id")


def downgrade() -> None:
    op.add_column(
        "notifications",
        sa.Column("user_id", sa.Integer(), nullable=True),
    )

    op.execute(
        """
        UPDATE notifications
        SET user_id = ("user"->'user_ids'->>0)::int
        WHERE "user" IS NOT NULL
          AND jsonb_array_length("user"->'user_ids') > 0
        """
    )

    op.create_foreign_key(
        "notifications_user_id_fkey",
        "notifications",
        "users",
        ["user_id"],
        ["user_id"],
    )
    op.drop_column("notifications", "user")
