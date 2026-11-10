"""Notification direct-send foundation columns (WhatAPI / SMTP metadata).

Revision ID: 0187_notif_direct_foundation
Revises: 0186_enforce_metsights_pro_gate
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0187_notif_direct_foundation"
down_revision = "0186_enforce_metsights_pro_gate"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "notification_services",
        sa.Column("template_name", sa.String(), nullable=True),
    )
    op.add_column(
        "notification_services",
        sa.Column(
            "template_variables",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    op.add_column(
        "notification_services",
        sa.Column("email_subject", sa.String(), nullable=True),
    )
    op.add_column(
        "notification_services",
        sa.Column("email_body", sa.Text(), nullable=True),
    )
    op.add_column(
        "notification_services",
        sa.Column("email_html", sa.Text(), nullable=True),
    )
    op.add_column(
        "notifications",
        sa.Column("queue_id", sa.String(), nullable=True),
    )
    op.create_index("ix_notifications_queue_id", "notifications", ["queue_id"])


def downgrade() -> None:
    op.drop_index("ix_notifications_queue_id", table_name="notifications")
    op.drop_column("notifications", "queue_id")
    op.drop_column("notification_services", "email_html")
    op.drop_column("notification_services", "email_body")
    op.drop_column("notification_services", "email_subject")
    op.drop_column("notification_services", "template_variables")
    op.drop_column("notification_services", "template_name")
