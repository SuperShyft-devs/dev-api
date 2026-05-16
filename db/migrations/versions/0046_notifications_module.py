"""Create notification_services and notifications tables.

Revision ID: 0046_notifications_module
Revises: 0045_comp_nutritionist
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0046_notifications_module"
down_revision = "0045_comp_nutritionist"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "notification_services",
        sa.Column("notification_service_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("service_key", sa.String(), nullable=False, unique=True),
        sa.Column("display_name", sa.String(), nullable=False),
        sa.Column("channel", sa.String(), nullable=False),
        sa.Column("webhook_path", sa.String(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("require_record_id", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("channel IN ('email', 'whatsapp')", name="ck_notification_services_channel"),
    )

    op.create_table(
        "notifications",
        sa.Column("notification_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "service_key",
            sa.String(),
            sa.ForeignKey("notification_services.service_key"),
            nullable=False,
        ),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("channel", sa.String(), nullable=False),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.user_id"), nullable=True),
        sa.Column("engagement_id", sa.Integer(), sa.ForeignKey("engagements.engagement_id"), nullable=True),
        sa.Column(
            "assessment_instance_id",
            sa.Integer(),
            sa.ForeignKey("assessment_instances.assessment_instance_id"),
            nullable=True,
        ),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("triggered_by_user_id", sa.Integer(), sa.ForeignKey("users.user_id"), nullable=True),
        sa.Column("dispatched_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("status IN ('pending', 'sent', 'failed')", name="ck_notifications_status"),
        sa.CheckConstraint("channel IN ('email', 'whatsapp')", name="ck_notifications_channel"),
    )


def downgrade() -> None:
    op.drop_table("notifications")
    op.drop_table("notification_services")
