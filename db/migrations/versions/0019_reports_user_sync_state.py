"""Add reports_user_sync_state table for trends freshness.

Revision ID: 0019_reports_user_sync_state
Revises: 0018_reports_blood_parameters
Create Date: 2026-03-24
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0019_reports_user_sync_state"
down_revision = "0018_reports_blood_parameters"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "reports_user_sync_state",
        sa.Column("sync_id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("last_synced_assessment_instance_id", sa.Integer(), nullable=True),
        sa.Column("sync_status", sa.String(), nullable=False, server_default="idle"),
        sa.Column("last_synced_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_sync_error", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["user_id"], ["users.user_id"]),
        sa.PrimaryKeyConstraint("sync_id"),
        sa.UniqueConstraint("user_id"),
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
