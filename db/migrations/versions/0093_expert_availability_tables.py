"""Create expert_availability and expert_availability_overrides tables.

Revision ID: 0093_expert_availability_tables
Revises: 0092_experts_effective_dates
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect


revision = "0093_expert_availability_tables"
down_revision = "0092_experts_effective_dates"
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    connection = op.get_bind()

    inspector = inspect(connection)
    if not _table_exists(inspector, "expert_availability"):
        op.create_table(
            "expert_availability",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column(
                "expert_id",
                sa.Integer(),
                sa.ForeignKey("experts.expert_id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("day_of_week", sa.Integer(), nullable=False),
            sa.Column("start_time", sa.Time(), nullable=False),
            sa.Column("end_time", sa.Time(), nullable=False),
            sa.Column("slot_duration", sa.Integer(), nullable=False),
            sa.Column("buffer_time", sa.Integer(), nullable=False, server_default="5"),
        )
        op.create_index("ix_expert_availability_expert_id", "expert_availability", ["expert_id"])

    inspector = inspect(connection)
    if not _table_exists(inspector, "expert_availability_overrides"):
        op.create_table(
            "expert_availability_overrides",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column(
                "expert_id",
                sa.Integer(),
                sa.ForeignKey("experts.expert_id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("override_date", sa.Date(), nullable=False),
            sa.Column("availability", sa.Boolean(), nullable=False),
            sa.Column("start_time", sa.Time(), nullable=True),
            sa.Column("end_time", sa.Time(), nullable=True),
            sa.Column("buffer_time", sa.Integer(), nullable=True),
        )
        op.create_index(
            "ix_expert_availability_overrides_expert_id",
            "expert_availability_overrides",
            ["expert_id"],
        )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
