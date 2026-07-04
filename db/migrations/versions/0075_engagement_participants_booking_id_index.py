"""Add index on engagement_participants.booking_id for webhook lookups.

Revision ID: 0075_participant_booking_idx
Revises: 0074_healthians_book
Create Date: 2026-07-05

Note: revision id must be <= 32 chars (alembic_version.version_num).
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect


revision = "0075_participant_booking_idx"
down_revision = "0074_healthians_book"
branch_labels = None
depends_on = None


def _index_exists(inspector: sa.Inspector, table_name: str, index_name: str) -> bool:
    if table_name not in inspector.get_table_names():
        return False
    return any(idx["name"] == index_name for idx in inspector.get_indexes(table_name))


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if not _index_exists(
        inspector,
        "engagement_participants",
        "ix_engagement_participants_booking_id",
    ):
        op.create_index(
            "ix_engagement_participants_booking_id",
            "engagement_participants",
            ["booking_id"],
        )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
