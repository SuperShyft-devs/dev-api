"""Add booking flow columns to engagements and engagement_participants.

Revision ID: 0077_engagement_booking_cols
Revises: 0076_query_perf_indexes
Create Date: 2026-07-06

New columns:
- engagements.created_at (timestamptz, NOT NULL, default now())
- engagements.healthians_zone_id (varchar, nullable)
- engagements.blood_collection_type (enum: home_collection/camp_collection, nullable)
- engagement_participants.blood_collection_time_slot_id (varchar, nullable)
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "0077_engagement_booking_cols"
down_revision = "0076_query_perf_indexes"
branch_labels = None
depends_on = None


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    if table_name not in inspector.get_table_names():
        return False
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    # --- engagements table ---

    if not _column_exists(inspector, "engagements", "created_at"):
        op.add_column(
            "engagements",
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.func.now(),
            ),
        )

    if not _column_exists(inspector, "engagements", "healthians_zone_id"):
        op.add_column(
            "engagements",
            sa.Column("healthians_zone_id", sa.String(), nullable=True),
        )

    blood_collection_type_enum = sa.Enum(
        "home_collection", "camp_collection",
        name="blood_collection_type_enum",
    )
    blood_collection_type_enum.create(connection, checkfirst=True)

    if not _column_exists(inspector, "engagements", "blood_collection_type"):
        op.add_column(
            "engagements",
            sa.Column(
                "blood_collection_type",
                blood_collection_type_enum,
                nullable=True,
            ),
        )

    # --- engagement_participants table ---

    if not _column_exists(inspector, "engagement_participants", "blood_collection_time_slot_id"):
        op.add_column(
            "engagement_participants",
            sa.Column("blood_collection_time_slot_id", sa.String(), nullable=True),
        )


def downgrade() -> None:
    op.drop_column("engagement_participants", "blood_collection_time_slot_id")
    op.drop_column("engagements", "blood_collection_type")
    op.drop_column("engagements", "healthians_zone_id")
    op.drop_column("engagements", "created_at")

    blood_collection_type_enum = sa.Enum(name="blood_collection_type_enum")
    blood_collection_type_enum.drop(op.get_bind(), checkfirst=True)
