"""Engagements: address/pincode, engagement_kind enum, nullable package FKs.

Revision ID: 0033_engagements_kind_enum
Revises: 0032_diag_custom_pkg
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect, text
from sqlalchemy.dialects.postgresql import ENUM


revision = "0033_engagements_kind_enum"
down_revision = "0032_diag_custom_pkg"
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if not _table_exists(inspector, "engagements"):
        return

    if not _column_exists(inspector, "engagements", "address"):
        op.add_column("engagements", sa.Column("address", sa.String(), nullable=True))
    if not _column_exists(inspector, "engagements", "pincode"):
        op.add_column("engagements", sa.Column("pincode", sa.String(), nullable=True))

    # Per product migration: all existing rows become bio_ai before enum cast.
    connection.execute(text("UPDATE engagements SET engagement_type = 'bio_ai'"))

    engagement_kind = ENUM(
        "bio_ai",
        "diagnostic",
        "doctor",
        "nutritionist",
        name="engagement_kind",
        create_type=True,
    )
    engagement_kind.create(connection, checkfirst=True)

    # Switch column to native enum (PostgreSQL).
    op.execute(
        text(
            "ALTER TABLE engagements ALTER COLUMN engagement_type TYPE engagement_kind "
            "USING (LOWER(TRIM(engagement_type)))::engagement_kind"
        )
    )

    op.alter_column(
        "engagements",
        "assessment_package_id",
        existing_type=sa.Integer(),
        nullable=True,
    )
    op.alter_column(
        "engagements",
        "diagnostic_package_id",
        existing_type=sa.Integer(),
        nullable=True,
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
