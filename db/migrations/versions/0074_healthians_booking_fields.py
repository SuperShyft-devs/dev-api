"""Add external_camp_id and Healthians booking participant fields.

Revision ID: 0074_healthians_book
Revises: 0073_diag_ext_pkg_id
Create Date: 2026-07-04

Note: revision id must be <= 32 chars (alembic_version.version_num).
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect


revision = "0074_healthians_book"
down_revision = "0073_diag_ext_pkg_id"
branch_labels = None
depends_on = None


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    if table_name not in inspector.get_table_names():
        return False
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if not _column_exists(inspector, "diagnostic_package", "external_camp_id"):
        op.add_column(
            "diagnostic_package",
            sa.Column("external_camp_id", sa.Integer(), nullable=True),
        )

    if not _column_exists(inspector, "engagement_participants", "barcode"):
        op.add_column(
            "engagement_participants",
            sa.Column("barcode", sa.String(), nullable=True),
        )

    if not _column_exists(inspector, "engagement_participants", "booking_id"):
        op.add_column(
            "engagement_participants",
            sa.Column("booking_id", sa.String(), nullable=True),
        )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
