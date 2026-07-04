"""Rename diagnostic_package.healthians_camp_id to external_package_id.

Revision ID: 0073_diag_ext_pkg_id
Revises: 0072_eng_location_fields
Create Date: 2026-07-04

Note: revision id must be <= 32 chars (alembic_version.version_num).
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect


revision = "0073_diag_ext_pkg_id"
down_revision = "0072_eng_location_fields"
branch_labels = None
depends_on = None


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    if table_name not in inspector.get_table_names():
        return False
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if _column_exists(inspector, "diagnostic_package", "healthians_camp_id"):
        op.alter_column(
            "diagnostic_package",
            "healthians_camp_id",
            new_column_name="external_package_id",
            existing_type=sa.Integer(),
            existing_nullable=True,
        )


def downgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if _column_exists(inspector, "diagnostic_package", "external_package_id"):
        op.alter_column(
            "diagnostic_package",
            "external_package_id",
            new_column_name="healthians_camp_id",
            existing_type=sa.Integer(),
            existing_nullable=True,
        )
