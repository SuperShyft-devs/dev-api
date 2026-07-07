"""Move external_camp_id from diagnostic_package to engagements.

Revision ID: 0080_eng_ext_camp_id
Revises: 0079_ep_booked_by_user_id
Create Date: 2026-07-07

Note: revision id must be <= 32 chars (alembic_version.version_num).
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "0080_eng_ext_camp_id"
down_revision = "0079_ep_booked_by_user_id"
branch_labels = None
depends_on = None


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    if table_name not in inspector.get_table_names():
        return False
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if not _column_exists(inspector, "engagements", "external_camp_id"):
        op.add_column(
            "engagements",
            sa.Column("external_camp_id", sa.Integer(), nullable=True),
        )

    if _column_exists(inspector, "diagnostic_package", "external_camp_id"):
        op.execute(
            """
            UPDATE engagements e
            SET external_camp_id = dp.external_camp_id
            FROM diagnostic_package dp
            WHERE e.diagnostic_package_id = dp.diagnostic_package_id
              AND dp.external_camp_id IS NOT NULL
              AND e.external_camp_id IS NULL
            """
        )

    if _column_exists(inspector, "diagnostic_package", "external_camp_id"):
        op.drop_column("diagnostic_package", "external_camp_id")


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
