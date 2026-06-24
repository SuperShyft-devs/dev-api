"""Add camp_no column to engagements.

Revision ID: 0063_engagements_camp_no
Revises: 0062_organizations_departments
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect


revision = "0063_engagements_camp_no"
down_revision = "0062_organizations_departments"
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

    if not _column_exists(inspector, "engagements", "camp_no"):
        op.add_column("engagements", sa.Column("camp_no", sa.BigInteger(), nullable=True))

    op.execute(
        """
        UPDATE engagements
        SET camp_no = CAST(
          organization_id::text
          || LPAD(EXTRACT(DAY FROM start_date)::text, 2, '0')
          || LPAD(EXTRACT(MONTH FROM start_date)::text, 2, '0')
          || LPAD((EXTRACT(YEAR FROM start_date)::int % 100)::text, 2, '0')
          AS BIGINT)
        WHERE organization_id IS NOT NULL
          AND start_date IS NOT NULL
          AND camp_no IS NULL
        """
    )


def downgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if _column_exists(inspector, "engagements", "camp_no"):
        op.drop_column("engagements", "camp_no")
