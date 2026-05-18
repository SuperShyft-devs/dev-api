"""Add complementary_doctor column to diagnostic_package.

Revision ID: 0049_comp_doctor
Revises: 0048_dx_pkg_display_order
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect


revision = "0049_comp_doctor"
down_revision = "0048_dx_pkg_display_order"
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

    if _table_exists(inspector, "diagnostic_package") and not _column_exists(
        inspector, "diagnostic_package", "complementary_doctor"
    ):
        op.add_column(
            "diagnostic_package",
            sa.Column(
                "complementary_doctor",
                sa.Boolean(),
                nullable=False,
                server_default=sa.text("false"),
            ),
        )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
