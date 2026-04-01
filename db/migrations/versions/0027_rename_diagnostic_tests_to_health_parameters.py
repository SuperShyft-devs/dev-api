"""Rename diagnostic_tests to health_parameters; add parameter_type enum.

Autogenerate is not wired in env.py; this migration is authored to match the ORM.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql


revision = "0027_health_parameters_rename"
down_revision = "0026_indiv_report_reports_col"
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

    if _table_exists(inspector, "health_parameters"):
        return

    if not _table_exists(inspector, "diagnostic_tests"):
        return

    health_parameter_type = postgresql.ENUM(
        "test",
        "metric",
        name="health_parameter_type",
        create_type=True,
    )
    health_parameter_type.create(connection, checkfirst=True)

    if not _column_exists(inspector, "diagnostic_tests", "parameter_type"):
        op.add_column(
            "diagnostic_tests",
            sa.Column(
                "parameter_type",
                health_parameter_type,
                nullable=False,
                server_default=sa.text("'test'::health_parameter_type"),
            ),
        )

    op.rename_table("diagnostic_tests", "health_parameters")


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
