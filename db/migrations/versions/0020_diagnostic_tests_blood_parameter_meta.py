"""Add blood-parameter metadata to diagnostic_tests.

This supports storing reference ranges and explanatory content per test.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "0020_diag_tests_blood_meta"
down_revision = "0019_reports_user_sync_state"
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

    if not _table_exists(inspector, "diagnostic_tests"):
        return

    # Key & display
    if not _column_exists(inspector, "diagnostic_tests", "parameter_key"):
        op.add_column("diagnostic_tests", sa.Column("parameter_key", sa.String(), nullable=True))
    if not _column_exists(inspector, "diagnostic_tests", "unit"):
        op.add_column("diagnostic_tests", sa.Column("unit", sa.String(), nullable=True))
    if not _column_exists(inspector, "diagnostic_tests", "meaning"):
        op.add_column("diagnostic_tests", sa.Column("meaning", sa.Text(), nullable=True))

    # Reference ranges by gender
    if not _column_exists(inspector, "diagnostic_tests", "lower_range_male"):
        op.add_column(
            "diagnostic_tests",
            sa.Column("lower_range_male", sa.Numeric(12, 4), nullable=True),
        )
    if not _column_exists(inspector, "diagnostic_tests", "higher_range_male"):
        op.add_column(
            "diagnostic_tests",
            sa.Column("higher_range_male", sa.Numeric(12, 4), nullable=True),
        )
    if not _column_exists(inspector, "diagnostic_tests", "lower_range_female"):
        op.add_column(
            "diagnostic_tests",
            sa.Column("lower_range_female", sa.Numeric(12, 4), nullable=True),
        )
    if not _column_exists(inspector, "diagnostic_tests", "higher_range_female"):
        op.add_column(
            "diagnostic_tests",
            sa.Column("higher_range_female", sa.Numeric(12, 4), nullable=True),
        )

    # Causes/effects and recommendations
    if not _column_exists(inspector, "diagnostic_tests", "causes_when_high"):
        op.add_column("diagnostic_tests", sa.Column("causes_when_high", sa.Text(), nullable=True))
    if not _column_exists(inspector, "diagnostic_tests", "causes_when_low"):
        op.add_column("diagnostic_tests", sa.Column("causes_when_low", sa.Text(), nullable=True))
    if not _column_exists(inspector, "diagnostic_tests", "effects_when_high"):
        op.add_column("diagnostic_tests", sa.Column("effects_when_high", sa.Text(), nullable=True))
    if not _column_exists(inspector, "diagnostic_tests", "effects_when_low"):
        op.add_column("diagnostic_tests", sa.Column("effects_when_low", sa.Text(), nullable=True))
    if not _column_exists(inspector, "diagnostic_tests", "what_to_do_when_low"):
        op.add_column("diagnostic_tests", sa.Column("what_to_do_when_low", sa.Text(), nullable=True))
    if not _column_exists(inspector, "diagnostic_tests", "what_to_do_when_high"):
        op.add_column("diagnostic_tests", sa.Column("what_to_do_when_high", sa.Text(), nullable=True))


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")

