"""Replace complementary_nutritionist and complementary_doctor with complementary_consultation JSON.

Revision ID: 0087_diag_comp_consult
Revises: 0086_expert_types
Create Date: 2026-07-15

Changes:
- Add complementary_consultation JSON column to diagnostic_package
- Migrate existing boolean data into the new JSON column
- Drop complementary_nutritionist and complementary_doctor columns
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect, text


revision = "0087_diag_comp_consult"
down_revision = "0086_expert_types"
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

    if not _table_exists(inspector, "diagnostic_package"):
        return

    if not _column_exists(inspector, "diagnostic_package", "complementary_consultation"):
        op.add_column(
            "diagnostic_package",
            sa.Column("complementary_consultation", sa.JSON(), nullable=True),
        )

    has_old_doctor = _column_exists(inspector, "diagnostic_package", "complementary_doctor")
    has_old_nutri = _column_exists(inspector, "diagnostic_package", "complementary_nutritionist")

    if has_old_doctor or has_old_nutri:
        doctor_expr = "COALESCE(complementary_doctor, false)" if has_old_doctor else "false"
        nutri_expr = "COALESCE(complementary_nutritionist, false)" if has_old_nutri else "false"
        connection.execute(
            text(
                f"""
                UPDATE diagnostic_package
                SET complementary_consultation = json_build_object(
                    'doctor', {doctor_expr},
                    'nutritionist', {nutri_expr}
                )
                WHERE complementary_consultation IS NULL
                """
            )
        )

    if has_old_doctor:
        op.drop_column("diagnostic_package", "complementary_doctor")
    if has_old_nutri:
        op.drop_column("diagnostic_package", "complementary_nutritionist")


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
