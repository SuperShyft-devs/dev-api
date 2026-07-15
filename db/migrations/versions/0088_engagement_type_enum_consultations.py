"""Migrate engagement_type enum values and add consultations JSON column.

Revision ID: 0088_eng_type_consult
Revises: 0087_diag_comp_consult
Create Date: 2026-07-15

Changes:
- Add new enum values: blood_test, consultation, blood_test_with_consultation, bio_ai_with_consultation
- Migrate data: diagnostic -> blood_test, doctor -> consultation, nutritionist -> consultation
- Remove old enum values: diagnostic, doctor, nutritionist
- Add consultations JSON column to engagements
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect, text


revision = "0088_eng_type_consult"
down_revision = "0087_diag_comp_consult"
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

    new_values = ["blood_test", "consultation", "blood_test_with_consultation", "bio_ai_with_consultation"]
    for val in new_values:
        try:
            connection.execute(text(f"ALTER TYPE engagement_kind ADD VALUE IF NOT EXISTS '{val}'"))
        except Exception:
            pass

    connection.execute(text("COMMIT"))

    connection.execute(
        text("UPDATE engagements SET engagement_type = 'blood_test' WHERE engagement_type = 'diagnostic'")
    )
    connection.execute(
        text("UPDATE engagements SET engagement_type = 'consultation' WHERE engagement_type = 'doctor'")
    )
    connection.execute(
        text("UPDATE engagements SET engagement_type = 'consultation' WHERE engagement_type = 'nutritionist'")
    )

    if not _column_exists(inspector, "engagements", "consultations"):
        op.add_column(
            "engagements",
            sa.Column("consultations", sa.JSON(), nullable=True),
        )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
