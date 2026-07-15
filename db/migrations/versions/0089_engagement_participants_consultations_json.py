"""Replace 3 boolean consultation columns with consultations JSON in engagement_participants.

Revision ID: 0089_participant_consult
Revises: 0088_eng_type_consult
Create Date: 2026-07-15

Changes:
- Add consultations JSON column to engagement_participants
- Migrate want_doctor_consultation, want_nutritionist_consultation, want_doctor_and_nutritionist_consultation
- Drop the 3 boolean columns
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect, text


revision = "0089_participant_consult"
down_revision = "0088_eng_type_consult"
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

    if not _table_exists(inspector, "engagement_participants"):
        return

    if not _column_exists(inspector, "engagement_participants", "consultations"):
        op.add_column(
            "engagement_participants",
            sa.Column("consultations", sa.JSON(), nullable=True),
        )

    has_doc = _column_exists(inspector, "engagement_participants", "want_doctor_consultation")
    has_nutri = _column_exists(inspector, "engagement_participants", "want_nutritionist_consultation")
    has_both = _column_exists(inspector, "engagement_participants", "want_doctor_and_nutritionist_consultation")

    if has_doc or has_nutri or has_both:
        doctor_expr = "COALESCE(want_doctor_consultation, false)" if has_doc else "false"
        nutri_expr = "COALESCE(want_nutritionist_consultation, false)" if has_nutri else "false"

        both_doctor = f"(COALESCE(want_doctor_and_nutritionist_consultation, false) OR {doctor_expr})" if has_both else doctor_expr
        both_nutri = f"(COALESCE(want_doctor_and_nutritionist_consultation, false) OR {nutri_expr})" if has_both else nutri_expr

        connection.execute(
            text(
                f"""
                UPDATE engagement_participants
                SET consultations = json_build_object(
                    'doctor', {both_doctor},
                    'nutritionist', {both_nutri}
                )
                WHERE consultations IS NULL
                """
            )
        )

    if has_doc:
        op.drop_column("engagement_participants", "want_doctor_consultation")
    if has_nutri:
        op.drop_column("engagement_participants", "want_nutritionist_consultation")
    if has_both:
        op.drop_column("engagement_participants", "want_doctor_and_nutritionist_consultation")


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
