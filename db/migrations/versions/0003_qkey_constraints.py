"""Enforce questionnaire question_key constraints.

Revision ID: 0003_qkey_constraints
Revises: 0002_qnr_cat_refactor
Create Date: 2026-03-11
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text


revision = "0003_qkey_constraints"
down_revision = "0002_qnr_cat_refactor"
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


def _constraint_exists(connection: sa.Connection, constraint_name: str) -> bool:
    result = connection.execute(
        text(
            """
            SELECT 1
            FROM information_schema.table_constraints
            WHERE constraint_name = :constraint_name
            LIMIT 1
            """
        ),
        {"constraint_name": constraint_name},
    )
    return result.scalar() is not None


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)
    table_name = "questionnaire_definitions"

    if not _table_exists(inspector, table_name) or not _column_exists(inspector, table_name, "question_key"):
        return

    # Normalize and populate missing question keys.
    connection.execute(
        text(
            """
            UPDATE questionnaire_definitions
            SET question_key = lower(trim(COALESCE(question_key, '')))
            """
        )
    )
    connection.execute(
        text(
            """
            UPDATE questionnaire_definitions
            SET question_key = CONCAT('q_', question_id::text)
            WHERE question_key = ''
            """
        )
    )

    # Resolve duplicates deterministically to allow unique constraint creation.
    connection.execute(
        text(
            """
            WITH ranked AS (
                SELECT question_id,
                       question_key,
                       ROW_NUMBER() OVER (PARTITION BY question_key ORDER BY question_id) AS rn
                FROM questionnaire_definitions
            )
            UPDATE questionnaire_definitions AS q
            SET question_key = CONCAT(r.question_key, '_', r.question_id::text)
            FROM ranked AS r
            WHERE q.question_id = r.question_id
              AND r.rn > 1
            """
        )
    )

    op.alter_column(
        table_name,
        "question_key",
        existing_type=sa.String(),
        nullable=False,
    )

    if not _constraint_exists(connection, "uq_questionnaire_definitions_question_key"):
        op.create_unique_constraint(
            "uq_questionnaire_definitions_question_key",
            table_name,
            ["question_key"],
        )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
