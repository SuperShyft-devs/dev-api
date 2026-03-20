"""Add display_order to questionnaire category-question mapping.

Revision ID: 0013_qnr_category_question_order
Revises: 0012_diag_tests_decouple
Create Date: 2026-03-20
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0013_qnr_category_question_order"
down_revision = "0012_diag_tests_decouple"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "questionnaire_category_questions",
        sa.Column("display_order", sa.Integer(), nullable=True),
    )

    # Preserve current behavior by seeding order by question_id within each category.
    op.execute(
        """
        WITH ranked AS (
          SELECT
            id,
            ROW_NUMBER() OVER (
              PARTITION BY category_id
              ORDER BY question_id ASC, id ASC
            ) AS seq
          FROM questionnaire_category_questions
        )
        UPDATE questionnaire_category_questions q
        SET display_order = ranked.seq
        FROM ranked
        WHERE q.id = ranked.id
        """
    )

    op.create_index(
        "ix_questionnaire_category_questions_category_order",
        "questionnaire_category_questions",
        ["category_id", "display_order"],
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
