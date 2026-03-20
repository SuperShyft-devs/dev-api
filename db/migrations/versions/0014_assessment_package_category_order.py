"""Add display_order to assessment package-category mapping.

Revision ID: 0014_pkg_category_display_order
Revises: 0013_qnr_category_question_order
Create Date: 2026-03-20
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0014_pkg_category_display_order"
down_revision = "0013_qnr_category_question_order"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "assessment_package_categories",
        sa.Column("display_order", sa.Integer(), nullable=True),
    )

    # Preserve current behavior by seeding order per package by insertion id.
    op.execute(
        """
        WITH ranked AS (
          SELECT
            id,
            ROW_NUMBER() OVER (
              PARTITION BY package_id
              ORDER BY id ASC
            ) AS seq
          FROM assessment_package_categories
        )
        UPDATE assessment_package_categories apc
        SET display_order = ranked.seq
        FROM ranked
        WHERE apc.id = ranked.id
        """
    )

    op.create_index(
        "ix_assessment_package_categories_package_order",
        "assessment_package_categories",
        ["package_id", "display_order"],
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
