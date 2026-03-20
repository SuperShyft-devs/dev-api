"""Add questionnaire visibility and prefill metadata.

Revision ID: 0015_qnr_visibility_prefill
Revises: 0014_pkg_category_display_order
Create Date: 2026-03-20
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0015_qnr_visibility_prefill"
down_revision = "0014_pkg_category_display_order"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "questionnaire_definitions",
        sa.Column("visibility_rules", sa.JSON(), nullable=True),
    )
    op.add_column(
        "questionnaire_definitions",
        sa.Column("prefill_from", sa.JSON(), nullable=True),
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
