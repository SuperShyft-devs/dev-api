"""Add sub_text to questionnaire_definitions.

Revision ID: 0122_qnr_sub_text
Revises: 0121_merge_slot_consult
Create Date: 2026-08-20
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0122_qnr_sub_text"
down_revision = "0121_merge_slot_consult"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "questionnaire_definitions",
        sa.Column("sub_text", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("questionnaire_definitions", "sub_text")
