"""questionnaire_category_key_category_of

Allow the same category_key for different category_of values (e.g. vitals/supershyft
and vitals/metsights).

Revision ID: 0060_category_key_category_of
Revises: 0059_metsights_sync_rework
Create Date: 2026-06-12 02:40:00.000000

"""

from __future__ import annotations

from alembic import op


revision = "0060_category_key_category_of"
down_revision = "0059_metsights_sync_rework"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("uq_questionnaire_categories_key", "questionnaire_categories", type_="unique")
    op.create_unique_constraint(
        "uq_questionnaire_categories_key_category_of",
        "questionnaire_categories",
        ["category_key", "category_of"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_questionnaire_categories_key_category_of", "questionnaire_categories", type_="unique")
    op.create_unique_constraint(
        "uq_questionnaire_categories_key",
        "questionnaire_categories",
        ["category_key"],
    )
