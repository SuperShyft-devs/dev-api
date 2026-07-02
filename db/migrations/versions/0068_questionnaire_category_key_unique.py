"""Restore global unique constraint on questionnaire_categories.category_key.

Rename supershyft vitals -> health_vitals so metsights can keep category_key vitals.

Revision ID: 0068_category_key_unique
Revises: 0067_diag_test_group_key
Create Date: 2026-07-02 00:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect, text


revision = "0068_category_key_unique"
down_revision = "0067_diag_test_group_key"
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _constraint_exists(inspector: sa.Inspector, table_name: str, constraint_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    return constraint_name in {uc["name"] for uc in inspector.get_unique_constraints(table_name)}


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)
    table_name = "questionnaire_categories"

    if not _table_exists(inspector, table_name):
        return

    connection.execute(
        text(
            """
            UPDATE questionnaire_categories
            SET category_key = 'health_vitals'
            WHERE category_key = 'vitals' AND category_of = 'supershyft'
            """
        )
    )

    inspector = inspect(connection)
    if _constraint_exists(inspector, table_name, "uq_questionnaire_categories_key_category_of"):
        op.drop_constraint("uq_questionnaire_categories_key_category_of", table_name, type_="unique")

    inspector = inspect(connection)
    if not _constraint_exists(inspector, table_name, "uq_questionnaire_categories_key"):
        op.create_unique_constraint(
            "uq_questionnaire_categories_key",
            table_name,
            ["category_key"],
        )


def downgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)
    table_name = "questionnaire_categories"

    if not _table_exists(inspector, table_name):
        return

    if _constraint_exists(inspector, table_name, "uq_questionnaire_categories_key"):
        op.drop_constraint("uq_questionnaire_categories_key", table_name, type_="unique")

    inspector = inspect(connection)
    if not _constraint_exists(inspector, table_name, "uq_questionnaire_categories_key_category_of"):
        op.create_unique_constraint(
            "uq_questionnaire_categories_key_category_of",
            table_name,
            ["category_key", "category_of"],
        )
