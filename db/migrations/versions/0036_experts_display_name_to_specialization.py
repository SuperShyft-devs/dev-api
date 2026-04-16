"""Rename experts.display_name to specialization.

Revision ID: 0036_experts_specialization
Revises: 0035_indiv_report_pdf_urls
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect, text


revision = "0036_experts_specialization"
down_revision = "0035_indiv_report_pdf_urls"
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

    if not _table_exists(inspector, "experts"):
        return

    if _column_exists(inspector, "experts", "display_name") and not _column_exists(
        inspector, "experts", "specialization"
    ):
        op.execute(text("ALTER TABLE experts RENAME COLUMN display_name TO specialization"))


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
