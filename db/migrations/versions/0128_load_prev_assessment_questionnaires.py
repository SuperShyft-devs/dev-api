"""Add load_prev_assessment_questionnaires to engagements.

Revision ID: 0128_load_prev_assessment_questionnaires
Revises: 0127_users_age_nullable
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "0128_load_prev_assessment_questionnaires"
down_revision = "0127_users_age_nullable"
branch_labels = None
depends_on = None


def _column_exists(inspector, table: str, column: str) -> bool:
    return any(col["name"] == column for col in inspector.get_columns(table))


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    if not _column_exists(inspector, "engagements", "load_prev_assessment_questionnaires"):
        op.add_column(
            "engagements",
            sa.Column(
                "load_prev_assessment_questionnaires",
                sa.Boolean(),
                nullable=False,
                server_default=sa.false(),
            ),
        )


def downgrade() -> None:
    """Downgrades are intentionally disabled."""
    raise RuntimeError("Downgrade is not supported")
