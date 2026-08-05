"""Add face_scan_link to engagement_participants.

Revision ID: 0108_ep_face_scan_link
Revises: 0107_eng_notif_normalize
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect


revision = "0108_ep_face_scan_link"
down_revision = "0107_eng_notif_normalize"
branch_labels = None
depends_on = None


def _column_exists(table: str, column: str) -> bool:
    bind = op.get_bind()
    inspector = inspect(bind)
    return any(c["name"] == column for c in inspector.get_columns(table))


def upgrade() -> None:
    if not _column_exists("engagement_participants", "face_scan_link"):
        op.add_column(
            "engagement_participants",
            sa.Column("face_scan_link", sa.String(), nullable=True),
        )


def downgrade() -> None:
    """Downgrades are intentionally disabled."""
    raise NotImplementedError("Downgrade is not supported")
