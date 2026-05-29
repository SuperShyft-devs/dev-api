"""Migrate engagement status from active/inactive/archived to running/completed.

Revision ID: 0053_eng_status
Revises: 0052_notif_user_json
"""

from __future__ import annotations

from alembic import op


revision = "0053_eng_status"
down_revision = "0052_notif_user_json"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE engagements
        SET status = 'running'
        WHERE status IS NULL
           OR TRIM(LOWER(status)) = ''
           OR LOWER(status) = 'active'
        """
    )
    op.execute(
        """
        UPDATE engagements
        SET status = 'completed'
        WHERE LOWER(status) IN ('inactive', 'archived')
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE engagements
        SET status = 'active'
        WHERE LOWER(status) = 'running'
        """
    )
    op.execute(
        """
        UPDATE engagements
        SET status = 'inactive'
        WHERE LOWER(status) = 'completed'
        """
    )
