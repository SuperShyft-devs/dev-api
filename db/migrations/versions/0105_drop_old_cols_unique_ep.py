"""Placeholder for revision already applied on supershyft_v2.

Revision ID: 0105_drop_old_cols_unique_ep
Revises: 0103_camp_reports_bts

This file was missing from the repo while the database alembic_version
already pointed at it. Kept as a no-op so the chain can resolve; schema
changes (unique engagement_participants, dropped cols) are assumed applied.
"""

from __future__ import annotations

revision = "0105_drop_old_cols_unique_ep"
down_revision = "0103_camp_reports_bts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Already applied on environments that stamped this revision.
    pass


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
