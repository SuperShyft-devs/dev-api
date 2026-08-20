"""Merge engagement slot info and consultation mode migration heads.

Revision ID: 0121_merge_slot_consult
Revises: 0117_engagement_slot_info, 0120_eng_consultation_mode
"""

from __future__ import annotations

revision = "0121_merge_slot_consult"
down_revision = ("0117_engagement_slot_info", "0120_eng_consultation_mode")
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
