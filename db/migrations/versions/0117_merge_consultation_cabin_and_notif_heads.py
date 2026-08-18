"""Merge consultation cabin and notification service migration heads.

Revision ID: 0117_merge_heads
Revises: 0114_cb_consultation_cabin, 0116_req_external_link
"""

from __future__ import annotations

revision = "0117_merge_heads"
down_revision = ("0114_cb_consultation_cabin", "0116_req_external_link")
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
