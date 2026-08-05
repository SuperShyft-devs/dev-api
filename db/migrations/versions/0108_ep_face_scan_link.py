"""Add face_scan_link to engagement_participants.

Revision ID: 0108_ep_face_scan_link
Revises: 0107_eng_notif_normalize

Uses a single ADD COLUMN IF NOT EXISTS with a short lock_timeout so deploy
fails fast on lock contention instead of hanging (common when API/cron still
holds locks on engagement_participants).
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0108_ep_face_scan_link"
down_revision = "0107_eng_notif_normalize"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Fail fast if another session holds AccessExclusiveLock on the table
    # (running API workers / long queries). Nullable ADD COLUMN itself is cheap.
    op.execute(sa.text("SET LOCAL lock_timeout = '15s'"))
    op.execute(
        sa.text(
            "ALTER TABLE engagement_participants "
            "ADD COLUMN IF NOT EXISTS face_scan_link VARCHAR"
        )
    )


def downgrade() -> None:
    op.execute(sa.text("SET LOCAL lock_timeout = '15s'"))
    op.execute(
        sa.text(
            "ALTER TABLE engagement_participants DROP COLUMN IF EXISTS face_scan_link"
        )
    )
