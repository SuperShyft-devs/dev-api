"""Add home-collection address/location fields to engagement_participants.

Revision ID: 0109_ep_home_collection_fields
Revises: 0108_ep_face_scan_link

Adds address, sub_locality, landmark, pincode, city, state, country,
latitude, longitude, and healthians_zone_id so that per-participant
home-collection bookings can store location independently of the engagement.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0109_ep_home_collection_fields"
down_revision = "0108_ep_face_scan_link"
branch_labels = None
depends_on = None

_COLUMNS = [
    ("address", "VARCHAR"),
    ("sub_locality", "VARCHAR"),
    ("landmark", "VARCHAR"),
    ("pincode", "VARCHAR"),
    ("city", "VARCHAR"),
    ("state", "VARCHAR"),
    ("country", "VARCHAR"),
    ("latitude", "DOUBLE PRECISION"),
    ("longitude", "DOUBLE PRECISION"),
    ("healthians_zone_id", "VARCHAR"),
]


def upgrade() -> None:
    op.execute(sa.text("SET LOCAL lock_timeout = '60s'"))
    for col_name, col_type in _COLUMNS:
        op.execute(
            sa.text(
                f"ALTER TABLE engagement_participants "
                f"ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
            )
        )


def downgrade() -> None:
    op.execute(sa.text("SET LOCAL lock_timeout = '15s'"))
    for col_name, _col_type in reversed(_COLUMNS):
        op.execute(
            sa.text(
                f"ALTER TABLE engagement_participants "
                f"DROP COLUMN IF EXISTS {col_name}"
            )
        )
