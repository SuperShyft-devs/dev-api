"""Add metsights_profile_id to users.

This stores the Metsights Profile UUID returned from `POST /profiles/`.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0024_users_mets_profile"
down_revision = "0023_chk_tpl_audience"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("users", sa.Column("metsights_profile_id", sa.String(), nullable=True))


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")

