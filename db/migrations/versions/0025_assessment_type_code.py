"""Add assessment_type_code to assessment_packages.

Stores Metsights assessment type code used for record creation payload.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0025_assess_type_code"
down_revision = "0024_users_mets_profile"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("assessment_packages", sa.Column("assessment_type_code", sa.String(), nullable=True))


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")

