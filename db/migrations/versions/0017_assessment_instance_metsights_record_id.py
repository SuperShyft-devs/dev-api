"""Add metsights_record_id to assessment_instances.

Revision ID: 0017_ai_metsights_record_id
Revises: 0016_checklists_module
Create Date: 2026-03-24
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0017_ai_metsights_record_id"
down_revision = "0016_checklists_module"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("assessment_instances", sa.Column("metsights_record_id", sa.String(), nullable=True))


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
