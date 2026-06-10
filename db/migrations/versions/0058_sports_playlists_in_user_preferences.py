"""sports_playlists_in_user_preferences

Revision ID: 0058_sports_playlists_in_user_preferences
Revises: 0057_pretest_guidelines
Create Date: 2026-06-08 14:45:26.511879

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = '0058_sports_playlists'
down_revision = '0057_pretest_guidelines'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'user_preferences',
        sa.Column('sports_playlists', sa.JSON(), server_default=sa.text("'{}'"), nullable=True)
    )


def downgrade() -> None:
    op.drop_column('user_preferences', 'sports_playlists')
