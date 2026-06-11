"""questionnaire_metsights_sync_rework

Add category_of to questionnaire_categories, metsights_sync to
questionnaire_definitions, and create integration_sync_logs table.

Revision ID: 0059_metsights_sync_rework
Revises: 0058_sports_playlists
Create Date: 2026-06-12 00:45:00.000000

"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


revision = '0059_metsights_sync_rework'
down_revision = '0058_sports_playlists'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        'questionnaire_categories',
        sa.Column('category_of', sa.String(20), server_default=sa.text("'supershyft'"), nullable=False),
    )

    op.add_column(
        'questionnaire_definitions',
        sa.Column('metsights_sync', sa.JSON(), nullable=True),
    )

    op.create_table(
        'integration_sync_logs',
        sa.Column('sync_log_id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('engagement_id', sa.Integer(), sa.ForeignKey('engagements.engagement_id', ondelete='SET NULL'), nullable=True),
        sa.Column('user_id', sa.Integer(), sa.ForeignKey('users.user_id', ondelete='SET NULL'), nullable=True),
        sa.Column('provider', sa.String(30), nullable=False),
        sa.Column('api_endpoint_url', sa.Text(), nullable=False),
        sa.Column('request_payload', JSONB(), nullable=True),
        sa.Column('response_payload', JSONB(), nullable=True),
        sa.Column('status', sa.String(20), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )

    op.create_index('ix_integration_sync_logs_user_id', 'integration_sync_logs', ['user_id'])
    op.create_index('ix_integration_sync_logs_engagement_id', 'integration_sync_logs', ['engagement_id'])
    op.create_index('ix_integration_sync_logs_provider', 'integration_sync_logs', ['provider'])


def downgrade() -> None:
    op.drop_index('ix_integration_sync_logs_provider', table_name='integration_sync_logs')
    op.drop_index('ix_integration_sync_logs_engagement_id', table_name='integration_sync_logs')
    op.drop_index('ix_integration_sync_logs_user_id', table_name='integration_sync_logs')
    op.drop_table('integration_sync_logs')
    op.drop_column('questionnaire_definitions', 'metsights_sync')
    op.drop_column('questionnaire_categories', 'category_of')
