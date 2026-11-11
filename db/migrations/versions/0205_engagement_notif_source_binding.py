"""Add source_binding_id on engagement notification slots."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0205_en_source_binding"
down_revision = "0204_eng_notif_slots"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "engagement_notifications",
        sa.Column("source_binding_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_engagement_notifications_source_binding",
        "engagement_notifications",
        "notification_events",
        ["source_binding_id"],
        ["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_engagement_notifications_source_binding",
        "engagement_notifications",
        type_="foreignkey",
    )
    op.drop_column("engagement_notifications", "source_binding_id")
