"""Create consultation_bookings table and migrate participant consultations JSON.

Revision ID: 0097_consultation_bookings
Revises: 0096_industries_and_ranking
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB


revision = "0097_consultation_bookings"
down_revision = "0096_industries_and_ranking"
branch_labels = None
depends_on = None

DEFAULT_CONSENT = '{"bio_ai": false, "blood_report": false, "questionnaire": false}'


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if not _table_exists(inspector, "consultation_bookings"):
        op.create_table(
            "consultation_bookings",
            sa.Column("consultation_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column(
                "engagement_participant_id",
                sa.Integer(),
                sa.ForeignKey("engagement_participants.engagement_participant_id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("expert_type", sa.String(100), nullable=False),
            sa.Column(
                "expert_id",
                sa.Integer(),
                sa.ForeignKey("experts.expert_id", ondelete="SET NULL"),
                nullable=True,
            ),
            sa.Column("want", sa.Boolean(), nullable=False, server_default="false"),
            sa.Column("consultation_date", sa.Date(), nullable=True),
            sa.Column("consultation_slot", sa.String(20), nullable=True),
            sa.Column("done", sa.Boolean(), nullable=False, server_default="false"),
            sa.Column("meet_link", sa.String(500), nullable=True),
            sa.Column(
                "consent",
                JSONB(),
                nullable=False,
                server_default=sa.text(
                    "'{\"bio_ai\": false, \"blood_report\": false, \"questionnaire\": false}'::jsonb"
                ),
            ),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.text("now()"),
            ),
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.text("now()"),
            ),
            sa.UniqueConstraint(
                "engagement_participant_id",
                "expert_type",
                name="uq_consultation_bookings_participant_expert_type",
            ),
        )
        op.create_index(
            "ix_consultation_bookings_participant_id",
            "consultation_bookings",
            ["engagement_participant_id"],
        )
        op.create_index(
            "ix_consultation_bookings_expert_id",
            "consultation_bookings",
            ["expert_id"],
        )
        op.create_index(
            "ix_consultation_bookings_consultation_date",
            "consultation_bookings",
            ["consultation_date"],
        )
        op.create_index(
            "ix_consultation_bookings_want",
            "consultation_bookings",
            ["want"],
        )
        op.create_index(
            "ix_consultation_bookings_done",
            "consultation_bookings",
            ["done"],
        )

    inspector = inspect(connection)

    if (
        _table_exists(inspector, "engagement_participants")
        and not _column_exists(inspector, "engagement_participants", "consultation_booking_ids")
    ):
        op.add_column(
            "engagement_participants",
            sa.Column("consultation_booking_ids", ARRAY(sa.Integer()), nullable=True),
        )

    inspector = inspect(connection)

    if (
        _table_exists(inspector, "consultation_bookings")
        and _column_exists(inspector, "engagement_participants", "consultations")
        and _column_exists(inspector, "engagement_participants", "consultation_booking_ids")
    ):
        connection.execute(
            text(
                """
                INSERT INTO consultation_bookings (
                    engagement_participant_id,
                    expert_type,
                    expert_id,
                    want,
                    consultation_date,
                    consultation_slot,
                    done,
                    meet_link,
                    consent
                )
                SELECT
                    ep.engagement_participant_id,
                    kv.key AS expert_type,
                    CASE
                        WHEN jsonb_typeof(kv.value) = 'object'
                             AND kv.value->>'expert_id' IS NOT NULL
                             AND kv.value->>'expert_id' != 'null'
                        THEN (kv.value->>'expert_id')::int
                        ELSE NULL
                    END AS expert_id,
                    CASE
                        WHEN jsonb_typeof(kv.value) = 'boolean' THEN kv.value::boolean
                        WHEN jsonb_typeof(kv.value) = 'object'
                        THEN COALESCE((kv.value->>'want')::boolean, false)
                        ELSE false
                    END AS want,
                    CASE
                        WHEN jsonb_typeof(kv.value) = 'object'
                             AND kv.value->>'date' IS NOT NULL
                             AND kv.value->>'date' != 'null'
                        THEN (kv.value->>'date')::date
                        ELSE NULL
                    END AS consultation_date,
                    CASE
                        WHEN jsonb_typeof(kv.value) = 'object'
                             AND kv.value->>'slot' IS NOT NULL
                             AND kv.value->>'slot' != 'null'
                        THEN kv.value->>'slot'
                        ELSE NULL
                    END AS consultation_slot,
                    CASE
                        WHEN jsonb_typeof(kv.value) = 'object'
                        THEN COALESCE((kv.value->>'done')::boolean, false)
                        ELSE false
                    END AS done,
                    CASE
                        WHEN jsonb_typeof(kv.value) = 'object'
                             AND kv.value->>'meet_link' IS NOT NULL
                             AND kv.value->>'meet_link' != 'null'
                        THEN kv.value->>'meet_link'
                        ELSE NULL
                    END AS meet_link,
                    '{"bio_ai": false, "blood_report": false, "questionnaire": false}'::jsonb AS consent
                FROM engagement_participants ep
                CROSS JOIN LATERAL jsonb_each(ep.consultations::jsonb) AS kv(key, value)
                WHERE ep.consultations IS NOT NULL
                  AND jsonb_typeof(ep.consultations::jsonb) = 'object'
                ON CONFLICT (engagement_participant_id, expert_type) DO NOTHING
                """
            ),
        )

        connection.execute(
            text(
                """
                UPDATE engagement_participants ep
                SET consultation_booking_ids = sub.ids
                FROM (
                    SELECT
                        cb.engagement_participant_id,
                        array_agg(cb.consultation_id ORDER BY cb.consultation_id) AS ids
                    FROM consultation_bookings cb
                    GROUP BY cb.engagement_participant_id
                ) sub
                WHERE ep.engagement_participant_id = sub.engagement_participant_id
                """
            )
        )

    inspector = inspect(connection)
    if _column_exists(inspector, "engagement_participants", "consultations"):
        op.drop_column("engagement_participants", "consultations")


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
