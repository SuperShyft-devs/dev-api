"""Replace category_id with category_ids on questionnaire_responses, add
is_submitted to assessment_category_progress, drop submitted_at from
questionnaire_responses, add unique constraint on engagement_participants.

Revision ID: 0110_cat_ids_is_submitted
Revises: 0109_ep_home_collection_fields
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect, text


revision = "0110_cat_ids_is_submitted"
down_revision = "0109_ep_home_collection_fields"
branch_labels = None
depends_on = None


def _column_exists(inspector, table: str, column: str) -> bool:
    return any(c["name"] == column for c in inspector.get_columns(table))


def _index_exists(inspector, table: str, index_name: str) -> bool:
    return any(i["name"] == index_name for i in inspector.get_indexes(table))


def _constraint_exists(connection, name: str) -> bool:
    row = connection.execute(
        text("SELECT 1 FROM pg_constraint WHERE conname = :n"),
        {"n": name},
    ).first()
    return row is not None


def upgrade() -> None:
    op.execute(text("SET LOCAL lock_timeout = '60s'"))
    connection = op.get_bind()
    inspector = inspect(connection)

    # ── 1. questionnaire_responses: add category_ids ARRAY(Integer) ──
    if not _column_exists(inspector, "questionnaire_responses", "category_ids"):
        op.add_column(
            "questionnaire_responses",
            sa.Column(
                "category_ids",
                sa.ARRAY(sa.Integer()),
                nullable=False,
                server_default="{}",
            ),
        )

    # Backfill: copy existing category_id into category_ids as a single-element array
    inspector = inspect(connection)
    if (
        _column_exists(inspector, "questionnaire_responses", "category_id")
        and _column_exists(inspector, "questionnaire_responses", "category_ids")
    ):
        connection.execute(
            text(
                """
                UPDATE questionnaire_responses
                SET category_ids = ARRAY[category_id]
                WHERE category_ids = '{}'
                  AND category_id IS NOT NULL
                """
            )
        )

    # Drop FK constraint on category_id
    if _column_exists(inspector, "questionnaire_responses", "category_id"):
        foreign_keys = inspector.get_foreign_keys("questionnaire_responses")
        for fk in foreign_keys:
            constrained = set(fk.get("constrained_columns") or [])
            name = fk.get("name")
            if "category_id" in constrained and name:
                op.drop_constraint(name, "questionnaire_responses", type_="foreignkey")

    # Drop index on category_id
    inspector = inspect(connection)
    if _index_exists(inspector, "questionnaire_responses", "ix_questionnaire_responses_category_id"):
        op.drop_index("ix_questionnaire_responses_category_id", table_name="questionnaire_responses")

    # Drop the old category_id column
    inspector = inspect(connection)
    if _column_exists(inspector, "questionnaire_responses", "category_id"):
        op.drop_column("questionnaire_responses", "category_id")

    # Create GIN index on category_ids for @> (contains) queries
    inspector = inspect(connection)
    if not _index_exists(inspector, "questionnaire_responses", "ix_qr_category_ids_gin"):
        op.execute(
            text(
                "CREATE INDEX ix_qr_category_ids_gin "
                "ON questionnaire_responses USING gin (category_ids)"
            )
        )

    # ── 2. questionnaire_responses: drop submitted_at ──
    inspector = inspect(connection)
    if _column_exists(inspector, "questionnaire_responses", "submitted_at"):
        op.drop_column("questionnaire_responses", "submitted_at")

    # ── 3. assessment_category_progress: add is_submitted ──
    inspector = inspect(connection)
    if not _column_exists(inspector, "assessment_category_progress", "is_submitted"):
        op.add_column(
            "assessment_category_progress",
            sa.Column(
                "is_submitted",
                sa.Boolean(),
                nullable=False,
                server_default=sa.text("false"),
            ),
        )

    # ── 4. engagement_participants: dedup then unique (engagement_id, user_id) ──
    # Deploy runs migrations before the backfill job, so dedup must happen here.
    if not _constraint_exists(connection, "uq_engagement_participants_engagement_user"):
        # Keep the row with the most consultation bookings; ties → highest id.
        # consultation_bookings has UNIQUE (engagement_participant_id, expert_type),
        # so drop colliding bookings on duplicate rows before re-pointing.
        connection.execute(
            text(
                """
                WITH ranked AS (
                    SELECT
                        ep.engagement_participant_id,
                        ep.engagement_id,
                        ep.user_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY ep.engagement_id, ep.user_id
                            ORDER BY (
                                SELECT COUNT(*)
                                FROM consultation_bookings cb
                                WHERE cb.engagement_participant_id = ep.engagement_participant_id
                            ) DESC,
                            ep.engagement_participant_id DESC
                        ) AS rn
                    FROM engagement_participants ep
                ),
                keepers AS (
                    SELECT engagement_participant_id AS keep_id, engagement_id, user_id
                    FROM ranked
                    WHERE rn = 1
                ),
                dupes AS (
                    SELECT r.engagement_participant_id AS del_id, k.keep_id
                    FROM ranked r
                    JOIN keepers k
                      ON k.engagement_id = r.engagement_id
                     AND k.user_id = r.user_id
                    WHERE r.rn > 1
                )
                DELETE FROM consultation_bookings cb
                USING dupes d
                WHERE cb.engagement_participant_id = d.del_id
                  AND EXISTS (
                      SELECT 1
                      FROM consultation_bookings keep_cb
                      WHERE keep_cb.engagement_participant_id = d.keep_id
                        AND keep_cb.expert_type = cb.expert_type
                  )
                """
            )
        )
        connection.execute(
            text(
                """
                WITH ranked AS (
                    SELECT
                        ep.engagement_participant_id,
                        ep.engagement_id,
                        ep.user_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY ep.engagement_id, ep.user_id
                            ORDER BY (
                                SELECT COUNT(*)
                                FROM consultation_bookings cb
                                WHERE cb.engagement_participant_id = ep.engagement_participant_id
                            ) DESC,
                            ep.engagement_participant_id DESC
                        ) AS rn
                    FROM engagement_participants ep
                ),
                keepers AS (
                    SELECT engagement_participant_id AS keep_id, engagement_id, user_id
                    FROM ranked
                    WHERE rn = 1
                ),
                dupes AS (
                    SELECT r.engagement_participant_id AS del_id, k.keep_id
                    FROM ranked r
                    JOIN keepers k
                      ON k.engagement_id = r.engagement_id
                     AND k.user_id = r.user_id
                    WHERE r.rn > 1
                )
                UPDATE consultation_bookings cb
                SET engagement_participant_id = d.keep_id
                FROM dupes d
                WHERE cb.engagement_participant_id = d.del_id
                """
            )
        )
        connection.execute(
            text(
                """
                WITH ranked AS (
                    SELECT
                        ep.engagement_participant_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY ep.engagement_id, ep.user_id
                            ORDER BY (
                                SELECT COUNT(*)
                                FROM consultation_bookings cb
                                WHERE cb.engagement_participant_id = ep.engagement_participant_id
                            ) DESC,
                            ep.engagement_participant_id DESC
                        ) AS rn
                    FROM engagement_participants ep
                )
                DELETE FROM engagement_participants ep
                USING ranked r
                WHERE ep.engagement_participant_id = r.engagement_participant_id
                  AND r.rn > 1
                """
            )
        )
        op.create_unique_constraint(
            "uq_engagement_participants_engagement_user",
            "engagement_participants",
            ["engagement_id", "user_id"],
        )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported for this migration")
