"""Backfill done=false on engagement_participants.consultations JSON.

Revision ID: 0095_participant_consult_done
Revises: 0094_override_status_consult
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect, text


revision = "0095_participant_consult_done"
down_revision = "0094_override_status_consult"
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if not (
        _table_exists(inspector, "engagement_participants")
        and _column_exists(inspector, "engagement_participants", "consultations")
    ):
        return

    connection.execute(
        text(
            """
            UPDATE engagement_participants
            SET consultations = (
                SELECT jsonb_object_agg(
                    key,
                    CASE
                        WHEN jsonb_typeof(value) = 'boolean' THEN
                            jsonb_build_object(
                                'want', value,
                                'date', null,
                                'slot', null,
                                'expert_id', null,
                                'done', false
                            )
                        WHEN jsonb_typeof(value) = 'object' THEN
                            jsonb_build_object(
                                'want', COALESCE((value->>'want')::boolean, false),
                                'date', value->>'date',
                                'slot', value->>'slot',
                                'expert_id', CASE
                                    WHEN value->>'expert_id' IS NULL
                                      OR value->>'expert_id' = 'null'
                                    THEN null
                                    ELSE (value->>'expert_id')::int
                                END,
                                'done', COALESCE((value->>'done')::boolean, false)
                            )
                        ELSE
                            jsonb_build_object(
                                'want', false,
                                'date', null,
                                'slot', null,
                                'expert_id', null,
                                'done', false
                            )
                    END
                )
                FROM jsonb_each(consultations::jsonb)
            )
            WHERE consultations IS NOT NULL
              AND jsonb_typeof(consultations::jsonb) = 'object'
            """
        )
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
