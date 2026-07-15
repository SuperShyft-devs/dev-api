"""Replace override availability with status; normalize participant consultations.

Revision ID: 0094_override_status_consult
Revises: 0093_expert_availability_tables
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect, text


revision = "0094_override_status_consult"
down_revision = "0093_expert_availability_tables"
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

    if _table_exists(inspector, "expert_availability_overrides"):
        if not _column_exists(inspector, "expert_availability_overrides", "status"):
            op.add_column(
                "expert_availability_overrides",
                sa.Column("status", sa.String(), nullable=True),
            )
            if _column_exists(inspector, "expert_availability_overrides", "availability"):
                connection.execute(
                    text(
                        """
                        UPDATE expert_availability_overrides
                        SET status = CASE
                            WHEN availability IS TRUE THEN 'available'
                            ELSE 'unavailable'
                        END
                        WHERE status IS NULL
                        """
                    )
                )
            else:
                connection.execute(
                    text(
                        """
                        UPDATE expert_availability_overrides
                        SET status = 'unavailable'
                        WHERE status IS NULL
                        """
                    )
                )
            op.alter_column(
                "expert_availability_overrides",
                "status",
                existing_type=sa.String(),
                nullable=False,
            )

        inspector = inspect(connection)
        if _column_exists(inspector, "expert_availability_overrides", "availability"):
            op.drop_column("expert_availability_overrides", "availability")

    inspector = inspect(connection)
    if _table_exists(inspector, "engagement_participants") and _column_exists(
        inspector, "engagement_participants", "consultations"
    ):
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
                                    'expert_id', null
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
                                    END
                                )
                            ELSE
                                jsonb_build_object(
                                    'want', false,
                                    'date', null,
                                    'slot', null,
                                    'expert_id', null
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
