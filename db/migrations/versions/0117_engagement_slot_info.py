"""Move engagements.slot_detail to engagement_slot_info table.

Revision ID: 0117_engagement_slot_info
Revises: 0116_req_external_link
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect


revision = "0117_engagement_slot_info"
down_revision = ("0116_req_external_link", "0114_cb_consultation_cabin")
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


def upgrade() -> None:
    op.execute(sa.text("SET LOCAL lock_timeout = '60s'"))
    connection = op.get_bind()
    inspector = inspect(connection)

    if not _table_exists(inspector, "engagement_slot_info"):
        op.create_table(
            "engagement_slot_info",
            sa.Column("slot_detail_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("slot_detail", sa.dialects.postgresql.JSONB(), nullable=False),
        )

    if not _column_exists(inspector, "engagements", "slot_detail_id"):
        op.add_column(
            "engagements",
            sa.Column(
                "slot_detail_id",
                sa.Integer(),
                sa.ForeignKey("engagement_slot_info.slot_detail_id", ondelete="SET NULL"),
                nullable=True,
            ),
        )
        op.create_index("ix_engagements_slot_detail_id", "engagements", ["slot_detail_id"])

    if _column_exists(inspector, "engagements", "slot_detail"):
        op.execute(
            sa.text(
                """
                DO $$
                DECLARE
                    rec RECORD;
                    new_id INT;
                BEGIN
                    FOR rec IN
                        SELECT engagement_id, slot_detail
                        FROM engagements
                        WHERE slot_detail IS NOT NULL
                          AND slot_detail_id IS NULL
                    LOOP
                        INSERT INTO engagement_slot_info (slot_detail)
                        VALUES (rec.slot_detail::jsonb)
                        RETURNING slot_detail_id INTO new_id;

                        UPDATE engagements
                        SET slot_detail_id = new_id
                        WHERE engagement_id = rec.engagement_id;
                    END LOOP;
                END $$;
                """
            )
        )

        # Dedup: same org + city + dates + identical slot_detail JSON → one slot_detail_id
        op.execute(
            sa.text(
                """
                WITH ranked AS (
                    SELECT
                        e.engagement_id,
                        e.slot_detail_id,
                        FIRST_VALUE(e.slot_detail_id) OVER (
                            PARTITION BY
                                e.organization_id,
                                lower(trim(e.city)),
                                e.start_date,
                                e.end_date,
                                esi.slot_detail::text
                            ORDER BY e.slot_detail_id
                        ) AS canonical_id
                    FROM engagements e
                    JOIN engagement_slot_info esi ON esi.slot_detail_id = e.slot_detail_id
                    WHERE e.slot_detail_id IS NOT NULL
                ),
                to_repoint AS (
                    SELECT engagement_id, canonical_id
                    FROM ranked
                    WHERE slot_detail_id <> canonical_id
                )
                UPDATE engagements e
                SET slot_detail_id = tr.canonical_id
                FROM to_repoint tr
                WHERE e.engagement_id = tr.engagement_id;
                """
            )
        )

        op.execute(
            sa.text(
                """
                DELETE FROM engagement_slot_info esi
                WHERE NOT EXISTS (
                    SELECT 1 FROM engagements e WHERE e.slot_detail_id = esi.slot_detail_id
                );
                """
            )
        )

        op.drop_column("engagements", "slot_detail")


def downgrade() -> None:
    op.execute(sa.text("SET LOCAL lock_timeout = '15s'"))
    connection = op.get_bind()
    inspector = inspect(connection)

    if not _column_exists(inspector, "engagements", "slot_detail"):
        op.add_column("engagements", sa.Column("slot_detail", sa.JSON(), nullable=True))

    if _column_exists(inspector, "engagements", "slot_detail_id"):
        op.execute(
            sa.text(
                """
                UPDATE engagements e
                SET slot_detail = esi.slot_detail
                FROM engagement_slot_info esi
                WHERE e.slot_detail_id = esi.slot_detail_id
                  AND e.slot_detail IS NULL;
                """
            )
        )
        op.drop_index("ix_engagements_slot_detail_id", table_name="engagements")
        op.drop_column("engagements", "slot_detail_id")

    if _table_exists(inspector, "engagement_slot_info"):
        op.drop_table("engagement_slot_info")
