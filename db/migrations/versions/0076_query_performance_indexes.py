"""Add query-performance indexes for hot-path API lookups.

Revision ID: 0076_query_perf_indexes
Revises: 0075_participant_booking_idx
Create Date: 2026-07-05

Note: revision id must be <= 32 chars (alembic_version.version_num).
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect, text


revision = "0076_query_perf_indexes"
down_revision = "0075_participant_booking_idx"
branch_labels = None
depends_on = None


def _index_exists(inspector: sa.Inspector, table_name: str, index_name: str) -> bool:
    if table_name not in inspector.get_table_names():
        return False
    return any(idx["name"] == index_name for idx in inspector.get_indexes(table_name))


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _create_index_if_missing(
    inspector: sa.Inspector,
    *,
    index_name: str,
    table_name: str,
    columns: list[str],
    unique: bool = False,
    postgresql_where: sa.TextClause | None = None,
) -> None:
    if not _table_exists(inspector, table_name):
        return
    if _index_exists(inspector, table_name, index_name):
        return
    kwargs: dict = {}
    if postgresql_where is not None:
        kwargs["postgresql_where"] = postgresql_where
    op.create_index(index_name, table_name, columns, unique=unique, **kwargs)


def _assert_no_duplicate_engagement_codes(connection: sa.Connection) -> None:
    if "engagements" not in inspect(connection).get_table_names():
        return
    duplicates = connection.execute(
        text(
            """
            SELECT engagement_code, COUNT(*) AS cnt
            FROM engagements
            GROUP BY engagement_code
            HAVING COUNT(*) > 1
            LIMIT 5
            """
        )
    ).fetchall()
    if duplicates:
        sample = ", ".join(f"{row.engagement_code!r} ({row.cnt})" for row in duplicates)
        raise RuntimeError(
            f"Cannot create unique index uq_engagements_engagement_code: duplicate engagement_code values found: {sample}"
        )


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    _assert_no_duplicate_engagement_codes(connection)

    # engagements
    _create_index_if_missing(
        inspector,
        index_name="uq_engagements_engagement_code",
        table_name="engagements",
        columns=["engagement_code"],
        unique=True,
    )
    _create_index_if_missing(
        inspector,
        index_name="ix_engagements_organization_id",
        table_name="engagements",
        columns=["organization_id"],
    )
    _create_index_if_missing(
        inspector,
        index_name="ix_engagements_camp_no",
        table_name="engagements",
        columns=["camp_no"],
    )

    # engagement_participants
    _create_index_if_missing(
        inspector,
        index_name="ix_ep_engagement_id_user_id",
        table_name="engagement_participants",
        columns=["engagement_id", "user_id"],
    )
    _create_index_if_missing(
        inspector,
        index_name="ix_ep_user_id",
        table_name="engagement_participants",
        columns=["user_id"],
    )
    _create_index_if_missing(
        inspector,
        index_name="ix_ep_engagement_date",
        table_name="engagement_participants",
        columns=["engagement_date"],
    )

    # assessment_instances
    _create_index_if_missing(
        inspector,
        index_name="ix_ai_user_id",
        table_name="assessment_instances",
        columns=["user_id"],
    )
    _create_index_if_missing(
        inspector,
        index_name="ix_ai_engagement_id",
        table_name="assessment_instances",
        columns=["engagement_id"],
    )
    _create_index_if_missing(
        inspector,
        index_name="ix_ai_metsights_record_id",
        table_name="assessment_instances",
        columns=["metsights_record_id"],
        postgresql_where=sa.text("metsights_record_id IS NOT NULL"),
    )

    # questionnaire_responses
    _create_index_if_missing(
        inspector,
        index_name="ix_qr_assessment_instance_id",
        table_name="questionnaire_responses",
        columns=["assessment_instance_id"],
    )

    # individual_health_report
    _create_index_if_missing(
        inspector,
        index_name="ix_ihr_user_engagement",
        table_name="individual_health_report",
        columns=["user_id", "engagement_id"],
    )
    _create_index_if_missing(
        inspector,
        index_name="ix_ihr_assessment_instance_id",
        table_name="individual_health_report",
        columns=["assessment_instance_id"],
        postgresql_where=sa.text("assessment_instance_id IS NOT NULL"),
    )

    # users
    _create_index_if_missing(
        inspector,
        index_name="ix_users_parent_id",
        table_name="users",
        columns=["parent_id"],
    )
    _create_index_if_missing(
        inspector,
        index_name="ix_users_metsights_profile_id",
        table_name="users",
        columns=["metsights_profile_id"],
        postgresql_where=sa.text("metsights_profile_id IS NOT NULL"),
    )

    # auth
    _create_index_if_missing(
        inspector,
        index_name="ix_auth_otp_sessions_user_id",
        table_name="auth_otp_sessions",
        columns=["user_id"],
    )
    _create_index_if_missing(
        inspector,
        index_name="ix_auth_tokens_user_id",
        table_name="auth_tokens",
        columns=["user_id"],
    )

    # employee
    _create_index_if_missing(
        inspector,
        index_name="ix_employee_user_id",
        table_name="employee",
        columns=["user_id"],
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
