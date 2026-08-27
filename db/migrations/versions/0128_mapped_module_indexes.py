"""Add query indexes for mapped module tables (hot-path lookups).

Revision ID: 0128_mapped_module_indexes
Revises: 0127_users_age_nullable
Create Date: 2026-08-27

Note: revision id must be <= 32 chars (alembic_version.version_num).
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect, text


revision = "0128_mapped_module_indexes"
down_revision = "0127_users_age_nullable"
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


def _create_gin_index_if_missing(
    inspector: sa.Inspector,
    *,
    index_name: str,
    table_name: str,
    column_name: str,
) -> None:
    if not _table_exists(inspector, table_name):
        return
    if _index_exists(inspector, table_name, index_name):
        return
    op.execute(
        text(
            f"CREATE INDEX {index_name} "
            f"ON {table_name} USING gin ({column_name})"
        )
    )


def _create_expression_index_if_missing(
    inspector: sa.Inspector,
    *,
    index_name: str,
    table_name: str,
    expression_sql: str,
    where_sql: str | None = None,
) -> None:
    if not _table_exists(inspector, table_name):
        return
    if _index_exists(inspector, table_name, index_name):
        return
    where_clause = f" WHERE {where_sql}" if where_sql else ""
    op.execute(
        text(
            f"CREATE INDEX {index_name} "
            f"ON {table_name} ({expression_sql}){where_clause}"
        )
    )


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    # ── P0: notifications ────────────────────────────────────────────────
    _create_index_if_missing(
        inspector,
        index_name="ix_notifications_pending_dispatched_at",
        table_name="notifications",
        columns=["dispatched_at"],
        postgresql_where=sa.text("status = 'pending' AND dispatched_at IS NOT NULL"),
    )
    _create_index_if_missing(
        inspector,
        index_name="ix_notifications_svc_eng_status",
        table_name="notifications",
        columns=["service_key", "engagement_id", "status"],
    )
    _create_index_if_missing(
        inspector,
        index_name="ix_notifications_engagement_id",
        table_name="notifications",
        columns=["engagement_id"],
    )
    _create_index_if_missing(
        inspector,
        index_name="ix_notifications_assessment_instance_id",
        table_name="notifications",
        columns=["assessment_instance_id"],
    )

    # ── P0: integration_sync_logs ──────────────────────────────────────
    _create_index_if_missing(
        inspector,
        index_name="ix_integration_sync_logs_provider_status_created",
        table_name="integration_sync_logs",
        columns=["provider", "status", "created_at"],
    )
    _create_gin_index_if_missing(
        inspector,
        index_name="ix_integration_sync_logs_request_payload_gin",
        table_name="integration_sync_logs",
        column_name="request_payload",
    )

    # ── P0: bookings ─────────────────────────────────────────────────────
    _create_index_if_missing(
        inspector,
        index_name="ix_bookings_user_entity",
        table_name="bookings",
        columns=["user_id", "entity_type", "entity_id"],
    )
    _create_index_if_missing(
        inspector,
        index_name="ix_bookings_status",
        table_name="bookings",
        columns=["status"],
    )

    # ── P0: experts ──────────────────────────────────────────────────────
    _create_index_if_missing(
        inspector,
        index_name="ix_experts_status_type",
        table_name="experts",
        columns=["status", "expert_type"],
    )
    _create_index_if_missing(
        inspector,
        index_name="ix_experts_user_id",
        table_name="experts",
        columns=["user_id"],
    )

    # ── P0: diagnostic_package ───────────────────────────────────────────
    _create_index_if_missing(
        inspector,
        index_name="ix_diagnostic_package_status_package_for",
        table_name="diagnostic_package",
        columns=["status", "package_for"],
    )
    _create_index_if_missing(
        inspector,
        index_name="ix_diagnostic_package_created_by_user_id",
        table_name="diagnostic_package",
        columns=["created_by_user_id"],
        postgresql_where=sa.text("created_by_user_id IS NOT NULL"),
    )

    # ── P1: data_audit_logs ──────────────────────────────────────────────
    _create_index_if_missing(
        inspector,
        index_name="ix_data_audit_logs_user_id",
        table_name="data_audit_logs",
        columns=["user_id"],
    )

    # ── P1: organizations ──────────────────────────────────────────────
    _create_index_if_missing(
        inspector,
        index_name="ix_organizations_status",
        table_name="organizations",
        columns=["status"],
    )
    _create_index_if_missing(
        inspector,
        index_name="ix_organizations_bd_employee_id",
        table_name="organizations",
        columns=["bd_employee_id"],
    )
    _create_index_if_missing(
        inspector,
        index_name="ix_organizations_industry_key",
        table_name="organizations",
        columns=["industry_key"],
    )

    # ── P1: support_tickets ────────────────────────────────────────────
    _create_index_if_missing(
        inspector,
        index_name="ix_support_tickets_status",
        table_name="support_tickets",
        columns=["status"],
    )
    _create_index_if_missing(
        inspector,
        index_name="ix_support_tickets_user_id",
        table_name="support_tickets",
        columns=["user_id"],
    )

    # ── P1: checklist_template_items ───────────────────────────────────
    _create_index_if_missing(
        inspector,
        index_name="ix_checklist_template_items_template_id",
        table_name="checklist_template_items",
        columns=["template_id"],
    )

    # ── P1: expert_availability_overrides ──────────────────────────────
    _create_index_if_missing(
        inspector,
        index_name="ix_expert_availability_overrides_expert_date",
        table_name="expert_availability_overrides",
        columns=["expert_id", "override_date"],
    )

    # ── P1: expert_reviews ─────────────────────────────────────────────
    _create_index_if_missing(
        inspector,
        index_name="ix_expert_reviews_expert_created",
        table_name="expert_reviews",
        columns=["expert_id", "created_at"],
    )

    # ── P1: assessment_packages ────────────────────────────────────────
    _create_index_if_missing(
        inspector,
        index_name="ix_assessment_packages_package_code",
        table_name="assessment_packages",
        columns=["package_code"],
    )
    _create_index_if_missing(
        inspector,
        index_name="ix_assessment_packages_type_status",
        table_name="assessment_packages",
        columns=["assessment_type_code", "status"],
    )

    # ── P1: diagnostic_package_filters_chips ───────────────────────────
    _create_index_if_missing(
        inspector,
        index_name="ix_diag_filter_chips_status_chip_for_order",
        table_name="diagnostic_package_filters_chips",
        columns=["status", "chip_for", "display_order"],
    )

    # ── P1: health_parameters ────────────────────────────────────────
    _create_index_if_missing(
        inspector,
        index_name="ix_health_parameters_parameter_type",
        table_name="health_parameters",
        columns=["parameter_type"],
    )
    _create_expression_index_if_missing(
        inspector,
        index_name="ix_health_parameters_parameter_key_lower",
        table_name="health_parameters",
        expression_sql="lower(parameter_key)",
        where_sql="parameter_key IS NOT NULL",
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
