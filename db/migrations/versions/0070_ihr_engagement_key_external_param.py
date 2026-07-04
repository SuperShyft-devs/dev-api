"""Nullable assessment_instance_id, unique (user_id, engagement_id), rename external_parameter_id.

Revision ID: 0070_ihr_eng_ext_param
Revises: 0069_blood_report_raw
Create Date: 2026-07-04

Note: revision id must be <= 32 chars (alembic_version.version_num).
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


revision = "0070_ihr_eng_ext_param"
down_revision = "0069_blood_report_raw"
branch_labels = None
depends_on = None


def _has_column(table: str, column: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return any(col["name"] == column for col in inspector.get_columns(table))


def _has_unique_constraint(table: str, name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return any(uc.get("name") == name for uc in inspector.get_unique_constraints(table))


def _column_nullable(table: str, column: str) -> bool | None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for col in inspector.get_columns(table):
        if col["name"] == column:
            return bool(col.get("nullable"))
    return None


def upgrade() -> None:
    # Dedupe individual_health_report: keep newest report_id per (user_id, engagement_id),
    # merging non-null fields from older rows into the keeper when the keeper is missing them.
    # Safe to re-run after a partial apply (no-op when already unique).
    op.execute(
        """
        WITH ranked AS (
            SELECT
                report_id,
                user_id,
                engagement_id,
                ROW_NUMBER() OVER (
                    PARTITION BY user_id, engagement_id
                    ORDER BY report_id DESC
                ) AS rn
            FROM individual_health_report
        ),
        keepers AS (
            SELECT report_id, user_id, engagement_id
            FROM ranked
            WHERE rn = 1
        ),
        merged AS (
            SELECT
                k.report_id AS keeper_id,
                (
                    SELECT r.reports
                    FROM individual_health_report r
                    WHERE r.user_id = k.user_id
                      AND r.engagement_id = k.engagement_id
                      AND r.reports IS NOT NULL
                    ORDER BY r.report_id DESC
                    LIMIT 1
                ) AS reports,
                (
                    SELECT r.blood_parameters
                    FROM individual_health_report r
                    WHERE r.user_id = k.user_id
                      AND r.engagement_id = k.engagement_id
                      AND r.blood_parameters IS NOT NULL
                    ORDER BY r.report_id DESC
                    LIMIT 1
                ) AS blood_parameters,
                (
                    SELECT r.blood_report_raw
                    FROM individual_health_report r
                    WHERE r.user_id = k.user_id
                      AND r.engagement_id = k.engagement_id
                      AND r.blood_report_raw IS NOT NULL
                    ORDER BY r.report_id DESC
                    LIMIT 1
                ) AS blood_report_raw,
                (
                    SELECT r.report_url
                    FROM individual_health_report r
                    WHERE r.user_id = k.user_id
                      AND r.engagement_id = k.engagement_id
                      AND r.report_url IS NOT NULL
                    ORDER BY r.report_id DESC
                    LIMIT 1
                ) AS report_url,
                (
                    SELECT r.diagnostic_report_url
                    FROM individual_health_report r
                    WHERE r.user_id = k.user_id
                      AND r.engagement_id = k.engagement_id
                      AND r.diagnostic_report_url IS NOT NULL
                    ORDER BY r.report_id DESC
                    LIMIT 1
                ) AS diagnostic_report_url,
                (
                    SELECT r.assessment_instance_id
                    FROM individual_health_report r
                    WHERE r.user_id = k.user_id
                      AND r.engagement_id = k.engagement_id
                      AND r.assessment_instance_id IS NOT NULL
                    ORDER BY r.report_id DESC
                    LIMIT 1
                ) AS assessment_instance_id
            FROM keepers k
        )
        UPDATE individual_health_report AS ihr
        SET
            reports = COALESCE(ihr.reports, m.reports),
            blood_parameters = COALESCE(ihr.blood_parameters, m.blood_parameters),
            blood_report_raw = COALESCE(ihr.blood_report_raw, m.blood_report_raw),
            report_url = COALESCE(ihr.report_url, m.report_url),
            diagnostic_report_url = COALESCE(ihr.diagnostic_report_url, m.diagnostic_report_url),
            assessment_instance_id = COALESCE(ihr.assessment_instance_id, m.assessment_instance_id)
        FROM merged m
        WHERE ihr.report_id = m.keeper_id
        """
    )
    op.execute(
        """
        DELETE FROM individual_health_report
        WHERE report_id IN (
            SELECT report_id
            FROM (
                SELECT
                    report_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY user_id, engagement_id
                        ORDER BY report_id DESC
                    ) AS rn
                FROM individual_health_report
            ) ranked
            WHERE rn > 1
        )
        """
    )

    if _column_nullable("individual_health_report", "assessment_instance_id") is False:
        op.alter_column(
            "individual_health_report",
            "assessment_instance_id",
            existing_type=sa.Integer(),
            nullable=True,
        )

    if not _has_unique_constraint(
        "individual_health_report",
        "uq_individual_health_report_user_engagement",
    ):
        op.create_unique_constraint(
            "uq_individual_health_report_user_engagement",
            "individual_health_report",
            ["user_id", "engagement_id"],
        )

    if _has_column("health_parameters", "healthians_parameter_id") and not _has_column(
        "health_parameters", "external_parameter_id"
    ):
        op.alter_column(
            "health_parameters",
            "healthians_parameter_id",
            new_column_name="external_parameter_id",
            existing_type=sa.Integer(),
            existing_nullable=True,
        )


def downgrade() -> None:
    if _has_column("health_parameters", "external_parameter_id") and not _has_column(
        "health_parameters", "healthians_parameter_id"
    ):
        op.alter_column(
            "health_parameters",
            "external_parameter_id",
            new_column_name="healthians_parameter_id",
            existing_type=sa.Integer(),
            existing_nullable=True,
        )

    if _has_unique_constraint(
        "individual_health_report",
        "uq_individual_health_report_user_engagement",
    ):
        op.drop_constraint(
            "uq_individual_health_report_user_engagement",
            "individual_health_report",
            type_="unique",
        )

    # Fill null assessment_instance_id from any assessment for the same user/engagement.
    op.execute(
        """
        UPDATE individual_health_report AS ihr
        SET assessment_instance_id = (
            SELECT ai.assessment_instance_id
            FROM assessment_instances AS ai
            WHERE ai.user_id = ihr.user_id
              AND ai.engagement_id = ihr.engagement_id
            ORDER BY ai.assessment_instance_id DESC
            LIMIT 1
        )
        WHERE ihr.assessment_instance_id IS NULL
        """
    )
    op.execute(
        """
        DELETE FROM individual_health_report
        WHERE assessment_instance_id IS NULL
        """
    )
    if _column_nullable("individual_health_report", "assessment_instance_id") is True:
        op.alter_column(
            "individual_health_report",
            "assessment_instance_id",
            existing_type=sa.Integer(),
            nullable=False,
        )
