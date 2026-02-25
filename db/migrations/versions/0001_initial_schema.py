"""Initial schema

Revision ID: 0001_initial_schema
Revises: 
Create Date: 2026-02-17

This migration creates the baseline tables.
It matches instructions/db-schema.txt.
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0001_initial_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "organizations",
        sa.Column("organization_id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String()),
        sa.Column("organization_type", sa.String()),
        sa.Column("logo", sa.String()),
        sa.Column("website_url", sa.String()),
        sa.Column("address", sa.Text()),
        sa.Column("pin_code", sa.String()),
        sa.Column("city", sa.String()),
        sa.Column("state", sa.String()),
        sa.Column("country", sa.String()),
        sa.Column("contact_name", sa.String()),
        sa.Column("contact_email", sa.String()),
        sa.Column("contact_phone", sa.String()),
        sa.Column("contact_designation", sa.String()),
        sa.Column("bd_employee_id", sa.Integer()),
        sa.Column("status", sa.String()),
        sa.Column("created_at", sa.DateTime(timezone=True)),
        sa.Column("created_employee_id", sa.Integer()),
        sa.Column("updated_at", sa.DateTime(timezone=True)),
        sa.Column("updated_employee_id", sa.Integer()),
    )

    op.create_table(
        "users",
        sa.Column("user_id", sa.Integer(), primary_key=True),
        sa.Column("first_name", sa.String()),
        sa.Column("last_name", sa.String()),
        sa.Column("phone", sa.String(), nullable=False),
        sa.Column("email", sa.String()),
        sa.Column("date_of_birth", sa.Date()),
        sa.Column("gender", sa.String()),
        sa.Column("address", sa.Text()),
        sa.Column("pin_code", sa.String()),
        sa.Column("city", sa.String()),
        sa.Column("state", sa.String()),
        sa.Column("country", sa.String()),
        sa.Column("referred_by", sa.String()),
        sa.Column("is_participant", sa.Boolean()),
        sa.Column("status", sa.String()),
        sa.Column("created_at", sa.DateTime(timezone=True)),
        sa.Column("updated_at", sa.DateTime(timezone=True)),
    )

    op.create_table(
        "employee",
        sa.Column("employee_id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("role", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True)),
        sa.Column("updated_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(["user_id"], ["users.user_id"]),
    )

    op.create_table(
        "assessment_packages",
        sa.Column("package_id", sa.Integer(), primary_key=True),
        sa.Column("package_code", sa.String()),
        sa.Column("display_name", sa.String()),
        sa.Column("status", sa.String()),
    )

    op.create_table(
        "diagnostic_package",
        sa.Column("diagnostic_package_id", sa.Integer(), primary_key=True),
        sa.Column("reference_id", sa.String()),
        sa.Column("package_name", sa.String()),
        sa.Column("diagnostic_provider", sa.String()),
        sa.Column("package_info", sa.JSON()),
        sa.Column("no_of_tests", sa.Integer()),
        sa.Column("status", sa.String()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )

    op.create_table(
        "engagements",
        sa.Column("engagement_id", sa.Integer(), primary_key=True),
        sa.Column("engagement_name", sa.String()),
        sa.Column("metsights_engagement_id", sa.String()),
        sa.Column("organization_id", sa.Integer()),
        sa.Column("engagement_code", sa.String(), nullable=False),
        sa.Column("engagement_type", sa.String()),
        sa.Column("assessment_package_id", sa.Integer(), nullable=False),
        sa.Column("diagnostic_package_id", sa.Integer(), nullable=True),
        sa.Column("city", sa.String()),
        sa.Column("slot_duration", sa.Integer()),
        sa.Column("start_date", sa.Date()),
        sa.Column("end_date", sa.Date()),
        sa.Column("status", sa.String()),
        sa.Column("participant_count", sa.Integer()),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.organization_id"]),
        sa.ForeignKeyConstraint(["assessment_package_id"], ["assessment_packages.package_id"]),
        sa.ForeignKeyConstraint(["diagnostic_package_id"], ["diagnostic_package.diagnostic_package_id"]),
    )

    op.create_table(
        "onboarding_assistant_assignment",
        sa.Column("onboarding_assistant_id", sa.Integer(), primary_key=True),
        sa.Column("employee_id", sa.Integer(), nullable=False),
        sa.Column("engagement_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["employee_id"], ["employee.employee_id"]),
        sa.ForeignKeyConstraint(["engagement_id"], ["engagements.engagement_id"]),
    )

    # Create unique constraint to prevent duplicate assignments
    op.create_unique_constraint(
        "uq_onboarding_assistant_assignment",
        "onboarding_assistant_assignment",
        ["engagement_id", "employee_id"],
    )

    # Create indexes for performance
    op.create_index(
        "ix_onboarding_assistant_assignment_engagement_id",
        "onboarding_assistant_assignment",
        ["engagement_id"],
    )
    op.create_index(
        "ix_onboarding_assistant_assignment_employee_id",
        "onboarding_assistant_assignment",
        ["employee_id"],
    )

    op.create_table(
        "engagement_time_slots",
        sa.Column("time_slot_id", sa.Integer(), primary_key=True),
        sa.Column("engagement_id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("slot_start_time", sa.Time(), nullable=False),
        sa.Column("engagement_date", sa.Date(), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.user_id"]),
        sa.ForeignKeyConstraint(["engagement_id"], ["engagements.engagement_id"]),
    )

    op.create_table(
        "assessment_instances",
        sa.Column("assessment_instance_id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("package_id", sa.Integer(), nullable=False),
        sa.Column("engagement_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String()),
        sa.Column("assigned_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(["user_id"], ["users.user_id"]),
        sa.ForeignKeyConstraint(["package_id"], ["assessment_packages.package_id"]),
        sa.ForeignKeyConstraint(["engagement_id"], ["engagements.engagement_id"]),
    )

    op.create_table(
        "questionnaire_definitions",
        sa.Column("question_id", sa.Integer(), primary_key=True),
        sa.Column("question_text", sa.Text(), nullable=False),
        sa.Column("question_type", sa.String(), nullable=False),
        sa.Column("options", sa.JSON()),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True)),
    )

    op.create_table(
        "assessment_package_questions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("package_id", sa.Integer(), nullable=False),
        sa.Column("question_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["package_id"], ["assessment_packages.package_id"]),
        sa.ForeignKeyConstraint(["question_id"], ["questionnaire_definitions.question_id"]),
    )

    op.create_table(
        "questionnaire_responses",
        sa.Column("response_id", sa.Integer(), primary_key=True),
        sa.Column("assessment_instance_id", sa.Integer(), nullable=False),
        sa.Column("question_id", sa.Integer(), nullable=False),
        sa.Column("answer", sa.JSON()),
        sa.Column("submitted_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(["assessment_instance_id"], ["assessment_instances.assessment_instance_id"]),
        sa.ForeignKeyConstraint(["question_id"], ["questionnaire_definitions.question_id"]),
    )

    op.create_table(
        "individual_health_report",
        sa.Column("report_id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("assessment_instance_id", sa.Integer(), nullable=False),
        sa.Column("engagement_id", sa.Integer(), nullable=False),
        sa.Column("metsights_output", sa.JSON()),
        sa.Column("diagnostics_output", sa.JSON()),
        sa.Column("created_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(["user_id"], ["users.user_id"]),
        sa.ForeignKeyConstraint(["assessment_instance_id"], ["assessment_instances.assessment_instance_id"]),
        sa.ForeignKeyConstraint(["engagement_id"], ["engagements.engagement_id"]),
    )

    op.create_table(
        "organization_health_report",
        sa.Column("report_id", sa.Integer(), primary_key=True),
        sa.Column("metsights_output", sa.JSON()),
        sa.Column("created_at", sa.DateTime(timezone=True)),
        sa.Column("engagement_id", sa.Integer(), nullable=False),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["engagement_id"], ["engagements.engagement_id"]),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.organization_id"]),
    )

    op.create_table(
        "auth_otp_sessions",
        sa.Column("session_id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("otp_hash", sa.String(), nullable=False),
        sa.Column("otp_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.user_id"]),
    )

    op.create_table(
        "auth_tokens",
        sa.Column("token_id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("refresh_token_hash", sa.String(), nullable=False),
        sa.Column("issued_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.user_id"]),
    )

    op.create_table(
        "data_audit_logs",
        sa.Column("audit_id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer()),
        sa.Column("session_id", sa.Integer()),
        sa.Column("action", sa.String(), nullable=False),
        sa.Column("ip_address", sa.String()),
        sa.Column("user_agent", sa.Text()),
        sa.Column("endpoint", sa.String()),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.user_id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["session_id"], ["auth_otp_sessions.session_id"], ondelete="SET NULL"),
    )

    op.create_foreign_key(
        "fk_org_bd_employee",
        "organizations",
        "employee",
        ["bd_employee_id"],
        ["employee_id"],
    )

    op.create_foreign_key(
        "fk_org_created_employee",
        "organizations",
        "employee",
        ["created_employee_id"],
        ["employee_id"],
    )

    op.create_foreign_key(
        "fk_org_updated_employee",
        "organizations",
        "employee",
        ["updated_employee_id"],
        ["employee_id"],
    )


def downgrade() -> None:
    """Downgrades are intentionally disabled.

    This project treats the database as a compliance record.
    Dropping tables can destroy history.
    """

    raise RuntimeError("Downgrade is not supported")
