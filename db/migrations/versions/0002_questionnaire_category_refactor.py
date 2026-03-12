"""Questionnaire and assessment category refactor.

Revision ID: 0002_qnr_cat_refactor
Revises: 0001_initial_schema
Create Date: 2026-03-11
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text


revision = "0002_qnr_cat_refactor"
down_revision = "0001_initial_schema"
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    columns = inspector.get_columns(table_name)
    return any(col["name"] == column_name for col in columns)


def _constraint_exists(connection: sa.Connection, constraint_name: str) -> bool:
    result = connection.execute(
        text(
            """
            SELECT 1
            FROM information_schema.table_constraints
            WHERE constraint_name = :constraint_name
            LIMIT 1
            """
        ),
        {"constraint_name": constraint_name},
    )
    return result.scalar() is not None


def _index_exists(inspector: sa.Inspector, table_name: str, index_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    indexes = inspector.get_indexes(table_name)
    return any(idx["name"] == index_name for idx in indexes)


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    if _table_exists(inspector, "questionnaire_definitions"):
        if not _column_exists(inspector, "questionnaire_definitions", "question_key"):
            op.add_column("questionnaire_definitions", sa.Column("question_key", sa.String(), nullable=True))
        if not _column_exists(inspector, "questionnaire_definitions", "category_id"):
            op.add_column("questionnaire_definitions", sa.Column("category_id", sa.Integer(), nullable=True))
        if not _column_exists(inspector, "questionnaire_definitions", "is_required"):
            op.add_column(
                "questionnaire_definitions",
                sa.Column("is_required", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            )
        if not _column_exists(inspector, "questionnaire_definitions", "is_read_only"):
            op.add_column(
                "questionnaire_definitions",
                sa.Column("is_read_only", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            )
        if not _column_exists(inspector, "questionnaire_definitions", "help_text"):
            op.add_column("questionnaire_definitions", sa.Column("help_text", sa.Text(), nullable=True))

    inspector = inspect(connection)
    if not _table_exists(inspector, "questionnaire_categories"):
        op.create_table(
            "questionnaire_categories",
            sa.Column("category_id", sa.Integer(), primary_key=True),
            sa.Column("category_key", sa.String(), nullable=False),
            sa.Column("display_name", sa.String(), nullable=False),
        )

    inspector = inspect(connection)
    if not _constraint_exists(connection, "uq_questionnaire_categories_key"):
        op.create_unique_constraint(
            "uq_questionnaire_categories_key",
            "questionnaire_categories",
            ["category_key"],
        )

    inspector = inspect(connection)
    if not _table_exists(inspector, "questionnaire_options"):
        op.create_table(
            "questionnaire_options",
            sa.Column("option_id", sa.Integer(), primary_key=True),
            sa.Column("question_id", sa.Integer(), nullable=False),
            sa.Column("option_value", sa.String(), nullable=False),
            sa.Column("display_name", sa.String(), nullable=False),
            sa.Column("tooltip_text", sa.Text(), nullable=True),
            sa.ForeignKeyConstraint(["question_id"], ["questionnaire_definitions.question_id"]),
        )

    inspector = inspect(connection)
    if not _constraint_exists(connection, "uq_questionnaire_options_question_value"):
        op.create_unique_constraint(
            "uq_questionnaire_options_question_value",
            "questionnaire_options",
            ["question_id", "option_value"],
        )

    inspector = inspect(connection)
    if not _table_exists(inspector, "assessment_package_categories"):
        op.create_table(
            "assessment_package_categories",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("package_id", sa.Integer(), nullable=False),
            sa.Column("category_id", sa.Integer(), nullable=False),
            sa.ForeignKeyConstraint(["package_id"], ["assessment_packages.package_id"]),
            sa.ForeignKeyConstraint(["category_id"], ["questionnaire_categories.category_id"]),
        )

    if not _constraint_exists(connection, "uq_assessment_package_categories_pair"):
        op.create_unique_constraint(
            "uq_assessment_package_categories_pair",
            "assessment_package_categories",
            ["package_id", "category_id"],
        )

    inspector = inspect(connection)
    if _table_exists(inspector, "questionnaire_definitions") and _table_exists(inspector, "questionnaire_categories"):
        fallback = connection.execute(
            text(
                """
                INSERT INTO questionnaire_categories (category_key, display_name)
                VALUES ('general', 'General')
                ON CONFLICT (category_key) DO NOTHING
                RETURNING category_id
                """
            )
        ).scalar_one_or_none()

        if fallback is None:
            fallback = connection.execute(
                text("SELECT category_id FROM questionnaire_categories WHERE category_key = 'general' LIMIT 1")
            ).scalar_one()

        connection.execute(
            text(
                """
                UPDATE questionnaire_definitions
                SET category_id = COALESCE(category_id, :fallback_category_id),
                    question_key = COALESCE(NULLIF(question_key, ''), CONCAT('q_', question_id::text)),
                    is_required = COALESCE(is_required, false),
                    is_read_only = COALESCE(is_read_only, false)
                """
            ),
            {"fallback_category_id": int(fallback)},
        )

    inspector = inspect(connection)
    if _table_exists(inspector, "questionnaire_definitions") and _column_exists(
        inspector, "questionnaire_definitions", "options"
    ):
        connection.execute(
            text(
                """
                INSERT INTO questionnaire_options (question_id, option_value, display_name)
                SELECT q.question_id, o.option_text, o.option_text
                FROM questionnaire_definitions AS q
                CROSS JOIN LATERAL json_array_elements_text(q.options) AS o(option_text)
                WHERE q.options IS NOT NULL
                  AND json_typeof(q.options) = 'array'
                ON CONFLICT (question_id, option_value) DO NOTHING
                """
            )
        )

    inspector = inspect(connection)
    if _table_exists(inspector, "assessment_package_questions") and _table_exists(inspector, "assessment_package_categories"):
        connection.execute(
            text(
                """
                INSERT INTO assessment_package_categories (package_id, category_id)
                SELECT DISTINCT apq.package_id, qd.category_id
                FROM assessment_package_questions AS apq
                JOIN questionnaire_definitions AS qd
                  ON qd.question_id = apq.question_id
                WHERE qd.category_id IS NOT NULL
                ON CONFLICT (package_id, category_id) DO NOTHING
                """
            )
        )

    inspector = inspect(connection)
    if _table_exists(inspector, "questionnaire_definitions") and not _constraint_exists(
        connection, "fk_questionnaire_definitions_category_id"
    ):
        op.create_foreign_key(
            "fk_questionnaire_definitions_category_id",
            "questionnaire_definitions",
            "questionnaire_categories",
            ["category_id"],
            ["category_id"],
        )

    inspector = inspect(connection)
    if _table_exists(inspector, "questionnaire_definitions") and not _index_exists(
        inspector, "questionnaire_definitions", "ix_questionnaire_definitions_category_id"
    ):
        op.create_index(
            "ix_questionnaire_definitions_category_id",
            "questionnaire_definitions",
            ["category_id"],
        )

    inspector = inspect(connection)
    if _table_exists(inspector, "assessment_package_categories") and not _index_exists(
        inspector, "assessment_package_categories", "ix_assessment_package_categories_package_id"
    ):
        op.create_index(
            "ix_assessment_package_categories_package_id",
            "assessment_package_categories",
            ["package_id"],
        )

    inspector = inspect(connection)
    if _table_exists(inspector, "assessment_package_categories") and not _index_exists(
        inspector, "assessment_package_categories", "ix_assessment_package_categories_category_id"
    ):
        op.create_index(
            "ix_assessment_package_categories_category_id",
            "assessment_package_categories",
            ["category_id"],
        )

    inspector = inspect(connection)
    if _table_exists(inspector, "assessment_package_questions"):
        op.drop_table("assessment_package_questions")

    # Intentionally keep legacy `questionnaire_definitions.options` column for backward compatibility.


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
