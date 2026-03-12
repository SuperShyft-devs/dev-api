"""Questionnaire multi-category and progress refactor.

Revision ID: 0004_qnr_multi_category_progress
Revises: 0003_qkey_constraints
Create Date: 2026-03-12
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect, text


revision = "0004_qnr_multi_category_progress"
down_revision = "0003_qkey_constraints"
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _column_exists(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    if not _table_exists(inspector, table_name):
        return False
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


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
    return any(idx["name"] == index_name for idx in inspector.get_indexes(table_name))


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)

    # 1) Add status to categories.
    if _table_exists(inspector, "questionnaire_categories") and not _column_exists(
        inspector, "questionnaire_categories", "status"
    ):
        op.add_column(
            "questionnaire_categories",
            sa.Column("status", sa.String(), nullable=True, server_default=sa.text("'active'")),
        )
        connection.execute(
            text(
                """
                UPDATE questionnaire_categories
                SET status = COALESCE(NULLIF(trim(status), ''), 'active')
                """
            )
        )
        op.alter_column("questionnaire_categories", "status", existing_type=sa.String(), nullable=False)

    inspector = inspect(connection)

    # 2) Create questionnaire_category_questions.
    if not _table_exists(inspector, "questionnaire_category_questions"):
        op.create_table(
            "questionnaire_category_questions",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("category_id", sa.Integer(), nullable=False),
            sa.Column("question_id", sa.Integer(), nullable=False),
            sa.ForeignKeyConstraint(["category_id"], ["questionnaire_categories.category_id"]),
            sa.ForeignKeyConstraint(["question_id"], ["questionnaire_definitions.question_id"]),
        )

    # Deduplicate pre-existing rows before adding unique constraint.
    if _table_exists(inspector, "questionnaire_category_questions"):
        connection.execute(
            text(
                """
                DELETE FROM questionnaire_category_questions a
                USING questionnaire_category_questions b
                WHERE a.id > b.id
                  AND a.category_id = b.category_id
                  AND a.question_id = b.question_id
                """
            )
        )

    if not _constraint_exists(connection, "uq_questionnaire_category_questions_pair"):
        op.create_unique_constraint(
            "uq_questionnaire_category_questions_pair",
            "questionnaire_category_questions",
            ["category_id", "question_id"],
        )

    inspector = inspect(connection)
    if not _index_exists(inspector, "questionnaire_category_questions", "ix_qcq_category_id"):
        op.create_index("ix_qcq_category_id", "questionnaire_category_questions", ["category_id"])
    if not _index_exists(inspector, "questionnaire_category_questions", "ix_qcq_question_id"):
        op.create_index("ix_qcq_question_id", "questionnaire_category_questions", ["question_id"])

    # 3) Backfill category-question mappings from legacy questionnaire_definitions.category_id.
    inspector = inspect(connection)
    if _table_exists(inspector, "questionnaire_definitions") and _column_exists(
        inspector, "questionnaire_definitions", "category_id"
    ):
        connection.execute(
            text(
                """
                INSERT INTO questionnaire_category_questions (category_id, question_id)
                SELECT DISTINCT qd.category_id, qd.question_id
                FROM questionnaire_definitions AS qd
                WHERE qd.category_id IS NOT NULL
                ON CONFLICT (category_id, question_id) DO NOTHING
                """
            )
        )

    # 4) Add and backfill questionnaire_responses.category_id.
    inspector = inspect(connection)
    if _table_exists(inspector, "questionnaire_responses") and not _column_exists(
        inspector, "questionnaire_responses", "category_id"
    ):
        op.add_column("questionnaire_responses", sa.Column("category_id", sa.Integer(), nullable=True))

    inspector = inspect(connection)
    if _table_exists(inspector, "questionnaire_responses") and _column_exists(
        inspector, "questionnaire_responses", "category_id"
    ):
        if _table_exists(inspector, "questionnaire_definitions") and _column_exists(
            inspector, "questionnaire_definitions", "category_id"
        ):
            connection.execute(
                text(
                    """
                    UPDATE questionnaire_responses AS qr
                    SET category_id = qd.category_id
                    FROM questionnaire_definitions AS qd
                    WHERE qr.question_id = qd.question_id
                      AND qr.category_id IS NULL
                      AND qd.category_id IS NOT NULL
                    """
                )
            )

        connection.execute(
            text(
                """
                UPDATE questionnaire_responses AS qr
                SET category_id = x.category_id
                FROM (
                    SELECT question_id, MIN(category_id) AS category_id
                    FROM questionnaire_category_questions
                    GROUP BY question_id
                ) AS x
                WHERE qr.question_id = x.question_id
                  AND qr.category_id IS NULL
                """
            )
        )

        # Fallback to a default "general" category for any residual nulls.
        fallback = connection.execute(
            text(
                """
                INSERT INTO questionnaire_categories (category_key, display_name, status)
                VALUES ('general', 'General', 'active')
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
                UPDATE questionnaire_responses
                SET category_id = :fallback_category_id
                WHERE category_id IS NULL
                """
            ),
            {"fallback_category_id": int(fallback)},
        )

        if not _constraint_exists(connection, "fk_questionnaire_responses_category_id"):
            op.create_foreign_key(
                "fk_questionnaire_responses_category_id",
                "questionnaire_responses",
                "questionnaire_categories",
                ["category_id"],
                ["category_id"],
            )

        op.alter_column("questionnaire_responses", "category_id", existing_type=sa.Integer(), nullable=False)

    inspector = inspect(connection)
    if _table_exists(inspector, "questionnaire_responses") and not _index_exists(
        inspector, "questionnaire_responses", "ix_questionnaire_responses_category_id"
    ):
        op.create_index(
            "ix_questionnaire_responses_category_id",
            "questionnaire_responses",
            ["category_id"],
        )

    # 5) Create assessment_category_progress.
    inspector = inspect(connection)
    if not _table_exists(inspector, "assessment_category_progress"):
        op.create_table(
            "assessment_category_progress",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("assessment_instance_id", sa.Integer(), nullable=False),
            sa.Column("category_id", sa.Integer(), nullable=False),
            sa.Column("status", sa.String(), nullable=False),
            sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
            sa.ForeignKeyConstraint(["assessment_instance_id"], ["assessment_instances.assessment_instance_id"]),
            sa.ForeignKeyConstraint(["category_id"], ["questionnaire_categories.category_id"]),
        )

    # Deduplicate pre-existing rows before adding unique constraint.
    if _table_exists(inspector, "assessment_category_progress"):
        connection.execute(
            text(
                """
                DELETE FROM assessment_category_progress a
                USING assessment_category_progress b
                WHERE a.id > b.id
                  AND a.assessment_instance_id = b.assessment_instance_id
                  AND a.category_id = b.category_id
                """
            )
        )

    if not _constraint_exists(connection, "uq_assessment_category_progress_pair"):
        op.create_unique_constraint(
            "uq_assessment_category_progress_pair",
            "assessment_category_progress",
            ["assessment_instance_id", "category_id"],
        )

    inspector = inspect(connection)
    if not _index_exists(inspector, "assessment_category_progress", "ix_acp_assessment_instance_id"):
        op.create_index(
            "ix_acp_assessment_instance_id",
            "assessment_category_progress",
            ["assessment_instance_id"],
        )
    if not _index_exists(inspector, "assessment_category_progress", "ix_acp_category_id"):
        op.create_index("ix_acp_category_id", "assessment_category_progress", ["category_id"])

    # 6) Drop legacy questionnaire_definitions.category_id if it still exists.
    inspector = inspect(connection)
    if _table_exists(inspector, "questionnaire_definitions") and _column_exists(
        inspector, "questionnaire_definitions", "category_id"
    ):
        foreign_keys = inspector.get_foreign_keys("questionnaire_definitions")
        for foreign_key in foreign_keys:
            constrained_columns = set(foreign_key.get("constrained_columns") or [])
            name = foreign_key.get("name")
            if "category_id" in constrained_columns and name:
                op.drop_constraint(name, "questionnaire_definitions", type_="foreignkey")

        inspector = inspect(connection)
        if _index_exists(inspector, "questionnaire_definitions", "ix_questionnaire_definitions_category_id"):
            op.drop_index("ix_questionnaire_definitions_category_id", table_name="questionnaire_definitions")

        op.drop_column("questionnaire_definitions", "category_id")


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
