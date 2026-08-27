"""Merge load_prev_assessment_questionnaires and mapped module index heads.

Revision ID: 0129_merge_indexes_prev_qnr
Revises: 0128_eng_load_prev_qnr, 0128_mapped_module_indexes
"""

from __future__ import annotations

revision = "0129_merge_indexes_prev_qnr"
down_revision = ("0128_eng_load_prev_qnr", "0128_mapped_module_indexes")
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
