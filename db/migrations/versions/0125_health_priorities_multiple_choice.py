"""health_priorities multiple_choice and metsights sync

Revision ID: 0125_health_priorities_multiple_choice
Revises: 0124_normalize_cabin_keys

Updates health_priorities to multiple_choice with correct metsights_sync and
coerces legacy single-choice string answers to one-element JSON arrays.

Downgrade is intentionally a no-op (data shape change is not reversed).
"""

from __future__ import annotations

import json

import sqlalchemy as sa
from alembic import op

revision = "0125_health_priorities_multiple_choice"
down_revision = "0124_normalize_cabin_keys"
branch_labels = None
depends_on = None

_HEALTH_PRIORITIES_METSIGHTS_SYNC = {
    "pull": {"enabled": True, "strategy": "passthrough"},
    "push": {
        "enabled": True,
        "strategy": "single_to_list",
        "min_list_size": 2,
        "max_list_size": 2,
        "fill_strategy": "deterministic_next",
        "fill_from_option_values": ["0", "1", "2", "3", "4", "5"],
    },
}


def upgrade() -> None:
    sync_json = json.dumps(_HEALTH_PRIORITIES_METSIGHTS_SYNC)
    op.execute(
        sa.text(
            """
            UPDATE questionnaire_definitions
            SET question_type = 'multiple_choice',
                sub_text = 'Choose your top two priorities.',
                metsights_sync = CAST(:sync_json AS jsonb)
            WHERE question_key = 'health_priorities'
            """
        ).bindparams(sync_json=sync_json)
    )
    op.execute(
        sa.text(
            """
            UPDATE questionnaire_responses qr
            SET answer = jsonb_build_array(qr.answer)
            FROM questionnaire_definitions qd
            WHERE qd.question_id = qr.question_id
              AND qd.question_key = 'health_priorities'
              AND jsonb_typeof(qr.answer) = 'string'
            """
        )
    )


def downgrade() -> None:
    pass
