"""Map expert consultation_modes from video/voice/chat to online.

Revision ID: 0184_expert_consultation_modes
Revises: 0183_redcliffe
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text


revision = "0184_expert_consultation_modes"
down_revision = "0183_redcliffe"
branch_labels = None
depends_on = None

_LEGACY_TO_ONLINE = frozenset({"video", "voice", "chat"})


def upgrade() -> None:
    conn = op.get_bind()
    rows = conn.execute(
        text("SELECT expert_id, consultation_modes FROM experts WHERE consultation_modes IS NOT NULL")
    ).fetchall()
    for expert_id, modes in rows:
        if not isinstance(modes, list):
            continue
        normalized: list[str] = []
        seen: set[str] = set()
        for raw in modes:
            if not isinstance(raw, str):
                continue
            key = raw.strip().lower()
            if key in _LEGACY_TO_ONLINE:
                key = "online"
            if key not in ("offline", "online"):
                continue
            if key in seen:
                continue
            seen.add(key)
            normalized.append(key)
        new_value = normalized if normalized else None
        if new_value is None:
            conn.execute(
                text("UPDATE experts SET consultation_modes = NULL WHERE expert_id = :expert_id"),
                {"expert_id": expert_id},
            )
        else:
            import json

            conn.execute(
                text(
                    "UPDATE experts SET consultation_modes = CAST(:modes AS json) WHERE expert_id = :expert_id"
                ),
                {"modes": json.dumps(new_value), "expert_id": expert_id},
            )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
