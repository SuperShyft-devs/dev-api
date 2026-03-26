"""Add audience to checklist_templates.

User-facing templates are used to show static preparation instructions to users.
Internal templates are operational tasks for employees.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op


# NOTE: `alembic_version.version_num` is VARCHAR(32) in this project,
# so revision ids must be <= 32 characters.
revision = "0023_chk_tpl_audience"
down_revision = "0022_platform_settings"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "checklist_templates",
        sa.Column("audience", sa.String(), nullable=False, server_default="internal"),
    )
    op.create_check_constraint(
        "ck_checklist_templates_audience",
        "checklist_templates",
        "audience IN ('internal', 'user')",
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")

