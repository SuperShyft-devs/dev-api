"""Link orders to multiple bookings (multi-member checkout)."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect, text


revision = "0031_order_bookings"
down_revision = "0030_diag_filter_chips"
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    connection = op.get_bind()
    inspector = inspect(connection)
    if not _table_exists(inspector, "order_bookings"):
        op.create_table(
            "order_bookings",
            sa.Column("order_id", sa.Integer(), sa.ForeignKey("orders.order_id", ondelete="CASCADE"), nullable=False),
            sa.Column("booking_id", sa.Integer(), sa.ForeignKey("bookings.booking_id", ondelete="CASCADE"), nullable=False),
            sa.PrimaryKeyConstraint("order_id", "booking_id", name="pk_order_bookings"),
        )
        op.create_index("ix_order_bookings_booking_id", "order_bookings", ["booking_id"])

    # One row per existing order (single-booking checkouts) so joins see payment via order.
    connection.execute(
        text(
            """
            INSERT INTO order_bookings (order_id, booking_id)
            SELECT o.order_id, o.booking_id
            FROM orders o
            WHERE NOT EXISTS (
                SELECT 1 FROM order_bookings ob
                WHERE ob.order_id = o.order_id AND ob.booking_id = o.booking_id
            )
            """
        )
    )


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
