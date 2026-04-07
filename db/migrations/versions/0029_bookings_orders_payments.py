"""Add bookings, orders, and payments tables for Razorpay checkout."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect


revision = "0029_payments_tables"
down_revision = "0028_qnr_healthy_habits"
branch_labels = None
depends_on = None


def _table_exists(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def upgrade() -> None:
    connection = op.get_bind()

    inspector = inspect(connection)
    if not _table_exists(inspector, "bookings"):
        op.create_table(
            "bookings",
            sa.Column("booking_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.user_id"), nullable=False),
            sa.Column("entity_type", sa.String(), nullable=False),
            sa.Column("entity_id", sa.Integer(), nullable=False),
            sa.Column("entity_name", sa.String(), nullable=False),
            sa.Column("amount_paise", sa.Integer(), nullable=False),
            sa.Column("currency", sa.String(), nullable=False, server_default=sa.text("'INR'")),
            sa.Column("status", sa.String(), nullable=False, server_default=sa.text("'pending'")),
            sa.Column("booked_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        )
        op.create_index("ix_bookings_user_id", "bookings", ["user_id"])

    inspector = inspect(connection)
    if not _table_exists(inspector, "orders"):
        op.create_table(
            "orders",
            sa.Column("order_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("booking_id", sa.Integer(), sa.ForeignKey("bookings.booking_id"), nullable=False),
            sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.user_id"), nullable=False),
            sa.Column("razorpay_order_id", sa.String(), nullable=False),
            sa.Column("amount_paise", sa.Integer(), nullable=False),
            sa.Column("currency", sa.String(), nullable=False, server_default=sa.text("'INR'")),
            sa.Column("status", sa.String(), nullable=False, server_default=sa.text("'created'")),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        )
        op.create_index("ix_orders_booking_id", "orders", ["booking_id"])
        op.create_index("ix_orders_user_id", "orders", ["user_id"])
        op.create_index("uq_orders_razorpay_order_id", "orders", ["razorpay_order_id"], unique=True)

    inspector = inspect(connection)
    if not _table_exists(inspector, "payments"):
        op.create_table(
            "payments",
            sa.Column("payment_id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column("order_id", sa.Integer(), sa.ForeignKey("orders.order_id"), nullable=False),
            sa.Column("booking_id", sa.Integer(), sa.ForeignKey("bookings.booking_id"), nullable=False),
            sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.user_id"), nullable=False),
            sa.Column("razorpay_payment_id", sa.String(), nullable=True),
            sa.Column("razorpay_order_id", sa.String(), nullable=False),
            sa.Column("razorpay_signature", sa.String(), nullable=True),
            sa.Column("amount_paise", sa.Integer(), nullable=False),
            sa.Column("currency", sa.String(), nullable=False, server_default=sa.text("'INR'")),
            sa.Column("status", sa.String(), nullable=False),
            sa.Column("payment_method", sa.String(), nullable=True),
            sa.Column("signature_verified", sa.Boolean(), nullable=False, server_default=sa.text("false")),
            sa.Column("failure_reason", sa.String(), nullable=True),
            sa.Column("paid_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        )
        op.create_index("ix_payments_order_id", "payments", ["order_id"])
        op.create_index("ix_payments_booking_id", "payments", ["booking_id"])
        op.create_index("ix_payments_user_id", "payments", ["user_id"])
        op.create_index("uq_payments_razorpay_payment_id", "payments", ["razorpay_payment_id"], unique=True)


def downgrade() -> None:
    raise RuntimeError("Downgrade is not supported")
