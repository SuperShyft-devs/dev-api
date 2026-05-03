"""Payments module SQLAlchemy models: bookings, orders, payments."""

from __future__ import annotations

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String, func, text
from sqlalchemy.dialects.postgresql import JSON

from db.base import Base


class Booking(Base):
    """A reserved slot / package / consultation with price snapshot at booking time."""

    __tablename__ = "bookings"

    booking_id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)
    entity_type = Column(String, nullable=False)
    entity_id = Column(Integer, nullable=False)
    entity_name = Column(String, nullable=False)
    amount_paise = Column(Integer, nullable=False)
    currency = Column(String, nullable=False, server_default=text("'INR'"))
    booking_type = Column(String, nullable=True)
    metadata_ = Column("metadata", JSON, nullable=True)
    status = Column(String, nullable=False, server_default=text("'pending'"))
    booked_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class OrderBooking(Base):
    """Associates one Razorpay order with every booking line in that checkout."""

    __tablename__ = "order_bookings"

    order_id = Column(Integer, ForeignKey("orders.order_id", ondelete="CASCADE"), primary_key=True)
    booking_id = Column(Integer, ForeignKey("bookings.booking_id", ondelete="CASCADE"), primary_key=True)


class Order(Base):
    """Razorpay order linked to a booking."""

    __tablename__ = "orders"

    order_id = Column(Integer, primary_key=True)
    booking_id = Column(Integer, ForeignKey("bookings.booking_id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)
    razorpay_order_id = Column(String, nullable=False, unique=True)
    amount_paise = Column(Integer, nullable=False)
    currency = Column(String, nullable=False, server_default=text("'INR'"))
    status = Column(String, nullable=False, server_default=text("'created'"))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class Payment(Base):
    """Payment attempt or completion record for a Razorpay order."""

    __tablename__ = "payments"

    payment_id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey("orders.order_id"), nullable=False)
    booking_id = Column(Integer, ForeignKey("bookings.booking_id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)
    razorpay_payment_id = Column(String, unique=True, nullable=True)
    razorpay_order_id = Column(String, nullable=False)
    razorpay_signature = Column(String, nullable=True)
    amount_paise = Column(Integer, nullable=False)
    currency = Column(String, nullable=False, server_default=text("'INR'"))
    status = Column(String, nullable=False)
    payment_method = Column(String, nullable=True)
    signature_verified = Column(Boolean, nullable=False, server_default=text("false"))
    failure_reason = Column(String, nullable=True)
    paid_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
