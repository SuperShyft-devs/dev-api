"""Engagements module models."""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    Enum as SAEnum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    Time,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import validates
from sqlalchemy.sql import func
from sqlalchemy.types import JSON

from db.base import Base
from db.column_types import (
    BloodBookingStatusColumn,
    EngagementStatusColumn,
    STATUS_BLOOD_BOOKING,
    blood_booking_provider_status_enum,
    diagnostic_provider_enum,
    status_server_default,
)
from modules.engagements.blood_booking_enums import BloodBookingRelation
from modules.engagements.enums import (
    BloodCollectionType,
    ConsultationMode,
    EngagementFeedbackOverallRating,
    EngagementKind,
    EngagementPaymentType,
    EngagementStatus,
    UserOnboardingPayloadInputType,
    UserOnboardingPayloadSection,
)

_blood_booking_relation = SAEnum(
    BloodBookingRelation,
    name="blood_booking_relation_enum",
    values_callable=lambda obj: [e.value for e in obj],
    create_type=False,
)
_engagement_kind = SAEnum(
    EngagementKind,
    name="engagement_kind",
    native_enum=True,
    values_callable=lambda obj: [e.value for e in obj],
    validate_strings=True,
    create_type=False,
)
_engagement_feedback_overall_rating = SAEnum(
    EngagementFeedbackOverallRating,
    name="engagement_feedback_overall_rating",
    values_callable=lambda obj: [e.value for e in obj],
    validate_strings=True,
    create_type=False,
)

_user_onboarding_payload_section = SAEnum(
    UserOnboardingPayloadSection,
    name="user_onboarding_payload_section",
    values_callable=lambda obj: [e.value for e in obj],
    validate_strings=True,
    create_type=False,
)

_user_onboarding_payload_input_type = SAEnum(
    UserOnboardingPayloadInputType,
    name="user_onboarding_payload_input_type",
    values_callable=lambda obj: [e.value for e in obj],
    validate_strings=True,
    create_type=False,
)
_engagement_payment_type = SAEnum(
    EngagementPaymentType,
    name="engagement_payment_type",
    values_callable=lambda obj: [e.value for e in obj],
    validate_strings=True,
    create_type=False,
)


class EngagementSlotInfo(Base):
    """Shared cabin schedule configuration for one or more engagements."""

    __tablename__ = "engagement_slot_info"

    slot_detail_id = Column(Integer, primary_key=True, autoincrement=True)
    slot_detail = Column(JSON, nullable=False)


class Engagement(Base):
    """SQLAlchemy model for `engagements` table."""

    __tablename__ = "engagements"
    __table_args__ = (
        Index("uq_engagements_engagement_code", "engagement_code", unique=True),
        Index("ix_engagements_organization_id", "organization_id"),
        Index("ix_engagements_camp_no", "camp_no"),
        CheckConstraint(
            "(diagnostic_package_id IS NOT NULL AND diagnostic_package_id_male IS NULL "
            "AND diagnostic_package_id_female IS NULL) "
            "OR (diagnostic_package_id IS NULL AND diagnostic_package_id_male IS NOT NULL "
            "AND diagnostic_package_id_female IS NOT NULL) "
            "OR (diagnostic_package_id IS NULL AND diagnostic_package_id_male IS NULL "
            "AND diagnostic_package_id_female IS NULL)",
            name="ck_engagements_dx_pkg_choice",
        ),
    )

    engagement_id = Column(Integer, primary_key=True)
    engagement_name = Column(String)
    metsights_engagement_id = Column(String, nullable=True)
    organization_id = Column(Integer, ForeignKey("organizations.organization_id"), nullable=True)
    camp_no = Column(BigInteger, nullable=True)
    engagement_code = Column(String, nullable=False)
    engagement_type = Column(Integer, ForeignKey("engagement_types.id"), nullable=True)
    consultations = Column(JSON, nullable=True)
    slot_detail_id = Column(Integer, ForeignKey("engagement_slot_info.slot_detail_id", ondelete="SET NULL"), nullable=True)
    assessment_package_id = Column(Integer, ForeignKey("assessment_packages.package_id"), nullable=True)
    diagnostic_package_id = Column(Integer, ForeignKey("diagnostic_package.diagnostic_package_id"), nullable=True)
    diagnostic_package_id_male = Column(Integer, ForeignKey("diagnostic_package.diagnostic_package_id"), nullable=True)
    diagnostic_package_id_female = Column(Integer, ForeignKey("diagnostic_package.diagnostic_package_id"), nullable=True)
    address = Column(String, nullable=True)
    sub_locality = Column(String, nullable=True)
    landmark = Column(String, nullable=True)
    pincode = Column(String, nullable=True)
    city = Column(String)
    state = Column(String, nullable=True)
    country = Column(String, nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    slot_duration = Column(Integer)
    start_date = Column(Date)
    end_date = Column(Date)
    status = Column(EngagementStatusColumn, nullable=True)
    create_profile_on_metsights = Column(Boolean, nullable=False, default=False, server_default="false")
    enroll_for_fitprint_full = Column(Boolean, nullable=False, default=False, server_default="false")
    load_prev_assessment_questionnaires = Column(Boolean, nullable=False, default=False, server_default="false")
    load_prev_questionnaire_category_keys = Column(ARRAY(String), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    healthians_zone_id = Column(String, nullable=True)
    external_camp_id = Column(Integer, nullable=True)
    blood_collection_type = Column(
        SAEnum(BloodCollectionType, name="blood_collection_type_enum", values_callable=lambda obj: [e.value for e in obj], create_type=False),
        nullable=True,
    )
    consultation_mode = Column(
        SAEnum(ConsultationMode, name="consultation_mode_enum", values_callable=lambda obj: [e.value for e in obj], create_type=False),
        nullable=True,
    )
    payment_type = Column(
        _engagement_payment_type,
        nullable=False,
        default=EngagementPaymentType.paid_by_group,
        server_default=EngagementPaymentType.paid_by_group.value,
    )
    auto_booking = Column(Boolean, nullable=True, default=False, server_default="false")
    enforce_metsights_pro_gate = Column(
        Boolean, nullable=False, default=False, server_default="false"
    )


class OnboardingAssistantAssignment(Base):
    """SQLAlchemy model for `onboarding_assistant_assignment` table.

    Exactly one of partner_id (phlebo/expert) or employee_id (admin/inferior_admin) is set.
    """

    __tablename__ = "onboarding_assistant_assignment"

    onboarding_assistant_id = Column(Integer, primary_key=True)
    partner_id = Column(Integer, ForeignKey("partners.partner_id"), nullable=True)
    employee_id = Column(Integer, ForeignKey("employee.employee_id", ondelete="CASCADE"), nullable=True)
    engagement_id = Column(Integer, ForeignKey("engagements.engagement_id"), nullable=False)

    __table_args__ = (
        UniqueConstraint("engagement_id", "partner_id", name="uq_onboarding_assistant_assignment"),
        Index("ix_onboarding_assistant_assignment_engagement_id", "engagement_id"),
        Index("ix_onboarding_assistant_assignment_partner_id", "partner_id"),
        Index("ix_onboarding_assistant_assignment_employee_id", "employee_id"),
        Index(
            "uq_oa_assignment_engagement_employee",
            "engagement_id",
            "employee_id",
            unique=True,
            postgresql_where=text("employee_id IS NOT NULL"),
        ),
        CheckConstraint(
            "(partner_id IS NOT NULL AND employee_id IS NULL) OR "
            "(partner_id IS NULL AND employee_id IS NOT NULL)",
            name="ck_oa_assignment_partner_xor_employee",
        ),
    )


class ParticipantBloodBooking(Base):
    """One blood collection attempt / Healthians booking for an engagement participant."""

    __tablename__ = "participant_blood_bookings"
    __table_args__ = (
        Index("ix_pbb_engagement_participant_id", "engagement_participant_id"),
        Index(
            "uq_pbb_booking_id",
            "booking_id",
            unique=True,
            postgresql_where=text(
                "booking_id IS NOT NULL AND btrim(booking_id) <> '' AND status = 1"
            ),
        ),
        Index(
            "ix_pbb_cabin_slot_occupancy",
            "engagement_participant_id",
            "collection_cabin",
            "collection_date",
            "collection_time",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    engagement_participant_id = Column(
        Integer,
        ForeignKey("engagement_participants.engagement_participant_id", ondelete="CASCADE"),
        nullable=False,
    )
    collection_date = Column(Date, nullable=True)
    collection_cabin = Column(String, nullable=True)
    collection_time = Column(Time, nullable=True)
    collection_time_slot_id = Column(String, nullable=True)
    booking_id = Column(String, nullable=True)
    barcode = Column(String, nullable=True)
    collected_at = Column(DateTime(timezone=True), nullable=True)
    relation = Column(
        _blood_booking_relation,
        nullable=False,
        server_default=BloodBookingRelation.primary.value,
    )
    parent_booking_id = Column(String, nullable=True)
    status = Column(
        BloodBookingStatusColumn,
        nullable=False,
        default="active",
        server_default=status_server_default("active", STATUS_BLOOD_BOOKING),
    )
    diagnostic_provider = Column(diagnostic_provider_enum, nullable=True)
    request_id = Column(String, nullable=True)
    token = Column(String, nullable=True)
    order_id = Column(BigInteger, nullable=True)
    alnum_order_id = Column(String, nullable=True)
    provider_status = Column(blood_booking_provider_status_enum, nullable=True)
    diagnostic_report_url = Column(Text, nullable=True)
    blood_parameters = Column(JSON, nullable=True)
    blood_report_raw = Column(JSON, nullable=True)
    blood_parameters_full_report = Column(Boolean, nullable=True)
    blood_parameters_verified_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


class EngagementParticipant(Base):
    """SQLAlchemy model for `engagement_participants` table."""

    __tablename__ = "engagement_participants"
    __table_args__ = (
        UniqueConstraint("engagement_id", "user_id", name="uq_engagement_participants_engagement_user"),
        Index("ix_ep_engagement_id_user_id", "engagement_id", "user_id"),
        Index("ix_ep_user_id", "user_id"),
        Index("ix_ep_booked_by_user_id", "booked_by_user_id"),
    )

    engagement_participant_id = Column(Integer, primary_key=True)
    engagement_id = Column(Integer, ForeignKey("engagements.engagement_id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)
    booked_by_user_id = Column(Integer, ForeignKey("users.user_id"), nullable=False)
    participants_employee_id = Column(String, nullable=True)
    participant_department = Column(String, nullable=True)
    participant_blood_group = Column(String, nullable=True)
    consultation_booking_ids = Column(ARRAY(Integer), nullable=True)
    face_scan_link = Column(String, nullable=True)
    address = Column(String, nullable=True)
    sub_locality = Column(String, nullable=True)
    landmark = Column(String, nullable=True)
    pincode = Column(String, nullable=True)
    city = Column(String, nullable=True)
    state = Column(String, nullable=True)
    country = Column(String, nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    healthians_zone_id = Column(String, nullable=True)
    paid = Column(Boolean, nullable=False, default=True, server_default="true")
    booker_message = Column(String(1000), nullable=True)
    booker_relation = Column(String(32), nullable=True)

    @validates("user_id")
    def _default_booked_by_user_id(self, _key: str, user_id: int) -> int:
        if self.booked_by_user_id is None and user_id is not None:
            self.booked_by_user_id = user_id
        return user_id


class EngagementType(Base):
    """SQLAlchemy model for `engagement_types` table."""

    __tablename__ = "engagement_types"

    id = Column(Integer, primary_key=True)
    code = Column(String, unique=True, nullable=False)
    display_name = Column(String, nullable=False)
    is_active = Column(Boolean, nullable=False, default=True, server_default="true")


class EngagementNotification(Base):
    """SQLAlchemy model for `engagement_notifications` table."""

    __tablename__ = "engagement_notifications"
    __table_args__ = (
        UniqueConstraint("engagement_id", "event_code", name="uq_engagement_notification_event"),
    )

    id = Column(Integer, primary_key=True)
    engagement_id = Column(Integer, ForeignKey("engagements.engagement_id"), nullable=False)
    event_code = Column(String, nullable=False)
    event_name = Column(String, nullable=True)
    recipient_type = Column(String, nullable=False, server_default="user")
    notification_services = Column(JSON, nullable=False)


class EngagementFeedback(Base):
    """Participant experience feedback for one engagement enrollment."""

    __tablename__ = "engagement_feedbacks"
    __table_args__ = (
        UniqueConstraint(
            "engagement_participant_id",
            name="uq_engagement_feedbacks_participant",
        ),
        Index("ix_engagement_feedbacks_created_at", "created_at"),
    )

    feedback_id = Column(Integer, primary_key=True, autoincrement=True)
    engagement_participant_id = Column(
        Integer,
        ForeignKey("engagement_participants.engagement_participant_id", ondelete="CASCADE"),
        nullable=False,
    )
    overall_rating = Column(_engagement_feedback_overall_rating, nullable=False)
    category_ratings = Column(JSON, nullable=False, server_default=text("'{}'"))
    comment = Column(String(2000), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())


class UserOnboardingPayloadField(Base):
    """Catalog of fields collected during user onboarding."""

    __tablename__ = "user_onboarding_payload_fields"

    id = Column(Integer, primary_key=True, autoincrement=True)
    payload_key = Column(String, nullable=False, unique=True)
    display_name = Column(String, nullable=True)
    section = Column(_user_onboarding_payload_section, nullable=False)
    input_type = Column(_user_onboarding_payload_input_type, nullable=False)


class CampUserOnboardingPayloadField(Base):
    """Per-camp onboarding requirement configuration."""

    __tablename__ = "camp_user_onboarding_payload_fields"

    id = Column(Integer, primary_key=True, autoincrement=True)
    camp_no = Column(BigInteger, nullable=False, unique=True, index=True)
    camp_code = Column(String(12), nullable=False, unique=True, index=True)
    onboarding_payload = Column(JSON, nullable=False, server_default=text("'{}'"))
