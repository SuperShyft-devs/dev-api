"""Database column types: integer status storage with string labels in Python."""

from __future__ import annotations

import enum
from collections.abc import Mapping
from typing import Any

from sqlalchemy import Enum as SAEnum, SmallInteger, TypeDecorator
from sqlalchemy.sql import expression

# --- Status label <-> code maps (codes start at 1, never reused) ---

STATUS_ACTIVE_INACTIVE: dict[str, int] = {"active": 1, "inactive": 2}

STATUS_ACTIVE_INACTIVE_ARCHIVED: dict[str, int] = {
    "active": 1,
    "inactive": 2,
    "archived": 3,
}

STATUS_ENGAGEMENT: dict[str, int] = {
    "draft": 1,
    "scheduled": 2,
    "running": 3,
    "completed": 4,
    "cancelled": 5,
    "published": 6,  # legacy staging/production rows; keep label in API responses
}

STATUS_INTEGRATION_SYNC: dict[str, int] = {
    "pending": 1,
    "success": 2,
    "failed": 3,
    "skipped": 4,
}

STATUS_NOTIFICATION: dict[str, int] = {
    "pending": 1,
    "sent": 2,
    "failed": 3,
}

STATUS_CHECKLIST_TASK: dict[str, int] = {
    "pending": 1,
    "done": 2,
}

STATUS_DISCOUNT_CODE: dict[str, int] = {
    "draft": 1,
    "active": 2,
    "paused": 3,
    "expired": 4,
    "finished": 5,
    "disabled": 6,
}

STATUS_DISCOUNT_INSTANCE: dict[str, int] = {
    "available": 1,
    "assigned": 2,
    "used": 3,
}

STATUS_DISCOUNT_USAGE: dict[str, int] = {
    "reserved": 1,
    "committed": 2,
    "released": 3,
}

STATUS_OVERRIDE: dict[str, int] = {
    "available": 1,
    "unavailable": 2,
    "booked": 3,
}

STATUS_BOOKING: dict[str, int] = {
    "pending": 1,
    "released": 2,
    "confirmed": 3,
    "failed": 4,
}

STATUS_ORDER: dict[str, int] = {
    "created": 1,
    "paid": 2,
    "attempted": 3,
}

STATUS_PAYMENT: dict[str, int] = {
    "success": 1,
    "failed": 2,
}

STATUS_REPORTS_SYNC: dict[str, int] = {
    "idle": 1,
    "in_progress": 2,
    "failed": 3,
}

STATUS_SUPPORT_TICKET: dict[str, int] = {
    "open": 1,
    "resolved": 2,
    "closed": 3,
}

STATUS_ASSESSMENT_INSTANCE: dict[str, int] = {
    "active": 1,
    "complete": 2,  # legacy prod rows; stored as same code as completed
    "completed": 2,
}

STATUS_ASSESSMENT_CATEGORY_PROGRESS: dict[str, int] = {
    "complete": 1,
    "incomplete": 2,
}

STATUS_BLOOD_BOOKING: dict[str, int] = {
    "active": 1,
    "superseded": 2,
    "cancelled": 3,
}


class StatusLabel(TypeDecorator[int]):
    """Store status as smallint; expose the same string labels as before."""

    impl = SmallInteger
    cache_ok = True

    def __init__(self, label_to_code: Mapping[str, int]) -> None:
        super().__init__()
        self._label_to_code = dict(label_to_code)
        self._code_to_label = {code: label for label, code in self._label_to_code.items()}

    def process_bind_param(self, value: Any, dialect: Any) -> int | None:
        if value is None:
            return None
        if isinstance(value, int):
            if value in self._code_to_label:
                return value
            raise ValueError(f"Unknown status code: {value}")
        label = value if isinstance(value, str) else str(value)
        code = self._label_to_code.get(label)
        if code is None:
            code = self._label_to_code.get(label.lower())
        if code is None:
            raise ValueError(f"Unknown status label: {label}")
        return code

    def process_result_value(self, value: int | None, dialect: Any) -> str | None:
        if value is None:
            return None
        try:
            return self._code_to_label[value]
        except KeyError:
            raise ValueError(f"Unknown status code in database: {value}") from None

    def coerce_compared_value(self, op: Any, value: Any) -> StatusLabel:
        """Ensure WHERE clauses bind string labels as smallint, not varchar."""
        return self

    def copy(self, **kwargs: Any) -> StatusLabel:
        return StatusLabel(self._label_to_code)


def status_code(label: str, mapping: Mapping[str, int]) -> int:
    return mapping[label]


def label_to_status_code(mapping: Mapping[str, int], label: str) -> int:
    """Map a status label to its DB smallint code (for raw SQL)."""
    return mapping[label]


def status_server_default(label: str, mapping: Mapping[str, int]) -> expression.TextClause:
    from sqlalchemy import text

    return text(str(mapping[label]))


# Pre-built status column types
ActiveInactiveStatus = StatusLabel(STATUS_ACTIVE_INACTIVE)
ActiveInactiveArchivedStatus = StatusLabel(STATUS_ACTIVE_INACTIVE_ARCHIVED)
EngagementStatusColumn = StatusLabel(STATUS_ENGAGEMENT)
IntegrationSyncStatusColumn = StatusLabel(STATUS_INTEGRATION_SYNC)
NotificationStatusColumn = StatusLabel(STATUS_NOTIFICATION)
ChecklistTaskStatusColumn = StatusLabel(STATUS_CHECKLIST_TASK)
DiscountCodeStatusColumn = StatusLabel(STATUS_DISCOUNT_CODE)
DiscountInstanceStatusColumn = StatusLabel(STATUS_DISCOUNT_INSTANCE)
DiscountUsageStatusColumn = StatusLabel(STATUS_DISCOUNT_USAGE)
OverrideStatusColumn = StatusLabel(STATUS_OVERRIDE)
BookingStatusColumn = StatusLabel(STATUS_BOOKING)
OrderStatusColumn = StatusLabel(STATUS_ORDER)
PaymentStatusColumn = StatusLabel(STATUS_PAYMENT)
ReportsSyncStatusColumn = StatusLabel(STATUS_REPORTS_SYNC)
SupportTicketStatusColumn = StatusLabel(STATUS_SUPPORT_TICKET)
AssessmentInstanceStatusColumn = StatusLabel(STATUS_ASSESSMENT_INSTANCE)
AssessmentCategoryProgressStatusColumn = StatusLabel(STATUS_ASSESSMENT_CATEGORY_PROGRESS)
BloodBookingStatusColumn = StatusLabel(STATUS_BLOOD_BOOKING)


def _pg_enum(enum_cls: type[enum.Enum], name: str) -> SAEnum:
    return SAEnum(
        enum_cls,
        name=name,
        native_enum=True,
        values_callable=lambda obj: [e.value for e in obj],
        validate_strings=True,
        create_type=False,
    )


# --- PostgreSQL enums (member values are exact API strings) ---


class DiagnosticProvider(str, enum.Enum):
    healthians = "healthians"
    healthians_legacy = "Healthians"  # exact casing stored on some rows
    health_labs = "HealthLabs"  # sample seed / legacy label
    orange_health = "orange_health"
    redcliffe = "redcliffe"


class BloodBookingProviderStatus(str, enum.Enum):
    scheduled = "Scheduled"


class DiagnosticCollectionType(str, enum.Enum):
    home_collection = "home_collection"
    centre_visit = "centre_visit"


class GenderSuitability(str, enum.Enum):
    male = "male"
    female = "female"
    both = "both"


class PackageFor(str, enum.Enum):
    public = "public"
    camp = "camp"


class FilterChipFor(str, enum.Enum):
    public_package = "public_package"
    custom_package = "custom_package"


class IntegrationProvider(str, enum.Enum):
    aurae = "aurae"
    n8n = "n8n"
    metsights = "metsights"
    healthians = "healthians"
    bio_ai_reports = "bio_ai_reports"
    nutrition_api = "nutrition_api"
    internal = "internal"
    google_maps = "google_maps"
    nominatim = "nominatim"
    orange_health = "orange_health"
    redcliffe = "redcliffe"


class UserGender(str, enum.Enum):
    male = "male"
    female = "female"
    male_capitalized = "Male"
    female_capitalized = "Female"


class UserRelationship(str, enum.Enum):
    self_ = "self"
    spouse = "spouse"
    child = "child"
    sibling = "sibling"
    parent = "parent"
    grandparent = "grandparent"
    other = "other"


class DietPreference(str, enum.Enum):
    veg = "veg"
    non_veg = "non_veg"
    vegan = "vegan"
    jain = "jain"
    eggetarian = "eggetarian"
    keto = "keto"


class PartnerRoleEnum(str, enum.Enum):
    phlebo = "phlebo"
    expert = "expert"
    organization_manager = "organization_manager"
    author = "author"


class BlogAuthorRole(str, enum.Enum):
    nutritionist = "nutritionist"
    doctor = "doctor"
    dietitian = "dietitian"
    fitness_trainer = "fitness_trainer"
    psychologist = "psychologist"
    mental_health_expert = "mental_health_expert"
    health_coach = "health_coach"
    wellness_expert = "wellness_expert"
    researcher = "researcher"
    medical_researcher = "medical_researcher"
    physiotherapist = "physiotherapist"
    yoga_instructor = "yoga_instructor"


class BlogPostStatus(str, enum.Enum):
    draft = "draft"
    published = "published"


class NotificationChannel(str, enum.Enum):
    email = "email"
    whatsapp = "whatsapp"


class DiscountTypeEnum(str, enum.Enum):
    percentage = "percentage"
    fixed = "fixed"
    percentage_capped = "percentage_capped"


class DiscountAudience(str, enum.Enum):
    everyone = "everyone"
    new_users = "new_users"
    existing_users = "existing_users"


class DiscountScopeMode(str, enum.Enum):
    general = "general"
    organization = "organization"
    camp = "camp"
    engagement = "engagement"


class DiscountPackageApplyMode(str, enum.Enum):
    all = "all"
    include = "include"
    exclude = "exclude"


class PerUserFrequency(str, enum.Enum):
    none = "none"
    day = "day"
    week = "week"
    month = "month"


class DiscountCodeKind(str, enum.Enum):
    shared = "shared"
    unique_pool = "unique_pool"


class DiscountScopeType(str, enum.Enum):
    general = "general"
    organization = "organization"
    camp = "camp"
    engagement = "engagement"


class DiscountPackageMode(str, enum.Enum):
    include = "include"
    exclude = "exclude"


class DiscountValidationOutcome(str, enum.Enum):
    ok = "ok"
    invalid = "invalid"
    ineligible = "ineligible"
    locked = "locked"
    rate_limited = "rate_limited"


class QuestionnaireCategoryOf(str, enum.Enum):
    supershyft = "supershyft"
    metsights = "metsights"


class HabitRuleConditionType(str, enum.Enum):
    option_match = "option_match"
    scale_range = "scale_range"


class BookingEntityType(str, enum.Enum):
    diagnostic_package = "diagnostic_package"
    expert = "expert"


class BookingType(str, enum.Enum):
    bio_ai = "bio_ai"
    blood_test = "blood_test"
    consultation = "consultation"


class CurrencyCode(str, enum.Enum):
    INR = "INR"


class ExportTypeEnum(str, enum.Enum):
    participants = "participants"
    database_backup = "database_backup"
    contact_reveal = "contact_reveal"


class ExportFormatEnum(str, enum.Enum):
    csv = "csv"
    xlsx = "xlsx"


class ExportSourceKindEnum(str, enum.Enum):
    engagement = "engagement"
    organization = "organization"
    camp = "camp"
    system = "system"
    user = "user"


class ChecklistAudience(str, enum.Enum):
    internal = "internal"
    user = "user"


diagnostic_provider_enum = _pg_enum(DiagnosticProvider, "diagnostic_provider_enum")
blood_booking_provider_status_enum = _pg_enum(
    BloodBookingProviderStatus, "blood_booking_provider_status_enum"
)
diagnostic_collection_type_enum = _pg_enum(DiagnosticCollectionType, "diagnostic_collection_type_enum")
gender_suitability_enum = _pg_enum(GenderSuitability, "gender_suitability_enum")
package_for_enum = _pg_enum(PackageFor, "package_for_enum")
filter_chip_for_enum = _pg_enum(FilterChipFor, "filter_chip_for_enum")
integration_provider_enum = _pg_enum(IntegrationProvider, "integration_provider_enum")
user_gender_enum = _pg_enum(UserGender, "user_gender_enum")
user_relationship_enum = _pg_enum(UserRelationship, "user_relationship_enum")
diet_preference_enum = _pg_enum(DietPreference, "diet_preference_enum")
partner_role_enum = _pg_enum(PartnerRoleEnum, "partner_role_enum")
blog_author_role_enum = _pg_enum(BlogAuthorRole, "author_role")
blog_post_status_enum = _pg_enum(BlogPostStatus, "post_status")
notification_channel_enum = _pg_enum(NotificationChannel, "notification_channel_enum")
discount_type_enum = _pg_enum(DiscountTypeEnum, "discount_type_enum")
discount_audience_enum = _pg_enum(DiscountAudience, "discount_audience_enum")
discount_scope_mode_enum = _pg_enum(DiscountScopeMode, "discount_scope_mode_enum")
discount_package_apply_mode_enum = _pg_enum(DiscountPackageApplyMode, "discount_package_apply_mode_enum")
per_user_frequency_enum = _pg_enum(PerUserFrequency, "per_user_frequency_enum")
discount_code_kind_enum = _pg_enum(DiscountCodeKind, "discount_code_kind_enum")
discount_scope_type_enum = _pg_enum(DiscountScopeType, "discount_scope_type_enum")
discount_package_mode_enum = _pg_enum(DiscountPackageMode, "discount_package_mode_enum")
discount_validation_outcome_enum = _pg_enum(DiscountValidationOutcome, "discount_validation_outcome_enum")
questionnaire_category_of_enum = _pg_enum(QuestionnaireCategoryOf, "questionnaire_category_of_enum")
habit_rule_condition_type_enum = _pg_enum(HabitRuleConditionType, "habit_rule_condition_type_enum")
booking_entity_type_enum = _pg_enum(BookingEntityType, "booking_entity_type_enum")
booking_type_enum = _pg_enum(BookingType, "booking_type_enum")
currency_code_enum = _pg_enum(CurrencyCode, "currency_code_enum")
export_type_enum = _pg_enum(ExportTypeEnum, "export_type_enum")
export_format_enum = _pg_enum(ExportFormatEnum, "export_format_enum")
export_source_kind_enum = _pg_enum(ExportSourceKindEnum, "export_source_kind_enum")
checklist_audience_enum = _pg_enum(ChecklistAudience, "checklist_audience_enum")
