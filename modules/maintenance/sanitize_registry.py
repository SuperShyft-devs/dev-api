"""Registry of DB columns to sanitize for legacy data cleanup."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from common.data_sanitize import SanitizeKind


class HandlerKind(str, Enum):
    SCALAR = "scalar"
    NESTED_JSON = "nested_json"
    SLOT_DETAIL = "slot_detail"
    QUESTIONNAIRE_ANSWER = "questionnaire_answer"
    ORG_DEPARTMENTS = "org_departments"
    NOTIFICATION_SERVICES = "notification_services"
    EXPERT_LANGUAGES = "expert_languages"
    CAMP_REPORT_JSON = "camp_report_json"


@dataclass(frozen=True)
class ColumnSpec:
    table: str
    column: str
    kind: SanitizeKind | None
    pk_columns: tuple[str, ...]
    required: bool = False
    handler: HandlerKind = HandlerKind.SCALAR


def all_column_specs() -> list[ColumnSpec]:
    """Full sweep of schema-validated columns, grouped by execution priority."""
    specs: list[ColumnSpec] = []

    def add(
        table: str,
        column: str,
        kind: SanitizeKind | None,
        pk: tuple[str, ...],
        *,
        required: bool = False,
        handler: HandlerKind = HandlerKind.SCALAR,
    ) -> None:
        specs.append(
            ColumnSpec(
                table=table,
                column=column,
                kind=kind,
                pk_columns=pk,
                required=required,
                handler=handler,
            )
        )

    # Tier 1 — user-facing
    for col, kind, req in (
        ("first_name", SanitizeKind.PERSON_NAME, False),
        ("last_name", SanitizeKind.PERSON_NAME, False),
        ("gender", SanitizeKind.SAFE_DISPLAY_NAME, False),
        ("address", SanitizeKind.ADDRESS_TEXT, False),
        ("pin_code", SanitizeKind.PIN_CODE, False),
        ("city", SanitizeKind.CITY_STATE_COUNTRY, False),
        ("state", SanitizeKind.CITY_STATE_COUNTRY, False),
        ("country", SanitizeKind.CITY_STATE_COUNTRY, False),
        ("referred_by", SanitizeKind.ENGAGEMENT_CODE, False),
        ("status", SanitizeKind.STATUS_STR, False),
        ("phone", SanitizeKind.PHONE, True),
        ("email", SanitizeKind.EMAIL, False),
    ):
        add("users", col, kind, ("user_id",), required=req)

    for col, kind in (
        ("participants_employee_id", SanitizeKind.SAFE_DISPLAY_NAME),
        ("participant_department", SanitizeKind.SAFE_DISPLAY_NAME),
        ("participant_blood_group", SanitizeKind.SAFE_DISPLAY_NAME),
        ("blood_collection_cabin", SanitizeKind.SLUG_KEY),
        ("address", SanitizeKind.ADDRESS_TEXT),
        ("sub_locality", SanitizeKind.LANDMARK_TEXT),
        ("landmark", SanitizeKind.LANDMARK_TEXT),
        ("pincode", SanitizeKind.PIN_CODE),
        ("city", SanitizeKind.CITY_STATE_COUNTRY),
        ("state", SanitizeKind.CITY_STATE_COUNTRY),
        ("country", SanitizeKind.CITY_STATE_COUNTRY),
    ):
        add("engagement_participants", col, kind, ("engagement_participant_id",))

    for col, kind, req in (
        ("engagement_name", SanitizeKind.SAFE_DISPLAY_NAME, False),
        ("engagement_code", SanitizeKind.ENGAGEMENT_CODE, True),
        ("address", SanitizeKind.ADDRESS_TEXT, False),
        ("sub_locality", SanitizeKind.LANDMARK_TEXT, False),
        ("landmark", SanitizeKind.LANDMARK_TEXT, False),
        ("pincode", SanitizeKind.PIN_CODE, False),
        ("city", SanitizeKind.CITY_STATE_COUNTRY, False),
        ("state", SanitizeKind.CITY_STATE_COUNTRY, False),
        ("country", SanitizeKind.CITY_STATE_COUNTRY, False),
        ("status", SanitizeKind.STATUS_STR, False),
    ):
        add("engagements", col, kind, ("engagement_id",), required=req)

    for col, kind in (
        ("name", SanitizeKind.SAFE_DISPLAY_NAME),
        ("address", SanitizeKind.ORG_ADDRESS_TEXT),
        ("pin_code", SanitizeKind.PIN_CODE),
        ("city", SanitizeKind.CITY_STATE_COUNTRY),
        ("state", SanitizeKind.CITY_STATE_COUNTRY),
        ("country", SanitizeKind.CITY_STATE_COUNTRY),
        ("industry_key", SanitizeKind.SLUG_KEY),
        ("status", SanitizeKind.STATUS_STR),
    ):
        add("organizations", col, kind, ("organization_id",))

    add(
        "organizations",
        "departments",
        None,
        ("organization_id",),
        handler=HandlerKind.ORG_DEPARTMENTS,
    )

    # Tier 2 — slot detail
    add(
        "engagement_slot_info",
        "slot_detail",
        None,
        ("slot_detail_id",),
        handler=HandlerKind.SLOT_DETAIL,
    )

    for col, kind in (
        ("consultation_cabin", SanitizeKind.SLUG_KEY),
        ("expert_type", SanitizeKind.SLUG_KEY),
        ("consultation_slot", SanitizeKind.SHORT_SAFE_TEXT),
        ("meet_link", SanitizeKind.SAFE_TEXT),
    ):
        add("consultation_bookings", col, kind, ("consultation_id",))

    # Tier 3 — questionnaire
    for col, kind, req in (
        ("question_key", SanitizeKind.SLUG_KEY, True),
        ("question_text", SanitizeKind.QUESTION_TEXT, True),
        ("question_type", SanitizeKind.SLUG_KEY, True),
        ("help_text", SanitizeKind.QUESTION_TEXT, False),
        ("sub_text", SanitizeKind.QUESTION_TEXT, False),
        ("status", SanitizeKind.STATUS_STR, True),
    ):
        add("questionnaire_definitions", col, kind, ("question_id",), required=req)

    for col in ("visibility_rules", "prefill_from", "metsights_sync"):
        add(
            "questionnaire_definitions",
            col,
            None,
            ("question_id",),
            handler=HandlerKind.NESTED_JSON,
        )

    for col, kind, req in (
        ("option_value", SanitizeKind.SLUG_KEY, True),
        ("display_name", SanitizeKind.SAFE_DISPLAY_NAME, True),
        ("tooltip_text", SanitizeKind.SAFE_TEXT, False),
    ):
        add("questionnaire_options", col, kind, ("option_id",), required=req)

    for col, kind, req in (
        ("category_key", SanitizeKind.SLUG_KEY, True),
        ("display_name", SanitizeKind.SAFE_DISPLAY_NAME, True),
        ("category_of", SanitizeKind.SLUG_KEY, True),
        ("status", SanitizeKind.STATUS_STR, True),
    ):
        add("questionnaire_categories", col, kind, ("category_id",), required=req)

    for col, kind, req in (
        ("habit_key", SanitizeKind.SLUG_KEY, False),
        ("habit_label", SanitizeKind.SAFE_DISPLAY_NAME, True),
        ("condition_type", SanitizeKind.SLUG_KEY, True),
        ("scale_unit", SanitizeKind.SAFE_TEXT, False),
        ("status", SanitizeKind.STATUS_STR, True),
    ):
        add("questionnaire_healthy_habit_rules", col, kind, ("rule_id",), required=req)

    add(
        "questionnaire_responses",
        "answer",
        None,
        ("response_id",),
        handler=HandlerKind.QUESTIONNAIRE_ANSWER,
    )

    # Tier 4 — admin / catalog
    for col, kind, req in (
        ("package_code", SanitizeKind.SLUG_KEY, True),
        ("display_name", SanitizeKind.SAFE_DISPLAY_NAME, True),
        ("assessment_type_code", SanitizeKind.SLUG_KEY, True),
        ("status", SanitizeKind.STATUS_STR, True),
    ):
        add("assessment_packages", col, kind, ("package_id",), required=req)

    add("assessment_instances", "status", SanitizeKind.STATUS_STR, ("assessment_instance_id",), required=True)

    for col, kind, req in (
        ("code", SanitizeKind.SLUG_KEY, True),
        ("display_name", SanitizeKind.SAFE_DISPLAY_NAME, True),
    ):
        add("engagement_types", col, kind, ("id",), required=req)

    for col, kind, req in (
        ("industry_key", SanitizeKind.SLUG_KEY, True),
        ("industry", SanitizeKind.SAFE_DISPLAY_NAME, True),
    ):
        add("industries", col, kind, ("id",), required=req)

    for col, kind, req in (
        ("type_key", SanitizeKind.SLUG_KEY, True),
        ("type", SanitizeKind.SAFE_DISPLAY_NAME, True),
    ):
        add("expert_types", col, kind, ("id",), required=req)

    for col, kind, req in (
        ("expert_type", SanitizeKind.SLUG_KEY, True),
        ("specialization", SanitizeKind.SAFE_DISPLAY_NAME, True),
        ("qualifications", SanitizeKind.CHECKLIST_TEXT, False),
        ("about_text", SanitizeKind.EXPERT_ABOUT_TEXT, False),
        ("status", SanitizeKind.STATUS_STR, True),
    ):
        add("experts", col, kind, ("expert_id",), required=req)

    add(
        "experts",
        "languages",
        None,
        ("expert_id",),
        handler=HandlerKind.EXPERT_LANGUAGES,
    )

    add("expert_expertise_tags", "tag_name", SanitizeKind.SAFE_DISPLAY_NAME, ("tag_id",), required=True)
    add("expert_reviews", "review_text", SanitizeKind.SAFE_TEXT, ("review_id",))

    for col, kind, req in (
        ("package_name", SanitizeKind.SAFE_DISPLAY_NAME, True),
        ("health_areas_covered", SanitizeKind.SAFE_TEXT, False),
        ("about_text", SanitizeKind.SAFE_TEXT, False),
        ("status", SanitizeKind.STATUS_STR, False),
    ):
        add("diagnostic_package", col, kind, ("diagnostic_package_id",), required=req)

    for col, kind, req in (
        ("chip_key", SanitizeKind.SLUG_KEY, True),
        ("display_name", SanitizeKind.SAFE_DISPLAY_NAME, True),
        ("status", SanitizeKind.STATUS_STR, False),
    ):
        add("diagnostic_package_filters_chips", col, kind, ("filter_chip_id",), required=req)

    for col, kind, req in (
        ("reason_text", SanitizeKind.SAFE_DISPLAY_NAME, True),
    ):
        add("diagnostic_package_reasons", col, kind, ("reason_id",), required=req)

    for col, kind, req in (
        ("tag_name", SanitizeKind.SAFE_DISPLAY_NAME, True),
    ):
        add("diagnostic_package_tags", col, kind, ("tag_id",), required=req)

    for col, kind, req in (
        ("group_key", SanitizeKind.SLUG_KEY, True),
        ("group_name", SanitizeKind.SAFE_DISPLAY_NAME, True),
        ("status", SanitizeKind.STATUS_STR, False),
    ):
        add("diagnostic_test_groups", col, kind, ("group_id",), required=req)

    for col, kind, req in (
        ("parameter_key", SanitizeKind.SLUG_KEY, True),
        ("test_name", SanitizeKind.SAFE_DISPLAY_NAME, True),
        ("status", SanitizeKind.STATUS_STR, False),
    ):
        add("health_parameters", col, kind, ("test_id",), required=req)

    for col, kind, req in (
        ("sample_type", SanitizeKind.SAFE_DISPLAY_NAME, True),
    ):
        add("diagnostic_package_samples", col, kind, ("sample_id",), required=req)

    for col, kind, req in (
        ("preparation_title", SanitizeKind.SAFE_DISPLAY_NAME, True),
    ):
        add("diagnostic_package_preparations", col, kind, ("preparation_id",), required=req)

    for col, kind, req in (
        ("name", SanitizeKind.SAFE_DISPLAY_NAME, True),
        ("description", SanitizeKind.CHECKLIST_TEXT, False),
        ("status", SanitizeKind.STATUS_STR, True),
    ):
        add("checklist_templates", col, kind, ("template_id",), required=req)

    for col, kind, req in (
        ("title", SanitizeKind.SAFE_DISPLAY_NAME, True),
        ("description", SanitizeKind.CHECKLIST_TEXT, False),
    ):
        add("checklist_template_items", col, kind, ("item_id",), required=req)

    for col, kind in (
        ("status", SanitizeKind.STATUS_STR),
        ("notes", SanitizeKind.CHECKLIST_TEXT),
    ):
        add("engagement_checklist_tasks", col, kind, ("task_id",))

    add("support_tickets", "query_text", SanitizeKind.SUPPORT_QUERY_TEXT, ("ticket_id",), required=True)
    add("support_tickets", "status", SanitizeKind.STATUS_STR, ("ticket_id",), required=True)

    for col, kind, req in (
        ("service_key", SanitizeKind.SERVICE_KEY, True),
        ("display_name", SanitizeKind.SAFE_DISPLAY_NAME, True),
        ("webhook_path", SanitizeKind.SAFE_TEXT, True),
    ):
        add("notification_services", col, kind, ("notification_service_id",), required=req)

    for col, kind, req in (
        ("event_code", SanitizeKind.SLUG_KEY, True),
        ("display_name", SanitizeKind.SAFE_DISPLAY_NAME, True),
        ("description", SanitizeKind.SAFE_TEXT, False),
    ):
        add("auto_notification_events", col, kind, ("id",), required=req)

    for table, pk in (
        ("engagement_notifications", "id"),
        ("engagement_notification_defaults", "id"),
    ):
        add(
            table,
            "notification_services",
            None,
            (pk,),
            handler=HandlerKind.NOTIFICATION_SERVICES,
        )

    for col, kind, req in (
        ("section", SanitizeKind.SAFE_DISPLAY_NAME, True),
        ("section_key", SanitizeKind.SLUG_KEY, True),
        ("description", SanitizeKind.SAFE_TEXT, False),
    ):
        add("camp_report_sections", col, kind, ("report_sections",), required=req)

    for col in ("report", "report_bts"):
        add(
            "camp_reports",
            col,
            None,
            ("report_id",),
            handler=HandlerKind.CAMP_REPORT_JSON,
        )

    return specs


TABLE_GROUPS: dict[str, list[str]] = {
    "users": ["users"],
    "organizations": ["organizations"],
    "engagements": ["engagements", "engagement_participants", "engagement_slot_info", "consultation_bookings"],
    "questionnaire": [
        "questionnaire_definitions",
        "questionnaire_options",
        "questionnaire_categories",
        "questionnaire_healthy_habit_rules",
        "questionnaire_responses",
    ],
    "catalog": [
        "assessment_packages",
        "assessment_instances",
        "engagement_types",
        "industries",
        "expert_types",
        "experts",
        "expert_expertise_tags",
        "expert_reviews",
        "diagnostic_package",
        "diagnostic_package_filters_chips",
        "diagnostic_package_reasons",
        "diagnostic_package_tags",
        "diagnostic_test_groups",
        "health_parameters",
        "diagnostic_package_samples",
        "diagnostic_package_preparations",
        "checklist_templates",
        "checklist_template_items",
        "engagement_checklist_tasks",
        "support_tickets",
        "notification_services",
        "auto_notification_events",
        "engagement_notifications",
        "engagement_notification_defaults",
        "camp_report_sections",
        "camp_reports",
    ],
}


def filter_specs(*, only: set[str] | None = None) -> list[ColumnSpec]:
    specs = all_column_specs()
    if not only:
        return specs
    allowed_tables: set[str] = set()
    for token in only:
        token = token.strip().lower()
        if token in TABLE_GROUPS:
            allowed_tables.update(TABLE_GROUPS[token])
        else:
            allowed_tables.add(token)
    return [spec for spec in specs if spec.table in allowed_tables]
