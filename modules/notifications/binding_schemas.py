"""Pydantic schemas for notification event bindings API."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from modules.engagement_notifications.service_config import NotificationServiceConfigItem


class NotificationBindingCreate(BaseModel):
    event_code: str = Field(min_length=1)
    event_name: str = Field(min_length=1)
    recipient_type: str = Field(min_length=1)
    notification_services: list[NotificationServiceConfigItem] = Field(default_factory=list)
    engagement_type_id: int | None = None


class NotificationBindingUpdate(BaseModel):
    event_name: str | None = None
    recipient_type: str | None = None
    notification_services: list[NotificationServiceConfigItem] | None = None
    engagement_type_id: int | None = None


class EmitConditionPayload(BaseModel):
    path: str = Field(default="payment.discount_code", min_length=1)
    op: Literal["is_empty", "is_not_empty", "equals", "one_of"]
    value: str | None = None


class EmitConditionFieldResponse(BaseModel):
    path: str
    label: str
    allowed_ops: list[str]


class NotificationBindingResponse(BaseModel):
    id: int
    event_code: str
    event_name: str
    recipient_type: str
    delivery_channel: str
    notification_services: list[dict]
    engagement_type_id: int | None = None
    recipient_config: dict | None = None
    service_key: str | None = None
    condition_key: str = "default"
    emit_condition: dict | None = None


class PayloadContactFieldResponse(BaseModel):
    path: str
    label: str
    value_type: Literal["phone", "email"]
    cardinality: Literal["scalar", "array"]
    description: str
    group: str = "Context"


class CatalogEventResponse(BaseModel):
    code: str
    name: str
    status: str
    variable_groups: list[str]
    variable_fields: list[str] = Field(default_factory=list)
    default_recipients: list[str]
    trigger_notes: str
    legacy_event_code: str | None = None
    recipient_payload_hints: dict[str, str] = Field(default_factory=dict)
    trigger_kind: Literal["http", "cron", "webhook", "internal"] = "http"
    allows_payload_recipient: bool = False
    payload_contact_fields: list[PayloadContactFieldResponse] = Field(default_factory=list)
    catalog_groups: list[str] = Field(default_factory=list)
    show_on_engagement_wizard: bool = False


class RecipientConfigPayload(BaseModel):
    phones: list[str] = Field(default_factory=list)
    emails: list[str] = Field(default_factory=list)
    payload_path: str | None = None
    value_type: Literal["phone", "email"] | None = None


class WizardTemplateSlot(BaseModel):
    index: int = Field(ge=1)
    source: str = Field(min_length=1)
    value: str | None = None


class WizardTemplateHeader(BaseModel):
    source: str = Field(min_length=1)
    value: str | None = None
    filename_source: str | None = None
    filename_value: str | None = None


class WizardServiceSpec(BaseModel):
    notification_service_id: int | None = None
    service_key: str | None = None
    display_name: str | None = None
    channel: str = Field(..., pattern=r"^(email|whatsapp)$")
    template_name: str | None = None
    template_variables: dict | list | None = None
    template_slots: list[WizardTemplateSlot] | None = None
    template_header: WizardTemplateHeader | None = None
    email_subject: str | None = None
    email_body: str | None = None
    email_html: str | None = None
    is_active: bool = True


class WizardBindingPayload(BaseModel):
    event_code: str = Field(min_length=1)
    recipient_type: str = Field(min_length=1)
    recipient_config: RecipientConfigPayload | None = None
    engagement_type_id: int | None = None
    emit_condition: EmitConditionPayload | None = None
    service: WizardServiceSpec


class EngagementNotificationWizardPayload(BaseModel):
    event_code: str = Field(min_length=1)
    service: WizardServiceSpec


class WhatsappTemplateMetaResponse(BaseModel):
    found: bool
    name: str = ""
    language: str | None = None
    category: str | None = None
    status: str | None = None
    parameter_format: str | None = None
    body_positional_count: int | None = None
    header_format: str | None = None
    requires_header_document: bool | None = None
    component_types: list[str] = Field(default_factory=list)
    body_text: str | None = None
