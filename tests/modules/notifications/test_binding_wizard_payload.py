"""Wizard validation for payload recipient bindings."""

from __future__ import annotations

import pytest

from core.exceptions import AppError
from modules.notifications.binding_schemas import (
    EmitConditionPayload,
    RecipientConfigPayload,
    WizardBindingPayload,
    WizardServiceSpec,
)
from modules.notifications.binding_wizard_service import NotificationBindingWizardService
from modules.notifications.events_catalog import get_event


def _payload_wizard(*, channel: str, path: str) -> WizardBindingPayload:
    return WizardBindingPayload(
        event_code="payments.completed",
        recipient_type="payload",
        recipient_config=RecipientConfigPayload(payload_path=path),
        service=WizardServiceSpec(channel=channel, template_name="camp1"),
    )


def test_payload_wizard_requires_phone_path_for_whatsapp():
    svc = NotificationBindingWizardService()
    ev = get_event("payments.completed")
    assert ev is not None
    with pytest.raises(AppError) as exc:
        svc._validate_recipient(_payload_wizard(channel="whatsapp", path="booker.email"), ev)
    assert "phone" in exc.value.message.lower()

    cfg = svc._validate_recipient(_payload_wizard(channel="whatsapp", path="booker.phone"), ev)
    assert cfg == {"payload_path": "booker.phone", "value_type": "phone"}


def test_payload_wizard_rejects_cron_event():
    svc = NotificationBindingWizardService()
    payload = WizardBindingPayload(
        event_code="reminders.pretest",
        recipient_type="payload",
        recipient_config=RecipientConfigPayload(payload_path="phone"),
        service=WizardServiceSpec(channel="whatsapp", template_name="camp1"),
    )
    with pytest.raises(AppError):
        svc._validate_recipient(payload, get_event("reminders.pretest"))


def test_emit_condition_validates_on_payments_completed():
    svc = NotificationBindingWizardService()
    ev = get_event("payments.completed")
    assert ev is not None
    payload = WizardBindingPayload(
        event_code="payments.completed",
        recipient_type="payload",
        recipient_config=RecipientConfigPayload(payload_path="booker.phone"),
        emit_condition=EmitConditionPayload(op="equals", value="VIP"),
        service=WizardServiceSpec(channel="whatsapp", template_name="camp1"),
    )
    key, cond = svc._validate_emit_condition(payload, ev)
    assert key == "dc-eq-vip"
    assert cond == {"path": "payment.discount_code", "op": "equals", "value": "VIP"}

    payload.emit_condition = None
    key2, cond2 = svc._validate_emit_condition(payload, ev)
    assert key2 == "default"
    assert cond2 is None
