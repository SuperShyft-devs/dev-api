"""Binding resolver filters by emit_condition."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from modules.notifications.binding_resolver import BindingResolver


@pytest.mark.asyncio
async def test_resolver_skips_non_matching_discount_condition():
    row_match = MagicMock()
    row_match.recipient_type = "payload"
    row_match.notification_services = [{"service_key": "payments-completed-whatsapp"}]
    row_match.event_name = "Payment completed"
    row_match.recipient_config = {"payload_path": "booker.phone", "value_type": "phone"}
    row_match.emit_condition = {
        "path": "payment.discount_code",
        "op": "equals",
        "value": "VIP",
    }

    row_other = MagicMock()
    row_other.recipient_type = "payload"
    row_other.notification_services = [{"service_key": "payments-completed-whatsapp-2"}]
    row_other.event_name = "Payment completed"
    row_other.recipient_config = {"payload_path": "booker.phone", "value_type": "phone"}
    row_other.emit_condition = {
        "path": "payment.discount_code",
        "op": "equals",
        "value": "STAFF",
    }

    repo = MagicMock()
    repo.list_for_event = AsyncMock(return_value=[row_match, row_other])

    resolver = BindingResolver(bindings_repo=repo)
    db = MagicMock()
    resolved = await resolver.resolve(
        db,
        event_code="payments.completed",
        context={"payment": {"discount_code": "VIP"}},
    )

    assert len(resolved) == 1
    assert resolved[0].notification_services[0]["service_key"] == "payments-completed-whatsapp"
