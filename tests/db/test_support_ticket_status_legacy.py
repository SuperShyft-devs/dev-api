"""Legacy support ticket status codes."""

from db.column_types import SupportTicketStatusColumn


def test_support_ticket_status_legacy_closed_maps_to_resolved():
    assert SupportTicketStatusColumn.process_result_value(3, None) == "resolved"
    assert SupportTicketStatusColumn.process_result_value(2, None) == "resolved"
    assert SupportTicketStatusColumn.process_result_value(1, None) == "open"
