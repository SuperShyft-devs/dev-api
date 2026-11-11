"""Context builder: session + engagement from onboard participant_details."""

from modules.notifications.context_builder import build_member_context
from modules.notifications.template_variables import resolve_slots


def test_session_and_engagement_from_participant_details():
    member = {
        "first_name": "Pratheek",
        "last_name": "Bedre",
        "phone": "8762830757",
        "email": "p@example.com",
    }
    pd = {
        "name": "Pratheek Bedre",
        "engagement_name": "My Camp Name",
        "engagement_code": "ABC456",
        "collection_date": "2026-11-20",
        "collection_time": "10:00",
        "cabin_name": "Cabin A",
    }
    ctx = build_member_context(member, participant_details=pd)
    assert ctx["session"]["date"] == "2026-11-20"
    assert ctx["session"]["time"] == "10:00"
    assert ctx["session"]["cabin"] == "Cabin A"
    assert ctx["engagement"]["engagement_name"] == "My Camp Name"
    assert ctx["engagement"]["engagement_code"] == "ABC456"

    tv = {
        "slots": [
            {"index": 1, "source": "literal", "value": "Hiii {{user.first_name}}"},
            {"index": 2, "source": "literal", "value": "onboarded to {{engagement.engagement_name}}"},
            {
                "index": 3,
                "source": "literal",
                "value": "Hii2 {{session.date}} {{session.cabin}} {{session.time}}",
            },
        ]
    }
    slots = resolve_slots(tv, ctx)
    assert slots[0] == "Hiii Pratheek"
    assert slots[1] == "onboarded to My Camp Name"
    assert slots[2] == "Hii2 2026-11-20 Cabin A 10:00"


def test_ops_recipient_gets_staff_and_participant_blocks():
    """Ops/staff binding: staff.* = notify recipient; participant.* = enrolled user."""
    ops = {
        "first_name": "Priya",
        "last_name": "Ops",
        "phone": "9000000001",
        "email": "ops@supershyft.com",
    }
    pd = {
        "name": "Pratheek Bedre",
        "phone": "8762830757",
        "email": "p@example.com",
        "participant_user_id": "42",
        "engagement_name": "Camp X",
    }
    ctx = build_member_context(ops, participant_details=pd)
    assert ctx["staff"]["first_name"] == "Priya"
    assert ctx["participant"]["name"] == "Pratheek Bedre"
    assert ctx["participant"]["first_name"] == "Pratheek"
    assert ctx["user"]["first_name"] == "Pratheek"

    tv = {
        "slots": [
            {"index": 1, "source": "literal", "value": "Hi {{staff.first_name}}"},
            {"index": 2, "source": "literal", "value": "New joiner: {{participant.name}} ({{participant.phone}})"},
        ]
    }
    slots = resolve_slots(tv, ctx)
    assert slots[0] == "Hi Priya"
    assert slots[1] == "New joiner: Pratheek Bedre (8762830757)"
