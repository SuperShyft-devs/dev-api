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
