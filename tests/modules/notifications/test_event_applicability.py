"""Engagement notification applicability rules."""

from modules.notifications.event_applicability import (
    engagement_notification_applicable,
    profile_from_mapping,
)


def test_camp_pretest_applicable():
    profile = profile_from_mapping(
        {
            "engagement_kind": "blood_test",
            "blood_collection_type": "camp_collection",
            "auto_booking": False,
        }
    )
    assert engagement_notification_applicable("reminders.pretest", profile)


def test_pretest_not_on_home_collection():
    profile = profile_from_mapping(
        {
            "engagement_kind": "blood_test",
            "blood_collection_type": "home_collection",
            "auto_booking": True,
        }
    )
    assert not engagement_notification_applicable("reminders.pretest", profile)
    assert engagement_notification_applicable("bookings.phlebo_assigned", profile)


def test_payments_not_on_engagement_wizard():
    profile = profile_from_mapping(
        {"engagement_kind": "blood_test", "blood_collection_type": "camp_collection"}
    )
    assert not engagement_notification_applicable("payments.completed", profile)
