"""Unit tests for participant home-collection location helpers."""

from modules.engagements.enums import BloodCollectionType
from modules.engagements.models import Engagement, EngagementParticipant
from modules.engagements.participant_location import (
    booking_location_field,
    is_home_blood_collection,
    use_participant_home_for_booking,
)


def test_home_collection_booking_location_uses_participant():
    engagement = Engagement(
        blood_collection_type=BloodCollectionType.home_collection,
        organization_id=None,
        address="Engagement Addr",
        pincode="560001",
    )
    participant = EngagementParticipant(address="Participant Home", pincode="110001")
    assert booking_location_field(participant, engagement, "address") == "Participant Home"
    assert booking_location_field(participant, engagement, "pincode") == "110001"
    assert use_participant_home_for_booking(engagement, participant) is True


def test_camp_collection_booking_location_uses_engagement():
    engagement = Engagement(
        blood_collection_type=BloodCollectionType.camp_collection,
        organization_id=99,
        address="Camp Venue",
        pincode="560100",
    )
    participant = EngagementParticipant(address="Home 42", pincode="560001")
    assert booking_location_field(participant, engagement, "address") == "Camp Venue"
    assert booking_location_field(participant, engagement, "pincode") == "560100"
    assert use_participant_home_for_booking(engagement, participant) is False


def test_is_home_blood_collection():
    assert is_home_blood_collection(
        Engagement(blood_collection_type=BloodCollectionType.home_collection)
    )
    assert not is_home_blood_collection(
        Engagement(blood_collection_type=BloodCollectionType.camp_collection)
    )
