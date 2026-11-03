"""Unit tests for participant home-collection location helpers."""

from modules.engagements.enums import BloodCollectionType
from modules.engagements.models import Engagement, EngagementParticipant
from modules.engagements.participant_location import (
    apply_geocoded_home_location_to_participant,
    booking_location_field,
    can_seed_participant_address_from_engagement,
    copy_engagement_text_address_to_participant,
    is_b2c_engagement,
    is_home_blood_collection,
    resolve_upcoming_slot_location_display,
    resolve_upcoming_slot_location_type,
    use_participant_home_for_booking,
)


def test_home_collection_booking_location_uses_participant_only():
    engagement = Engagement(
        blood_collection_type=BloodCollectionType.home_collection,
        organization_id=None,
        address="Engagement Addr",
        pincode="560001",
        latitude=12.0,
        longitude=77.0,
    )
    participant = EngagementParticipant(address="Participant Home", pincode="110001")
    assert booking_location_field(participant, engagement, "address") == "Participant Home"
    assert booking_location_field(participant, engagement, "pincode") == "110001"
    assert booking_location_field(participant, engagement, "latitude") is None
    assert use_participant_home_for_booking(engagement, participant) is True


def test_home_collection_does_not_read_engagement_geo_when_participant_empty():
    engagement = Engagement(
        blood_collection_type=BloodCollectionType.home_collection,
        organization_id=None,
        address="A 201, Andheri West",
        pincode="400053",
        latitude=19.1298098,
        longitude=72.8365958,
        healthians_zone_id="264",
    )
    participant = EngagementParticipant()
    assert booking_location_field(participant, engagement, "pincode") is None
    assert booking_location_field(participant, engagement, "latitude") is None


def test_can_seed_participant_address_from_engagement():
    engagement = Engagement(
        blood_collection_type=BloodCollectionType.home_collection,
        organization_id=None,
    )
    assert can_seed_participant_address_from_engagement(engagement, participant_count=1)
    assert not can_seed_participant_address_from_engagement(engagement, participant_count=2)
    b2b = Engagement(
        blood_collection_type=BloodCollectionType.home_collection,
        organization_id=99,
    )
    assert not can_seed_participant_address_from_engagement(b2b, participant_count=1)


def test_copy_engagement_text_address_to_participant_skips_geo():
    engagement = Engagement(
        address="Engagement Addr",
        pincode="400001",
        latitude=19.0,
        longitude=72.0,
        healthians_zone_id="99",
    )
    participant = EngagementParticipant()
    copy_engagement_text_address_to_participant(participant, engagement)
    assert participant.address == "Engagement Addr"
    assert participant.pincode == "400001"
    assert participant.latitude is None
    assert participant.healthians_zone_id is None


def test_apply_geocoded_home_location_to_participant():
    participant = EngagementParticipant()
    apply_geocoded_home_location_to_participant(
        participant,
        geocoded={"latitude": 19.07, "longitude": 72.87, "state": "MH", "country": "IN"},
        address_line="Flat 12",
        city="Mumbai",
        pincode_fallback="400001",
    )
    assert participant.address == "Flat 12"
    assert participant.pincode == "400001"
    assert participant.latitude == 19.07
    assert participant.longitude == 72.87
    assert participant.state == "MH"


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


def test_is_b2c_engagement():
    assert is_b2c_engagement(Engagement(organization_id=None))
    assert not is_b2c_engagement(Engagement(organization_id=1))


def test_resolve_upcoming_slot_location_display_home_chain():
    bct = BloodCollectionType.home_collection
    assert (
        resolve_upcoming_slot_location_display(
            blood_collection_type=bct,
            participant_address="P Addr",
            engagement_address="E Addr",
            engagement_city="City",
            user_address="U Addr",
            user_city="UCity",
            organization_id=None,
            participant_count=1,
        )
        == "P Addr"
    )
    assert (
        resolve_upcoming_slot_location_display(
            blood_collection_type=bct,
            participant_address=None,
            engagement_address="E Addr",
            engagement_city="City",
            user_address="U Addr",
            user_city="UCity",
            organization_id=None,
            participant_count=1,
        )
        == "E Addr"
    )
    assert (
        resolve_upcoming_slot_location_display(
            blood_collection_type=bct,
            participant_address=None,
            engagement_address="E Addr",
            engagement_city="City",
            user_address="U Addr",
            user_city="UCity",
            organization_id=None,
            participant_count=2,
        )
        == "U Addr"
    )


def test_resolve_upcoming_slot_location_type():
    assert (
        resolve_upcoming_slot_location_type(
            blood_collection_type=BloodCollectionType.home_collection,
            organization_id=1,
        )
        == "home_collection"
    )
    assert (
        resolve_upcoming_slot_location_type(
            blood_collection_type=BloodCollectionType.camp_collection,
            organization_id=None,
        )
        == "venue"
    )
    assert (
        resolve_upcoming_slot_location_type(
            blood_collection_type=None,
            organization_id=5,
        )
        == "venue"
    )
