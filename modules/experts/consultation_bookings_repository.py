"""Repository for consultation_bookings table."""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from modules.engagements.models import EngagementParticipant
from modules.experts.consultations import empty_consent, normalize_preference
from modules.experts.models import ConsultationBooking


class ConsultationBookingsRepository:
    async def get_by_id(self, db: AsyncSession, consultation_id: int) -> ConsultationBooking | None:
        return await db.get(ConsultationBooking, consultation_id)

    async def get_by_ids(self, db: AsyncSession, consultation_ids: list[int]) -> list[ConsultationBooking]:
        if not consultation_ids:
            return []
        result = await db.execute(
            select(ConsultationBooking)
            .where(ConsultationBooking.consultation_id.in_(consultation_ids))
            .order_by(ConsultationBooking.consultation_id.asc())
        )
        return list(result.scalars().all())

    async def get_for_participant(self, db: AsyncSession, participant_id: int) -> list[ConsultationBooking]:
        result = await db.execute(
            select(ConsultationBooking)
            .where(ConsultationBooking.engagement_participant_id == participant_id)
            .order_by(ConsultationBooking.consultation_id.asc())
        )
        return list(result.scalars().all())

    async def get_by_participant_and_type(
        self,
        db: AsyncSession,
        participant_id: int,
        expert_type: str,
    ) -> ConsultationBooking | None:
        result = await db.execute(
            select(ConsultationBooking)
            .where(ConsultationBooking.engagement_participant_id == participant_id)
            .where(ConsultationBooking.expert_type == expert_type)
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def get_for_participants_batch(
        self,
        db: AsyncSession,
        participant_ids: list[int],
    ) -> dict[int, list[ConsultationBooking]]:
        if not participant_ids:
            return {}
        result = await db.execute(
            select(ConsultationBooking)
            .where(ConsultationBooking.engagement_participant_id.in_(participant_ids))
            .order_by(ConsultationBooking.consultation_id.asc())
        )
        grouped: dict[int, list[ConsultationBooking]] = {}
        for booking in result.scalars().all():
            grouped.setdefault(booking.engagement_participant_id, []).append(booking)
        return grouped

    def _append_booking_id(self, participant: EngagementParticipant, consultation_id: int) -> None:
        ids = list(participant.consultation_booking_ids or [])
        if consultation_id not in ids:
            ids.append(consultation_id)
            participant.consultation_booking_ids = ids

    async def create_or_update_for_type(
        self,
        db: AsyncSession,
        participant: EngagementParticipant,
        expert_type: str,
        *,
        want: bool | None = None,
        consultation_date: Any = None,
        consultation_slot: str | None = None,
        expert_id: int | None = None,
        done: bool | None = None,
        meet_link: str | None = None,
        consent: dict[str, Any] | None = None,
        clear_scheduling: bool = False,
    ) -> ConsultationBooking:
        booking = await self.get_by_participant_and_type(
            db,
            participant.engagement_participant_id,
            expert_type,
        )
        if booking is None:
            booking = ConsultationBooking(
                engagement_participant_id=participant.engagement_participant_id,
                expert_type=expert_type,
                want=want if want is not None else False,
                consent=empty_consent(),
            )
            db.add(booking)
            await db.flush()
            self._append_booking_id(participant, booking.consultation_id)
            db.add(participant)

        if want is not None:
            booking.want = want
        if clear_scheduling:
            booking.consultation_date = None
            booking.consultation_slot = None
            booking.expert_id = None
            booking.done = False
            booking.meet_link = None
        if consultation_date is not None:
            booking.consultation_date = consultation_date
        if consultation_slot is not None:
            booking.consultation_slot = consultation_slot
        if expert_id is not None:
            booking.expert_id = expert_id
        if done is not None:
            booking.done = done
        if meet_link is not None:
            booking.meet_link = meet_link
        if consent is not None:
            current = empty_consent()
            if isinstance(booking.consent, dict):
                current.update(booking.consent)
            current.update(consent)
            booking.consent = current

        db.add(booking)
        await db.flush()
        return booking

    async def sync_from_want_map(
        self,
        db: AsyncSession,
        participant: EngagementParticipant,
        consultations_map: dict[str, Any],
    ) -> list[ConsultationBooking]:
        from datetime import date as date_type

        bookings: list[ConsultationBooking] = []
        for expert_type, raw_pref in consultations_map.items():
            pref = normalize_preference(raw_pref)
            existing = await self.get_by_participant_and_type(
                db,
                participant.engagement_participant_id,
                expert_type,
            )
            want = bool(pref.get("want"))
            if not want and existing is None:
                continue

            consultation_date = None
            if want and pref.get("date"):
                consultation_date = date_type.fromisoformat(str(pref["date"])[:10])

            booking = await self.create_or_update_for_type(
                db,
                participant,
                expert_type,
                want=want,
                consultation_date=consultation_date,
                consultation_slot=pref.get("slot") if want else None,
                expert_id=pref.get("expert_id") if want else None,
                done=bool(pref.get("done")) if want else False,
                meet_link=pref.get("meet_link") if want else None,
                clear_scheduling=not want,
            )
            bookings.append(booking)
        return bookings
