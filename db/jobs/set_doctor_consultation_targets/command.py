"""One-time job: fill doctor consultation want=true to a random 85–92% target per engagement.

Keeps existing doctor Yes responses and randomly sets Yes on participants still at No
until each engagement reaches approximately x% (x drawn uniformly from 85..92).

Entrypoint::

    python -m db.jobs.set_doctor_consultation_targets --dry-run
    python -m db.jobs.set_doctor_consultation_targets --yes
    python -m db.jobs.set_doctor_consultation_targets --dry-run --seed 42 --codes NFGU0926
"""

from __future__ import annotations

import argparse
import asyncio
import random
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from db.engine import create_job_engine, job_session_factory
from modules.engagements.models import Engagement, EngagementParticipant
from modules.engagements.repository import EngagementsRepository
from modules.experts.consultation_bookings_repository import ConsultationBookingsRepository

DEFAULT_ENGAGEMENT_CODES = (
    "NFGU0926",
    "NMGU0926",
    "NFHY0926",
    "NMHY0926",
    "NFBA0826",
    "NMBA0826",
    "NFPU0926",
    "NMPU0926",
)

DOCTOR_EXPERT_TYPE = "doctor"
TARGET_MIN_PERCENT = 85
TARGET_MAX_PERCENT = 92


@dataclass(frozen=True)
class EngagementRunResult:
    engagement_code: str
    engagement_id: int | None
    x_percent: int | None
    total: int
    already_yes: int
    target: int
    would_set: int
    applied: int
    status: str


def _participant_has_doctor_yes(
    participant_id: int,
    bookings_by_participant: dict[int, list],
) -> bool:
    for booking in bookings_by_participant.get(participant_id, []):
        if booking.expert_type == DOCTOR_EXPERT_TYPE and booking.want:
            return True
    return False


def _compute_target(total: int, x_percent: int) -> int:
    if total <= 0:
        return 0
    return round(total * x_percent / 100)


async def _list_participants_for_engagement(
    db: AsyncSession,
    engagement_id: int,
) -> list[EngagementParticipant]:
    result = await db.execute(
        select(EngagementParticipant)
        .where(EngagementParticipant.engagement_id == engagement_id)
        .order_by(EngagementParticipant.engagement_participant_id.asc())
    )
    return list(result.scalars().all())


async def _process_engagement(
    db: AsyncSession,
    *,
    engagement_code: str,
    rng: random.Random,
    dry_run: bool,
    consultation_repo: ConsultationBookingsRepository,
) -> EngagementRunResult:
    engagements_repo = EngagementsRepository()
    engagement = await engagements_repo.get_engagement_by_code(db, engagement_code)
    if engagement is None:
        return EngagementRunResult(
            engagement_code=engagement_code,
            engagement_id=None,
            x_percent=None,
            total=0,
            already_yes=0,
            target=0,
            would_set=0,
            applied=0,
            status="skipped:not_found",
        )

    participants = await _list_participants_for_engagement(db, engagement.engagement_id)
    total = len(participants)
    if total == 0:
        return EngagementRunResult(
            engagement_code=engagement_code,
            engagement_id=engagement.engagement_id,
            x_percent=None,
            total=0,
            already_yes=0,
            target=0,
            would_set=0,
            applied=0,
            status="skipped:no_participants",
        )

    participant_ids = [participant.engagement_participant_id for participant in participants]
    bookings_by_participant = await consultation_repo.get_for_participants_batch(db, participant_ids)

    already_yes_ids = {
        participant.engagement_participant_id
        for participant in participants
        if _participant_has_doctor_yes(participant.engagement_participant_id, bookings_by_participant)
    }
    candidate_participants = [
        participant
        for participant in participants
        if participant.engagement_participant_id not in already_yes_ids
    ]

    already_yes = len(already_yes_ids)
    x_percent = rng.randint(TARGET_MIN_PERCENT, TARGET_MAX_PERCENT)
    target = _compute_target(total, x_percent)

    if already_yes >= target:
        return EngagementRunResult(
            engagement_code=engagement_code,
            engagement_id=engagement.engagement_id,
            x_percent=x_percent,
            total=total,
            already_yes=already_yes,
            target=target,
            would_set=0,
            applied=0,
            status="skipped:already_at_target",
        )

    need = target - already_yes
    selected = rng.sample(candidate_participants, need)

    if dry_run:
        return EngagementRunResult(
            engagement_code=engagement_code,
            engagement_id=engagement.engagement_id,
            x_percent=x_percent,
            total=total,
            already_yes=already_yes,
            target=target,
            would_set=len(selected),
            applied=0,
            status="dry_run",
        )

    for participant in selected:
        await consultation_repo.sync_from_want_map(
            db,
            participant,
            {DOCTOR_EXPERT_TYPE: {"want": True}},
        )

    return EngagementRunResult(
        engagement_code=engagement_code,
        engagement_id=engagement.engagement_id,
        x_percent=x_percent,
        total=total,
        already_yes=already_yes,
        target=target,
        would_set=len(selected),
        applied=len(selected),
        status="applied",
    )


def _print_summary(results: list[EngagementRunResult], *, dry_run: bool) -> None:
    mode = "dry-run" if dry_run else "applied"
    print(f"Set doctor consultation targets ({mode})")
    print(
        f"{'code':<12} {'eng_id':>7} {'x%':>4} {'total':>6} {'yes':>5} "
        f"{'target':>6} {'set':>5} {'status'}"
    )
    print("-" * 72)
    for row in results:
        eng_id = str(row.engagement_id) if row.engagement_id is not None else "-"
        x_percent = str(row.x_percent) if row.x_percent is not None else "-"
        set_count = row.would_set if dry_run else row.applied
        print(
            f"{row.engagement_code:<12} {eng_id:>7} {x_percent:>4} {row.total:>6} "
            f"{row.already_yes:>5} {row.target:>6} {set_count:>5} {row.status}"
        )

    total_applied = sum(row.applied for row in results)
    total_would_set = sum(row.would_set for row in results)
    if dry_run:
        print(f"\nTotal would_set={total_would_set}")
    else:
        print(f"\nTotal applied={total_applied}")


async def run_set_doctor_consultation_targets(
    *,
    yes: bool,
    dry_run: bool,
    seed: int | None,
    engagement_codes: list[str],
) -> list[EngagementRunResult]:
    settings.validate()

    if not yes and not dry_run:
        raise SystemExit(
            "Refusing to run without explicit confirmation. Re-run with --yes to apply changes, "
            "or --dry-run to preview."
        )

    rng = random.Random(seed)
    engine = create_job_engine()
    session_factory = job_session_factory(engine)
    consultation_repo = ConsultationBookingsRepository()
    results: list[EngagementRunResult] = []

    async with session_factory() as session:
        async with session.begin():
            for engagement_code in engagement_codes:
                result = await _process_engagement(
                    session,
                    engagement_code=engagement_code,
                    rng=rng,
                    dry_run=dry_run,
                    consultation_repo=consultation_repo,
                )
                results.append(result)

    await engine.dispose()
    return results


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "For each target engagement, pick a random x in 85–92%% and set doctor "
            "consultation want=true on randomly selected participants who are still No "
            "until approximately x%% of participants have Yes. Existing Yes values are kept."
        )
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Apply changes. Without this flag (and without --dry-run), the command exits without writing.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report how many participants would be updated without writing.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional RNG seed for reproducible target percentages and participant selection.",
    )
    parser.add_argument(
        "--codes",
        nargs="+",
        default=list(DEFAULT_ENGAGEMENT_CODES),
        metavar="CODE",
        help="Engagement codes to process (default: the eight configured camp codes).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    engagement_codes = [code.strip() for code in args.codes if code.strip()]
    if not engagement_codes:
        raise SystemExit("No engagement codes provided.")

    results = asyncio.run(
        run_set_doctor_consultation_targets(
            yes=args.yes,
            dry_run=args.dry_run,
            seed=args.seed,
            engagement_codes=engagement_codes,
        )
    )
    _print_summary(results, dry_run=args.dry_run)
    return 0
