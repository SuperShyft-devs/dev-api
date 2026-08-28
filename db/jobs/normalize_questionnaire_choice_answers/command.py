"""Normalize choice questionnaire answers to option_value codes.

Entrypoint::

    python -m db.jobs.normalize_questionnaire_choice_answers --dry-run
    python -m db.jobs.normalize_questionnaire_choice_answers --dry-run --phone 9769422110
    python -m db.jobs.normalize_questionnaire_choice_answers --yes --assessment-instance-id 11963
    python -m db.jobs.normalize_questionnaire_choice_answers --yes --report /tmp/normalize-choice.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

from core.config import settings
from db.engine import create_job_engine, job_session_factory
from modules.maintenance.normalize_questionnaire_choice_answers import (
    normalize_questionnaire_choice_answers,
)

logger = logging.getLogger(__name__)


async def run_normalize(
    *,
    yes: bool,
    dry_run: bool,
    assessment_instance_id: int | None,
    phone: str | None,
    report_path: str | None,
) -> dict:
    settings.validate()
    if not yes and not dry_run:
        raise SystemExit(
            "Refusing to run without explicit confirmation. Re-run with --yes to apply changes, "
            "or --dry-run to preview."
        )

    engine = create_job_engine()
    session_factory = job_session_factory(engine)

    async with session_factory() as session:
        report = await normalize_questionnaire_choice_answers(
            session,
            dry_run=dry_run,
            assessment_instance_id=assessment_instance_id,
            phone=phone,
        )
        if not dry_run:
            await session.commit()

    await engine.dispose()
    result = report.to_dict()

    if report_path:
        path = Path(report_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
        logger.info("Wrote report to %s", path)

    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Rewrite questionnaire_responses choice answers from display labels "
            "to questionnaire_options.option_value codes (e.g. after Metsights import)."
        )
    )
    parser.add_argument("--yes", action="store_true", help="Apply changes.")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing.")
    parser.add_argument(
        "--assessment-instance-id",
        type=int,
        default=None,
        help="Only process responses for this assessment instance.",
    )
    parser.add_argument(
        "--phone",
        type=str,
        default=None,
        help="Only process assessment instances owned by users with this phone (last 10 digits).",
    )
    parser.add_argument(
        "--report",
        type=str,
        default=None,
        help="Write JSON report to this path.",
    )
    return parser


def _print_summary(result: dict) -> None:
    mode = "dry-run" if result.get("dry_run") else "applied"
    print(f"\nNormalize questionnaire choice answers ({mode}):")
    print(f"  scanned={result.get('scanned')}")
    print(f"  updated={result.get('updated')}")
    print(f"  unchanged={result.get('unchanged')}")
    print(f"  unmatched={result.get('unmatched')}")
    if result.get("assessment_instance_id") is not None:
        print(f"  assessment_instance_id={result['assessment_instance_id']}")
    if result.get("phone"):
        print(f"  phone={result['phone']}")

    for item in (result.get("changes") or [])[:20]:
        print(
            f"  change aid={item.get('assessment_instance_id')} "
            f"q={item.get('question_key')}: {item.get('from')!r} -> {item.get('to')!r}"
        )
    unmatched = result.get("unmatched_samples") or []
    if unmatched:
        print(f"  unmatched samples ({min(len(unmatched), 10)} of {result.get('unmatched')}):")
        for item in unmatched[:10]:
            print(
                f"    aid={item.get('assessment_instance_id')} "
                f"q={item.get('question_key')}: {item.get('answer')!r} ({item.get('reason')})"
            )
    print()


def main(argv: list[str] | None = None) -> int:
    load_dotenv(override=False)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    args = _build_parser().parse_args(argv)
    result = asyncio.run(
        run_normalize(
            yes=args.yes,
            dry_run=args.dry_run,
            assessment_instance_id=args.assessment_instance_id,
            phone=args.phone,
            report_path=args.report,
        )
    )
    _print_summary(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
