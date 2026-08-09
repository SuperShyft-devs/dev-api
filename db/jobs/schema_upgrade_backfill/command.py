"""One-off schema upgrade backfill.

Handles three tasks in order:
1. Backfill ``category_ids`` on ``questionnaire_responses``
2. Backfill ``is_submitted`` on ``assessment_category_progress``
3. Dedup ``engagement_participants`` on ``(engagement_id, user_id)``

Entrypoint: ``python -m db.jobs.schema_upgrade_backfill --yes``
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from core.config import settings

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

_METSIGHTS_SUBRESOURCE_TO_CATEGORY_KEYS: dict[str, list[str]] = {
    "physical_measurement": ["physical-measurement", "anthropometry"],
    "vital_parameter": ["vitals", "health_vitals"],
    "diet_lifestyle_parameter": ["diet-lifestyle-parameters", "lifestyle_habits", "nutrition_log"],
    "blood_parameter": ["blood-parameters", "advanced-blood-parameters"],
    "advanced_blood_parameter": ["advanced-blood-parameters"],
    "fitness_parameter": ["fitness-parameters", "anthropometry"],
}

_BATCH_SIZE = 500
_MS_SYNC_FLUSH_EVERY = 25
_RATE_LIMIT_WAIT_SECONDS = 60
_RATE_LIMIT_MAX_RETRIES = 8

# Nested keys on GET /records/:id/ that carry questionnaire payloads.
_CATEGORY_KEY_TO_DETAIL_FIELD: dict[str, str] = {
    "physical-measurement": "physical_measurement",
    "vitals": "vital_parameter",
    "diet-lifestyle-parameters": "diet_lifestyle_parameter",
    "blood-parameters": "blood_parameter",
    "advanced-blood-parameters": "advanced_blood_parameter",
    "fitness-parameters": "fitness_parameter",
}


def _is_rate_limited(exc: BaseException) -> bool:
    text_blob = str(exc).lower()
    if "429" in text_blob or "rate limit" in text_blob or "too many requests" in text_blob:
        return True
    if getattr(exc, "error_code", None) == "METSIGHTS_RATE_LIMITED":
        return True
    status_code = getattr(exc, "status_code", None)
    if status_code == 429:
        return True
    response = getattr(exc, "response", None)
    if getattr(response, "status_code", None) == 429:
        return True
    cause = getattr(exc, "__cause__", None)
    if cause is not None and cause is not exc:
        return _is_rate_limited(cause)
    return False


async def _call_metsights_with_retry(label: str, coro_factory):
    """Run an async MetSights call; on rate-limit, wait 60s and retry."""
    attempt = 0
    while True:
        try:
            return await coro_factory()
        except Exception as exc:
            if not _is_rate_limited(exc) or attempt >= _RATE_LIMIT_MAX_RETRIES:
                raise
            attempt += 1
            logger.warning(
                "  Rate limited on %s (attempt %d/%d) — waiting %ds then continuing…",
                label,
                attempt,
                _RATE_LIMIT_MAX_RETRIES,
                _RATE_LIMIT_WAIT_SECONDS,
            )
            await asyncio.sleep(_RATE_LIMIT_WAIT_SECONDS)


async def _ensure_category_progress(
    db: AsyncSession,
    *,
    assessment_instance_id: int,
    category_id: int,
    status: str,
    is_submitted: bool,
    dry_run: bool,
) -> None:
    if dry_run:
        return
    existing = await db.execute(
        text(
            """
            SELECT id FROM assessment_category_progress
            WHERE assessment_instance_id = :iid AND category_id = :cid
            LIMIT 1
            """
        ),
        {"iid": assessment_instance_id, "cid": category_id},
    )
    row = existing.first()
    mark_complete = status == "complete"
    if row is None:
        await db.execute(
            text(
                """
                INSERT INTO assessment_category_progress
                    (assessment_instance_id, category_id, status, is_submitted, completed_at)
                VALUES (
                    :iid, :cid, CAST(:status AS varchar), :submitted,
                    CASE WHEN :mark_complete THEN NOW() ELSE NULL END
                )
                """
            ),
            {
                "iid": assessment_instance_id,
                "cid": category_id,
                "status": status,
                "submitted": is_submitted,
                "mark_complete": mark_complete,
            },
        )
        return

    await db.execute(
        text(
            """
            UPDATE assessment_category_progress
            SET status = CAST(:status AS varchar),
                is_submitted = CASE WHEN :submitted THEN true ELSE is_submitted END,
                completed_at = CASE
                    WHEN :mark_complete THEN COALESCE(completed_at, NOW())
                    ELSE completed_at
                END
            WHERE assessment_instance_id = :iid
              AND category_id = :cid
            """
        ),
        {
            "iid": assessment_instance_id,
            "cid": category_id,
            "status": status,
            "submitted": is_submitted,
            "mark_complete": mark_complete,
        },
    )


async def _step1_backfill_category_ids(db: AsyncSession, *, dry_run: bool) -> dict:
    """For each questionnaire_response, set category_ids = all category_ids the question
    belongs to within the instance's package (via questionnaire_category_questions
    intersected with assessment_package_categories)."""

    logger.info("Step 1: Backfill category_ids on questionnaire_responses")

    result = await db.execute(text("""
        SELECT qr.response_id,
               qr.question_id,
               ai.package_id
        FROM questionnaire_responses qr
        JOIN assessment_instances ai ON ai.assessment_instance_id = qr.assessment_instance_id
        WHERE qr.category_ids = '{}'
           OR array_length(qr.category_ids, 1) <= 1
        ORDER BY qr.response_id
    """))
    rows = result.all()
    logger.info("  Found %d responses to backfill", len(rows))

    if not rows:
        return {"responses_processed": 0, "responses_updated": 0}

    question_package_pairs: set[tuple[int, int]] = set()
    for row in rows:
        question_package_pairs.add((int(row.question_id), int(row.package_id)))

    category_cache: dict[tuple[int, int], list[int]] = {}
    for qid, pid in question_package_pairs:
        res = await db.execute(text("""
            SELECT qcq.category_id
            FROM questionnaire_category_questions qcq
            JOIN assessment_package_categories apc
              ON apc.category_id = qcq.category_id
             AND apc.package_id = :pid
            WHERE qcq.question_id = :qid
            ORDER BY qcq.category_id
        """), {"qid": qid, "pid": pid})
        category_cache[(qid, pid)] = [int(r[0]) for r in res.all()]

    updated = 0
    for i, row in enumerate(rows):
        cat_ids = category_cache.get((int(row.question_id), int(row.package_id)), [])
        if not cat_ids:
            continue
        if not dry_run:
            await db.execute(
                text("UPDATE questionnaire_responses SET category_ids = :cats WHERE response_id = :rid"),
                {"cats": cat_ids, "rid": row.response_id},
            )
        updated += 1
        if (i + 1) % _BATCH_SIZE == 0:
            if not dry_run:
                await db.flush()
            logger.info("  Processed %d / %d responses", i + 1, len(rows))

    if not dry_run:
        await db.flush()

    logger.info("  Step 1 complete: %d / %d updated", updated, len(rows))
    return {"responses_processed": len(rows), "responses_updated": updated}


async def _step2_backfill_is_submitted(
    db: AsyncSession,
    *,
    dry_run: bool,
    metsights_service,
    sync_service,
    engagement_id: int | None = None,
) -> dict:
    """Sync category progress + is_submitted from MetSights record completeness.

    For each assessment_instance with a metsights_record_id:
      1. GET /records/{id}/
      2. Import answers for complete sub-resources
      3. Mark matching metsights + supershyft category progress as complete/submitted
      4. Mark instance completed when record (or all package categories) is complete

    Then a local pass marks category status=complete when all required questions
    are answered (without forcing is_submitted).
    """
    logger.info("Step 2: Backfill is_submitted via MetSights + local completeness")

    # Fast path: already-completed instances
    if not dry_run:
        result = await db.execute(
            text(
                """
                UPDATE assessment_category_progress acp
                SET is_submitted = true
                FROM assessment_instances ai
                WHERE ai.assessment_instance_id = acp.assessment_instance_id
                  AND lower(ai.status) = 'completed'
                  AND acp.is_submitted = false
                RETURNING acp.id
                """
            )
        )
        completed_fast = len(result.all())
    else:
        result = await db.execute(
            text(
                """
                SELECT acp.id
                FROM assessment_category_progress acp
                JOIN assessment_instances ai ON ai.assessment_instance_id = acp.assessment_instance_id
                WHERE lower(ai.status) = 'completed'
                  AND acp.is_submitted = false
                """
            )
        )
        completed_fast = len(result.all())
    logger.info("  Fast-path completed instances: %d progress rows", completed_fast)

    # Category key -> id cache
    cat_rows = await db.execute(
        text(
            """
            SELECT category_id, category_key, category_of
            FROM questionnaire_categories
            """
        )
    )
    key_to_ids: dict[str, list[int]] = {}
    id_to_key: dict[int, str] = {}
    for row in cat_rows.all():
        id_to_key[int(row.category_id)] = row.category_key
        key_to_ids.setdefault(row.category_key, []).append(int(row.category_id))

    # Package -> category ids
    pkg_rows = await db.execute(
        text(
            """
            SELECT package_id, category_id
            FROM assessment_package_categories
            """
        )
    )
    package_category_ids: dict[int, list[int]] = {}
    for row in pkg_rows.all():
        package_category_ids.setdefault(int(row.package_id), []).append(int(row.category_id))

    where_eng = "AND ai.engagement_id = :eid" if engagement_id is not None else ""
    params: dict = {}
    if engagement_id is not None:
        params["eid"] = engagement_id

    instances = await db.execute(
        text(
            f"""
            SELECT ai.assessment_instance_id,
                   ai.user_id,
                   ai.package_id,
                   ai.status,
                   ai.metsights_record_id,
                   ap.assessment_type_code
            FROM assessment_instances ai
            JOIN assessment_packages ap ON ap.package_id = ai.package_id
            WHERE ai.metsights_record_id IS NOT NULL
              AND ai.metsights_record_id != ''
              {where_eng}
            ORDER BY ai.assessment_instance_id
            """
        ),
        params,
    )
    instance_rows = instances.all()
    logger.info("  Instances with MetSights record to process: %d", len(instance_rows))

    records_fetched = 0
    records_failed = 0
    categories_marked = 0
    categories_imported = 0
    instances_completed = 0
    skipped_already_synced = 0

    for i, inst in enumerate(instance_rows, start=1):
        iid = int(inst.assessment_instance_id)
        uid = int(inst.user_id)
        pid = int(inst.package_id)
        mrid = (inst.metsights_record_id or "").strip()
        type_code = (inst.assessment_type_code or "").strip()
        pkg_cat_ids = package_category_ids.get(pid, [])
        pkg_keys = {id_to_key[cid] for cid in pkg_cat_ids if cid in id_to_key}

        # Skip if every package metsights category already submitted
        existing = await db.execute(
            text(
                """
                SELECT qc.category_key, acp.status, acp.is_submitted
                FROM assessment_category_progress acp
                JOIN questionnaire_categories qc ON qc.category_id = acp.category_id
                WHERE acp.assessment_instance_id = :iid
                  AND qc.category_of = 'metsights'
                """
            ),
            {"iid": iid},
        )
        existing_map = {
            row.category_key: (row.status, bool(row.is_submitted)) for row in existing.all()
        }
        metsights_pkg_keys = [
            id_to_key[cid]
            for cid in pkg_cat_ids
            if cid in id_to_key and id_to_key[cid] in {
                "physical-measurement",
                "vitals",
                "diet-lifestyle-parameters",
                "blood-parameters",
                "advanced-blood-parameters",
                "fitness-parameters",
            }
        ]
        if metsights_pkg_keys and all(
            existing_map.get(k, (None, False))[1] for k in metsights_pkg_keys
        ):
            skipped_already_synced += 1
            continue

        try:
            record = await _call_metsights_with_retry(
                f"GET /records/{mrid}/",
                lambda: metsights_service.get_record_detail(record_id=mrid),
            )
            records_fetched += 1
        except Exception as exc:
            records_failed += 1
            logger.warning("  record=%s instance=%s fetch failed: %s", mrid, iid, exc)
            continue

        if not isinstance(record, dict):
            records_failed += 1
            continue

        top_complete = bool(record.get("is_complete"))
        # Categories MetSights reports complete via nested sub-resources
        complete_keys: set[str] = set()
        # Categories that actually have nested payload data to import
        importable_keys: set[str] = set()

        if type_code == "7":
            sub_names = ("fitness_parameter",)
        else:
            sub_names = (
                "physical_measurement",
                "vital_parameter",
                "diet_lifestyle_parameter",
                "blood_parameter",
                "advanced_blood_parameter",
            )

        for sub_name in sub_names:
            sub = record.get(sub_name)
            if isinstance(sub, dict) and sub.get("is_complete"):
                for ck in _METSIGHTS_SUBRESOURCE_TO_CATEGORY_KEYS.get(sub_name, []):
                    complete_keys.add(ck)
            if isinstance(sub, dict) and any(
                k not in {"is_complete", "id", "created_at", "updated_at", "uuid"} and sub.get(k) not in (None, "", [])
                for k in sub.keys()
            ):
                for ck in _METSIGHTS_SUBRESOURCE_TO_CATEGORY_KEYS.get(sub_name, []):
                    # Only import the metsights category that maps 1:1 to this subresource
                    if _CATEGORY_KEY_TO_DETAIL_FIELD.get(ck) == sub_name:
                        importable_keys.add(ck)

        if top_complete:
            # Mark all package categories submitted, but only import when nested data exists
            complete_keys.update(k for k in pkg_keys if k in _CATEGORY_KEY_TO_DETAIL_FIELD)

        # Import only metsights categories that have nested payload data
        for ck in sorted(importable_keys | (complete_keys & set(_CATEGORY_KEY_TO_DETAIL_FIELD))):
            if ck not in pkg_keys:
                continue
            detail_field = _CATEGORY_KEY_TO_DETAIL_FIELD.get(ck)
            nested = record.get(detail_field) if detail_field else None
            if not isinstance(nested, dict):
                # e.g. advanced-blood-parameters often absent — mark only, don't import
                for cid in pkg_cat_ids:
                    if id_to_key.get(cid) == ck:
                        await _ensure_category_progress(
                            db,
                            assessment_instance_id=iid,
                            category_id=cid,
                            status="complete",
                            is_submitted=ck in complete_keys or top_complete,
                            dry_run=dry_run,
                        )
                        categories_marked += 1
                continue

            cat_row = await db.execute(
                text(
                    """
                    SELECT category_id, category_of
                    FROM questionnaire_categories
                    WHERE category_key = :ck AND category_of = 'metsights'
                    LIMIT 1
                    """
                ),
                {"ck": ck},
            )
            crow = cat_row.first()
            if crow is None:
                continue

            if not dry_run:
                try:
                    result = await _call_metsights_with_retry(
                        f"import {ck} for {mrid}",
                        lambda ck=ck: sync_service.import_category_from_metsights(
                            db,
                            assessment_instance_id=iid,
                            user_id=uid,
                            category_key=ck,
                            category_of="metsights",
                            reload=0,
                            employee_ok=True,
                            record_detail=record,
                        ),
                    )
                    categories_imported += int(result.get("responses_imported") or 0)
                except Exception as exc:
                    logger.warning(
                        "  import failed instance=%s category=%s: %s", iid, ck, exc
                    )

            for cid in pkg_cat_ids:
                if id_to_key.get(cid) != ck:
                    continue
                await _ensure_category_progress(
                    db,
                    assessment_instance_id=iid,
                    category_id=cid,
                    status="complete",
                    is_submitted=True,
                    dry_run=dry_run,
                )
                categories_marked += 1

            # Also mark related supershyft keys from the mapping
            for related_ck in _METSIGHTS_SUBRESOURCE_TO_CATEGORY_KEYS.get(
                _CATEGORY_KEY_TO_DETAIL_FIELD.get(ck, ""), []
            ):
                if related_ck == ck:
                    continue
                for cid in pkg_cat_ids:
                    if id_to_key.get(cid) == related_ck:
                        await _ensure_category_progress(
                            db,
                            assessment_instance_id=iid,
                            category_id=cid,
                            status="complete",
                            is_submitted=True,
                            dry_run=dry_run,
                        )
                        categories_marked += 1

        if top_complete or (metsights_pkg_keys and all(k in complete_keys for k in metsights_pkg_keys)):
            for cid in pkg_cat_ids:
                await _ensure_category_progress(
                    db,
                    assessment_instance_id=iid,
                    category_id=cid,
                    status="complete",
                    is_submitted=True,
                    dry_run=dry_run,
                )
                categories_marked += 1
            if not dry_run and (inst.status or "").lower() != "completed":
                await db.execute(
                    text(
                        """
                        UPDATE assessment_instances
                        SET status = 'completed', completed_at = COALESCE(completed_at, NOW())
                        WHERE assessment_instance_id = :iid
                        """
                    ),
                    {"iid": iid},
                )
                instances_completed += 1

        if i % _MS_SYNC_FLUSH_EVERY == 0:
            if not dry_run:
                await db.flush()
                await db.commit()
            logger.info(
                "  Processed %d / %d records (fetched=%d failed=%d imported_responses=%d marked=%d)",
                i,
                len(instance_rows),
                records_fetched,
                records_failed,
                categories_imported,
                categories_marked,
            )

    if not dry_run:
        await db.flush()

    # Local completeness: create/update metsights progress from required answers
    logger.info("  Local completeness pass for required questions…")
    local_complete = 0
    local_sql = text(
        f"""
        WITH pkg AS (
            SELECT ai.assessment_instance_id, ai.package_id, apc.category_id
            FROM assessment_instances ai
            JOIN assessment_package_categories apc ON apc.package_id = ai.package_id
            JOIN questionnaire_categories qc ON qc.category_id = apc.category_id
            WHERE qc.category_of = 'metsights'
            {('AND ai.engagement_id = :eid' if engagement_id is not None else '')}
        ),
        required AS (
            SELECT pkg.assessment_instance_id, pkg.category_id, qcq.question_id
            FROM pkg
            JOIN questionnaire_category_questions qcq ON qcq.category_id = pkg.category_id
            JOIN questionnaire_definitions qd ON qd.question_id = qcq.question_id
            WHERE qd.is_required = true
        ),
        answered AS (
            SELECT r.assessment_instance_id, r.category_id, r.question_id
            FROM required r
            JOIN questionnaire_responses qr
              ON qr.assessment_instance_id = r.assessment_instance_id
             AND qr.question_id = r.question_id
             AND qr.category_ids @> ARRAY[r.category_id]
        ),
        completeness AS (
            SELECT r.assessment_instance_id, r.category_id,
                   COUNT(*) AS required_count,
                   COUNT(a.question_id) AS answered_count
            FROM required r
            LEFT JOIN answered a
              ON a.assessment_instance_id = r.assessment_instance_id
             AND a.category_id = r.category_id
             AND a.question_id = r.question_id
            GROUP BY r.assessment_instance_id, r.category_id
        )
        SELECT assessment_instance_id, category_id
        FROM completeness
        WHERE required_count > 0 AND required_count = answered_count
        """
    )
    local_rows = await db.execute(local_sql, params if engagement_id is not None else {})
    for row in local_rows.all():
        await _ensure_category_progress(
            db,
            assessment_instance_id=int(row.assessment_instance_id),
            category_id=int(row.category_id),
            status="complete",
            is_submitted=False,  # don't force submitted from local-only
            dry_run=dry_run,
        )
        local_complete += 1

    if not dry_run:
        await db.flush()

    logger.info(
        "  Step 2 complete: fetched=%d failed=%d imported_responses=%d "
        "marked=%d local_complete=%d instances_completed=%d skipped_synced=%d",
        records_fetched,
        records_failed,
        categories_imported,
        categories_marked,
        local_complete,
        instances_completed,
        skipped_already_synced,
    )
    return {
        "completed_fast_path": completed_fast,
        "records_fetched": records_fetched,
        "records_failed": records_failed,
        "responses_imported": categories_imported,
        "categories_marked": categories_marked,
        "local_complete_marked": local_complete,
        "instances_completed": instances_completed,
        "skipped_already_synced": skipped_already_synced,
    }


async def _step3_dedup_engagement_participants(db: AsyncSession, *, dry_run: bool) -> dict:
    """Find duplicate (engagement_id, user_id) rows and keep the one with most data."""

    logger.info("Step 3: Dedup engagement_participants")

    result = await db.execute(text("""
        SELECT engagement_id, user_id, COUNT(*) as cnt,
               array_agg(engagement_participant_id ORDER BY engagement_participant_id) as ids
        FROM engagement_participants
        GROUP BY engagement_id, user_id
        HAVING COUNT(*) > 1
        ORDER BY engagement_id, user_id
    """))
    dup_groups = result.all()
    logger.info("  Found %d duplicate groups", len(dup_groups))

    if not dup_groups:
        return {"duplicate_groups": 0, "rows_deleted": 0, "skipped": 0}

    deleted_total = 0
    skipped = 0

    for group in dup_groups:
        eid = int(group.engagement_id)
        uid = int(group.user_id)
        ep_ids = [int(x) for x in group.ids]

        # Count related data for each duplicate
        scores: dict[int, int] = {}
        for ep_id in ep_ids:
            # Count assessment instances
            ai_res = await db.execute(text("""
                SELECT COUNT(*) FROM assessment_instances
                WHERE engagement_id = :eid AND user_id = :uid
            """), {"eid": eid, "uid": uid})
            ai_count = int(ai_res.scalar_one())

            # Count consultation bookings for this specific ep
            cb_res = await db.execute(text("""
                SELECT COUNT(*) FROM consultation_bookings
                WHERE engagement_participant_id = :epid
            """), {"epid": ep_id})
            cb_count = int(cb_res.scalar_one())

            scores[ep_id] = ai_count + cb_count

        # Keep the one with the highest score, ties broken by highest ID
        keep_id = max(ep_ids, key=lambda x: (scores.get(x, 0), x))
        delete_ids = [x for x in ep_ids if x != keep_id]

        # Re-point consultation bookings from deleted rows to kept row.
        # Skip / drop bookings that would collide on (participant, expert_type).
        for del_id in delete_ids:
            if not dry_run:
                await db.execute(text("""
                    DELETE FROM consultation_bookings cb
                    WHERE cb.engagement_participant_id = :del_id
                      AND EXISTS (
                          SELECT 1 FROM consultation_bookings keep_cb
                          WHERE keep_cb.engagement_participant_id = :keep_id
                            AND keep_cb.expert_type = cb.expert_type
                      )
                """), {"keep_id": keep_id, "del_id": del_id})
                await db.execute(text("""
                    UPDATE consultation_bookings
                    SET engagement_participant_id = :keep_id
                    WHERE engagement_participant_id = :del_id
                """), {"keep_id": keep_id, "del_id": del_id})

        # Delete duplicates
        if not dry_run:
            await db.execute(text("""
                DELETE FROM engagement_participants
                WHERE engagement_participant_id = ANY(:ids)
            """), {"ids": delete_ids})

        deleted_total += len(delete_ids)
        logger.info(
            "  engagement_id=%d user_id=%d: kept ep_id=%d, deleted %d",
            eid, uid, keep_id, len(delete_ids),
        )

    if not dry_run:
        await db.flush()

    logger.info("  Step 3 complete: %d groups, %d rows deleted, %d skipped", len(dup_groups), deleted_total, skipped)
    return {
        "duplicate_groups": len(dup_groups),
        "rows_deleted": deleted_total,
        "skipped": skipped,
    }


async def run_backfill(
    db: AsyncSession,
    *,
    dry_run: bool,
    metsights_service,
    sync_service,
    engagement_id: int | None = None,
) -> dict:
    """Run all three backfill steps in order."""
    results: dict = {}

    results["step1_category_ids"] = await _step1_backfill_category_ids(db, dry_run=dry_run)
    results["step2_is_submitted"] = await _step2_backfill_is_submitted(
        db,
        dry_run=dry_run,
        metsights_service=metsights_service,
        sync_service=sync_service,
        engagement_id=engagement_id,
    )
    results["step3_dedup_participants"] = await _step3_dedup_engagement_participants(db, dry_run=dry_run)

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Schema upgrade backfill")
    parser.add_argument("--yes", action="store_true", help="Actually apply changes (default is dry-run)")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without applying")
    parser.add_argument(
        "--engagement-id",
        type=int,
        default=None,
        help="Optional: limit MetSights sync (step 2) to one engagement",
    )
    args = parser.parse_args()

    dry_run = not args.yes or args.dry_run

    if dry_run:
        logger.info("DRY RUN mode — no changes will be committed")
    else:
        logger.info("LIVE mode — changes will be committed")
    if args.engagement_id is not None:
        logger.info("Limiting MetSights sync to engagement_id=%s", args.engagement_id)

    async def _run() -> dict:
        from modules.assessments.dependencies import get_assessments_service
        from modules.engagements.dependencies import get_engagements_service
        from modules.metsights.client import MetsightsClient
        from modules.metsights.service import MetsightsService
        from modules.metsights.sync_service import MetsightsSyncService
        from modules.platform_settings.dependencies import get_platform_settings_service_readonly
        from modules.questionnaire.repository import QuestionnaireRepository
        from modules.users.repository import UsersRepository

        settings.validate()
        engine = create_async_engine(settings.DATABASE_URL, echo=False, pool_pre_ping=True)
        session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)

        metsights_service = MetsightsService(client=MetsightsClient())
        sync_service = MetsightsSyncService(
            metsights_service=metsights_service,
            users_repository=UsersRepository(),
            engagements_service=get_engagements_service(),
            assessments_service=get_assessments_service(),
            platform_settings_service=get_platform_settings_service_readonly(),
            questionnaire_repository=QuestionnaireRepository(),
        )

        async with session_factory() as db:
            try:
                results = await run_backfill(
                    db,
                    dry_run=dry_run,
                    metsights_service=metsights_service,
                    sync_service=sync_service,
                    engagement_id=args.engagement_id,
                )
                if not dry_run:
                    await db.commit()
                    logger.info("All changes committed.")
                else:
                    await db.rollback()
                    logger.info("Dry run complete — rolled back.")
                return results
            except Exception:
                await db.rollback()
                logger.exception("Backfill failed, rolled back")
                raise
            finally:
                await engine.dispose()

    results = asyncio.run(_run())

    logger.info("=== RESULTS ===")
    for step_name, step_result in results.items():
        logger.info("  %s: %s", step_name, step_result)

    sys.exit(0)
