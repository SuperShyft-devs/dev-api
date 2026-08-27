"""Nvidia load_prev questionnaire cleanup (dry-run by default).

Fixes bad side-effects from ``load_prev_assessment_questionnaires`` on the 8
NVIDIA 2026 engagements before the vitals/blood copy skip was deployed.

Phases
------
A. Delete clinical (vitals / health_vitals / blood-parameters /
   advanced-blood-parameters) responses + reset matching category progress.
B. Estimate unreviewed non-clinical answers from journalctl PUT counts
   (one Next ≈ one PUT for the mobile wizard) and propose deleting answers
   beyond the estimated review frontier; then re-sync category progress.

Safety
------
* Default mode is ``--dry-run``: writes reports only, never mutates.
* Mutation requires both ``--execute`` and ``--i-approve-deletes``.
* DB session is READ ONLY unless execute is approved.

Examples
--------
    python -m db.jobs.nvidia_load_prev_cleanup --dry-run \\
        --report-dir /tmp/nvidia_load_prev_cleanup

    # after reviewing the report:
    python -m db.jobs.nvidia_load_prev_cleanup --execute --i-approve-deletes \\
        --report-dir /tmp/nvidia_load_prev_cleanup \\
        --put-counts-file /tmp/nvidia_put_counts.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import sys
import time
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

ENGAGEMENT_CODES = (
    "NFGU0926",
    "NMGU0926",
    "NFHY0926",
    "NMHY0926",
    "NFBA0826",
    "NMBA0826",
    "NFPU0926",
    "NMPU0926",
)

CLINICAL_CATEGORY_KEYS = (
    "vitals",
    "health_vitals",
    "blood-parameters",
    "advanced-blood-parameters",
)

PUT_RE = re.compile(
    r'PUT /questionnaire/(?P<aid>\d+)/category/(?P<cid>\d+)/responses'
)


@dataclass
class ProgressBar:
    total: int
    label: str
    started_at: float = field(default_factory=time.monotonic)
    done: int = 0
    _last_render_at: float = field(default_factory=time.monotonic)

    def update(self, n: int = 1) -> None:
        self.done = min(self.total, self.done + n)
        self._render(force=False)

    def set(self, value: int) -> None:
        self.done = min(self.total, max(0, value))
        self._render(force=False)

    def _render(self, *, force: bool) -> None:
        total = max(self.total, 1)
        now = time.monotonic()
        finished = self.done >= self.total
        if not force and not finished and (now - self._last_render_at) < 0.25 and self.done % 250 != 0:
            return
        self._last_render_at = now
        pct = 100.0 * self.done / total
        elapsed = max(now - self.started_at, 0.001)
        rate = self.done / elapsed
        remaining = (total - self.done) / rate if rate > 0 else 0.0
        bar_len = 28
        filled = int(bar_len * self.done / total)
        bar = "#" * filled + "-" * (bar_len - filled)
        msg = (
            f"\r{self.label}: [{bar}] {pct:5.1f}% "
            f"({self.done}/{self.total}) "
            f"elapsed {elapsed:6.1f}s ETA {remaining:6.1f}s"
        )
        sys.stdout.write(msg)
        sys.stdout.flush()
        if finished:
            sys.stdout.write("\n")
            sys.stdout.flush()


@dataclass
class PlannedDelete:
    response_id: int
    assessment_instance_id: int
    user_id: int
    engagement_code: str
    question_id: int
    question_key: str | None
    category_ids: list[int]
    category_keys: list[str]
    reason: str
    phase: str
    put_count: int | None = None
    reviewed_estimate: int | None = None
    question_display_order: int | None = None


@dataclass
class PlannedProgressReset:
    progress_id: int
    assessment_instance_id: int
    category_id: int
    category_key: str
    engagement_code: str
    old_status: str
    new_status: str
    reason: str
    phase: str


def _load_env() -> None:
    # Prefer live API env when running on the server.
    for candidate in (
        Path("/var/www/backend/api/.env"),
        Path(".env"),
        Path(__file__).resolve().parents[3] / ".env",
    ):
        if candidate.is_file():
            load_dotenv(candidate, override=False)
            return


async def _connect():
    import asyncpg
    from urllib.parse import urlparse
    import os

    url = os.environ.get("DATABASE_URL", "")
    if not url:
        raise SystemExit("DATABASE_URL is not set")
    url = url.replace("postgresql+asyncpg://", "postgresql://").replace(
        "postgresql+psycopg://", "postgresql://"
    )
    parsed = urlparse(url)
    logger.info(
        "Connecting to DB host=%s db=%s user=%s",
        parsed.hostname,
        (parsed.path or "/").lstrip("/"),
        parsed.username,
    )
    return await asyncpg.connect(url)


def parse_put_counts_from_journal_text(text: str) -> dict[tuple[int, int], int]:
    """Count PUT /questionnaire/{aid}/category/{cid}/responses occurrences."""
    counts: dict[tuple[int, int], int] = defaultdict(int)
    for match in PUT_RE.finditer(text):
        counts[(int(match.group("aid")), int(match.group("cid")))] += 1
    return dict(counts)


def parse_put_counts_unique_seconds(lines: list[str]) -> dict[tuple[int, int], int]:
    """Count at most one PUT per (aid, cid, timestamp-second) to dampen retries."""
    seen: set[tuple[int, int, str]] = set()
    counts: dict[tuple[int, int], int] = defaultdict(int)
    # journalctl lines start with: Aug 27 18:47:44 ...
    ts_re = re.compile(r"^([A-Z][a-z]{2}\s+\d+\s+\d{2}:\d{2}:\d{2})")
    for line in lines:
        m = PUT_RE.search(line)
        if not m:
            continue
        ts_m = ts_re.search(line)
        ts = ts_m.group(1) if ts_m else line[:20]
        key = (int(m.group("aid")), int(m.group("cid")), ts)
        if key in seen:
            continue
        seen.add(key)
        counts[(key[0], key[1])] += 1
    return dict(counts)


async def fetch_scope(conn) -> dict[str, Any]:
    eng_rows = await conn.fetch(
        """
        SELECT engagement_id, engagement_code, engagement_name,
               load_prev_assessment_questionnaires, enroll_for_fitprint_full,
               assessment_package_id
        FROM engagements
        WHERE engagement_code = ANY($1::text[])
        ORDER BY engagement_code
        """,
        list(ENGAGEMENT_CODES),
    )
    eng_ids = [r["engagement_id"] for r in eng_rows]
    eng_by_id = {r["engagement_id"]: r for r in eng_rows}

    cat_rows = await conn.fetch(
        """
        SELECT category_id, category_key, display_name
        FROM questionnaire_categories
        WHERE category_key = ANY($1::text[])
           OR category_id IN (
             SELECT DISTINCT apc.category_id
             FROM assessment_package_categories apc
             JOIN assessment_instances ai ON ai.package_id = apc.package_id
             WHERE ai.engagement_id = ANY($2::int[])
           )
        ORDER BY category_id
        """,
        list(CLINICAL_CATEGORY_KEYS),
        eng_ids,
    )
    cat_by_id = {r["category_id"]: r for r in cat_rows}
    clinical_ids = {
        r["category_id"]
        for r in cat_rows
        if r["category_key"] in CLINICAL_CATEGORY_KEYS
    }

    load_prev_instances = await conn.fetch(
        """
        SELECT DISTINCT
            ai.assessment_instance_id,
            ai.user_id,
            ai.engagement_id,
            ai.package_id,
            ai.assigned_at,
            e.engagement_code
        FROM data_audit_logs dal
        JOIN assessment_instances ai ON ai.user_id = dal.user_id
        JOIN engagements e ON e.engagement_id = ai.engagement_id
        WHERE dal.action = 'USER_ENGAGEMENT_ONBOARD_LOAD_PREV_QUESTIONNAIRES'
          AND ai.engagement_id = ANY($1::int[])
          AND ai.package_id = 2
          AND dal.timestamp >= ai.assigned_at - INTERVAL '1 hour'
          AND dal.timestamp <= ai.assigned_at + INTERVAL '1 hour'
        ORDER BY ai.assessment_instance_id
        """,
        eng_ids,
    )

    return {
        "engagements": [dict(r) for r in eng_rows],
        "eng_ids": eng_ids,
        "eng_by_id": eng_by_id,
        "cat_by_id": cat_by_id,
        "clinical_ids": clinical_ids,
        "load_prev_instances": [dict(r) for r in load_prev_instances],
    }


async def plan_phase_a_clinical(conn, scope: dict[str, Any]) -> tuple[list[PlannedDelete], list[PlannedProgressReset]]:
    eng_ids = scope["eng_ids"]
    clinical_keys = list(CLINICAL_CATEGORY_KEYS)

    rows = await conn.fetch(
        """
        SELECT
            qr.response_id,
            qr.assessment_instance_id,
            qr.question_id,
            qr.category_ids,
            qd.question_key,
            ai.user_id,
            e.engagement_code,
            ARRAY(
              SELECT qc.category_key
              FROM questionnaire_categories qc
              WHERE qc.category_id = ANY(qr.category_ids)
                AND qc.category_key = ANY($2::text[])
            ) AS matched_clinical_keys
        FROM questionnaire_responses qr
        JOIN assessment_instances ai ON ai.assessment_instance_id = qr.assessment_instance_id
        JOIN engagements e ON e.engagement_id = ai.engagement_id
        LEFT JOIN questionnaire_definitions qd ON qd.question_id = qr.question_id
        WHERE ai.engagement_id = ANY($1::int[])
          AND EXISTS (
            SELECT 1 FROM questionnaire_categories qc
            WHERE qc.category_id = ANY(qr.category_ids)
              AND qc.category_key = ANY($2::text[])
          )
        ORDER BY qr.response_id
        """,
        eng_ids,
        clinical_keys,
    )

    deletes: list[PlannedDelete] = []
    bar = ProgressBar(total=len(rows), label="Phase A plan responses")
    for row in rows:
        deletes.append(
            PlannedDelete(
                response_id=int(row["response_id"]),
                assessment_instance_id=int(row["assessment_instance_id"]),
                user_id=int(row["user_id"]),
                engagement_code=row["engagement_code"],
                question_id=int(row["question_id"]),
                question_key=row["question_key"],
                category_ids=list(row["category_ids"] or []),
                category_keys=list(row["matched_clinical_keys"] or []),
                reason=(
                    "Clinical measurement category must be captured at camp day; "
                    "answer was copied via load_prev_assessment_questionnaires "
                    f"(categories={list(row['matched_clinical_keys'] or [])})."
                ),
                phase="A_clinical",
            )
        )
        bar.update(1)

    progress_rows = await conn.fetch(
        """
        SELECT
            acp.id AS progress_id,
            acp.assessment_instance_id,
            acp.category_id,
            acp.status,
            qc.category_key,
            e.engagement_code
        FROM assessment_category_progress acp
        JOIN assessment_instances ai ON ai.assessment_instance_id = acp.assessment_instance_id
        JOIN engagements e ON e.engagement_id = ai.engagement_id
        JOIN questionnaire_categories qc ON qc.category_id = acp.category_id
        WHERE ai.engagement_id = ANY($1::int[])
          AND qc.category_key = ANY($2::text[])
          AND lower(acp.status) = 'complete'
        ORDER BY acp.id
        """,
        eng_ids,
        clinical_keys,
    )

    resets: list[PlannedProgressReset] = []
    bar2 = ProgressBar(total=len(progress_rows), label="Phase A plan progress")
    for row in progress_rows:
        resets.append(
            PlannedProgressReset(
                progress_id=int(row["progress_id"]),
                assessment_instance_id=int(row["assessment_instance_id"]),
                category_id=int(row["category_id"]),
                category_key=row["category_key"],
                engagement_code=row["engagement_code"],
                old_status=row["status"],
                new_status="incomplete",
                reason=(
                    "Category was auto-marked complete after load_prev copied "
                    f"clinical answers ({row['category_key']}); reset after wiping those answers."
                ),
                phase="A_clinical",
            )
        )
        bar2.update(1)

    return deletes, resets


async def plan_phase_b_unreviewed(
    conn,
    scope: dict[str, Any],
    put_counts_raw: dict[tuple[int, int], int],
    put_counts_unique: dict[tuple[int, int], int],
    *,
    count_mode: str = "unique_second",
) -> tuple[list[PlannedDelete], list[PlannedProgressReset]]:
    """Estimate review frontier from PUT counts; plan deletes for answers beyond it.

    NVIDIA enrollment used an external form that sends exactly one question per
    PUT /questionnaire/{id}/category/{cid}/responses. PUT count for
    (assessment_instance_id, category_id) therefore approximates how many
    questions were reviewed in that category (display_order order).
    """
    instances = scope["load_prev_instances"]
    if not instances:
        return [], []

    instance_ids = [int(r["assessment_instance_id"]) for r in instances]
    clinical_ids = scope["clinical_ids"]
    cat_by_id = scope["cat_by_id"]

    # Active questions per category ordered by display_order
    q_rows = await conn.fetch(
        """
        SELECT qcq.category_id, qcq.question_id, qcq.display_order,
               qd.question_key, qd.is_required, qd.status
        FROM questionnaire_category_questions qcq
        JOIN questionnaire_definitions qd ON qd.question_id = qcq.question_id
        WHERE qcq.category_id = ANY($1::int[])
        ORDER BY qcq.category_id, qcq.display_order NULLS LAST, qcq.question_id
        """,
        list(cat_by_id.keys()),
    )
    questions_by_cat: dict[int, list[dict]] = defaultdict(list)
    for row in q_rows:
        if (row["status"] or "").lower() != "active":
            continue
        questions_by_cat[int(row["category_id"])].append(dict(row))

    # Non-clinical responses on load_prev instances
    resp_rows = await conn.fetch(
        """
        SELECT
            qr.response_id,
            qr.assessment_instance_id,
            qr.question_id,
            qr.category_ids,
            qd.question_key,
            ai.user_id,
            e.engagement_code
        FROM questionnaire_responses qr
        JOIN assessment_instances ai ON ai.assessment_instance_id = qr.assessment_instance_id
        JOIN engagements e ON e.engagement_id = ai.engagement_id
        LEFT JOIN questionnaire_definitions qd ON qd.question_id = qr.question_id
        WHERE qr.assessment_instance_id = ANY($1::int[])
          AND NOT EXISTS (
            SELECT 1 FROM unnest(qr.category_ids) AS cid
            WHERE cid = ANY($2::int[])
          )
        ORDER BY qr.assessment_instance_id, qr.question_id
        """,
        instance_ids,
        list(clinical_ids),
    )

    counts = put_counts_unique if count_mode == "unique_second" else put_counts_raw

    deletes: list[PlannedDelete] = []
    delete_ids: set[int] = set()
    required_by_cat: dict[int, set[int]] = {
        cid: {int(q["question_id"]) for q in qs if q.get("is_required")}
        for cid, qs in questions_by_cat.items()
        if cid not in clinical_ids
    }

    bar = ProgressBar(total=len(resp_rows), label="Phase B plan responses")
    # A response may belong to multiple non-clinical categories (e.g. anthropometry
    # mirrored on supershyft + metsights). External form PUTs hit one category_id
    # with one question each; use the MAX put count across owning categories as
    # the review frontier.
    for row in resp_rows:
        aid = int(row["assessment_instance_id"])
        qid = int(row["question_id"])
        cats = [int(c) for c in (row["category_ids"] or []) if int(c) not in clinical_ids]
        if not cats:
            bar.update(1)
            continue

        put_by_cat = {cid: int(counts.get((aid, cid), 0)) for cid in cats}
        put_count = max(put_by_cat.values()) if put_by_cat else 0
        reviewed_estimate = put_count
        primary_cid = max(cats, key=lambda c: (put_by_cat.get(c, 0), -c))
        ordered = questions_by_cat.get(primary_cid, [])
        order_index = next(
            (i for i, q in enumerate(ordered) if int(q["question_id"]) == qid),
            None,
        )
        if order_index is None:
            # Fall back to any owning category's order
            for cid in cats:
                ordered = questions_by_cat.get(cid, [])
                order_index = next(
                    (i for i, q in enumerate(ordered) if int(q["question_id"]) == qid),
                    None,
                )
                if order_index is not None:
                    primary_cid = cid
                    break

        unreviewed = False
        detail = ""
        if order_index is None:
            unreviewed = True
            detail = (
                f"question not in active ordered list for cats={cats} "
                f"(puts={put_by_cat})"
            )
        elif order_index >= reviewed_estimate:
            unreviewed = True
            cat_key = (cat_by_id.get(primary_cid) or {}).get("category_key", str(primary_cid))
            detail = (
                f"{cat_key}: order_index={order_index} >= reviewed_estimate={reviewed_estimate} "
                f"(puts/{count_mode}={put_by_cat})"
            )

        if unreviewed:
            delete_ids.add(int(row["response_id"]))
            deletes.append(
                PlannedDelete(
                    response_id=int(row["response_id"]),
                    assessment_instance_id=aid,
                    user_id=int(row["user_id"]),
                    engagement_code=row["engagement_code"],
                    question_id=qid,
                    question_key=row["question_key"],
                    category_ids=cats,
                    category_keys=[
                        (cat_by_id.get(c) or {}).get("category_key", str(c)) for c in cats
                    ],
                    reason=(
                        "Estimated unreviewed after load_prev copy. "
                        "External form: one question per PUT; max PUT count across "
                        "owning categories ≈ questions reviewed in display order; "
                        "keep only answers before the review frontier. "
                        + detail
                    ),
                    phase="B_unreviewed",
                    put_count=put_count,
                    reviewed_estimate=reviewed_estimate,
                    question_display_order=order_index,
                )
            )
        bar.update(1)

    # Survivors after Phase B deletes → remaining answered question ids
    remaining_answered: dict[tuple[int, int], set[int]] = defaultdict(set)
    for row in resp_rows:
        if int(row["response_id"]) in delete_ids:
            continue
        aid = int(row["assessment_instance_id"])
        qid = int(row["question_id"])
        for cid in [int(c) for c in (row["category_ids"] or []) if int(c) not in clinical_ids]:
            remaining_answered[(aid, cid)].add(qid)

    # Progress resets: for each (instance, non-clinical category) that is complete,
    # mark incomplete if not all required questions remain answered.
    progress_rows = await conn.fetch(
        """
        SELECT
            acp.id AS progress_id,
            acp.assessment_instance_id,
            acp.category_id,
            acp.status,
            qc.category_key,
            e.engagement_code
        FROM assessment_category_progress acp
        JOIN assessment_instances ai ON ai.assessment_instance_id = acp.assessment_instance_id
        JOIN engagements e ON e.engagement_id = ai.engagement_id
        JOIN questionnaire_categories qc ON qc.category_id = acp.category_id
        WHERE ai.assessment_instance_id = ANY($1::int[])
          AND NOT (acp.category_id = ANY($2::int[]))
          AND lower(acp.status) = 'complete'
        ORDER BY acp.id
        """,
        instance_ids,
        list(clinical_ids),
    )

    resets: list[PlannedProgressReset] = []
    bar2 = ProgressBar(total=len(progress_rows), label="Phase B plan progress")
    for row in progress_rows:
        aid = int(row["assessment_instance_id"])
        cid = int(row["category_id"])
        required = required_by_cat.get(cid, set())
        answered = remaining_answered.get((aid, cid), set())
        put_count = int(counts.get((aid, cid), 0))
        all_required_ok = bool(required) and required.issubset(answered)
        if required and not all_required_ok:
            resets.append(
                PlannedProgressReset(
                    progress_id=int(row["progress_id"]),
                    assessment_instance_id=aid,
                    category_id=cid,
                    category_key=row["category_key"],
                    engagement_code=row["engagement_code"],
                    old_status=row["status"],
                    new_status="incomplete",
                    reason=(
                        f"After removing estimated-unreviewed answers, required questions "
                        f"no longer fully answered (puts={put_count}, "
                        f"required={len(required)}, still_answered_required="
                        f"{len(required & answered)})."
                    ),
                    phase="B_unreviewed",
                )
            )
        elif put_count == 0:
            # Complete with zero PUTs => never reviewed; force incomplete even if
            # answers remain (Phase B may keep none when put_count=0).
            resets.append(
                PlannedProgressReset(
                    progress_id=int(row["progress_id"]),
                    assessment_instance_id=aid,
                    category_id=cid,
                    category_key=row["category_key"],
                    engagement_code=row["engagement_code"],
                    old_status=row["status"],
                    new_status="incomplete",
                    reason=(
                        "Category marked complete after load_prev copy but journalctl "
                        "shows 0 PUT /responses for this instance/category (never reviewed)."
                    ),
                    phase="B_unreviewed",
                )
            )
        bar2.update(1)

    return deletes, resets


async def apply_plan(
    conn,
    deletes: list[PlannedDelete],
    resets: list[PlannedProgressReset],
) -> dict[str, int]:
    deleted = 0
    reset = 0
    bar = ProgressBar(total=len(deletes), label="Deleting responses")
    # Delete in chunks
    chunk = 500
    ids = [d.response_id for d in deletes]
    for i in range(0, len(ids), chunk):
        batch = ids[i : i + chunk]
        await conn.execute(
            "DELETE FROM questionnaire_responses WHERE response_id = ANY($1::int[])",
            batch,
        )
        deleted += len(batch)
        bar.set(deleted)

    bar2 = ProgressBar(total=len(resets), label="Resetting progress")
    for r in resets:
        await conn.execute(
            """
            UPDATE assessment_category_progress
            SET status = 'incomplete', completed_at = NULL
            WHERE id = $1
            """,
            r.progress_id,
        )
        reset += 1
        bar2.update(1)

    return {"deleted_responses": deleted, "reset_progress_rows": reset}


def write_reports(
    report_dir: Path,
    *,
    meta: dict[str, Any],
    phase_a_deletes: list[PlannedDelete],
    phase_a_resets: list[PlannedProgressReset],
    phase_b_deletes: list[PlannedDelete],
    phase_b_resets: list[PlannedProgressReset],
) -> None:
    report_dir.mkdir(parents=True, exist_ok=True)

    def dump_json(name: str, payload: Any) -> None:
        (report_dir / name).write_text(
            json.dumps(payload, indent=2, default=str), encoding="utf-8"
        )

    dump_json("00_meta.json", meta)
    dump_json("01_phase_a_response_deletes.json", [asdict(x) for x in phase_a_deletes])
    dump_json("02_phase_a_progress_resets.json", [asdict(x) for x in phase_a_resets])
    dump_json("03_phase_b_response_deletes.json", [asdict(x) for x in phase_b_deletes])
    dump_json("04_phase_b_progress_resets.json", [asdict(x) for x in phase_b_resets])

    # CSV summaries
    def write_csv(name: str, rows: list[dict[str, Any]], columns: list[str]) -> None:
        import csv

        path = report_dir / name
        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    write_csv(
        "phase_a_response_deletes.csv",
        [asdict(x) for x in phase_a_deletes],
        [
            "response_id",
            "assessment_instance_id",
            "user_id",
            "engagement_code",
            "question_id",
            "question_key",
            "category_keys",
            "reason",
            "phase",
        ],
    )
    write_csv(
        "phase_b_response_deletes.csv",
        [asdict(x) for x in phase_b_deletes],
        [
            "response_id",
            "assessment_instance_id",
            "user_id",
            "engagement_code",
            "question_id",
            "question_key",
            "category_keys",
            "put_count",
            "reviewed_estimate",
            "question_display_order",
            "reason",
            "phase",
        ],
    )

    # Markdown report
    by_eng_a: dict[str, int] = defaultdict(int)
    for d in phase_a_deletes:
        by_eng_a[d.engagement_code] += 1
    by_eng_b: dict[str, int] = defaultdict(int)
    for d in phase_b_deletes:
        by_eng_b[d.engagement_code] += 1
    by_cat_a: dict[str, int] = defaultdict(int)
    for d in phase_a_deletes:
        for k in d.category_keys:
            by_cat_a[k] += 1

    md = f"""# Nvidia load_prev questionnaire cleanup — APPROVAL REPORT

Generated (UTC): {meta.get("generated_at")}
Mode: **{meta.get("mode")}**
PUT count mode (Phase B): `{meta.get("put_count_mode")}`

## Scope

Engagements: {", ".join(ENGAGEMENT_CODES)}

| Metric | Value |
| --- | ---: |
| Load-prev Metsights Pro instances | {meta.get("load_prev_instance_count")} |
| Phase A response deletes | {len(phase_a_deletes)} |
| Phase A progress resets | {len(phase_a_resets)} |
| Phase B response deletes | {len(phase_b_deletes)} |
| Phase B progress resets | {len(phase_b_resets)} |
| Journal PUT events matched (raw) | {meta.get("put_events_raw")} |
| Journal PUT events (unique second) | {meta.get("put_events_unique")} |

## Problem → fix mapping

1. **Clinical answers copied** (vitals / blood) → Phase A deletes those
   `questionnaire_responses` and resets complete → incomplete on matching
   `assessment_category_progress`.
2. **Unknown review progress** → Phase B uses journalctl
   `PUT /questionnaire/{{id}}/category/{{cid}}/responses` counts. External form
   sends one question per PUT, so count ≈ questions reviewed in display order.
3. **Categories marked complete without review** → progress rows reset when
   required answers no longer remain after Phase B, or when PUT count is 0.

## Important heuristic notes (Phase B)

- External enrollment form: **one question per PUT** (confirmed for this incident).
- PUT count for `(instance, category)` ≈ questions reviewed (display order).
- Duplicate same-second PUTs (CDN retries) are dampened via `unique_second` mode.
- Still an estimate if users go back/re-save; prefer reviewing CSVs before `--execute`.

## Phase A by engagement

| Engagement | Planned response deletes |
| --- | ---: |
{chr(10).join(f"| {k} | {v} |" for k, v in sorted(by_eng_a.items())) or "| — | 0 |"}

## Phase A by clinical category key (row may count in multiple keys)

| Category key | Mentions on planned deletes |
| --- | ---: |
{chr(10).join(f"| {k} | {v} |" for k, v in sorted(by_cat_a.items())) or "| — | 0 |"}

## Phase B by engagement

| Engagement | Planned response deletes |
| --- | ---: |
{chr(10).join(f"| {k} | {v} |" for k, v in sorted(by_eng_b.items())) or "| — | 0 |"}

## Files in this report directory

- `00_meta.json`
- `01_phase_a_response_deletes.json` / `phase_a_response_deletes.csv`
- `02_phase_a_progress_resets.json`
- `03_phase_b_response_deletes.json` / `phase_b_response_deletes.csv`
- `04_phase_b_progress_resets.json`

## Execution (only after approval)

```bash
python -m db.jobs.nvidia_load_prev_cleanup --execute --i-approve-deletes \\
  --report-dir {report_dir} \\
  --put-counts-file {meta.get("put_counts_file") or "/path/to/put_counts.json"}
```

**WE DO NOT DELETE WITHOUT APPROVAL.**
"""
    (report_dir / "APPROVAL_REPORT.md").write_text(md, encoding="utf-8")
    logger.info("Wrote approval report to %s", report_dir / "APPROVAL_REPORT.md")


async def extract_journal_puts(
    *,
    since: str,
    instance_ids: set[int],
    out_file: Path | None,
) -> tuple[dict[tuple[int, int], int], dict[tuple[int, int], int], list[str]]:
    """Run journalctl and return raw + unique-second counts (filtered to instances)."""
    import subprocess

    cmd = [
        "sudo",
        "-n",
        "journalctl",
        "-u",
        "supershyft-api",
        "--since",
        since,
        "--no-pager",
    ]
    # Fallback: sudo with password not supported here; caller may pass --journal-file
    logger.info("Running: %s", " ".join(cmd))
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    except Exception as exc:
        raise SystemExit(f"Failed to run journalctl: {exc}") from exc

    if proc.returncode != 0:
        raise SystemExit(
            "journalctl failed (need passwordless sudo -n or pass --journal-file). "
            f"stderr={proc.stderr[:500]}"
        )

    lines = [
        ln
        for ln in proc.stdout.splitlines()
        if "PUT /questionnaire/" in ln and "/responses" in ln
    ]
    if out_file:
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

    raw_all = parse_put_counts_from_journal_text("\n".join(lines))
    uniq_all = parse_put_counts_unique_seconds(lines)

    raw = {k: v for k, v in raw_all.items() if k[0] in instance_ids}
    uniq = {k: v for k, v in uniq_all.items() if k[0] in instance_ids}
    return raw, uniq, lines


def load_put_counts_file(path: Path) -> tuple[dict[tuple[int, int], int], dict[tuple[int, int], int]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    raw = { (int(a), int(c)): int(v) for a, c, v in payload.get("raw", []) }
    uniq = { (int(a), int(c)): int(v) for a, c, v in payload.get("unique_second", []) }
    return raw, uniq


def save_put_counts_file(
    path: Path,
    raw: dict[tuple[int, int], int],
    uniq: dict[tuple[int, int], int],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "raw": [[a, c, v] for (a, c), v in sorted(raw.items())],
        "unique_second": [[a, c, v] for (a, c), v in sorted(uniq.items())],
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


async def async_main(args: argparse.Namespace) -> int:
    _load_env()
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)

    execute = bool(args.execute)
    if execute and not args.i_approve_deletes:
        raise SystemExit("Refusing to execute without --i-approve-deletes")
    if execute and args.dry_run:
        raise SystemExit("Pass either --dry-run or --execute, not both")
    if not execute:
        args.dry_run = True

    conn = await _connect()
    try:
        if args.dry_run:
            await conn.execute("BEGIN READ ONLY")
        else:
            await conn.execute("BEGIN")

        scope = await fetch_scope(conn)
        instance_ids = {
            int(r["assessment_instance_id"]) for r in scope["load_prev_instances"]
        }
        logger.info(
            "Scope: %s engagements, %s load_prev metsights instances",
            len(scope["eng_ids"]),
            len(instance_ids),
        )

        put_counts_file = Path(args.put_counts_file) if args.put_counts_file else report_dir / "put_counts.json"
        journal_extract = report_dir / "journal_puts_extract.log"

        if args.journal_file:
            text = Path(args.journal_file).read_text(encoding="utf-8", errors="replace")
            lines = text.splitlines()
            raw_all = parse_put_counts_from_journal_text(text)
            uniq_all = parse_put_counts_unique_seconds(lines)
            raw = {k: v for k, v in raw_all.items() if k[0] in instance_ids}
            uniq = {k: v for k, v in uniq_all.items() if k[0] in instance_ids}
        elif put_counts_file.is_file() and not args.refresh_puts:
            raw, uniq = load_put_counts_file(put_counts_file)
            logger.info("Loaded put counts from %s", put_counts_file)
        else:
            # Try reading a pre-exported file path first; else journalctl
            if args.skip_journal:
                raw, uniq = {}, {}
                logger.warning("Skipping journal; Phase B will treat all PUT counts as 0")
            else:
                raw, uniq, _lines = await extract_journal_puts(
                    since=args.since,
                    instance_ids=instance_ids,
                    out_file=journal_extract,
                )
            save_put_counts_file(put_counts_file, raw, uniq)

        phase_a_deletes, phase_a_resets = await plan_phase_a_clinical(conn, scope)
        phase_b_deletes, phase_b_resets = await plan_phase_b_unreviewed(
            conn,
            scope,
            raw,
            uniq,
            count_mode=args.put_count_mode,
        )

        meta = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "mode": "EXECUTE" if execute else "DRY_RUN",
            "engagement_codes": list(ENGAGEMENT_CODES),
            "load_prev_instance_count": len(instance_ids),
            "put_count_mode": args.put_count_mode,
            "put_counts_file": str(put_counts_file),
            "put_events_raw": sum(raw.values()),
            "put_events_unique": sum(uniq.values()),
            "phase_a_delete_count": len(phase_a_deletes),
            "phase_a_reset_count": len(phase_a_resets),
            "phase_b_delete_count": len(phase_b_deletes),
            "phase_b_reset_count": len(phase_b_resets),
            "fix_sha_skip_clinical_copy": "f9cee9d74ec264541ecbca3ea360e42af05b8fe8",
        }

        write_reports(
            report_dir,
            meta=meta,
            phase_a_deletes=phase_a_deletes,
            phase_a_resets=phase_a_resets,
            phase_b_deletes=phase_b_deletes,
            phase_b_resets=phase_b_resets,
        )

        if execute:
            all_deletes = phase_a_deletes + phase_b_deletes
            all_resets = phase_a_resets + phase_b_resets
            result = await apply_plan(conn, all_deletes, all_resets)
            await conn.execute("COMMIT")
            logger.info("EXECUTE complete: %s", result)
            (report_dir / "05_execute_result.json").write_text(
                json.dumps(result, indent=2), encoding="utf-8"
            )
        else:
            await conn.execute("ROLLBACK")
            logger.info(
                "DRY-RUN complete. Review %s then re-run with --execute --i-approve-deletes",
                report_dir / "APPROVAL_REPORT.md",
            )

        return 0
    except Exception:
        await conn.execute("ROLLBACK")
        raise
    finally:
        await conn.close()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dry-run", action="store_true", default=False, help="Plan only (default).")
    p.add_argument("--execute", action="store_true", help="Apply deletes (requires approval flag).")
    p.add_argument(
        "--i-approve-deletes",
        action="store_true",
        help="Explicit approval gate for --execute.",
    )
    p.add_argument(
        "--report-dir",
        default="/tmp/nvidia_load_prev_cleanup",
        help="Directory for approval report + CSV/JSON artifacts.",
    )
    p.add_argument("--since", default="28 hours ago", help="journalctl --since value.")
    p.add_argument("--journal-file", default=None, help="Use a pre-exported journal extract.")
    p.add_argument("--put-counts-file", default=None, help="Load/save PUT count JSON.")
    p.add_argument("--refresh-puts", action="store_true", help="Re-parse journal even if cache exists.")
    p.add_argument("--skip-journal", action="store_true", help="Phase B with zero PUT counts.")
    p.add_argument(
        "--put-count-mode",
        choices=("unique_second", "raw"),
        default="unique_second",
        help="How to count PUTs for review frontier.",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args(argv)
    return asyncio.run(async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
