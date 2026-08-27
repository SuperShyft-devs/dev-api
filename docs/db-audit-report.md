# PostgreSQL / DB Operations Audit Report

**Date:** 2026-08-27  
**Scope:** `dev-api` FastAPI + SQLAlchemy 2 async + asyncpg  
**Production target:** 4 vCPU, ~200 concurrent users, 4 Uvicorn workers

## A. Architecture

| Component | Implementation |
|-----------|----------------|
| Framework | FastAPI + Uvicorn (`main.py`) |
| ORM | SQLAlchemy 2.0 async |
| Driver | asyncpg (`postgresql+asyncpg://`) |
| Session DI | `db/session.py` `get_db()` — rollback on exit, no auto-commit |
| API pool | `DATABASE_POOL_SIZE=5`, `DATABASE_MAX_OVERFLOW=5`, `pool_timeout=10s` |
| Statement timeout | `DATABASE_STATEMENT_TIMEOUT_MS=30000` via `connect_args` |
| Cron jobs | Shared `db/engine.py` `create_job_engine()` with `NullPool` |
| Background work | 22 CLI jobs under `db/jobs/`; camp refresh API enqueues in-process jobs |
| Request correlation | `core/logging.py` `request_id_middleware` |

## B. Critical issues (resolved)

| ID | Issue | Remediation | Status |
|----|-------|-------------|--------|
| P0-1 | Metsights push holds txn during HTTP | `release_request_transaction` + isolated audit | Fixed |
| P0-2 | `tracked_metsights_call` on shared session | Isolated `persist/finalize_integration_sync_log_isolated` | Fixed |
| P0-3 | `load_blood_reports` bulk txn + HTTP loop | Release after bulk read + per-participant before HTTP | Fixed |
| P0-4 | Bookings Healthians flows | `release_request_transaction` before client calls | Fixed |
| P0-5 | Console Healthians flows | Same pattern + geocoding | Fixed |
| P0-6 | Reports resolvers HTTP while txn open | Release before Metsights/Healthians fetch | Fixed |
| P0-7 | Camp KPI Metsights gather | Release before `asyncio.gather` | Fixed |
| P0-8 | Aurae webhook pending log + dispatch | Isolated audit + release before notifications | Fixed |
| P0-9 | Healthians webhook forward | Release before forward HTTP | Fixed |
| P0-10 | `import_category_from_metsights` caller txn | Release before Metsights GET | Fixed |

## C. Connection leaks

**Root cause:** SQLAlchemy auto-begins transactions on first query; external HTTP awaited on same session → Postgres `idle in transaction`.

**Pattern applied:** `db/transaction.py` `release_request_transaction()` + isolated audit sessions for pending integration logs.

## D. Idle transaction paths

All P0 paths listed in §B now commit/rollback before outbound I/O. Safety net: PostgreSQL `idle_in_transaction_session_timeout=60s` (documented in `.env.example`).

## E. Slow queries / observability

- `db/observability.py` — SQLAlchemy event listeners log queries > `DATABASE_SLOW_QUERY_MS` (default 500ms) with `request_id`
- `GET /health/db` — pool metrics for admins

## F. N+1 optimizations

| Area | Change |
|------|--------|
| Metsights category import | Batch `get_definitions_by_keys` |
| Engagement questionnaire status | Batch category_id lookup (single query) |
| Engagement completeness summary | Default `limit=100`, max 500 |

## G. External API + transaction

Isolated audit logging used for: `tracked_integration_call`, `tracked_metsights_call`, Healthians isolated helpers, notifications dispatch, Metsights push/import audit rows.

## H. Pool configuration

See `docs/postgresql-monitoring-runbook.md` for connection budget and monitoring SQL.

## I. Concurrency expectations

- 4 workers × 10 max connections = 40 API pool ceiling
- Cron overlap uses NullPool (no persistent job pools)
- Camp section refresh API returns **202** + `job_id` by default (`?async=false` for sync)

## J. Test results

| Suite | Result |
|-------|--------|
| `tests/modules/audit/test_idle_transaction_fixes.py` | Pass |
| `tests/db/test_session.py` | Pass |
| `tests/modules/reports/test_nutrition_api_sync_logs.py` | Fixed engagement_type seed |
| Broader CI | Pre-existing engagement_type string seeds in legacy tests — helper at `tests/helpers/engagement_types.py` |

## Acceptance checklist

| Item | PR | Status |
|------|-----|--------|
| P0 idle-in-transaction paths | PR1 | Complete |
| Pool timeouts + job NullPool | PR2 | Complete |
| Camp async refresh, BioAI to_thread, completeness cap | PR3 | Complete |
| Metsights import batch, questionnaire batch | PR4 | Complete |
| Slow query log, pool endpoint | PR5 | Complete |
| Test seeds + idle txn tests | PR6 | Complete |
| Concurrency script | Phase 3 | `scripts/concurrency_test.py` |
| Monitoring runbook | Phase 4 | `docs/postgresql-monitoring-runbook.md` |
