# PostgreSQL Monitoring Runbook

Operational SQL for diagnosing connection leaks, idle transactions, and pool pressure on Supershyft production/staging.

## Idle in transaction

```sql
SELECT pid, usename, application_name, state, xact_start, query_start, left(query, 120) AS query_preview
FROM pg_stat_activity
WHERE state = 'idle in transaction'
ORDER BY xact_start;
```

**Pass criteria (steady load):** no sustained rows during Healthians/Metsights/n8n webhook bursts.

## Connection count by state

```sql
SELECT state, count(*) FROM pg_stat_activity GROUP BY state ORDER BY count DESC;
```

## Longest transactions

```sql
SELECT pid, now() - xact_start AS duration, left(query, 200) AS query_preview
FROM pg_stat_activity
WHERE xact_start IS NOT NULL
ORDER BY duration DESC
LIMIT 20;
```

## Blocking / blocked

```sql
SELECT blocked.pid AS blocked_pid,
       blocking.pid AS blocking_pid,
       left(blocked.query, 120) AS blocked_query,
       left(blocking.query, 120) AS blocking_query
FROM pg_stat_activity blocked
JOIN pg_stat_activity blocking ON blocking.pid = ANY(pg_blocking_pids(blocked.pid));
```

## Application pool metrics

Admin endpoint: `GET /health/db` (requires employee admin token).

Returns SQLAlchemy pool `checked_out`, `overflow`, `size`.

## Connection budget (4 Uvicorn workers, 4 vCPU)

| Setting | Value |
|---------|-------|
| `DATABASE_POOL_SIZE` | 5 |
| `DATABASE_MAX_OVERFLOW` | 5 |
| Max API connections | 4 × (5 + 5) = **40** |
| Cron jobs | `NullPool` — one connection per job run |
| PostgreSQL `idle_in_transaction_session_timeout` | **60s** (infra/DBA) |

Keep `workers × (pool_size + max_overflow) + cron_headroom < max_connections`.

## Staged concurrency testing

From `dev-api/`:

```bash
python scripts/concurrency_test.py --url https://staging-api.example.com/health --users 50 --duration 60
```

Run at 10 / 25 / 50 / 100 / 200 concurrent users while monitoring the queries above.

**Pass criteria:** p95 latency stable; no growth in `idle in transaction` count over 5 minutes; pool checked-out bounded.
