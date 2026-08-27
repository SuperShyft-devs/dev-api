"""Concurrency test script for staging — monitors pool + idle-in-transaction guidance.

Usage (staging only):
    python scripts/concurrency_test.py --url https://staging-api.example.com/health --users 50 --duration 60

Pair with PostgreSQL monitoring queries in docs/postgresql-monitoring-runbook.md.
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time

import httpx


async def _worker(client: httpx.AsyncClient, url: str, stop_at: float, latencies: list[float], errors: list[str]) -> None:
    while time.perf_counter() < stop_at:
        started = time.perf_counter()
        try:
            response = await client.get(url)
            if response.status_code >= 400:
                errors.append(f"HTTP {response.status_code}")
        except Exception as exc:
            errors.append(str(exc))
        else:
            latencies.append((time.perf_counter() - started) * 1000)


async def run_load(*, url: str, users: int, duration: int) -> dict:
    stop_at = time.perf_counter() + duration
    latencies: list[float] = []
    errors: list[str] = []
    async with httpx.AsyncClient(timeout=30.0) as client:
        await asyncio.gather(
            *[_worker(client, url, stop_at, latencies, errors) for _ in range(users)]
        )
    if not latencies:
        return {"users": users, "duration_s": duration, "requests": 0, "errors": len(errors)}
    sorted_lat = sorted(latencies)
    return {
        "users": users,
        "duration_s": duration,
        "requests": len(latencies),
        "errors": len(errors),
        "p50_ms": statistics.median(sorted_lat),
        "p95_ms": sorted_lat[int(0.95 * len(sorted_lat)) - 1],
        "p99_ms": sorted_lat[int(0.99 * len(sorted_lat)) - 1],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Staged HTTP concurrency probe")
    parser.add_argument("--url", required=True, help="Target URL (e.g. /health)")
    parser.add_argument("--users", type=int, default=10)
    parser.add_argument("--duration", type=int, default=30)
    args = parser.parse_args()
    result = asyncio.run(run_load(url=args.url, users=args.users, duration=args.duration))
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
