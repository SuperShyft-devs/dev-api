"""In-process async camp report refresh jobs (API enqueue, cron still uses CLI)."""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from db.session import AsyncSessionLocal
from modules.employee.service import EmployeeContext
from modules.reports.camp_reports_service import CampReportsService

logger = logging.getLogger(__name__)


class CampRefreshJobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class CampRefreshJob:
    job_id: str
    status: CampRefreshJobStatus
    created_at: datetime
    camp_no: int
    section: str
    result: dict[str, Any] | None = None
    error: str | None = None


_jobs: dict[str, CampRefreshJob] = {}
_running_tasks: set[asyncio.Task] = set()


def get_camp_refresh_job(job_id: str) -> CampRefreshJob | None:
    return _jobs.get(job_id)


async def _run_refresh_job(
    *,
    job_id: str,
    service: CampReportsService,
    employee: EmployeeContext,
    camp_no: int,
    section: str,
    department: str | None,
    city: str | None,
    ip_address: str,
    user_agent: str,
    endpoint: str,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
) -> None:
    job = _jobs[job_id]
    job.status = CampRefreshJobStatus.RUNNING
    factory = session_factory or AsyncSessionLocal
    try:
        async with factory() as db:
            result = await service.refresh_camp_report_section(
                db,
                employee=employee,
                camp_no=camp_no,
                section=section,
                department=department,
                city=city,
                ip_address=ip_address,
                user_agent=user_agent,
                endpoint=endpoint,
            )
            await db.commit()
        job.result = result
        job.status = CampRefreshJobStatus.COMPLETED
    except Exception as exc:
        logger.exception("Camp refresh job %s failed", job_id)
        job.error = str(exc)
        job.status = CampRefreshJobStatus.FAILED


def enqueue_camp_refresh_job(
    *,
    service: CampReportsService,
    employee: EmployeeContext,
    camp_no: int,
    section: str,
    department: str | None,
    city: str | None,
    ip_address: str,
    user_agent: str,
    endpoint: str,
) -> str:
    job_id = str(uuid.uuid4())
    _jobs[job_id] = CampRefreshJob(
        job_id=job_id,
        status=CampRefreshJobStatus.PENDING,
        created_at=datetime.now(timezone.utc),
        camp_no=camp_no,
        section=section,
    )
    task = asyncio.create_task(
        _run_refresh_job(
            job_id=job_id,
            service=service,
            employee=employee,
            camp_no=camp_no,
            section=section,
            department=department,
            city=city,
            ip_address=ip_address,
            user_agent=user_agent,
            endpoint=endpoint,
        )
    )
    _running_tasks.add(task)
    task.add_done_callback(_running_tasks.discard)
    return job_id
