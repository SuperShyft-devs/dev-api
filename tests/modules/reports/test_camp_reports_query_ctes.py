"""Regression: camp report analytics queries use CTEs instead of duplicating enrolled scans."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.dialects import postgresql

from modules.reports.camp_reports_repository import CampReportsRepository


def _compile(query) -> str:
    return str(
        query.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )


def test_bio_ai_user_status_query_uses_single_enrolled_cte():
    sql = _compile(
        CampReportsRepository._bio_ai_user_status_query(camp_no=8310826)
    )
    assert "WITH enrolled_users AS" in sql
    assert sql.count("PARTITION BY engagement_participants.user_id") == 1


def test_kpi_blood_candidates_query_uses_enrolled_cte():
    enrolled = CampReportsRepository._enrolled_users_ranked_cte(camp_no=8310826)
    ranked_assessments = select(enrolled.c.user_id).select_from(enrolled).cte("ranked_assessments")
    query = (
        select(enrolled.c.user_id)
        .select_from(enrolled)
        .outerjoin(ranked_assessments, ranked_assessments.c.user_id == enrolled.c.user_id)
    )
    sql = _compile(query)
    assert "WITH enrolled_users AS" in sql
    assert sql.count("PARTITION BY engagement_participants.user_id") == 1
