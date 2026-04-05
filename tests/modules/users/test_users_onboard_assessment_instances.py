from __future__ import annotations

import pytest
from sqlalchemy import text


@pytest.mark.asyncio
async def test_onboard_is_idempotent_for_assessment_instance(async_client, test_db_session):
    """B2C onboarding uses platform default assessment package from seed, not ad-hoc package rows."""
    payload = {
        "age": 30,
        "first_name": "Idem",
        "phone": "7777700000",
        "city": "Delhi",
        "blood_collection_date": "2026-02-01",
        "blood_collection_time_slot": "10:00",
    }

    resp1 = await async_client.post("/users/public/onboard", json=payload)
    assert resp1.status_code == 200

    resp2 = await async_client.post("/users/public/onboard", json=payload)
    assert resp2.status_code == 200

    body1 = resp1.json()["data"]
    body2 = resp2.json()["data"]

    # Each onboarding creates its own engagement.
    assert body1["engagement_id"] != body2["engagement_id"]

    count1 = (
        await test_db_session.execute(
            text(
                "SELECT COUNT(*) AS c FROM assessment_instances WHERE user_id = :uid AND engagement_id = :eid"
            ),
            {"uid": body1["user_id"], "eid": body1["engagement_id"]},
        )
    ).scalar_one()

    count2 = (
        await test_db_session.execute(
            text(
                "SELECT COUNT(*) AS c FROM assessment_instances WHERE user_id = :uid AND engagement_id = :eid"
            ),
            {"uid": body2["user_id"], "eid": body2["engagement_id"]},
        )
    ).scalar_one()

    assert int(count1) == 1
    assert int(count2) == 1
