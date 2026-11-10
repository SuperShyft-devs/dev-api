"""Seed employee_department rows in tests."""

from __future__ import annotations

from sqlalchemy import text


async def seed_employee_departments(
    db,
    *,
    employee_id: int,
    departments: list[str],
    commit: bool = True,
) -> None:
    unique = list(dict.fromkeys(d.strip().lower() for d in departments if d and d.strip()))
    await db.execute(
        text("DELETE FROM employee_department WHERE employee_id = :eid"),
        {"eid": employee_id},
    )
    for dept in unique:
        await db.execute(
            text(
                "INSERT INTO employee_department (employee_id, department) "
                "VALUES (:eid, CAST(:dept AS employee_department_enum)) "
                "ON CONFLICT DO NOTHING"
            ),
            {"eid": employee_id, "dept": dept},
        )
    if commit:
        await db.commit()
    else:
        await db.flush()
