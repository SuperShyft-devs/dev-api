"""HTTP routes for discount codes (admin + checkout validate)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from sqlalchemy.ext.asyncio import AsyncSession

from common.responses import success_response
from core.dependencies import get_current_user
from core.network import get_client_ip
from core.rate_limit import limiter
from db.session import get_db
from modules.discounts.schemas import (
    AllowlistUploadRequest,
    BulkInstancesRequest,
    CheckoutContext,
    DiscountAutoApplyRequest,
    DiscountCodeCreate,
    DiscountCodeUpdate,
    DiscountStatusUpdate,
    DiscountValidateRequest,
)
from modules.discounts.service import DiscountService
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext

router = APIRouter(prefix="/discounts", tags=["discounts"])


def get_discount_service() -> DiscountService:
    return DiscountService()


def _err(result: dict) -> JSONResponse | None:
    err = result.get("_error")
    if not err:
        return None
    code, msg = err
    return JSONResponse(status_code=code, content={"success": False, "message": msg})


# ── Public (authenticated user) ──────────────────────────────────────────────


@router.post("/validate")
@limiter.limit("10/minute")
async def validate_discount(
    payload: DiscountValidateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    service: DiscountService = Depends(get_discount_service),
):
    result = await service.validate_for_user(
        db,
        code=payload.code,
        user_id=current_user.user_id,
        items=payload.items,
        context=payload.context,
        client_ip=get_client_ip(request),
        endpoint="/discounts/validate",
    )
    err = _err(result)
    if err:
        await db.commit()
        return err
    await db.commit()
    return success_response(result)


@router.post("/auto-apply")
@limiter.limit("20/minute")
async def auto_apply_discount(
    payload: DiscountAutoApplyRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_user),
    service: DiscountService = Depends(get_discount_service),
):
    result = await service.auto_apply(
        db,
        user_id=current_user.user_id,
        items=payload.items,
        context=payload.context,
        client_ip=get_client_ip(request),
        endpoint="/discounts/auto-apply",
    )
    err = _err(result)
    if err:
        await db.commit()
        return err
    await db.commit()
    return success_response(result)


# ── Admin ────────────────────────────────────────────────────────────────────


@router.get("")
@limiter.limit("60/minute")
async def list_discounts(
    request: Request,
    status: str | None = None,
    search: str | None = None,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    _employee: EmployeeContext = Depends(get_current_employee),
    service: DiscountService = Depends(get_discount_service),
):
    data = await service.list_codes(db, status=status, search=search, limit=limit, offset=offset)
    return success_response(data)


@router.post("")
@limiter.limit("60/minute")
async def create_discount(
    payload: DiscountCodeCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: DiscountService = Depends(get_discount_service),
):
    result = await service.create_code(db, payload, actor_id=employee.user_id)
    err = _err(result)
    if err:
        await db.rollback()
        return err
    await db.commit()
    return success_response(result)


@router.get("/reports/summary")
@limiter.limit("60/minute")
async def reports_summary(
    request: Request,
    db: AsyncSession = Depends(get_db),
    _employee: EmployeeContext = Depends(get_current_employee),
    service: DiscountService = Depends(get_discount_service),
):
    return success_response(await service.reports_summary(db))


@router.get("/support-lookup")
@limiter.limit("60/minute")
async def support_lookup(
    request: Request,
    code: str = Query(..., min_length=1),
    phone: str | None = None,
    db: AsyncSession = Depends(get_db),
    _employee: EmployeeContext = Depends(get_current_employee),
    service: DiscountService = Depends(get_discount_service),
):
    data = await service.support_lookup(
        db,
        code=code,
        phone=phone,
        items=None,
        context=CheckoutContext(),
    )
    return success_response(data)


@router.get("/options/packages")
@limiter.limit("60/minute")
async def options_packages(
    request: Request,
    package_for: str | None = None,
    search: str | None = None,
    db: AsyncSession = Depends(get_db),
    _employee: EmployeeContext = Depends(get_current_employee),
    service: DiscountService = Depends(get_discount_service),
):
    return success_response(await service.options_packages(db, package_for=package_for, search=search))


@router.get("/options/organizations")
@limiter.limit("60/minute")
async def options_organizations(
    request: Request,
    search: str | None = None,
    db: AsyncSession = Depends(get_db),
    _employee: EmployeeContext = Depends(get_current_employee),
    service: DiscountService = Depends(get_discount_service),
):
    return success_response(await service.options_organizations(db, search=search))


@router.get("/options/camps")
@limiter.limit("60/minute")
async def options_camps(
    request: Request,
    search: str | None = None,
    organization_id: int | None = None,
    db: AsyncSession = Depends(get_db),
    _employee: EmployeeContext = Depends(get_current_employee),
    service: DiscountService = Depends(get_discount_service),
):
    return success_response(
        await service.options_camps(db, search=search, organization_id=organization_id)
    )


@router.get("/options/engagements")
@limiter.limit("60/minute")
async def options_engagements(
    request: Request,
    search: str | None = None,
    organization_id: int | None = None,
    db: AsyncSession = Depends(get_db),
    _employee: EmployeeContext = Depends(get_current_employee),
    service: DiscountService = Depends(get_discount_service),
):
    return success_response(
        await service.options_engagements(db, search=search, organization_id=organization_id)
    )


@router.get("/{discount_code_id}")
@limiter.limit("60/minute")
async def get_discount(
    discount_code_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    _employee: EmployeeContext = Depends(get_current_employee),
    service: DiscountService = Depends(get_discount_service),
):
    result = await service.get_code(db, discount_code_id)
    err = _err(result)
    if err:
        return err
    return success_response(result)


@router.patch("/{discount_code_id}")
@limiter.limit("60/minute")
async def update_discount(
    discount_code_id: int,
    payload: DiscountCodeUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: DiscountService = Depends(get_discount_service),
):
    result = await service.update_code(db, discount_code_id, payload, actor_id=employee.user_id)
    err = _err(result)
    if err:
        await db.rollback()
        return err
    await db.commit()
    return success_response(result)


@router.post("/{discount_code_id}/status")
@limiter.limit("60/minute")
async def update_status(
    discount_code_id: int,
    payload: DiscountStatusUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: DiscountService = Depends(get_discount_service),
):
    result = await service.set_status(db, discount_code_id, payload.action, actor_id=employee.user_id)
    err = _err(result)
    if err:
        await db.rollback()
        return err
    await db.commit()
    return success_response(result)


@router.get("/{discount_code_id}/audit")
@limiter.limit("60/minute")
async def list_audit(
    discount_code_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    _employee: EmployeeContext = Depends(get_current_employee),
    service: DiscountService = Depends(get_discount_service),
):
    rows = await service.repository.list_audit(db, discount_code_id)
    return success_response(
        [
            {
                "audit_id": r.audit_id,
                "action": r.action,
                "actor_user_id": r.actor_user_id,
                "diff": r.diff,
                "created_at": r.created_at,
            }
            for r in rows
        ]
    )


@router.post("/{discount_code_id}/instances/bulk")
@limiter.limit("60/minute")
async def bulk_instances(
    discount_code_id: int,
    payload: BulkInstancesRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: DiscountService = Depends(get_discount_service),
):
    result = await service.bulk_instances(
        db,
        discount_code_id,
        count=payload.count,
        prefix=payload.prefix,
        actor_id=employee.user_id,
    )
    err = _err(result)
    if err:
        await db.rollback()
        return err
    await db.commit()
    return success_response(result)


@router.get("/{discount_code_id}/instances")
@limiter.limit("60/minute")
async def download_instances(
    discount_code_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    _employee: EmployeeContext = Depends(get_current_employee),
    service: DiscountService = Depends(get_discount_service),
):
    csv_text = await service.instances_csv(db, discount_code_id)
    return PlainTextResponse(
        csv_text,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="discount_{discount_code_id}_codes.csv"'},
    )


@router.post("/{discount_code_id}/allowlist")
@limiter.limit("60/minute")
async def upload_allowlist(
    discount_code_id: int,
    payload: AllowlistUploadRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    employee: EmployeeContext = Depends(get_current_employee),
    service: DiscountService = Depends(get_discount_service),
):
    result = await service.upload_allowlist(
        db, discount_code_id, payload.entries, actor_id=employee.user_id
    )
    err = _err(result)
    if err:
        await db.rollback()
        return err
    await db.commit()
    return success_response(result)


@router.get("/{discount_code_id}/report")
@limiter.limit("60/minute")
async def code_report(
    discount_code_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    _employee: EmployeeContext = Depends(get_current_employee),
    service: DiscountService = Depends(get_discount_service),
):
    result = await service.report(db, discount_code_id)
    err = _err(result)
    if err:
        return err
    return success_response(result)
