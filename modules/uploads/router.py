"""Uploads HTTP routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, File, UploadFile

from common.responses import success_response
from modules.employee.dependencies import get_current_employee
from modules.employee.service import EmployeeContext
from modules.uploads.service import UploadsService


router = APIRouter(prefix="/uploads", tags=["uploads"])


def get_uploads_service() -> UploadsService:
    return UploadsService()


@router.post("/users/profile-photo")
async def upload_user_profile_photo(
    file: UploadFile = File(...),
    _: EmployeeContext = Depends(get_current_employee),
    uploads_service: UploadsService = Depends(get_uploads_service),
):
    url = await uploads_service.save_user_profile_photo(file)
    return success_response({"url": url})


@router.post("/organizations/logo")
async def upload_organization_logo(
    file: UploadFile = File(...),
    _: EmployeeContext = Depends(get_current_employee),
    uploads_service: UploadsService = Depends(get_uploads_service),
):
    url = await uploads_service.save_organization_logo(file)
    return success_response({"url": url})
