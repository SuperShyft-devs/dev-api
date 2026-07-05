"""Authenticated user PDF uploads (no DB; public URL via MEDIA_BASE_URL)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, File, UploadFile

from common.responses import success_response
from core.dependencies import get_current_user
from modules.uploads.service import UploadsService


router = APIRouter(prefix="/uploads", tags=["uploads"])


def get_uploads_service() -> UploadsService:
    return UploadsService()


@router.post("/bio-ai/pdf")
async def upload_bio_ai_pdf(
    file: UploadFile = File(...),
    _current_user=Depends(get_current_user),
    uploads_service: UploadsService = Depends(get_uploads_service),
):
    url = await uploads_service.save_bio_ai_pdf(file)
    return success_response({"url": url})


@router.post("/blood-parameters/pdf")
async def upload_blood_parameters_pdf(
    file: UploadFile = File(...),
    _current_user=Depends(get_current_user),
    uploads_service: UploadsService = Depends(get_uploads_service),
):
    url = await uploads_service.save_blood_parameters_pdf(file)
    return success_response({"url": url})
