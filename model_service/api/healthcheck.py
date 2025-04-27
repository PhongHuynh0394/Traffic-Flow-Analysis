from fastapi import APIRouter, File, UploadFile
router = APIRouter()

@router.get("/healthcheck")
async def healthcheck():
    return {"status": "ok", "message": "Service is running"}