from fastapi import APIRouter, Depends
from api.models import HealthResponse
from api.auth import api_key_auth
import platform
import sys

router = APIRouter(
    prefix="/health",
    tags=["health"]
)

@router.get("", response_model=HealthResponse)
async def health_check():
    """
    Health check endpoint to verify API is running.
    """
    return {
        "status": "ok",
        "version": "1.0.0",
    }
