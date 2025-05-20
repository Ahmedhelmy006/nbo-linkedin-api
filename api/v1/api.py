# api/v1/api.py
from fastapi import APIRouter
from api.v1.endpoints import lookup, health, scraper

# Create main v1 router
router = APIRouter(prefix="/v1")

# Include endpoint routers
router.include_router(lookup.router)  # This includes the lookup router
router.include_router(health.router)
router.include_router(scraper.router)  # New: Include the scraper router
