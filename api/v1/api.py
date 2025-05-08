from fastapi import APIRouter
from api.v1.endpoints import lookup, health

# Create main v1 router
router = APIRouter(prefix="/v1")

# Include endpoint routers
router.include_router(lookup.router)  # This includes the lookup router
router.include_router(health.router)
