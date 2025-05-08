# api/v1/endpoints/lookup.py (modified)
from fastapi import APIRouter, HTTPException, Depends
import logging
from api.models import LinkedInLookupResponse
from services.orchestrator import LinkedInOrchestrator
from config.settings import settings
from api.auth import api_key_auth
# Initialize logger
logger = logging.getLogger(__name__)

# Remove dependencies from router
router = APIRouter(
    prefix="/lookup",
    tags=["lookup"]
)

# Remove the Depends(api_key_auth) dependency
@router.get("/{email}/{name}", response_model=LinkedInLookupResponse)
async def lookup_linkedin(email: str, name: str = None):
    """
    Perform reverse lookup from email to LinkedIn profile without authentication (temporary test).
    """
    try:
        logger.info(f"Received lookup request for email: {email}")
        
        # Initialize orchestrator
        orchestrator = LinkedInOrchestrator()
        
        # Perform lookup
        result = await orchestrator.orchestrate_lookup(
            email=email,
            full_name=name,
            location_city=None,
            location_state=None,
            location_country=None
        )
        
        return result
    except Exception as e:
        logger.error(f"Error processing lookup request: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
