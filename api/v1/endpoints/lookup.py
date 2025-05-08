# api/v1/endpoints/lookup.py (modified)
from fastapi import APIRouter, HTTPException, Depends
import logging
from api.models import LinkedInLookupResponse
from services.orchestrator import LinkedInOrchestrator
from config.settings import settings
from api.auth import api_key_auth
# Initialize logger
logger = logging.getLogger(__name__)

# Create router without dependencies (we'll handle auth directly)
router = APIRouter(
    prefix="/lookup",  # Changed from "/linkedin"
    tags=["lookup"],    # Changed from ["linkedin"]
    dependencies=[Depends(api_key_auth)]
)

@router.get("/email={email}-name={name}/{api_key}", response_model=LinkedInLookupResponse)
async def lookup_linkedin(email: str, name: str = None, api_key: str = None):
    """
    Perform reverse lookup from email to LinkedIn profile using URL parameters.
    
    - **email**: Email address to look up (required)
    - **name**: Full name of the person (optional)
    - **api_key**: API key for authentication (required)
    """
    # First, validate API key
    if api_key != settings.API_KEY:
        logger.warning(f"Invalid API key attempted: {api_key}")
        raise HTTPException(status_code=401, detail="Invalid API key")
    
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
