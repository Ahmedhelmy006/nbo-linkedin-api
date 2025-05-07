from fastapi import APIRouter, Depends, HTTPException
import logging
from api.models import LinkedInLookupRequest, LinkedInLookupResponse
from api.auth import api_key_auth
from services.orchestrator import LinkedInOrchestrator

# Initialize logger
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(
    prefix="/lookup",  # Changed from "/linkedin"
    tags=["lookup"]    # Changed from ["linkedin"]
    dependencies=[Depends(api_key_auth)]
)

@router.post("", response_model=LinkedInLookupResponse)  # Changed from "/lookup" to just ""
async def lookup_linkedin(request: LinkedInLookupRequest):
    """
    Perform reverse lookup from email to LinkedIn profile.
    
    - **email**: Email address to look up (required)
    - **full_name**: Full name of the person (optional)
    - **location_city**: City location (optional)
    - **location_state**: State/province location (optional)
    - **location_country**: Country location (optional)
    """
    try:
        logger.info(f"Received lookup request for email: {request.email}")
        
        # Initialize orchestrator
        orchestrator = LinkedInOrchestrator()
        
        # Perform lookup
        result = await orchestrator.orchestrate_lookup(
            email=request.email,
            full_name=request.full_name,
            location_city=request.location_city,
            location_state=request.location_state,
            location_country=request.location_country
        )
        
        return result
    except Exception as e:
        logger.error(f"Error processing lookup request: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
