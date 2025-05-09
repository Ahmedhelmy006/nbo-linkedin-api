# api/v1/endpoints/lookup.py
from fastapi import APIRouter, HTTPException, Depends
import logging
from api.models import LinkedInLookupResponse
from services.orchestrator import LinkedInOrchestrator
from config.settings import settings
from api.auth import api_key_auth

# Initialize logger
logger = logging.getLogger(__name__)

# Create router with prefix and tags
router = APIRouter(
    prefix="/lookup",
    tags=["lookup"]
)

# Keep the original route for backward compatibility
@router.get("/{email}/{name}", response_model=LinkedInLookupResponse)
async def lookup_linkedin(email: str, name: str = None, api_key: str = Depends(api_key_auth)):
    """
    Perform reverse lookup from email to LinkedIn profile.
    
    Args:
        email: Email address to look up
        name: Full name of the person (optional)
        api_key: API key for authentication (from dependency)
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

# Add a new route to handle requests without a name
@router.get("/{email}", response_model=LinkedInLookupResponse)
async def lookup_linkedin_no_name(email: str, api_key: str = Depends(api_key_auth)):
    """
    Perform reverse lookup from email to LinkedIn profile without a name.
    
    Args:
        email: Email address to look up
        api_key: API key for authentication (from dependency)
    """
    # Reuse the existing function with name as None
    return await lookup_linkedin(email, None, api_key)
