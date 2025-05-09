# api/v1/endpoints/lookup.py
from fastapi import APIRouter, HTTPException, Depends, Query
import logging
from typing import Optional
from api.models import LinkedInLookupRequest, LinkedInLookupResponse
from services.orchestrator import LinkedInOrchestrator
from config.settings import settings
from api.auth import api_key_auth
from utils.param_validator import ParamValidator

# Initialize logger
logger = logging.getLogger(__name__)

# Create router with prefix and tags
router = APIRouter(
    prefix="/lookup",
    tags=["lookup"]
)

# Keep the original routes for backward compatibility
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
        
        # Extract first name if provided
        first_name = None
        last_name = None
        
        if name:
            name_parts = name.split()
            if len(name_parts) >= 2:
                first_name = name_parts[0]
                last_name = " ".join(name_parts[1:])
            elif len(name_parts) == 1:
                first_name = name_parts[0]
        
        # Perform lookup
        result = await orchestrator.orchestrate_lookup(
            email=email,
            first_name=first_name,
            last_name=last_name,
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

# Add a new flexible endpoint with query parameters
@router.get("", response_model=LinkedInLookupResponse)
async def lookup_linkedin_with_params(
    email: str,
    first_name: Optional[str] = Query(None, description="First name of the person"),
    last_name: Optional[str] = Query(None, description="Last name of the person"),
    location_city: Optional[str] = Query(None, description="City location"),
    location_state: Optional[str] = Query(None, description="State/province location"),
    location_country: Optional[str] = Query(None, description="Country location"),
    api_key: str = Depends(api_key_auth)
):
    """
    Perform reverse lookup from email to LinkedIn profile with detailed parameters.
    
    This endpoint allows providing more detailed information to improve search accuracy.
    
    Args:
        email: Email address to look up
        first_name: First name of the person (optional)
        last_name: Last name of the person (optional)
        location_city: City location (optional)
        location_state: State/province location (optional)
        location_country: Country location (optional)
        api_key: API key for authentication (from dependency)
    """
    try:
        logger.info(f"Received detailed lookup request for email: {email}")
        
        # Validate email
        if not ParamValidator.validate_email(email):
            raise HTTPException(status_code=400, detail=f"Invalid email format: {email}")
        
        # Initialize orchestrator
        orchestrator = LinkedInOrchestrator()
        
        # Perform lookup with all parameters
        result = await orchestrator.orchestrate_lookup(
            email=email,
            first_name=first_name,
            last_name=last_name,
            location_city=location_city,
            location_state=location_state,
            location_country=location_country
        )
        
        return result
    except ValueError as ve:
        logger.error(f"Validation error: {str(ve)}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Error processing detailed lookup request: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# Add a POST endpoint for more complex requests
@router.post("", response_model=LinkedInLookupResponse)
async def lookup_linkedin_post(
    request: LinkedInLookupRequest,
    api_key: str = Depends(api_key_auth)
):
    """
    Perform reverse lookup from email to LinkedIn profile using POST request.
    
    This endpoint accepts a JSON request body with all required parameters.
    
    Args:
        request: LinkedIn lookup request object
        api_key: API key for authentication (from dependency)
    """
    try:
        logger.info(f"Received POST lookup request for email: {request.email}")
        
        # Initialize orchestrator
        orchestrator = LinkedInOrchestrator()
        
        # Perform lookup with all parameters from request body
        result = await orchestrator.orchestrate_lookup(
            email=request.email,
            first_name=request.first_name,
            last_name=request.last_name,
            location_city=request.location_city,
            location_state=request.location_state,
            location_country=request.location_country
        )
        
        return result
    except Exception as e:
        logger.error(f"Error processing POST lookup request: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
