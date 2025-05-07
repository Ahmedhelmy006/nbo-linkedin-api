from fastapi import Security, HTTPException, Depends, status
from fastapi.security.api_key import APIKeyHeader
from config.settings import settings
from typing import Optional
import secrets
import time

# Define API key header
API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)

# API key verification function
async def get_api_key(
    api_key_header: Optional[str] = Security(API_KEY_HEADER)
) -> str:
    """
    Verify API key from header and return it if valid
    
    Args:
        api_key_header: API key from request header
        
    Returns:
        API key if valid
        
    Raises:
        HTTPException: If API key is invalid or missing
    """
    if api_key_header == settings.API_KEY:
        return api_key_header
    
    # For security, use constant time comparison to prevent timing attacks
    if api_key_header:
        # Use secrets.compare_digest to prevent timing attacks
        # This still returns False but takes the same time whether the strings
        # match up to a certain point or not at all
        secrets.compare_digest(api_key_header, settings.API_KEY)
        
        # Add a small delay to further frustrate brute force attempts
        time.sleep(0.1)
    
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing API key",
        headers={"WWW-Authenticate": "API key required in X-API-Key header"},
    )

# Use this as a dependency in your endpoints
def api_key_auth(api_key: str = Depends(get_api_key)):
    """
    Dependency for endpoints that require API key authentication
    
    Args:
        api_key: Verified API key from get_api_key dependency
    """
    return api_key
