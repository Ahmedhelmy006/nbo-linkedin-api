from pydantic import BaseModel, EmailStr, Field
from typing import Optional

class LinkedInLookupRequest(BaseModel):
    """Request model for LinkedIn profile lookup."""
    email: EmailStr = Field(..., description="Email address to look up")
    first_name: Optional[str] = Field(None, description="First name of the person (optional)")
    last_name: Optional[str] = Field(None, description="Last name of the person (optional)")
    location_city: Optional[str] = Field(None, description="City location (optional)")
    location_state: Optional[str] = Field(None, description="State/province location (optional)")
    location_country: Optional[str] = Field(None, description="Country location (optional)")

class LinkedInLookupResponse(BaseModel):
    """Response model for LinkedIn profile lookup."""
    email: str = Field(..., description="Email address that was looked up")
    linkedin_url: Optional[str] = Field(None, description="LinkedIn profile URL if found")
    success: bool = Field(..., description="Whether the lookup was successful")
    method_used: str = Field(..., description="Method used: google_search, rocketreach_primary, rocketreach_fallback, or none")
    domain_type: str = Field(..., description="Domain type: work, personal, or unknown")
    google_domain_used: Optional[str] = Field(None, description="Google domain used for successful search (if applicable)")
    processing_time_ms: int = Field(..., description="Processing time in milliseconds")
    error_message: Optional[str] = Field(None, description="Error message if lookup failed")

class HealthResponse(BaseModel):
    """Response model for health check endpoint."""
    status: str = Field(..., description="API status: ok or error")
    version: str = Field(..., description="API version")
