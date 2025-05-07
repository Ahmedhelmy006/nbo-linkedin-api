from pydantic import BaseModel, EmailStr, Field
from typing import Optional

class LinkedInLookupRequest(BaseModel):
    """Request model for LinkedIn profile lookup."""
    email: EmailStr = Field(..., description="Email address to look up")
    full_name: Optional[str] = Field(None, description="Full name of the person (optional)")
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
    processing_time_ms: int = Field(..., description="Processing time in milliseconds")
    error_message: Optional[str] = Field(None, description="Error message if lookup failed")

class HealthResponse(BaseModel):
    """Response model for health check endpoint."""
    status: str = Field(..., description="API status: ok or error")
    version: str = Field(..., description="API version")
