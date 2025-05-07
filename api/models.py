from pydantic import BaseModel, EmailStr, Field
from typing import Optional

class LinkedInLookupRequest(BaseModel):
    email: EmailStr
    full_name: Optional[str] = None
    location_city: Optional[str] = None
    location_state: Optional[str] = None
    location_country: Optional[str] = None

class LinkedInLookupResponse(BaseModel):
    email: str
    linkedin_url: Optional[str] = None
    success: bool
    method_used: str = Field(description="Method used: google_search, rocketreach_primary, or rocketreach_fallback")
    domain_type: str = Field(description="work or personal")
    processing_time_ms: int
    
class HealthCheckResponse(BaseModel):
    status: str
    database: str
    api_version: str
