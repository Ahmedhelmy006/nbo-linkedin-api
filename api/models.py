# api/models.py
from pydantic import BaseModel, EmailStr, Field, HttpUrl
from typing import Optional, List, Dict, Any, Union

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
    method_used: str = Field(..., description="Method used: database_cache, google_search, rocketreach_primary, rocketreach_fallback, or none")
    domain_type: str = Field(..., description="Domain type: work, personal, or unknown")
    google_domain_used: Optional[str] = Field(None, description="Google domain used for successful search (if applicable)")
    database_found: bool = Field(..., description="Whether the email was found in the subscribers database")
    database_updated: bool = Field(..., description="Whether the database was updated with a new LinkedIn URL")
    processing_time_ms: int = Field(..., description="Processing time in milliseconds")
    error_message: Optional[str] = Field(None, description="Error message if lookup failed")

class HealthResponse(BaseModel):
    """Response model for health check endpoint."""
    status: str = Field(..., description="API status: ok or error")
    version: str = Field(..., description="API version")

# New models for LinkedIn scraping

class LinkedInScraperRateLimit(BaseModel):
    """Model for LinkedIn scraper rate limit information."""
    is_allowed: bool = Field(..., description="Whether the request is allowed under rate limits")
    remaining: int = Field(..., description="Remaining requests for the day")

class LinkedInScraperRequest(BaseModel):
    """Request model for LinkedIn profile scraping."""
    linkedin_url: str = Field(..., description="LinkedIn profile URL to scrape")

class LinkedInBulkScraperRequest(BaseModel):
    """Request model for bulk LinkedIn profile scraping."""
    linkedin_urls: List[str] = Field(..., description="List of LinkedIn profile URLs to scrape")
    
class LinkedInScraperResponse(BaseModel):
    """Response model for LinkedIn profile scraping."""
    success: bool = Field(..., description="Whether the scraping was successful")
    linkedin_url: str = Field(..., description="LinkedIn profile URL that was scraped")
    error: Optional[str] = Field(None, description="Error message if scraping failed")
    profile_data: Optional[Dict[str, Any]] = Field(None, description="Scraped profile data")
    processing_time_ms: int = Field(..., description="Processing time in milliseconds")
    rate_limit: Optional[LinkedInScraperRateLimit] = Field(None, description="Rate limit information")

class LinkedInBulkScraperResult(BaseModel):
    """Model for individual result in bulk scraping."""
    linkedin_url: str = Field(..., description="LinkedIn profile URL")
    success: bool = Field(..., description="Whether scraping this profile was successful")
    error: Optional[str] = Field(None, description="Error message if scraping failed")
    profile_data: Optional[Dict[str, Any]] = Field(None, description="Scraped profile data")

class LinkedInBulkScraperResponse(BaseModel):
    """Response model for bulk LinkedIn profile scraping."""
    success: bool = Field(..., description="Whether the bulk scraping was successful overall")
    error: Optional[str] = Field(None, description="Error message if scraping failed")
    valid_count: int = Field(..., description="Number of valid LinkedIn URLs")
    invalid_count: int = Field(..., description="Number of invalid LinkedIn URLs")
    invalid_urls: List[str] = Field(..., description="List of invalid LinkedIn URLs")
    results: List[LinkedInBulkScraperResult] = Field(..., description="Results for each LinkedIn URL")
    processing_time_ms: int = Field(..., description="Processing time in milliseconds")
    rate_limit: Optional[LinkedInScraperRateLimit] = Field(None, description="Rate limit information")

class RateLimitStatsResponse(BaseModel):
    """Response model for rate limit statistics."""
    date: str = Field(..., description="Current date")
    used: int = Field(..., description="Number of requests used today")
    limit: int = Field(..., description="Daily limit")
    remaining: int = Field(..., description="Remaining requests for today")
    used_percent: float = Field(..., description="Percentage of limit used")
    last_updated: str = Field(..., description="Timestamp of last update")
