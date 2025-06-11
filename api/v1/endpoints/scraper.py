# api/v1/endpoints/scraper.py
from fastapi import APIRouter, HTTPException, Depends, Query, Body
import logging
from typing import Optional, List
from api.models import (
    LinkedInScraperRequest,
    LinkedInScraperResponse,
    LinkedInBulkScraperRequest,
    LinkedInBulkScraperResponse,
    CookieUsageStatsResponse,
    CookieChoice
)
from services.linkedin_scraper import LinkedInScraper
from api.auth import api_key_auth

# Initialize logger
logger = logging.getLogger(__name__)

# Create router with prefix and tags
router = APIRouter(
    prefix="/scraper",
    tags=["scraper"]
)

@router.get("/cookie_usage", response_model=CookieUsageStatsResponse)
async def get_cookie_usage_stats(
    cookie_name: Optional[CookieChoice] = Query(None, description="Specific cookie name or None for all cookies"),
    api_key: str = Depends(api_key_auth)
):
    """
    Get current cookie usage statistics for LinkedIn scraping.
    
    Args:
        cookie_name: Specific cookie name (main, backup, personal) or None for all
        api_key: API key for authentication (from dependency)
    """
    try:
        logger.info(f"Received request for cookie usage statistics: {cookie_name}")
        
        # Scrape profile with specified cookies
        result = await scraper.scrape_profile(linkedin_url, cookie_name)
        
        # Check if rate limit exceeded
        if not result["success"] and result.get("rate_limit", {}).get("is_allowed") is False:
            logger.warning(f"Rate limit exceeded for {cookie_name} cookies: {result.get('error')}")
            
            # Build detailed 429 response
            response_detail = {
                "success": False,
                "error": result.get("error"),
                "cookie_used": cookie_name,
                "current_usage": result.get("current_usage", 70),
                "limit": result.get("limit", 70),
                "remaining": result.get("rate_limit", {}).get("remaining", 0),
                "other_cookies_remaining": result.get("other_cookies_remaining", {}),
                "reset_time": result.get("reset_time", "00:00:00 UTC")
            }
            
            raise HTTPException(
                status_code=429, 
                detail=response_detail,
                headers={"X-Rate-Limit-Remaining": str(result.get("rate_limit", {}).get("remaining", 0))}
            )
        
        return result
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Error scraping LinkedIn profile: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.post("/lkd_bulk_scraper", response_model=LinkedInBulkScraperResponse)
async def scrape_linkedin_profiles_bulk(
    request: LinkedInBulkScraperRequest,
    api_key: str = Depends(api_key_auth)
):
    """
    Scrape multiple LinkedIn profiles in bulk using specified cookies.
    
    Args:
        request: LinkedIn bulk scraper request with list of profile URLs and cookie choice
        api_key: API key for authentication (from dependency)
    """
    try:
        linkedin_urls = request.linkedin_urls
        cookie_name = request.cookies.value
        logger.info(f"Received request to scrape {len(linkedin_urls)} LinkedIn profiles using {cookie_name} cookies")
        
        # Check for empty URL list
        if not linkedin_urls:
            raise HTTPException(status_code=400, detail="No LinkedIn URLs provided")
        
        # Check for too many URLs
        if len(linkedin_urls) > 70:
            raise HTTPException(status_code=400, detail="Maximum 70 URLs allowed per request")
        
        # Initialize scraper
        scraper = LinkedInScraper()
        
        # Scrape profiles in bulk with specified cookies
        result = await scraper.scrape_profiles_bulk(linkedin_urls, cookie_name)
        
        # Check if rate limit exceeded
        if not result["success"] and result.get("rate_limit", {}).get("is_allowed") is False:
            logger.warning(f"Rate limit exceeded for bulk {cookie_name} cookies: {result.get('error')}")
            
            # Build detailed 429 response
            response_detail = {
                "success": False,
                "error": result.get("error"),
                "cookie_used": cookie_name,
                "current_usage": result.get("current_usage", 70),
                "limit": result.get("limit", 70),
                "remaining": result.get("rate_limit", {}).get("remaining", 0),
                "other_cookies_remaining": result.get("other_cookies_remaining", {}),
                "reset_time": result.get("reset_time", "00:00:00 UTC")
            }
            
            raise HTTPException(
                status_code=429, 
                detail=response_detail,
                headers={"X-Rate-Limit-Remaining": str(result.get("rate_limit", {}).get("remaining", 0))}
            )
        
        return result
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Error during bulk LinkedIn scraping: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get("/lkd_scraper", response_model=LinkedInScraperResponse)
async def scrape_linkedin_profile_get(
    linkedin_url: str,
    cookies: CookieChoice,
    api_key: str = Depends(api_key_auth)
):
    """
    Scrape a single LinkedIn profile using GET request with specified cookies.
    
    Args:
        linkedin_url: LinkedIn profile URL to scrape
        cookies: Cookie file to use (main, backup, personal)
        api_key: API key for authentication (from dependency)
    """
    try:
        logger.info(f"Received GET request to scrape LinkedIn profile: {linkedin_url} using {cookies} cookies")
        
        # Initialize scraper
        scraper = LinkedInScraper()
        
        # Scrape profile with specified cookies
        result = await scraper.scrape_profile(linkedin_url, cookies.value)
        
        # Check if rate limit exceeded
        if not result["success"] and result.get("rate_limit", {}).get("is_allowed") is False:
            logger.warning(f"Rate limit exceeded for {cookies} cookies: {result.get('error')}")
            
            # Build detailed 429 response
            response_detail = {
                "success": False,
                "error": result.get("error"),
                "cookie_used": cookies.value,
                "current_usage": result.get("current_usage", 70),
                "limit": result.get("limit", 70),
                "remaining": result.get("rate_limit", {}).get("remaining", 0),
                "other_cookies_remaining": result.get("other_cookies_remaining", {}),
                "reset_time": result.get("reset_time", "00:00:00 UTC")
            }
            
            raise HTTPException(
                status_code=429, 
                detail=response_detail,
                headers={"X-Rate-Limit-Remaining": str(result.get("rate_limit", {}).get("remaining", 0))}
            )
        
        return result
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Error scraping LinkedIn profile: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
        # Get cookie usage stats
        stats = scraper.get_cookie_usage_stats(cookie_name)
        
        return stats
    except Exception as e:
        logger.error(f"Error getting cookie usage stats: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.post("/lkd_scraper", response_model=LinkedInScraperResponse)
async def scrape_linkedin_profile(
    request: LinkedInScraperRequest,
    api_key: str = Depends(api_key_auth)
):
    """
    Scrape a single LinkedIn profile using specified cookies.
    
    Args:
        request: LinkedIn scraper request with profile URL and cookie choice
        api_key: API key for authentication (from dependency)
    """
    try:
        linkedin_url = request.linkedin_url
        cookie_name = request.cookies.value
        logger.info(f"Received request to scrape LinkedIn profile: {linkedin_url} using {cookie_name} cookies")
        
        # Initialize scraper
        scraper = LinkedInScraper()
