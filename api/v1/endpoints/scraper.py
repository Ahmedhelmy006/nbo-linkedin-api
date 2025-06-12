# api/v1/endpoints/scraper.py
from fastapi import APIRouter, HTTPException, Depends, Query
import logging
from typing import Optional, List
from services.linkedin_scraper import LinkedInScraper
from services.scraper_rate_limiter import ScraperRateLimiter
from api.auth import api_key_auth

# Initialize logger
logger = logging.getLogger(__name__)

# Create router with prefix and tags
router = APIRouter(
    prefix="/scraper",
    tags=["scraper"]
)

@router.get("/rate_limit")
async def get_rate_limit_stats(api_key: str = Depends(api_key_auth)):
    """
    Get current rate limit statistics for LinkedIn scraping.
    
    Args:
        api_key: API key for authentication (from dependency)
    """
    try:
        logger.info("Received request for rate limit statistics")
        
        # Initialize rate limiter
        rate_limiter = ScraperRateLimiter()
        
        # Get rate limit stats
        stats = rate_limiter.get_usage_stats()
        
        return stats
    except Exception as e:
        logger.error(f"Error getting rate limit stats: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get("/cookie_usage")
async def get_cookie_usage_stats(
    cookie_name: Optional[str] = Query(None, description="Specific cookie name or None for all cookies"),
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
        
        # Initialize scraper
        scraper = LinkedInScraper()
        
        # Get cookie usage stats
        stats = scraper.get_cookie_usage_stats(cookie_name)
        
        return stats
    except Exception as e:
        logger.error(f"Error getting cookie usage stats: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.post("/lkd_scraper")
async def scrape_linkedin_profile(
    request: dict,
    api_key: str = Depends(api_key_auth)
):
    """
    Scrape a single LinkedIn profile using specified cookies.
    
    Args:
        request: Request body containing linkedin_url and cookies
        api_key: API key for authentication (from dependency)
    """
    try:
        linkedin_url = request.get("linkedin_url")
        cookies = request.get("cookies")
        
        if not linkedin_url:
            raise HTTPException(status_code=400, detail="linkedin_url is required")
        
        if not cookies:
            raise HTTPException(status_code=400, detail="cookies parameter is required (main, backup, or personal)")
        
        if cookies not in ["main", "backup", "personal"]:
            raise HTTPException(status_code=400, detail="cookies must be one of: main, backup, personal")
        
        logger.info(f"Received request to scrape LinkedIn profile: {linkedin_url} using {cookies} cookies")
        
        # Initialize scraper
        scraper = LinkedInScraper()
        
        # Scrape profile with specified cookies
        result = await scraper.scrape_profile(linkedin_url, cookies)
        
        # Check if rate limit exceeded
        if not result["success"] and result.get("rate_limit", {}).get("is_allowed") is False:
            logger.warning(f"Rate limit exceeded for {cookies} cookies: {result.get('error')}")
            
            # Build detailed 429 response
            response_detail = {
                "success": False,
                "error": result.get("error"),
                "cookie_used": cookies,
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

@router.post("/lkd_bulk_scraper")
async def scrape_linkedin_profiles_bulk(
    request: dict,
    api_key: str = Depends(api_key_auth)
):
    """
    Scrape multiple LinkedIn profiles in bulk using specified cookies.
    
    Args:
        request: Request body containing linkedin_urls list and cookies
        api_key: API key for authentication (from dependency)
    """
    try:
        linkedin_urls = request.get("linkedin_urls")
        cookies = request.get("cookies")
        
        if not linkedin_urls or not isinstance(linkedin_urls, list):
            raise HTTPException(status_code=400, detail="linkedin_urls list is required")
        
        if not cookies:
            raise HTTPException(status_code=400, detail="cookies parameter is required (main, backup, or personal)")
        
        if cookies not in ["main", "backup", "personal"]:
            raise HTTPException(status_code=400, detail="cookies must be one of: main, backup, personal")
        
        logger.info(f"Received request to scrape {len(linkedin_urls)} LinkedIn profiles using {cookies} cookies")
        
        # Check for empty URL list
        if not linkedin_urls:
            raise HTTPException(status_code=400, detail="No LinkedIn URLs provided")
        
        # Check for too many URLs
        if len(linkedin_urls) > 70:
            raise HTTPException(status_code=400, detail="Maximum 70 URLs allowed per request")
        
        # Initialize scraper
        scraper = LinkedInScraper()
        
        # Scrape profiles in bulk with specified cookies
        result = await scraper.scrape_profiles_bulk(linkedin_urls, cookies)
        
        # Check if rate limit exceeded
        if not result["success"] and result.get("rate_limit", {}).get("is_allowed") is False:
            logger.warning(f"Rate limit exceeded for bulk {cookies} cookies: {result.get('error')}")
            
            # Build detailed 429 response
            response_detail = {
                "success": False,
                "error": result.get("error"),
                "cookie_used": cookies,
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

@router.get("/lkd_scraper")
async def scrape_linkedin_profile_get(
    linkedin_url: str,
    cookies: str,
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
        if cookies not in ["main", "backup", "personal"]:
            raise HTTPException(status_code=400, detail="cookies must be one of: main, backup, personal")
        
        logger.info(f"Received GET request to scrape LinkedIn profile: {linkedin_url} using {cookies} cookies")
        
        # Initialize scraper
        scraper = LinkedInScraper()
        
        # Scrape profile with specified cookies
        result = await scraper.scrape_profile(linkedin_url, cookies)
        
        # Check if rate limit exceeded
        if not result["success"] and result.get("rate_limit", {}).get("is_allowed") is False:
            logger.warning(f"Rate limit exceeded for {cookies} cookies: {result.get('error')}")
            
            # Build detailed 429 response
            response_detail = {
                "success": False,
                "error": result.get("error"),
                "cookie_used": cookies,
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
