# api/v1/endpoints/scraper.py
from fastapi import APIRouter, HTTPException, Depends, Query, Body
import logging
from typing import Optional, List
from api.models import (
    LinkedInScraperRequest,
    LinkedInScraperResponse,
    LinkedInBulkScraperRequest,
    LinkedInBulkScraperResponse,
    RateLimitStatsResponse
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

@router.get("/rate_limit", response_model=RateLimitStatsResponse)
async def get_rate_limit_stats(api_key: str = Depends(api_key_auth)):
    """
    Get current rate limit statistics for LinkedIn scraping.
    
    Args:
        api_key: API key for authentication (from dependency)
    """
    try:
        logger.info("Received request for rate limit statistics")
        
        # Initialize scraper
        scraper = LinkedInScraper()
        
        # Get rate limit stats
        stats = scraper.get_rate_limit_stats()
        
        return stats
    except Exception as e:
        logger.error(f"Error getting rate limit stats: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.post("/lkd_scraper", response_model=LinkedInScraperResponse)
async def scrape_linkedin_profile(
    request: LinkedInScraperRequest,
    api_key: str = Depends(api_key_auth)
):
    """
    Scrape a single LinkedIn profile.
    
    Args:
        request: LinkedIn scraper request with profile URL
        api_key: API key for authentication (from dependency)
    """
    try:
        linkedin_url = request.linkedin_url
        logger.info(f"Received request to scrape LinkedIn profile: {linkedin_url}")
        
        # Initialize scraper
        scraper = LinkedInScraper()
        
        # Scrape profile
        result = await scraper.scrape_profile(linkedin_url)
        
        # Check if rate limit exceeded
        if not result["success"] and result.get("rate_limit", {}).get("is_allowed") is False:
            logger.warning(f"Rate limit exceeded for LinkedIn scraping: {result.get('error')}")
            raise HTTPException(
                status_code=429, 
                detail=result.get("error", "Rate limit exceeded"),
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
    Scrape multiple LinkedIn profiles in bulk.
    
    Args:
        request: LinkedIn bulk scraper request with list of profile URLs
        api_key: API key for authentication (from dependency)
    """
    try:
        linkedin_urls = request.linkedin_urls
        logger.info(f"Received request to scrape {len(linkedin_urls)} LinkedIn profiles")
        
        # Check for empty URL list
        if not linkedin_urls:
            raise HTTPException(status_code=400, detail="No LinkedIn URLs provided")
        
        # Check for too many URLs
        if len(linkedin_urls) > 50:
            raise HTTPException(status_code=400, detail="Maximum 50 URLs allowed per request")
        
        # Initialize scraper
        scraper = LinkedInScraper()
        
        # Scrape profiles in bulk
        result = await scraper.scrape_profiles_bulk(linkedin_urls)
        
        # Check if rate limit exceeded
        if not result["success"] and result.get("rate_limit", {}).get("is_allowed") is False:
            logger.warning(f"Rate limit exceeded for bulk LinkedIn scraping: {result.get('error')}")
            raise HTTPException(
                status_code=429, 
                detail=result.get("error", "Rate limit exceeded"),
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
    api_key: str = Depends(api_key_auth)
):
    """
    Scrape a single LinkedIn profile using GET request.
    
    Args:
        linkedin_url: LinkedIn profile URL to scrape
        api_key: API key for authentication (from dependency)
    """
    try:
        logger.info(f"Received GET request to scrape LinkedIn profile: {linkedin_url}")
        
        # Initialize scraper
        scraper = LinkedInScraper()
        
        # Scrape profile
        result = await scraper.scrape_profile(linkedin_url)
        
        # Check if rate limit exceeded
        if not result["success"] and result.get("rate_limit", {}).get("is_allowed") is False:
            logger.warning(f"Rate limit exceeded for LinkedIn scraping: {result.get('error')}")
            raise HTTPException(
                status_code=429, 
                detail=result.get("error", "Rate limit exceeded"),
                headers={"X-Rate-Limit-Remaining": str(result.get("rate_limit", {}).get("remaining", 0))}
            )
        
        return result
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Error scraping LinkedIn profile: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
