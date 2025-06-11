# services/linkedin_scraper.py
"""
LinkedIn profile scraper for the NBO Pipeline.

This module provides functionality for scraping LinkedIn profile data
using the Apify platform with cookie-based rate limiting and database storage.
"""
import os
import json
import logging
import time
from typing import Dict, List, Any, Optional, Tuple, Union
from pathlib import Path

from apify_client import ApifyClient
from config.settings import settings
from .cookie_usage_tracker import cookie_usage_tracker
# Import the database repository
from database.repositories.linkedin_profile_repository import linkedin_profile_repo

logger = logging.getLogger(__name__)

class LinkedInScraper:
    """
    Service for scraping LinkedIn profiles using Apify with cookie management.
    """
    
    def __init__(self):
        """Initialize the LinkedIn scraper."""
        # Get Apify API key from settings
        try:
            self.api_key = getattr(settings, 'APIFY_API_KEY', None)
            if not self.api_key:
                logger.error("APIFY_API_KEY not set in settings")
                raise ValueError("APIFY_API_KEY not set in settings")
        except AttributeError:
            logger.error("APIFY_API_KEY not found in settings")
            raise ValueError("APIFY_API_KEY not found in settings")
        
        # Get LinkedIn Scraper Actor ID from settings
        try:
            self.actor_id = getattr(settings, 'LINKEDIN_SCRAPER_ACTOR_ID', None)
            if not self.actor_id:
                logger.error("LINKEDIN_SCRAPER_ACTOR_ID not set in settings")
                raise ValueError("LINKEDIN_SCRAPER_ACTOR_ID not set in settings")
        except AttributeError:
            logger.error("LINKEDIN_SCRAPER_ACTOR_ID not found in settings")
            raise ValueError("LINKEDIN_SCRAPER_ACTOR_ID not found in settings")
        
        # Initialize Apify client
        self.client = ApifyClient(self.api_key)
        
        logger.info(f"LinkedIn scraper initialized with actor ID: {self.actor_id}")
    
    def _load_cookies(self, cookie_name: str) -> List[Dict]:
        """
        Load LinkedIn cookies from the specified cookie file.
        
        Args:
            cookie_name: Name of the cookie file (main, backup, personal)
        
        Returns:
            List of cookie objects or empty list if file not found
        """
        # Use direct path to cookie files
        cookies_path = f"/home/developer/nbo_linkedin_api/data/{cookie_name}.json"
        
        try:
            logger.info(f"Loading cookies from: {cookies_path}")
            with open(cookies_path, 'r') as f:
                raw_cookies = f.read()
                cookies_json = json.loads(raw_cookies)
                logger.info(f"Successfully loaded {cookie_name} cookies JSON with {len(cookies_json)} items")
                return cookies_json
        except Exception as e:
            logger.error(f"Could not load {cookie_name} cookies file: {e}")
            return []
    
    async def scrape_profile(self, linkedin_url: str, cookie_name: str) -> Dict:
        """
        Scrape a single LinkedIn profile using specified cookies and store it in database.
        
        Args:
            linkedin_url: LinkedIn profile URL to scrape
            cookie_name: Cookie file to use (main, backup, personal)
            
        Returns:
            Dictionary with scrape results and status
        """
        start_time = time.time()
        
        # Validate URL
        if not self._is_valid_linkedin_url(linkedin_url):
            logger.error(f"Invalid LinkedIn URL: {linkedin_url}")
            return {
                "success": False,
                "linkedin_url": linkedin_url,
                "error": "Invalid LinkedIn profile URL format",
                "profile_data": None,
                "processing_time_ms": int((time.time() - start_time) * 1000),
                "rate_limit": {
                    "is_allowed": False,
                    "remaining": 0,
                    "cookie_used": cookie_name
                }
            }
        
        # Check cookie-specific rate limit
        is_allowed, remaining = await cookie_usage_tracker.check_rate_limit(cookie_name, 1)
        if not is_allowed:
            logger.warning(f"Rate limit exceeded for {cookie_name} cookies: {remaining} remaining")
            
            # Get detailed error response
            other_cookies = cookie_usage_tracker.get_other_cookies_remaining(cookie_name)
            
            return {
                "success": False,
                "linkedin_url": linkedin_url,
                "error": f"Daily rate limit exceeded for '{cookie_name}' cookies. Remaining: {remaining}",
                "profile_data": None,
                "processing_time_ms": int((time.time() - start_time) * 1000),
                "rate_limit": {
                    "is_allowed": False,
                    "remaining": remaining,
                    "cookie_used": cookie_name
                },
                "current_usage": 70,
                "limit": 70,
                "other_cookies_remaining": other_cookies,
                "reset_time": "00:00:00 UTC"
            }
        
        # Load the specified cookies
        cookies = self._load_cookies(cookie_name)
        if not cookies:
            logger.error(f"Failed to load {cookie_name} cookies")
            return {
                "success": False,
                "linkedin_url": linkedin_url,
                "error": f"Failed to load {cookie_name} cookies",
                "profile_data": None,
                "processing_time_ms": int((time.time() - start_time) * 1000),
                "rate_limit": {
                    "is_allowed": True,
                    "remaining": remaining,
                    "cookie_used": cookie_name
                }
            }
        
        try:
            # Prepare input for Apify actor
            run_input = {
                "urls": [linkedin_url],
                "cookie": cookies,
                "proxy": {
                    "useApifyProxy": True
                }
            }
            
            logger.info(f"Starting actor run for URL: {linkedin_url} using {cookie_name} cookies")
            
            # Start the actor and wait for it to finish
            run = self.client.actor(self.actor_id).call(run_input=run_input, wait_secs=300)
            
            logger.info(f"Actor run completed with status: {run.get('status')}")
            
            # Process results
            if run and run.get("status") == "SUCCEEDED":
                # Get dataset items
                dataset_items = list(self.client.dataset(run["defaultDatasetId"]).iterate_items())
                
                if dataset_items:
                    # Increment usage counter
                    remaining = await cookie_usage_tracker.increment_usage(cookie_name, 1)
                    
                    # Return the first item (should be only one for a single URL)
                    profile_data = dataset_items[0]
                    
                    # Store in database
                    db_success = await self._store_profile_in_database(profile_data, linkedin_url)
                    if db_success:
                        logger.info(f"Profile data stored in database for URL: {linkedin_url}")
                    else:
                        logger.warning(f"Failed to store profile data in database for URL: {linkedin_url}")
                    
                    return {
                        "success": True,
                        "linkedin_url": linkedin_url,
                        "error": None,
                        "profile_data": profile_data,
                        "processing_time_ms": int((time.time() - start_time) * 1000),
                        "rate_limit": {
                            "is_allowed": True,
                            "remaining": remaining,
                            "cookie_used": cookie_name
                        }
                    }
                else:
                    return {
                        "success": False,
                        "linkedin_url": linkedin_url,
                        "error": "No profile data found",
                        "profile_data": None,
                        "processing_time_ms": int((time.time() - start_time) * 1000),
                        "rate_limit": {
                            "is_allowed": True,
                            "remaining": remaining,
                            "cookie_used": cookie_name
                        }
                    }
            else:
                # Get run details for error information
                run_id = run.get("id") if run else None
                error_msg = f"Actor run failed with status: {run.get('status') if run else 'Unknown'}"
                
                if run_id:
                    try:
                        run_details = self.client.run(run_id).get()
                        error_msg = f"Actor run failed. Status: {run_details.get('status')}. Error: {run_details.get('errorMessage')}"
                    except Exception as e:
                        logger.error(f"Error getting run details: {e}")
                
                return {
                    "success": False,
                    "linkedin_url": linkedin_url,
                    "error": error_msg,
                    "profile_data": None,
                    "processing_time_ms": int((time.time() - start_time) * 1000),
                    "rate_limit": {
                        "is_allowed": True,
                        "remaining": remaining,
                        "cookie_used": cookie_name
                    }
                }
                
        except Exception as e:
            logger.error(f"Error scraping LinkedIn profile {linkedin_url}: {e}")
            
            return {
                "success": False,
                "linkedin_url": linkedin_url,
                "error": f"Scraping error: {str(e)}",
                "profile_data": None,
                "processing_time_ms": int((time.time() - start_time) * 1000),
                "rate_limit": {
                    "is_allowed": True,
                    "remaining": remaining,
                    "cookie_used": cookie_name
                }
            }
    
    async def scrape_profiles_bulk(self, linkedin_urls: List[str], cookie_name: str) -> Dict:
        """
        Scrape multiple LinkedIn profiles in bulk using specified cookies and store them in database.
        
        Args:
            linkedin_urls: List of LinkedIn profile URLs to scrape
            cookie_name: Cookie file to use (main, backup, personal)
            
        Returns:
            Dictionary with scrape results and status
        """
        start_time = time.time()
        
        # Validate URLs and filter out invalid ones
        valid_urls = []
        invalid_urls = []
        
        for url in linkedin_urls:
            if self._is_valid_linkedin_url(url):
                valid_urls.append(url)
            else:
                invalid_urls.append(url)
        
        if not valid_urls:
            logger.error("No valid LinkedIn URLs provided")
            return {
                "success": False,
                "error": "No valid LinkedIn profile URLs provided",
                "valid_count": 0,
                "invalid_count": len(invalid_urls),
                "invalid_urls": invalid_urls,
                "results": [],
                "processing_time_ms": int((time.time() - start_time) * 1000),
                "rate_limit": {
                    "is_allowed": False,
                    "remaining": 0,
                    "cookie_used": cookie_name
                }
            }
        
        # Check cookie-specific rate limit for bulk request
        url_count = len(valid_urls)
        is_allowed, remaining = await cookie_usage_tracker.check_rate_limit(cookie_name, url_count)
        
        if not is_allowed:
            logger.warning(f"Rate limit would be exceeded for bulk scraping with {cookie_name} cookies: {url_count} URLs requested, {remaining} remaining")
            
            # Get detailed error response
            other_cookies = cookie_usage_tracker.get_other_cookies_remaining(cookie_name)
            
            return {
                "success": False,
                "error": f"Daily rate limit would be exceeded for '{cookie_name}' cookies. Requested: {url_count}, Remaining: {remaining}",
                "valid_count": len(valid_urls),
                "invalid_count": len(invalid_urls),
                "invalid_urls": invalid_urls,
                "results": [],
                "processing_time_ms": int((time.time() - start_time) * 1000),
                "rate_limit": {
                    "is_allowed": False,
                    "remaining": remaining,
                    "cookie_used": cookie_name
                },
                "current_usage": 70 - remaining,
                "limit": 70,
                "other_cookies_remaining": other_cookies,
                "reset_time": "00:00:00 UTC"
            }
        
        # Load the specified cookies
        cookies = self._load_cookies(cookie_name)
        if not cookies:
            logger.error(f"Failed to load {cookie_name} cookies")
            return {
                "success": False,
                "error": f"Failed to load {cookie_name} cookies",
                "valid_count": len(valid_urls),
                "invalid_count": len(invalid_urls),
                "invalid_urls": invalid_urls,
                "results": [],
                "processing_time_ms": int((time.time() - start_time) * 1000),
                "rate_limit": {
                    "is_allowed": True,
                    "remaining": remaining,
                    "cookie_used": cookie_name
                }
            }
        
        try:
            # Prepare input for Apify actor
            run_input = {
                "urls": valid_urls,
                "cookie": cookies,
                "proxy": {
                    "useApifyProxy": True,
                    "apifyProxyCountry": "EG"  # Egypt as default
                }
            }
            
            logger.info(f"Starting actor run for {len(valid_urls)} URLs using {cookie_name} cookies")
            
            # Start the actor and wait for it to finish
            run = self.client.actor(self.actor_id).call(run_input=run_input, wait_secs=900)  # Longer timeout for bulk
            
            logger.info(f"Actor run completed with status: {run.get('status')}")
            
            # Process results
            if run and run.get("status") == "SUCCEEDED":
                # Get dataset items
                dataset_items = list(self.client.dataset(run["defaultDatasetId"]).iterate_items())
                
                if dataset_items:
                    # Increment usage counter
                    remaining = await cookie_usage_tracker.increment_usage(cookie_name, len(valid_urls))
                    
                    # Map results to their URLs and store in database
                    results = []
                    
                    # Create a map of profile URLs to their data
                    url_to_data = {}
                    for item in dataset_items:
                        if "url" in item:
                            url_to_data[item["url"]] = item
                    
                    # Create result entries for each valid URL
                    for url in valid_urls:
                        if url in url_to_data:
                            profile_data = url_to_data[url]
                            
                            # Store in database
                            db_success = await self._store_profile_in_database(profile_data, url)
                            if db_success:
                                logger.info(f"Profile data stored in database for URL: {url}")
                            else:
                                logger.warning(f"Failed to store profile data in database for URL: {url}")
                            
                            results.append({
                                "linkedin_url": url,
                                "success": True,
                                "error": None,
                                "profile_data": profile_data
                            })
                        else:
                            results.append({
                                "linkedin_url": url,
                                "success": False,
                                "error": "Profile not found in results",
                                "profile_data": None
                            })
                    
                    return {
                        "success": True,
                        "error": None,
                        "valid_count": len(valid_urls),
                        "invalid_count": len(invalid_urls),
                        "invalid_urls": invalid_urls,
                        "results": results,
                        "processing_time_ms": int((time.time() - start_time) * 1000),
                        "rate_limit": {
                            "is_allowed": True,
                            "remaining": remaining,
                            "cookie_used": cookie_name
                        }
                    }
                else:
                    return {
                        "success": False,
                        "error": "No profile data found",
                        "valid_count": len(valid_urls),
                        "invalid_count": len(invalid_urls),
                        "invalid_urls": invalid_urls,
                        "results": [],
                        "processing_time_ms": int((time.time() - start_time) * 1000),
                        "rate_limit": {
                            "is_allowed": True,
                            "remaining": remaining,
                            "cookie_used": cookie_name
                        }
                    }
            else:
                # Get run details for error information
                run_id = run.get("id") if run else None
                error_msg = f"Actor run failed with status: {run.get('status') if run else 'Unknown'}"
                
                if run_id:
                    try:
                        run_details = self.client.run(run_id).get()
                        error_msg = f"Actor run failed. Status: {run_details.get('status')}. Error: {run_details.get('errorMessage')}"
                    except Exception as e:
                        logger.error(f"Error getting run details: {e}")
                
                return {
                    "success": False,
                    "error": error_msg,
                    "valid_count": len(valid_urls),
                    "invalid_count": len(invalid_urls),
                    "invalid_urls": invalid_urls,
                    "results": [],
                    "processing_time_ms": int((time.time() - start_time) * 1000),
                    "rate_limit": {
                        "is_allowed": True,
                        "remaining": remaining,
                        "cookie_used": cookie_name
                    }
                }
                
        except Exception as e:
            logger.error(f"Error during bulk scraping: {e}")
            
            return {
                "success": False,
                "error": f"Bulk scraping error: {str(e)}",
                "valid_count": len(valid_urls),
                "invalid_count": len(invalid_urls),
                "invalid_urls": invalid_urls,
                "results": [],
                "processing_time_ms": int((time.time() - start_time) * 1000),
                "rate_limit": {
                    "is_allowed": True,
                    "remaining": remaining,
                    "cookie_used": cookie_name
                }
            }
    
    async def _store_profile_in_database(self, profile_data: Dict[str, Any], linkedin_url: str) -> bool:
        """
        Store scraped profile data in the database.
        
        Args:
            profile_data: Scraped LinkedIn profile data
            linkedin_url: LinkedIn URL from the request
            
        Returns:
            True if successful, False otherwise
        """
        try:
            success = await linkedin_profile_repo.store_profile(profile_data, linkedin_url)
            if success:
                logger.info(f"Profile stored in database successfully for {linkedin_url}")
            else:
                logger.warning(f"Failed to store profile in database for {linkedin_url} (may be due to invalid URL format)")
            return success
        except Exception as e:
            logger.warning(f"Database storage failed for {linkedin_url}: {e}")
            return False
    
    def get_cookie_usage_stats(self, cookie_name: str = None) -> Dict:
        """
        Get current cookie usage statistics.
        
        Args:
            cookie_name: Specific cookie name or None for all cookies
        
        Returns:
            Dictionary with usage statistics
        """
        return cookie_usage_tracker.get_usage_stats(cookie_name)
    
    def _is_valid_linkedin_url(self, url: str) -> bool:
        """
        Check if a URL is a valid LinkedIn profile URL.
        
        Args:
            url: URL to check
            
        Returns:
            True if URL is valid, False otherwise
        """
        if not url or not isinstance(url, str):
            return False
        
        # Basic validation for LinkedIn profile URL
        valid_patterns = [
            "linkedin.com/in/",
            "www.linkedin.com/in/"
        ]
        
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
        
        # Initialize scraper
        scraper = LinkedInScraper()
        
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
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")# services/linkedin_scraper.py
"""
LinkedIn profile scraper for the NBO Pipeline.

This module provides functionality for scraping LinkedIn profile data
using the Apify platform and storing it in the database.
"""
import os
import json
import logging
import time
from typing import Dict, List, Any, Optional, Tuple, Union
from pathlib import Path

from apify_client import ApifyClient
from config.settings import settings
from .scraper_rate_limiter import ScraperRateLimiter
# Import the new database repository
from database.repositories.linkedin_profile_repository import linkedin_profile_repo

logger = logging.getLogger(__name__)

class LinkedInScraper:
    """
    Service for scraping LinkedIn profiles using Apify.
    """
    
    def __init__(self):
        """Initialize the LinkedIn scraper."""
        # Get Apify API key from settings
        try:
            self.api_key = getattr(settings, 'APIFY_API_KEY', None)
            if not self.api_key:
                logger.error("APIFY_API_KEY not set in settings")
                raise ValueError("APIFY_API_KEY not set in settings")
        except AttributeError:
            logger.error("APIFY_API_KEY not found in settings")
            raise ValueError("APIFY_API_KEY not found in settings")
        
        # Get LinkedIn Scraper Actor ID from settings
        try:
            self.actor_id = getattr(settings, 'LINKEDIN_SCRAPER_ACTOR_ID', None)
            if not self.actor_id:
                logger.error("LINKEDIN_SCRAPER_ACTOR_ID not set in settings")
                raise ValueError("LINKEDIN_SCRAPER_ACTOR_ID not set in settings")
        except AttributeError:
            logger.error("LINKEDIN_SCRAPER_ACTOR_ID not found in settings")
            raise ValueError("LINKEDIN_SCRAPER_ACTOR_ID not found in settings")
        
        # Initialize Apify client
        self.client = ApifyClient(self.api_key)
        
        # Initialize rate limiter
        self.rate_limiter = ScraperRateLimiter()
        
        # Load cookies for authentication
        self.cookies = self._load_cookies()
        
        logger.info(f"LinkedIn scraper initialized with actor ID: {self.actor_id}")
    
    def _load_cookies(self) -> List[Dict]:
        """
        Load LinkedIn cookies from file.
        
        Returns:
            List of cookie objects or empty list if file not found
        """
        # Use direct path to cookies.json
        cookies_path = "/home/developer/nbo_linkedin_api/data/cookies.json"
        
        try:
            logger.info(f"Loading cookies from: {cookies_path}")
            with open(cookies_path, 'r') as f:
                raw_cookies = f.read()
                cookies_json = json.loads(raw_cookies)
                logger.info(f"Successfully loaded cookies JSON with {len(cookies_json)} items")
                return cookies_json
        except Exception as e:
            logger.error(f"Could not load cookies file: {e}")
            return []
    
    async def scrape_profile(self, linkedin_url: str) -> Dict:
        """
        Scrape a single LinkedIn profile and store it in database.
        
        Args:
            linkedin_url: LinkedIn profile URL to scrape
            
        Returns:
            Dictionary with scrape results and status
        """
        start_time = time.time()
        
        # Validate URL
        if not self._is_valid_linkedin_url(linkedin_url):
            logger.error(f"Invalid LinkedIn URL: {linkedin_url}")
            return {
                "success": False,
                "linkedin_url": linkedin_url,
                "error": "Invalid LinkedIn profile URL format",
                "profile_data": None,
                "processing_time_ms": int((time.time() - start_time) * 1000)
            }
        
        # Check rate limit
        is_allowed, remaining = self.rate_limiter.check_rate_limit(1)
        if not is_allowed:
            logger.warning(f"Rate limit exceeded for LinkedIn scraping: {remaining} remaining")
            return {
                "success": False,
                "linkedin_url": linkedin_url,
                "error": f"Daily rate limit exceeded. Remaining: {remaining}",
                "profile_data": None,
                "processing_time_ms": int((time.time() - start_time) * 1000),
                "rate_limit": {
                    "is_allowed": False,
                    "remaining": remaining
                }
            }
        
        try:
            # Prepare input for Apify actor
            run_input = {
                "urls": [linkedin_url],
                "cookie": self.cookies,
                "proxy": {
                    "useApifyProxy": True
                }
            }
            
            logger.info(f"Starting actor run for URL: {linkedin_url}")
            
            # Start the actor and wait for it to finish
            run = self.client.actor(self.actor_id).call(run_input=run_input, wait_secs=300)
            
            logger.info(f"Actor run completed with status: {run.get('status')}")
            
            # Process results
            if run and run.get("status") == "SUCCEEDED":
                # Get dataset items
                dataset_items = list(self.client.dataset(run["defaultDatasetId"]).iterate_items())
                
                # Increment usage counter
                remaining = self.rate_limiter.increment_usage(1)
                
                if dataset_items:
                    # Return the first item (should be only one for a single URL)
                    profile_data = dataset_items[0]
                    
                    # Store in database
                    db_success = await self._store_profile_in_database(profile_data, linkedin_url)
                    if db_success:
                        logger.info(f"Profile data stored in database for URL: {linkedin_url}")
                    else:
                        logger.warning(f"Failed to store profile data in database for URL: {linkedin_url}")
                    
                    return {
                        "success": True,
                        "linkedin_url": linkedin_url,
                        "error": None,
                        "profile_data": profile_data,
                        "processing_time_ms": int((time.time() - start_time) * 1000),
                        "rate_limit": {
                            "is_allowed": True,
                            "remaining": remaining
                        }
                    }
                else:
                    return {
                        "success": False,
                        "linkedin_url": linkedin_url,
                        "error": "No profile data found",
                        "profile_data": None,
                        "processing_time_ms": int((time.time() - start_time) * 1000),
                        "rate_limit": {
                            "is_allowed": True,
                            "remaining": remaining
                        }
                    }
            else:
                # Get run details for error information
                run_id = run.get("id") if run else None
                error_msg = f"Actor run failed with status: {run.get('status') if run else 'Unknown'}"
                
                if run_id:
                    try:
                        run_details = self.client.run(run_id).get()
                        error_msg = f"Actor run failed. Status: {run_details.get('status')}. Error: {run_details.get('errorMessage')}"
                    except Exception as e:
                        logger.error(f"Error getting run details: {e}")
                
                return {
                    "success": False,
                    "linkedin_url": linkedin_url,
                    "error": error_msg,
                    "profile_data": None,
                    "processing_time_ms": int((time.time() - start_time) * 1000),
                    "rate_limit": {
                        "is_allowed": True,
                        "remaining": remaining
                    }
                }
                
        except Exception as e:
            logger.error(f"Error scraping LinkedIn profile {linkedin_url}: {e}")
            
            return {
                "success": False,
                "linkedin_url": linkedin_url,
                "error": f"Scraping error: {str(e)}",
                "profile_data": None,
                "processing_time_ms": int((time.time() - start_time) * 1000),
                "rate_limit": {
                    "is_allowed": True,
                    "remaining": self.rate_limiter.get_usage_stats()["remaining"]
                }
            }
    
    async def scrape_profiles_bulk(self, linkedin_urls: List[str]) -> Dict:
        """
        Scrape multiple LinkedIn profiles in bulk and store them in database.
        
        Args:
            linkedin_urls: List of LinkedIn profile URLs to scrape
            
        Returns:
            Dictionary with scrape results and status
        """
        start_time = time.time()
        
        # Validate URLs and filter out invalid ones
        valid_urls = []
        invalid_urls = []
        
        for url in linkedin_urls:
            if self._is_valid_linkedin_url(url):
                valid_urls.append(url)
            else:
                invalid_urls.append(url)
        
        if not valid_urls:
            logger.error("No valid LinkedIn URLs provided")
            return {
                "success": False,
                "error": "No valid LinkedIn profile URLs provided",
                "valid_count": 0,
                "invalid_count": len(invalid_urls),
                "invalid_urls": invalid_urls,
                "results": [],
                "processing_time_ms": int((time.time() - start_time) * 1000)
            }
        
        # Check rate limit
        url_count = len(valid_urls)
        is_allowed, remaining = self.rate_limiter.check_rate_limit(url_count)
        
        if not is_allowed:
            logger.warning(f"Rate limit would be exceeded for bulk scraping: {url_count} URLs requested, {remaining} remaining")
            return {
                "success": False,
                "error": f"Daily rate limit would be exceeded. Requested: {url_count}, Remaining: {remaining}",
                "valid_count": len(valid_urls),
                "invalid_count": len(invalid_urls),
                "invalid_urls": invalid_urls,
                "results": [],
                "processing_time_ms": int((time.time() - start_time) * 1000),
                "rate_limit": {
                    "is_allowed": False,
                    "remaining": remaining
                }
            }
        
        try:
            # Prepare input for Apify actor
            run_input = {
                "urls": valid_urls,
                "cookie": self.cookies,
                "proxy": {
                    "useApifyProxy": True,
                    "apifyProxyCountry": "EG"  # Egypt as default
                }
            }
            
            logger.info(f"Starting actor run for {len(valid_urls)} URLs")
            
            # Start the actor and wait for it to finish
            run = self.client.actor(self.actor_id).call(run_input=run_input, wait_secs=900)  # Longer timeout for bulk
            
            logger.info(f"Actor run completed with status: {run.get('status')}")
            
            # Process results
            if run and run.get("status") == "SUCCEEDED":
                # Get dataset items
                dataset_items = list(self.client.dataset(run["defaultDatasetId"]).iterate_items())
                
                # Increment usage counter
                remaining = self.rate_limiter.increment_usage(len(valid_urls))
                
                if dataset_items:
                    # Map results to their URLs and store in database
                    results = []
                    
                    # Create a map of profile URLs to their data
                    url_to_data = {}
                    for item in dataset_items:
                        if "url" in item:
                            url_to_data[item["url"]] = item
                    
                    # Create result entries for each valid URL
                    for url in valid_urls:
                        if url in url_to_data:
                            profile_data = url_to_data[url]
                            
                            # Store in database
                            db_success = await self._store_profile_in_database(profile_data, url)
                            if db_success:
                                logger.info(f"Profile data stored in database for URL: {url}")
                            else:
                                logger.warning(f"Failed to store profile data in database for URL: {url}")
                            
                            results.append({
                                "linkedin_url": url,
                                "success": True,
                                "error": None,
                                "profile_data": profile_data
                            })
                        else:
                            results.append({
                                "linkedin_url": url,
                                "success": False,
                                "error": "Profile not found in results",
                                "profile_data": None
                            })
                    
                    return {
                        "success": True,
                        "error": None,
                        "valid_count": len(valid_urls),
                        "invalid_count": len(invalid_urls),
                        "invalid_urls": invalid_urls,
                        "results": results,
                        "processing_time_ms": int((time.time() - start_time) * 1000),
                        "rate_limit": {
                            "is_allowed": True,
                            "remaining": remaining
                        }
                    }
                else:
                    return {
                        "success": False,
                        "error": "No profile data found",
                        "valid_count": len(valid_urls),
                        "invalid_count": len(invalid_urls),
                        "invalid_urls": invalid_urls,
                        "results": [],
                        "processing_time_ms": int((time.time() - start_time) * 1000),
                        "rate_limit": {
                            "is_allowed": True,
                            "remaining": remaining
                        }
                    }
            else:
                # Get run details for error information
                run_id = run.get("id") if run else None
                error_msg = f"Actor run failed with status: {run.get('status') if run else 'Unknown'}"
                
                if run_id:
                    try:
                        run_details = self.client.run(run_id).get()
                        error_msg = f"Actor run failed. Status: {run_details.get('status')}. Error: {run_details.get('errorMessage')}"
                    except Exception as e:
                        logger.error(f"Error getting run details: {e}")
                
                return {
                    "success": False,
                    "error": error_msg,
                    "valid_count": len(valid_urls),
                    "invalid_count": len(invalid_urls),
                    "invalid_urls": invalid_urls,
                    "results": [],
                    "processing_time_ms": int((time.time() - start_time) * 1000),
                    "rate_limit": {
                        "is_allowed": True,
                        "remaining": self.rate_limiter.get_usage_stats()["remaining"]
                    }
                }
                
        except Exception as e:
            logger.error(f"Error during bulk scraping: {e}")
            
            return {
                "success": False,
                "error": f"Bulk scraping error: {str(e)}",
                "valid_count": len(valid_urls),
                "invalid_count": len(invalid_urls),
                "invalid_urls": invalid_urls,
                "results": [],
                "processing_time_ms": int((time.time() - start_time) * 1000),
                "rate_limit": {
                    "is_allowed": True,
                    "remaining": self.rate_limiter.get_usage_stats()["remaining"]
                }
            }
    
    async def _store_profile_in_database(self, profile_data: Dict[str, Any], linkedin_url: str) -> bool:
        """
        Store scraped profile data in the database.
        
        Args:
            profile_data: Scraped LinkedIn profile data
            linkedin_url: LinkedIn URL from the request
            
        Returns:
            True if successful, False otherwise
        """
        try:
            success = await linkedin_profile_repo.store_profile(profile_data, linkedin_url)
            if success:
                logger.info(f"Profile stored in database successfully for {linkedin_url}")
            else:
                logger.warning(f"Failed to store profile in database for {linkedin_url} (may be due to invalid URL format)")
            return success
        except Exception as e:
            logger.warning(f"Database storage failed for {linkedin_url}: {e}")
            return False
    
    def get_rate_limit_stats(self) -> Dict:
        """
        Get current rate limit statistics.
        
        Returns:
            Dictionary with rate limit statistics
        """
        return self.rate_limiter.get_usage_stats()
    
    def _is_valid_linkedin_url(self, url: str) -> bool:
        """
        Check if a URL is a valid LinkedIn profile URL.
        
        Args:
            url: URL to check
            
        Returns:
            True if URL is valid, False otherwise
        """
        if not url or not isinstance(url, str):
            return False
        
        # Basic validation for LinkedIn profile URL
        valid_patterns = [
            "linkedin.com/in/",
            "www.linkedin.com/in/"
        ]
        
        return any(pattern in url.lower() for pattern in valid_patterns)
