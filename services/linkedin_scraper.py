# services/linkedin_scraper.py
"""
LinkedIn profile scraper for the NBO Pipeline.

This module provides functionality for scraping LinkedIn profile data
using the Apify platform with cookie-based rate limiting, JSON caching, and database storage.
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
    Service for scraping LinkedIn profiles using Apify with cookie management and JSON caching.
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
    
    def _load_cookies(self, cookie_name: str = "main") -> List[Dict]:
        """
        Load LinkedIn cookies from the specified cookie file.
        
        Args:
            cookie_name: Name of the cookie file (main, backup, personal)
        
        Returns:
            List of cookie objects or empty list if file not found
        """
        # Use direct path to cookie files - fallback to old cookies.json if new files don't exist
        if cookie_name in ["main", "backup", "personal"]:
            cookies_path = f"/home/developer/nbo_linkedin_api/data/{cookie_name}.json"
        else:
            # Fallback to original cookies.json for backward compatibility
            cookies_path = "/home/developer/nbo_linkedin_api/data/cookies.json"
        
        try:
            logger.info(f"Loading cookies from: {cookies_path}")
            with open(cookies_path, 'r') as f:
                raw_cookies = f.read()
                cookies_json = json.loads(raw_cookies)
                logger.info(f"Successfully loaded {cookie_name} cookies JSON with {len(cookies_json)} items")
                return cookies_json
        except Exception as e:
            logger.error(f"Could not load {cookie_name} cookies file: {e}")
            # Try fallback to original cookies.json
            if cookie_name != "cookies":
                logger.info("Trying fallback to original cookies.json")
                return self._load_cookies("cookies")
            return []
    
    async def scrape_profile(self, linkedin_url: str, cookie_name: str = "main") -> Dict:
        """
        Scrape a single LinkedIn profile using specified cookies and store it in database.
        First checks JSON cache to avoid unnecessary scraping.
        
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
                },
                "data_source": "validation_error"
            }
        
        # Check JSON cache first
        try:
            cached_data = await linkedin_profile_repo.check_json_cache(linkedin_url)
            if cached_data:
                logger.info(f"Returning cached data for URL: {linkedin_url}")
                return {
                    "success": True,
                    "linkedin_url": linkedin_url,
                    "error": None,
                    "profile_data": cached_data,
                    "processing_time_ms": int((time.time() - start_time) * 1000),
                    "rate_limit": {
                        "is_allowed": True,
                        "remaining": 999,  # Don't count against rate limit for cached data
                        "cookie_used": cookie_name
                    },
                    "data_source": "cached"
                }
        except Exception as e:
            logger.warning(f"Error checking JSON cache for {linkedin_url}: {e}")
        
        # If not cached, proceed with scraping
        logger.info(f"No cached data found, proceeding with scraping for: {linkedin_url}")
        
        # Check cookie-specific rate limit
        try:
            is_allowed, remaining = await cookie_usage_tracker.check_rate_limit(cookie_name, 1)
            if not is_allowed:
                logger.warning(f"Rate limit exceeded for {cookie_name} cookies: {remaining} remaining")
                
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
                    "reset_time": "00:00:00 UTC",
                    "data_source": "rate_limited"
                }
        except Exception as e:
            logger.warning(f"Cookie usage tracker not available: {e}")
            remaining = 999
        
        # Load cookies
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
                },
                "data_source": "cookie_error"
            }
        
        # Scrape with Apify
        try:
            run_input = {
                "urls": [linkedin_url],
                "cookie": cookies,
                "proxy": {"useApifyProxy": True}
            }
            
            logger.info(f"Starting actor run for URL: {linkedin_url} using {cookie_name} cookies")
            run = self.client.actor(self.actor_id).call(run_input=run_input, wait_secs=300)
            logger.info(f"Actor run completed with status: {run.get('status')}")
            
            if run and run.get("status") == "SUCCEEDED":
                dataset_items = list(self.client.dataset(run["defaultDatasetId"]).iterate_items())
                
                if dataset_items:
                    # Increment usage counter
                    try:
                        remaining = await cookie_usage_tracker.increment_usage(cookie_name, 1)
                    except Exception as e:
                        logger.warning(f"Could not increment usage counter: {e}")
                        remaining = remaining - 1 if remaining > 0 else 0
                    
                    profile_data = dataset_items[0]
                    
                    # Store in both relational database and JSON cache
                    db_success = await linkedin_profile_repo.store_profile_with_json_cache(profile_data, linkedin_url)
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
                        },
                        "data_source": "scraped"
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
                        },
                        "data_source": "scraped"
                    }
            else:
                # Handle failed runs
                error_msg = f"Actor run failed with status: {run.get('status') if run else 'Unknown'}"
                run_id = run.get("id") if run else None
                
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
                    },
                    "data_source": "scraped"
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
                },
                "data_source": "scraped"
            }
    
    async def scrape_profiles_bulk(self, linkedin_urls: List[str], cookie_name: str = "main") -> Dict:
        """
        Scrape multiple LinkedIn profiles in bulk using specified cookies and store them in database.
        Checks JSON cache first for each URL to avoid unnecessary scraping.
        
        Args:
            linkedin_urls: List of LinkedIn profile URLs to scrape
            cookie_name: Cookie file to use (main, backup, personal)
            
        Returns:
            Dictionary with scrape results and status
        """
        start_time = time.time()
        
        # Validate URLs
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
                },
                "data_source": "validation_error"
            }
        
        # Check JSON cache for each URL
        cached_results = []
        urls_to_scrape = []
        
        for url in valid_urls:
            try:
                cached_data = await linkedin_profile_repo.check_json_cache(url)
                if cached_data:
                    logger.info(f"Found cached data for URL: {url}")
                    cached_results.append({
                        "linkedin_url": url,
                        "success": True,
                        "error": None,
                        "profile_data": cached_data,
                        "data_source": "cached"
                    })
                else:
                    urls_to_scrape.append(url)
            except Exception as e:
                logger.warning(f"Error checking cache for {url}: {e}")
                urls_to_scrape.append(url)  # If cache check fails, scrape it
        
        logger.info(f"Found {len(cached_results)} cached profiles, need to scrape {len(urls_to_scrape)} profiles")
        
        # If no URLs need scraping, return cached results only
        if not urls_to_scrape:
            return {
                "success": True,
                "error": None,
                "valid_count": len(valid_urls),
                "invalid_count": len(invalid_urls),
                "invalid_urls": invalid_urls,
                "results": cached_results,
                "processing_time_ms": int((time.time() - start_time) * 1000),
                "rate_limit": {
                    "is_allowed": True,
                    "remaining": 999,  # Don't count against rate limit for cached data
                    "cookie_used": cookie_name
                },
                "data_source": "cached",
                "cached_count": len(cached_results),
                "scraped_count": 0
            }
        
        # Check rate limit for URLs that need scraping
        try:
            url_count = len(urls_to_scrape)
            is_allowed, remaining = await cookie_usage_tracker.check_rate_limit(cookie_name, url_count)
            
            if not is_allowed:
                logger.warning(f"Rate limit would be exceeded for bulk scraping with {cookie_name} cookies: {url_count} URLs requested, {remaining} remaining")
                
                other_cookies = cookie_usage_tracker.get_other_cookies_remaining(cookie_name)
                
                # Return cached results with rate limit error for new URLs
                return {
                    "success": len(cached_results) > 0,
                    "error": f"Daily rate limit would be exceeded for '{cookie_name}' cookies. Requested: {url_count}, Remaining: {remaining}. Returned {len(cached_results)} cached results.",
                    "valid_count": len(valid_urls),
                    "invalid_count": len(invalid_urls),
                    "invalid_urls": invalid_urls,
                    "results": cached_results,
                    "processing_time_ms": int((time.time() - start_time) * 1000),
                    "rate_limit": {
                        "is_allowed": False,
                        "remaining": remaining,
                        "cookie_used": cookie_name
                    },
                    "current_usage": 70 - remaining,
                    "limit": 70,
                    "other_cookies_remaining": other_cookies,
                    "reset_time": "00:00:00 UTC",
                    "data_source": "mixed",
                    "cached_count": len(cached_results),
                    "scraped_count": 0
                }
        except Exception as e:
            logger.warning(f"Cookie usage tracker not available: {e}")
            remaining = 999
        
        # Load cookies
        cookies = self._load_cookies(cookie_name)
        if not cookies:
            logger.error(f"Failed to load {cookie_name} cookies")
            return {
                "success": len(cached_results) > 0,
                "error": f"Failed to load {cookie_name} cookies. Returned {len(cached_results)} cached results.",
                "valid_count": len(valid_urls),
                "invalid_count": len(invalid_urls),
                "invalid_urls": invalid_urls,
                "results": cached_results,
                "processing_time_ms": int((time.time() - start_time) * 1000),
                "rate_limit": {
                    "is_allowed": True,
                    "remaining": remaining,
                    "cookie_used": cookie_name
                },
                "data_source": "mixed",
                "cached_count": len(cached_results),
                "scraped_count": 0
            }
        
        # Scrape remaining URLs
        try:
            run_input = {
                "urls": urls_to_scrape,
                "cookie": cookies,
                "proxy": {
                    "useApifyProxy": True,
                    "apifyProxyCountry": "EG"
                }
            }
            
            logger.info(f"Starting actor run for {len(urls_to_scrape)} URLs using {cookie_name} cookies")
            run = self.client.actor(self.actor_id).call(run_input=run_input, wait_secs=900)
            logger.info(f"Actor run completed with status: {run.get('status')}")
            
            scraped_results = []
            
            if run and run.get("status") == "SUCCEEDED":
                dataset_items = list(self.client.dataset(run["defaultDatasetId"]).iterate_items())
                
                if dataset_items:
                    # Increment usage counter
                    try:
                        remaining = await cookie_usage_tracker.increment_usage(cookie_name, len(urls_to_scrape))
                    except Exception as e:
                        logger.warning(f"Could not increment usage counter: {e}")
                        remaining = remaining - len(urls_to_scrape) if remaining > len(urls_to_scrape) else 0
                    
                    # Map results to URLs
                    url_to_data = {}
                    for item in dataset_items:
                        if "url" in item:
                            url_to_data[item["url"]] = item
                    
                    # Process each URL
                    for url in urls_to_scrape:
                        if url in url_to_data:
                            profile_data = url_to_data[url]
                            
                            # Store in both relational database and JSON cache
                            db_success = await linkedin_profile_repo.store_profile_with_json_cache(profile_data, url)
                            if db_success:
                                logger.info(f"Profile data stored in database for URL: {url}")
                            else:
                                logger.warning(f"Failed to store profile data in database for URL: {url}")
                            
                            scraped_results.append({
                                "linkedin_url": url,
                                "success": True,
                                "error": None,
                                "profile_data": profile_data,
                                "data_source": "scraped"
                            })
                        else:
                            scraped_results.append({
                                "linkedin_url": url,
                                "success": False,
                                "error": "Profile not found in results",
                                "profile_data": None,
                                "data_source": "scraped"
                            })
                else:
                    # No scraped data found
                    for url in urls_to_scrape:
                        scraped_results.append({
                            "linkedin_url": url,
                            "success": False,
                            "error": "No profile data found",
                            "profile_data": None,
                            "data_source": "scraped"
                        })
            else:
                # Handle failed runs
                error_msg = f"Actor run failed with status: {run.get('status') if run else 'Unknown'}"
                run_id = run.get("id") if run else None
                
                if run_id:
                    try:
                        run_details = self.client.run(run_id).get()
                        error_msg = f"Actor run failed. Status: {run_details.get('status')}. Error: {run_details.get('errorMessage')}"
                    except Exception as e:
                        logger.error(f"Error getting run details: {e}")
                
                for url in urls_to_scrape:
                    scraped_results.append({
                        "linkedin_url": url,
                        "success": False,
                        "error": error_msg,
                        "profile_data": None,
                        "data_source": "scraped"
                    })
            
            # Combine results
            all_results = cached_results + scraped_results
            successful_results = [r for r in all_results if r["success"]]
            overall_success = len(successful_results) > 0
            
            return {
                "success": overall_success,
                "error": None if overall_success else "All scraping attempts failed",
                "valid_count": len(valid_urls),
                "invalid_count": len(invalid_urls),
                "invalid_urls": invalid_urls,
                "results": all_results,
                "processing_time_ms": int((time.time() - start_time) * 1000),
                "rate_limit": {
                    "is_allowed": True,
                    "remaining": remaining,
                    "cookie_used": cookie_name
                },
                "data_source": "mixed" if cached_results and scraped_results else ("cached" if cached_results else "scraped"),
                "cached_count": len(cached_results),
                "scraped_count": len([r for r in scraped_results if r["success"]])
            }
                
        except Exception as e:
            logger.error(f"Error during bulk scraping: {e}")
            
            return {
                "success": len(cached_results) > 0,
                "error": f"Bulk scraping error: {str(e)}. Returned {len(cached_results)} cached results.",
                "valid_count": len(valid_urls),
                "invalid_count": len(invalid_urls),
                "invalid_urls": invalid_urls,
                "results": cached_results,
                "processing_time_ms": int((time.time() - start_time) * 1000),
                "rate_limit": {
                    "is_allowed": True,
                    "remaining": remaining,
                    "cookie_used": cookie_name
                },
                "data_source": "cached" if cached_results else "error",
                "cached_count": len(cached_results),
                "scraped_count": 0
            }
    
    def get_cookie_usage_stats(self, cookie_name: str = None) -> Dict:
        """
        Get current cookie usage statistics.
        
        Args:
            cookie_name: Specific cookie name or None for all cookies
        
        Returns:
            Dictionary with usage statistics
        """
        try:
            return cookie_usage_tracker.get_usage_stats(cookie_name)
        except Exception as e:
            logger.warning(f"Cookie usage tracker not available: {e}")
            return {"error": "Cookie usage tracker not available", "details": str(e)}
    
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
        
        valid_patterns = [
            "linkedin.com/in/",
            "www.linkedin.com/in/"
        ]
        
        return any(pattern in url.lower() for pattern in valid_patterns)
