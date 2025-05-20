# services/linkedin_scraper.py
"""
LinkedIn profile scraper for the NBO Pipeline.

This module provides functionality for scraping LinkedIn profile data
using the Apify platform.
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
        Scrape a single LinkedIn profile.
        
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
        Scrape multiple LinkedIn profiles in bulk.
        
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
                    # Map results to their URLs
                    results = []
                    
                    # Create a map of profile URLs to their data
                    url_to_data = {}
                    for item in dataset_items:
                        if "url" in item:
                            url_to_data[item["url"]] = item
                    
                    # Create result entries for each valid URL
                    for url in valid_urls:
                        if url in url_to_data:
                            results.append({
                                "linkedin_url": url,
                                "success": True,
                                "error": None,
                                "profile_data": url_to_data[url]
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
