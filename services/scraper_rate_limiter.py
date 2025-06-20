# services/scraper_rate_limiter.py
"""
Rate limiter for LinkedIn profile scraping.

This module provides functionality for tracking and enforcing
daily rate limits for LinkedIn profile scraping.
"""
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from utils.cloud_storage import CloudStorageManager
from config.settings import settings

logger = logging.getLogger(__name__)

class ScraperRateLimiter:
    """
    Rate limiter for LinkedIn profile scraping.
    Tracks usage and enforces daily limits using cloud storage.
    """
    
    def __init__(self):
        """Initialize the scraper rate limiter with cloud storage integration."""
        # Initialize cloud storage manager
        self.cloud_storage = CloudStorageManager(
            bucket_name="lookup_status",
            file_path="linkedin_scraper_usage.json/linkedin_scraper_usage.json"
        )
        
        # Get daily limit from settings (default: 70)
        try:
            self.daily_limit = getattr(settings, 'LINKEDIN_SCRAPER_DAILY_LIMIT', 1000)
        except AttributeError:
            logger.warning("LINKEDIN_SCRAPER_DAILY_LIMIT not found in settings, using default value of 70")
            self.daily_limit = 1000
        
        logger.info(f"LinkedIn scraper rate limiter initialized with daily limit of {self.daily_limit}")
    
    def check_rate_limit(self, urls_count: int = 1) -> Tuple[bool, int]:
        """
        Check if the scraping operation is within rate limits.
        
        Args:
            urls_count: Number of URLs to be scraped
            
        Returns:
            Tuple of (is_allowed, remaining_requests)
        """
        # Get current usage
        usage_data = self._get_usage_data()
        
        # Get today's date as string
        today = datetime.now().strftime("%Y-%m-%d")
        
        # Check if today's date exists in usage data
        if today not in usage_data or self._should_reset_count(usage_data, today):
            # Reset for new day
            usage_data[today] = {
                "count": 0,
                "last_updated": datetime.now().isoformat()
            }
            logger.info(f"Initialized tracking for new day: {today}")
        
        # Calculate remaining requests
        current_count = usage_data[today]["count"]
        remaining = self.daily_limit - current_count
        
        # Check if the request would exceed the limit
        if urls_count > remaining:
            logger.warning(f"Rate limit would be exceeded: {urls_count} requested, {remaining} remaining")
            return False, remaining
        
        logger.info(f"Rate limit check passed: {urls_count} requested, {remaining} remaining")
        return True, remaining
    
    def increment_usage(self, urls_count: int = 1) -> int:
        """
        Increment usage counter after successful scraping.
        
        Args:
            urls_count: Number of URLs that were scraped
            
        Returns:
            Remaining requests for the day
        """
        # Get current usage
        usage_data = self._get_usage_data()
        
        # Get today's date as string
        today = datetime.now().strftime("%Y-%m-%d")
        
        # Initialize if needed
        if today not in usage_data or self._should_reset_count(usage_data, today):
            usage_data[today] = {
                "count": 0,
                "last_updated": datetime.now().isoformat()
            }
        
        # Increment count
        usage_data[today]["count"] += urls_count
        usage_data[today]["last_updated"] = datetime.now().isoformat()
        
        # Calculate remaining
        remaining = self.daily_limit - usage_data[today]["count"]
        
        # Save updated usage data
        self._save_usage_data(usage_data)
        
        logger.info(f"Incremented usage by {urls_count}, {remaining} requests remaining today")
        return remaining
    
    def get_usage_stats(self) -> Dict:
        """
        Get current usage statistics.
        
        Returns:
            Dictionary with usage statistics
        """
        # Get current usage
        usage_data = self._get_usage_data()
        
        # Get today's date as string
        today = datetime.now().strftime("%Y-%m-%d")
        
        # Initialize if needed
        if today not in usage_data or self._should_reset_count(usage_data, today):
            usage_data[today] = {
                "count": 0,
                "last_updated": datetime.now().isoformat()
            }
            # Save initialized data
            self._save_usage_data(usage_data)
        
        # Calculate statistics
        current_count = usage_data[today]["count"]
        remaining = self.daily_limit - current_count
        used_percent = (current_count / self.daily_limit) * 100 if self.daily_limit > 0 else 0
        
        return {
            "date": today,
            "used": current_count,
            "limit": self.daily_limit,
            "remaining": remaining,
            "used_percent": round(used_percent, 2),
            "last_updated": usage_data[today]["last_updated"]
        }
    
    def _get_usage_data(self) -> Dict:
        """
        Get usage data from cloud storage.
        
        Returns:
            Dictionary with usage data by date
        """
        try:
            # Read from cloud storage
            data = self.cloud_storage.read_json()
            
            # If no data found, initialize with empty dict
            if not data:
                logger.info("No existing usage data found, initializing new data")
                return {}
            
            return data
        except Exception as e:
            logger.error(f"Error reading usage data: {e}")
            return {}
    
    def _save_usage_data(self, usage_data: Dict) -> bool:
        """
        Save usage data to cloud storage.
        
        Args:
            usage_data: Usage data to save
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Write to cloud storage
            success = self.cloud_storage.write_json(usage_data)
            
            if not success:
                logger.error("Failed to save usage data to cloud storage")
            
            return success
        except Exception as e:
            logger.error(f"Error saving usage data: {e}")
            return False
    
    def _should_reset_count(self, usage_data: Dict, today: str) -> bool:
        """
        Check if the count should be reset for the new day.
        Handles the case where data exists for today but might be from yesterday.
        
        Args:
            usage_data: Current usage data
            today: Today's date string
            
        Returns:
            True if count should be reset, False otherwise
        """
        # If today's date doesn't exist in data, it should be reset
        if today not in usage_data:
            return True
        
        try:
            # Parse the last_updated timestamp
            last_updated_str = usage_data[today].get("last_updated")
            if not last_updated_str:
                return True
            
            last_updated = datetime.fromisoformat(last_updated_str)
            
            # Check if the last update was yesterday or earlier
            now = datetime.now()
            today_start = datetime(now.year, now.month, now.day, 0, 0, 0)
            
            # If last update was before today, reset
            if last_updated < today_start:
                logger.info(f"Resetting count for new day. Last update was at {last_updated}")
                return True
            
            return False
        except Exception as e:
            logger.error(f"Error checking if count should be reset: {e}")
            # Default to reset for safety
            return True
