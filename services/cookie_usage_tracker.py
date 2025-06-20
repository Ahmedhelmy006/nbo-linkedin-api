# services/cookie_usage_tracker.py
"""
Cookie usage tracker for LinkedIn scraping.
Tracks daily usage per cookie file and enforces rate limits.
"""
import json
import logging
from datetime import datetime, date
from typing import Dict, Tuple, Any
from pathlib import Path

logger = logging.getLogger(__name__)

class CookieUsageTracker:
    """
    Tracks cookie usage for LinkedIn scraping with daily rate limits.
    """
    
    def __init__(self, usage_file: str = "data/cookie_usage.json"):
        """
        Initialize the cookie usage tracker.
        
        Args:
            usage_file: Path to the usage tracking JSON file
        """
        self.usage_file = Path(usage_file)
        self.daily_limit = 500
        self.cookie_names = ["main", "backup", "personal"]
        
        # Ensure the data directory exists
        self.usage_file.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Cookie usage tracker initialized with file: {self.usage_file}")
    
    def _load_usage_data(self) -> Dict[str, Any]:
        """
        Load usage data from file.
        
        Returns:
            Usage data dictionary
        """
        try:
            if self.usage_file.exists():
                with open(self.usage_file, 'r') as f:
                    data = json.load(f)
                
                # Validate and clean old data
                today = date.today().isoformat()
                if data.get('date') != today:
                    # Reset for new day
                    logger.info(f"Resetting usage counters for new day: {today}")
                    data = self._create_fresh_data(today)
                
                return data
            else:
                # Create fresh data
                today = date.today().isoformat()
                logger.info(f"Creating fresh usage data for: {today}")
                return self._create_fresh_data(today)
                
        except Exception as e:
            logger.error(f"Error loading usage data: {e}")
            # Return fresh data on error
            today = date.today().isoformat()
            return self._create_fresh_data(today)
    
    def _create_fresh_data(self, today: str) -> Dict[str, Any]:
        """
        Create fresh usage data structure.
        
        Args:
            today: Today's date in ISO format
            
        Returns:
            Fresh usage data dictionary
        """
        return {
            "date": today,
            "usage": {
                "main": 0,
                "backup": 0,
                "personal": 0
            },
            "last_updated": datetime.now().isoformat()
        }
    
    def _save_usage_data(self, data: Dict[str, Any]) -> bool:
        """
        Save usage data to file.
        
        Args:
            data: Usage data to save
            
        Returns:
            True if successful, False otherwise
        """
        try:
            data["last_updated"] = datetime.now().isoformat()
            
            with open(self.usage_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            return True
            
        except Exception as e:
            logger.error(f"Error saving usage data: {e}")
            return False
    
    async def check_rate_limit(self, cookie_name: str, requested_count: int) -> Tuple[bool, int]:
        """
        Check if the request is within rate limits for the specified cookie.
        
        Args:
            cookie_name: Name of the cookie file (main, backup, personal)
            requested_count: Number of profiles to be scraped
            
        Returns:
            Tuple of (is_allowed, remaining_requests)
        """
        try:
            # Validate cookie name
            if cookie_name not in self.cookie_names:
                logger.warning(f"Invalid cookie name: {cookie_name}")
                return False, 0
            
            # Load current usage
            usage_data = self._load_usage_data()
            current_usage = usage_data["usage"].get(cookie_name, 0)
            
            # Calculate remaining requests
            remaining = self.daily_limit - current_usage
            
            # Check if request would exceed limit
            if requested_count > remaining:
                logger.warning(f"Rate limit would be exceeded for '{cookie_name}': {requested_count} requested, {remaining} remaining")
                return False, remaining
            
            logger.info(f"Rate limit check passed for '{cookie_name}': {requested_count} requested, {remaining} remaining")
            return True, remaining
            
        except Exception as e:
            logger.error(f"Error checking rate limit: {e}")
            return False, 0
    
    async def increment_usage(self, cookie_name: str, count: int) -> int:
        """
        Increment usage counter for the specified cookie.
        
        Args:
            cookie_name: Name of the cookie file
            count: Number of profiles that were scraped
            
        Returns:
            Remaining requests for the day
        """
        try:
            # Validate cookie name
            if cookie_name not in self.cookie_names:
                logger.warning(f"Invalid cookie name: {cookie_name}")
                return 0
            
            # Load current usage
            usage_data = self._load_usage_data()
            
            # Increment usage
            current_usage = usage_data["usage"].get(cookie_name, 0)
            usage_data["usage"][cookie_name] = current_usage + count
            
            # Calculate remaining
            remaining = self.daily_limit - usage_data["usage"][cookie_name]
            
            # Save updated data
            self._save_usage_data(usage_data)
            
            logger.info(f"Incremented usage for '{cookie_name}' by {count}, {remaining} requests remaining")
            return max(0, remaining)
            
        except Exception as e:
            logger.error(f"Error incrementing usage: {e}")
            return 0
    
    def get_usage_stats(self, cookie_name: str = None) -> Dict[str, Any]:
        """
        Get usage statistics for a specific cookie or all cookies.
        
        Args:
            cookie_name: Name of the cookie file, or None for all cookies
            
        Returns:
            Usage statistics dictionary
        """
        try:
            usage_data = self._load_usage_data()
            
            if cookie_name:
                # Return stats for specific cookie
                if cookie_name not in self.cookie_names:
                    return {"error": f"Invalid cookie name: {cookie_name}"}
                
                current_usage = usage_data["usage"].get(cookie_name, 0)
                remaining = self.daily_limit - current_usage
                
                return {
                    "cookie_name": cookie_name,
                    "date": usage_data["date"],
                    "used": current_usage,
                    "limit": self.daily_limit,
                    "remaining": max(0, remaining),
                    "used_percent": round((current_usage / self.daily_limit) * 100, 2),
                    "last_updated": usage_data["last_updated"]
                }
            else:
                # Return stats for all cookies
                stats = {
                    "date": usage_data["date"],
                    "last_updated": usage_data["last_updated"],
                    "cookies": {}
                }
                
                for cookie in self.cookie_names:
                    current_usage = usage_data["usage"].get(cookie, 0)
                    remaining = self.daily_limit - current_usage
                    
                    stats["cookies"][cookie] = {
                        "used": current_usage,
                        "limit": self.daily_limit,
                        "remaining": max(0, remaining),
                        "used_percent": round((current_usage / self.daily_limit) * 100, 2)
                    }
                
                return stats
                
        except Exception as e:
            logger.error(f"Error getting usage stats: {e}")
            return {"error": str(e)}
    
    def get_other_cookies_remaining(self, exclude_cookie: str) -> Dict[str, int]:
        """
        Get remaining requests for other cookie files.
        
        Args:
            exclude_cookie: Cookie name to exclude from results
            
        Returns:
            Dictionary mapping cookie names to remaining requests
        """
        try:
            usage_data = self._load_usage_data()
            other_cookies = {}
            
            for cookie in self.cookie_names:
                if cookie != exclude_cookie:
                    current_usage = usage_data["usage"].get(cookie, 0)
                    remaining = max(0, self.daily_limit - current_usage)
                    other_cookies[cookie] = remaining
            
            return other_cookies
            
        except Exception as e:
            logger.error(f"Error getting other cookies remaining: {e}")
            return {}

# Global tracker instance
cookie_usage_tracker = CookieUsageTracker()
