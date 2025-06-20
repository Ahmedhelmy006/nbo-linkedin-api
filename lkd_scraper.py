#!/usr/bin/env python3
"""
Enhanced LinkedIn Profile Scraper - All-in-One Background Service
Processes LinkedIn URLs with persistent state, verification, and rate limiting.
"""

import asyncio
import logging
import requests
import time
import json
from datetime import datetime, timedelta, date
from typing import List, Tuple, Optional, Dict
import sys
import os
from pathlib import Path

# Add project path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.connection import db_manager
from config.settings import settings

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("enhanced_scraper.log")
    ]
)

logger = logging.getLogger(__name__)

# ============================================================================
# CUSTOM EXCEPTIONS
# ============================================================================

class ProfileVerificationError(Exception):
    """Custom exception for profile verification failures."""
    pass

# ============================================================================
# SCRAPING STATE MANAGER
# ============================================================================

class ScrapingStateManager:
    """
    Manages persistent rate limiting state with immediate persistence.
    State is saved after every profile scrape to ensure no progress is lost.
    """
    
    def __init__(self, file_path: str = "data/scraping_status.json"):
        """
        Initialize the state manager.
        
        Args:
            file_path: Path to the JSON state file
        """
        self.file_path = Path(file_path)
        self.state = {}
        
        # Ensure the data directory exists
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Load existing state or create default
        self.load_state()
        
        logger.info(f"Scraping state manager initialized with file: {self.file_path}")
    
    def load_state(self) -> None:
        """Load state from JSON file or create default state."""
        try:
            if self.file_path.exists():
                with open(self.file_path, 'r') as f:
                    self.state = json.load(f)
                
                # Validate and clean state if needed
                today = date.today().isoformat()
                if self.state.get('date') != today:
                    logger.info(f"New day detected, resetting usage counters for: {today}")
                    self._create_fresh_state(today)
                else:
                    logger.info(f"Loaded existing state for {today}")
                    self._validate_state()
            else:
                logger.info("No existing state file found, creating fresh state")
                today = date.today().isoformat()
                self._create_fresh_state(today)
                
        except Exception as e:
            logger.error(f"Error loading state: {e}")
            logger.info("Creating fresh state due to load error")
            today = date.today().isoformat()
            self._create_fresh_state(today)
    
    def save_state(self) -> bool:
        """Save current state to JSON file."""
        try:
            self.state["last_updated"] = datetime.now().isoformat()
            
            with open(self.file_path, 'w') as f:
                json.dump(self.state, f, indent=2)
            
            logger.debug(f"State saved successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error saving state: {e}")
            return False
    
    def _create_fresh_state(self, today: str) -> None:
        """Create fresh state structure."""
        self.state = {
            "date": today,
            "usage": {"main": 0, "backup": 0, "personal": 0},
            "limits": {"main": 100, "backup": 70, "personal": 10},
            "last_updated": datetime.now().isoformat()
        }
        self.save_state()
        logger.info(f"Created fresh state for {today}")
    
    def _validate_state(self) -> None:
        """Validate and fix state structure if needed."""
        if "usage" not in self.state:
            self.state["usage"] = {"main": 0, "backup": 0, "personal": 0}
        
        if "limits" not in self.state:
            self.state["limits"] = {"main":100, "backup": 70, "personal": 10}
        
        # Ensure all cookie types exist
        for cookie in ["main", "backup", "personal"]:
            if cookie not in self.state["usage"]:
                self.state["usage"][cookie] = 0
            if cookie not in self.state["limits"]:
                default_limits = {"main": 100, "backup": 100, "personal": 10}
                self.state["limits"][cookie] = default_limits[cookie]
    
    def reset_if_new_day(self) -> bool:
        """Check if it's a new day and reset counters if needed."""
        today = date.today().isoformat()
        if self.state.get('date') != today:
            logger.info(f"NEW DAY - Resetting usage counters from {self.state.get('date')} to {today}")
            self._create_fresh_state(today)
            return True
        return False
    
    def check_rate_limit(self, cookie: str, count: int = 1) -> tuple[bool, int]:
        """Check if the request is within rate limits."""
        if cookie not in self.state["usage"]:
            logger.warning(f"Unknown cookie type: {cookie}")
            return False, 0
        
        current_usage = self.state["usage"][cookie]
        limit = self.state["limits"][cookie]
        remaining = limit - current_usage
        
        is_allowed = count <= remaining
        
        logger.debug(f"Rate limit check for '{cookie}': {count} requested, {remaining} remaining, allowed: {is_allowed}")
        
        return is_allowed, remaining
    
    def increment_usage(self, cookie: str, count: int = 1) -> int:
        """Increment usage counter and immediately save state."""
        if cookie not in self.state["usage"]:
            logger.warning(f"Unknown cookie type: {cookie}")
            return 0
        
        # Increment usage
        self.state["usage"][cookie] += count
        
        # Calculate remaining
        remaining = self.state["limits"][cookie] - self.state["usage"][cookie]
        remaining = max(0, remaining)  # Don't go negative
        
        # IMMEDIATELY save state after increment
        self.save_state()
        
        logger.info(f"Incremented '{cookie}' usage by {count}, {remaining} requests remaining")
        
        return remaining
    
    def get_available_cookie(self) -> Optional[str]:
        """Get next available cookie that hasn't hit daily limit."""
        for cookie in ["main", "backup", "personal"]:
            current_usage = self.state["usage"].get(cookie, 0)
            limit = self.state["limits"].get(cookie, 0)
            
            if current_usage < limit:
                logger.debug(f"Available cookie found: {cookie} ({current_usage}/{limit})")
                return cookie
        
        logger.warning("No available cookies - all limits exhausted")
        return None
    
    def get_usage_stats(self) -> Dict:
        """Get current usage statistics."""
        stats = {
            "date": self.state.get("date"),
            "last_updated": self.state.get("last_updated"),
            "total_used": sum(self.state["usage"].values()),
            "total_limit": sum(self.state["limits"].values())
        }
        
        for cookie in ["main", "backup", "personal"]:
            usage = self.state["usage"].get(cookie, 0)
            limit = self.state["limits"].get(cookie, 0)
            remaining = max(0, limit - usage)
            
            stats[cookie] = {
                "used": usage,
                "limit": limit,
                "remaining": remaining
            }
        
        return stats

# ============================================================================
# PROFILE VERIFIER
# ============================================================================

class ProfileVerifier:
    """Verifies successful profile storage and updates scraped status."""
    
    def __init__(self):
        """Initialize the profile verifier."""
        logger.info("Profile verifier initialized")
    
    async def verify_profile_stored(self, linkedin_url: str) -> bool:
        """Verify that a profile is properly stored in linkedin_json_profiles."""
        try:
            query = """
                SELECT linkedin_url, json_profile, created_at 
                FROM linkedin_json_profiles 
                WHERE linkedin_url = $1 
                AND json_profile IS NOT NULL
            """
            
            result = await db_manager.execute_single(query, linkedin_url)
            
            if result:
                logger.info(f"✅ Profile verified in storage: {linkedin_url}")
                return True
            else:
                logger.error(f"❌ Profile NOT found or has NULL data: {linkedin_url}")
                return False
                
        except Exception as e:
            error_msg = f"Database error during verification for {linkedin_url}: {e}"
            logger.error(error_msg)
            raise ProfileVerificationError(error_msg)
    
    async def mark_profile_scraped(self, subscriber_id: str, linkedin_url: str) -> bool:
        """Mark a profile as scraped in the subscribers table."""
        try:
            query = """
                UPDATE subscribers 
                SET scraped = TRUE 
                WHERE id = $1
            """
            
            success = await db_manager.execute_update(query, int(subscriber_id))
            
            if success:
                logger.info(f"✅ Marked profile as scraped: ID={subscriber_id}, URL={linkedin_url}")
                return True
            else:
                logger.error(f"❌ Failed to mark profile as scraped: ID={subscriber_id}, URL={linkedin_url}")
                return False
                
        except Exception as e:
            error_msg = f"Database error marking profile as scraped: ID={subscriber_id}, URL={linkedin_url}, Error={e}"
            logger.error(error_msg)
            raise ProfileVerificationError(error_msg)
    
    async def verify_and_mark_complete(self, subscriber_id: str, linkedin_url: str) -> None:
        """Complete verification and marking process."""
        try:
            # Step 1: Verify profile is stored
            is_stored = await self.verify_profile_stored(linkedin_url)
            
            if not is_stored:
                # Critical failure - profile should be stored but isn't
                error_details = {
                    "subscriber_id": subscriber_id,
                    "linkedin_url": linkedin_url,
                    "issue": "Profile not found in linkedin_json_profiles after successful API response",
                    "timestamp": datetime.now().isoformat()
                }
                
                self.log_verification_failure(linkedin_url, error_details)
                
                raise ProfileVerificationError(
                    f"CRITICAL: Profile storage verification failed for {linkedin_url}. "
                    f"API returned success but profile not found in database. "
                    f"This indicates a storage system bug that needs immediate attention."
                )
            
            # Step 2: Mark as scraped
            marked_success = await self.mark_profile_scraped(subscriber_id, linkedin_url)
            
            if not marked_success:
                # Critical failure - can't mark as complete
                error_details = {
                    "subscriber_id": subscriber_id,
                    "linkedin_url": linkedin_url,
                    "issue": "Failed to mark profile as scraped in subscribers table",
                    "timestamp": datetime.now().isoformat()
                }
                
                self.log_verification_failure(linkedin_url, error_details)
                
                raise ProfileVerificationError(
                    f"CRITICAL: Failed to mark profile as scraped: ID={subscriber_id}, URL={linkedin_url}. "
                    f"Profile is stored but can't update scraped status."
                )
            
            # Success
            logger.info(f"🎉 Profile processing completed successfully: {linkedin_url}")
            
        except ProfileVerificationError:
            raise
        except Exception as e:
            error_details = {
                "subscriber_id": subscriber_id,
                "linkedin_url": linkedin_url,
                "issue": f"Unexpected error during verification: {e}",
                "error_type": type(e).__name__,
                "timestamp": datetime.now().isoformat()
            }
            
            self.log_verification_failure(linkedin_url, error_details)
            
            raise ProfileVerificationError(
                f"CRITICAL: Unexpected error during profile verification for {linkedin_url}: {e}"
            )
    
    def log_verification_failure(self, url: str, details: dict) -> None:
        """Log detailed error information for debugging."""
        logger.error("=" * 80)
        logger.error("PROFILE VERIFICATION FAILURE - DEBUGGING INFORMATION")
        logger.error("=" * 80)
        logger.error(f"LinkedIn URL: {url}")
        logger.error(f"Subscriber ID: {details.get('subscriber_id', 'Unknown')}")
        logger.error(f"Issue: {details.get('issue', 'Unknown')}")
        logger.error(f"Timestamp: {details.get('timestamp', 'Unknown')}")
        logger.error("=" * 80)
        logger.error("RECOMMENDED ACTIONS:")
        logger.error("1. Check database connectivity and permissions")
        logger.error("2. Verify linkedin_json_profiles table structure")
        logger.error("3. Check API logs for storage errors")
        logger.error("4. Test manual profile scraping for this URL")
        logger.error("5. Check disk space and database constraints")
        logger.error("=" * 80)

# ============================================================================
# ENHANCED LINKEDIN SCRAPER
# ============================================================================

class EnhancedLinkedInScraper:
    """Enhanced LinkedIn scraper with persistent state and verification."""
    
    def __init__(self):
        """Initialize the enhanced scraper."""
        self.api_url = "http://34.159.101.162:8000"
        self.api_key = settings.API_KEY
        self.timeout = 300  # 5 minutes
        self.batch_size = 20
        
        # Initialize components
        self.state_manager = ScrapingStateManager()
        self.verifier = ProfileVerifier()
        
        logger.info("Enhanced LinkedIn Scraper initialized")
        logger.info(f"API URL: {self.api_url}")
        logger.info(f"Batch size: {self.batch_size}")
    
    def validate_and_fix_url(self, url: str) -> Tuple[bool, Optional[str]]:
        """
        Validate and fix LinkedIn URLs.
        
        Args:
            url: LinkedIn URL to validate and fix
            
        Returns:
            Tuple of (is_valid, fixed_url)
            - (True, fixed_url): URL is valid or was successfully fixed
            - (False, None): URL is invalid and cannot be fixed
        """
        if not url or not isinstance(url, str):
            return False, None
        
        url = url.strip()
        
        # Check if URL is already properly formatted
        if url.startswith('https://'):
            return True, url
        
        # Fix URLs starting with www.linkedin
        if url.startswith('www.linkedin'):
            fixed_url = 'https://' + url
            logger.info(f"Fixed URL: {url} → {fixed_url}")
            return True, fixed_url
        
        # Fix URLs starting with http://
        if url.startswith('http://'):
            fixed_url = url.replace('http://', 'https://')
            logger.info(f"Fixed URL: {url} → {fixed_url}")
            return True, fixed_url
        
        # All other cases are invalid
        logger.warning(f"Invalid URL that cannot be fixed: {url}")
        return False, None
    
    async def get_unscraped_urls(self) -> List[Tuple[str, str]]:
        """Get unscraped LinkedIn URLs from database."""
        try:
            query = """
                SELECT id, linkedin_profile_url 
                FROM subscribers 
                WHERE linkedin_profile_url IS NOT NULL 
                AND linkedin_profile_url != ''
                AND scraped = FALSE
                ORDER BY id
                LIMIT $1
            """
            
            results = await db_manager.execute_query(query, self.batch_size)
            
            if results:
                urls = [(str(row['id']), row['linkedin_profile_url']) for row in results]
                logger.info(f"Found {len(urls)} unscraped URLs to process")
                return urls
            else:
                logger.info("No unscraped URLs found")
                return []
                
        except Exception as e:
            logger.error(f"Database error getting unscraped URLs: {e}")
            return []
    
    async def check_for_new_profiles(self) -> int:
        """Check how many unscraped profiles exist."""
        try:
            query = """
                SELECT COUNT(*) as count 
                FROM subscribers 
                WHERE linkedin_profile_url IS NOT NULL 
                AND linkedin_profile_url != ''
                AND scraped = FALSE
            """
            
            result = await db_manager.execute_single(query)
            count = result['count'] if result else 0
            
            logger.info(f"Found {count} unscraped profiles in database")
            return count
            
        except Exception as e:
            logger.error(f"Error checking for new profiles: {e}")
            return 0
    
    def scrape_profile(self, linkedin_url: str, cookie: str) -> Tuple[bool, Optional[str]]:
        """Send scrape request to API."""
        try:
            params = {
                "linkedin_url": linkedin_url,
                "cookies": cookie
            }
            headers = {"X-API-Key": self.api_key}
            
            logger.info(f"Scraping {linkedin_url} with {cookie}")
            
            scraper_url = f"{self.api_url}/v1/scraper/lkd_scraper"
            
            response = requests.get(
                scraper_url, 
                params=params, 
                headers=headers, 
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                # Parse JSON response to check actual success
                try:
                    response_data = response.json()
                    api_success = response_data.get('success', False)
                    
                    if api_success:
                        logger.info(f"✅ API SUCCESS: {linkedin_url}")
                        return True, None
                    else:
                        # API returned 200 but success=false
                        error_msg = response_data.get('error', 'Unknown API error')
                        logger.warning(f"⚠️ API FAILED (profile not found): {linkedin_url} - {error_msg}")
                        return False, f"profile_not_found: {error_msg}"
                        
                except json.JSONDecodeError:
                    error_msg = "Invalid JSON response from API"
                    logger.error(f"❌ JSON ERROR: {linkedin_url} - {error_msg}")
                    return False, error_msg
                    
            elif response.status_code == 429:
                logger.warning(f"⚠️ RATE LIMIT: {cookie} exhausted")
                return False, "rate_limit"
            else:
                error_msg = f"API error {response.status_code}: {response.text}"
                logger.error(f"❌ API ERROR: {linkedin_url} - {error_msg}")
                return False, error_msg
                
        except requests.exceptions.Timeout:
            error_msg = "Request timeout"
            logger.error(f"⏰ TIMEOUT: {linkedin_url} - {error_msg}")
            return False, error_msg
        except Exception as e:
            error_msg = f"Request exception: {e}"
            logger.error(f"💥 EXCEPTION: {linkedin_url} - {error_msg}")
            return False, error_msg
    
    def handle_scraping_error(self, url: str, error: str) -> None:
        """Handle scraping errors without affecting cookie logic."""
        logger.warning(f"Scraping failed for {url}: {error}")
        logger.info("Continuing to next profile (not treating as cookie exhaustion)")
    
    async def process_profile_batch(self) -> bool:
        """Process a batch of profiles with verification."""
        # Reset daily counters if needed
        self.state_manager.reset_if_new_day()
        
        # Get current cookie
        current_cookie = self.state_manager.get_available_cookie()
        if not current_cookie:
            logger.info("All cookies exhausted for today")
            return False
        
        # Get URLs to process
        urls = await self.get_unscraped_urls()
        if not urls:
            logger.info("No unscraped URLs found")
            return False
        
        logger.info(f"Processing batch of {len(urls)} profiles using {current_cookie} cookies")
        
        # Process each URL
        for subscriber_id, linkedin_url in urls:
            try:
                # Validate and fix the LinkedIn URL
                is_valid, fixed_url = self.validate_and_fix_url(linkedin_url)
                
                if not is_valid:
                    # Invalid URL that cannot be fixed - mark as scraped and skip
                    logger.warning(f"Skipping invalid URL: {linkedin_url}")
                    try:
                        marked_success = await self.verifier.mark_profile_scraped(subscriber_id, linkedin_url)
                        if marked_success:
                            logger.info(f"✅ Marked invalid URL as scraped: {linkedin_url}")
                        else:
                            logger.warning(f"⚠️ Failed to mark invalid URL as scraped: {linkedin_url}")
                    except Exception as mark_error:
                        logger.error(f"Error marking invalid URL as scraped: {mark_error}")
                    continue  # Skip to next URL
                
                # Use the fixed URL for scraping
                linkedin_url = fixed_url
                
                # Check if current cookie still available
                is_allowed, remaining = self.state_manager.check_rate_limit(current_cookie, 1)
                if not is_allowed:
                    current_cookie = self.state_manager.get_available_cookie()
                    if not current_cookie:
                        logger.info("All cookies exhausted during processing")
                        break
                    logger.info(f"Switched to cookie: {current_cookie}")
                
                # Scrape profile
                success, error = self.scrape_profile(linkedin_url, current_cookie)
                
                if success:
                    # Increment usage IMMEDIATELY after successful API call
                    remaining = self.state_manager.increment_usage(current_cookie, 1)
                    
                    # Verify and mark complete
                    await self.verifier.verify_and_mark_complete(subscriber_id, linkedin_url)
                    
                    logger.info(f"Usage: {current_cookie} = {self.state_manager.state['usage'][current_cookie]}/{self.state_manager.state['limits'][current_cookie]}")
                
                elif error == "rate_limit":
                    # Mark this cookie as exhausted
                    self.state_manager.state["usage"][current_cookie] = self.state_manager.state["limits"][current_cookie]
                    self.state_manager.save_state()
                    
                    # Try to get another cookie
                    current_cookie = self.state_manager.get_available_cookie()
                    if not current_cookie:
                        logger.info("All cookies exhausted")
                        break
                    logger.info(f"Switched to cookie: {current_cookie}")
                
                elif error and error.startswith("profile_not_found"):
                    # Profile doesn't exist (404) - mark as scraped to avoid re-processing
                    logger.info(f"Profile not found (404), marking as scraped to skip in future: {linkedin_url}")
                    try:
                        marked_success = await self.verifier.mark_profile_scraped(subscriber_id, linkedin_url)
                        if marked_success:
                            logger.info(f"✅ Marked 404 profile as scraped: {linkedin_url}")
                        else:
                            logger.warning(f"⚠️ Failed to mark 404 profile as scraped: {linkedin_url}")
                    except Exception as mark_error:
                        logger.error(f"Error marking 404 profile as scraped: {mark_error}")
                
                else:
                    # Handle other errors (don't affect cookie logic)
                    self.handle_scraping_error(linkedin_url, error)
                
                # Small delay between requests
                await asyncio.sleep(3)
            
            except ProfileVerificationError as e:
                logger.error(f"CRITICAL VERIFICATION ERROR: {e}")
                logger.error("TERMINATING SCRIPT FOR DEBUGGING")
                raise
            except Exception as e:
                logger.error(f"Unexpected error processing {linkedin_url}: {e}")
                continue
        
        return True
    
    async def wait_for_new_profiles(self) -> None:
        """Wait 6 hours for new profiles to be added."""
        logger.info("All profiles scraped. Entering 6-hour sleep cycle to check for new profiles.")
        
        # Sleep for 6 hours
        sleep_seconds = 6 * 60 * 60  # 6 hours
        logger.info(f"Sleeping for {sleep_seconds} seconds (6 hours)")
        await asyncio.sleep(sleep_seconds)
        
        logger.info("6-hour sleep completed. Checking for new profiles...")
    
    def time_until_tomorrow(self) -> int:
        """Calculate seconds until midnight."""
        now = datetime.now()
        tomorrow = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        return int((tomorrow - now).total_seconds())
    
    async def run(self) -> None:
        """Main processing loop."""
        logger.info("Starting Enhanced LinkedIn Scraper")
        
        # Test API connection
        try:
            health_url = f"{self.api_url}/v1/health"
            response = requests.get(health_url, timeout=10)
            if response.status_code == 200:
                logger.info("API connection test: SUCCESS")
            else:
                logger.error(f"API connection test: FAILED - {response.status_code}")
                return
        except Exception as e:
            logger.error(f"Cannot connect to API: {e}")
            return
        
        # Main processing loop
        while True:
            try:
                # Check for unscraped profiles
                unscraped_count = await self.check_for_new_profiles()
                
                if unscraped_count > 0:
                    # Process profiles in daily cycle
                    logger.info(f"Starting daily scraping cycle for {unscraped_count} profiles")
                    
                    while True:
                        # Process batch
                        batch_success = await self.process_profile_batch()
                        
                        if not batch_success:
                            # No more profiles or cookies exhausted
                            unscraped_count = await self.check_for_new_profiles()
                            
                            if unscraped_count == 0:
                                logger.info("All profiles processed!")
                                break
                            else:
                                # Cookies exhausted but profiles remain
                                sleep_time = self.time_until_tomorrow()
                                logger.info(f"Cookies exhausted. {unscraped_count} profiles remaining. Sleeping {sleep_time} seconds until tomorrow.")
                                await asyncio.sleep(sleep_time)
                                continue
                        
                        # Log progress
                        stats = self.state_manager.get_usage_stats()
                        logger.info(f"Daily progress: {stats['total_used']}/{stats['total_limit']}")
                        
                        # Sleep before next batch
                        await asyncio.sleep(10)
                else:
                    # No unscraped profiles - enter 6-hour cycle
                    await self.wait_for_new_profiles()
                
            except ProfileVerificationError:
                # Critical error - terminate for debugging
                logger.error("Script terminated due to profile verification failure")
                break
            except KeyboardInterrupt:
                logger.info("Shutdown requested")
                break
            except Exception as e:
                logger.error(f"Main loop error: {e}")
                await asyncio.sleep(60)
        
        logger.info("Enhanced LinkedIn Scraper stopped")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

async def main():
    """Main execution function."""
    try:
        logger.info("Starting Enhanced LinkedIn Scraper")
        
        # Test database connection
        connection_ok = await db_manager.test_connection()
        if not connection_ok:
            logger.error("Database connection failed. Exiting.")
            return
        
        logger.info("Database connection successful")
        
        # Run enhanced scraper
        scraper = EnhancedLinkedInScraper()
        await scraper.run()
        
        logger.info("Enhanced scraper completed")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
