#!/usr/bin/env python3
"""
LinkedIn Profile Scraper - Background Service
Processes LinkedIn URLs from database and sends them to scraping API.
"""

import asyncio
import logging
import requests
import time
from datetime import datetime, timedelta
from typing import List, Tuple, Optional
import sys
import os

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
        logging.FileHandler("scraper.log")
    ]
)

logger = logging.getLogger(__name__)

class LinkedInScraper:
    def __init__(self):
        self.api_url = "http://34.159.101.162:8000"
        self.api_key = settings.API_KEY
        self.timeout = 300  # 5 minutes
        
        # Daily limits
        self.limits = {"main": 70, "backup": 70, "personal": 40}
        self.usage = {"main": 0, "backup": 0, "personal": 0}
        self.last_reset = datetime.now().date()
        
        logger.info("LinkedIn Scraper initialized")
        logger.info(f"API URL: {self.api_url}")
        logger.info(f"Timeout: {self.timeout} seconds")

    def reset_if_new_day(self):
        """Reset counters if new day"""
        today = datetime.now().date()
        if today > self.last_reset:
            logger.info("NEW DAY - Resetting usage counters")
            self.usage = {"main": 0, "backup": 0, "personal": 0}
            self.last_reset = today

    def get_available_cookie(self) -> Optional[str]:
        """Get next available cookie"""
        for cookie in ["main", "backup", "personal"]:
            if self.usage[cookie] < self.limits[cookie]:
                return cookie
        return None

    def scrape_profile(self, linkedin_url: str, cookie: str) -> bool:
        """Send scrape request to API"""
        try:
            params = {
                "linkedin_url": linkedin_url,
                "cookies": cookie
            }
            headers = {"X-API-Key": self.api_key}
            
            logger.info(f"Scraping {linkedin_url} with {cookie}")
            
            response = requests.get(
                self.api_url, 
                params=params, 
                headers=headers, 
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                logger.info(f"SUCCESS: {linkedin_url}")
                return True
            elif response.status_code == 429:
                logger.warning(f"RATE LIMIT: {cookie} exhausted")
                self.usage[cookie] = self.limits[cookie]  # Mark as exhausted
                return False
            else:
                logger.error(f"ERROR {response.status_code}: {linkedin_url}")
                return False
                
        except requests.exceptions.Timeout:
            logger.error(f"TIMEOUT: {linkedin_url}")
            return False
        except Exception as e:
            logger.error(f"EXCEPTION: {linkedin_url} - {e}")
            return False

    async def get_urls(self) -> List[Tuple[str, str]]:
        """Get LinkedIn URLs from database"""
        try:
            query = """
                SELECT id, linkedin_profile_url 
                FROM subscribers 
                WHERE linkedin_profile_url IS NOT NULL 
                AND linkedin_profile_url != ''
                ORDER BY id
                LIMIT 20
            """
            
            results = await db_manager.execute_query(query)  # Fixed: removed the parameter
            
            if results:
                urls = [(str(row['id']), row['linkedin_profile_url']) for row in results]
                logger.info(f"Found {len(urls)} URLs to process")
                return urls
            else:
                logger.info("No URLs found")
                return []
                
        except Exception as e:
            logger.error(f"Database error: {e}")
            return []

    def time_until_tomorrow(self) -> int:
        """Seconds until midnight"""
        now = datetime.now()
        tomorrow = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        return int((tomorrow - now).total_seconds())

    async def run(self):
        """Main processing loop"""
        logger.info("Starting LinkedIn Scraper")
        
        # Test API connection
        try:
            health_url = "http://34.159.101.162:8000/v1/health"
            response = requests.get(health_url, timeout=10)
            if response.status_code == 200:
                logger.info("API connection test: SUCCESS")
            else:
                logger.error(f"API connection test: FAILED - {response.status_code}")
                return
        except Exception as e:
            logger.error(f"Cannot connect to API: {e}")
            return

        # Main loop
        while True:
            try:
                # Reset daily counters if needed
                self.reset_if_new_day()
                
                # Check if any cookies available
                current_cookie = self.get_available_cookie()
                if not current_cookie:
                    sleep_time = self.time_until_tomorrow()
                    logger.info(f"All cookies exhausted. Sleeping {sleep_time} seconds until tomorrow")
                    await asyncio.sleep(sleep_time)
                    continue
                
                # Get URLs to process
                urls = await self.get_urls()
                if not urls:
                    logger.info("No URLs to process. Sleeping 5 minutes")
                    await asyncio.sleep(300)
                    continue
                
                # Process URLs
                for subscriber_id, linkedin_url in urls:
                    # Check if current cookie still available
                    if self.usage[current_cookie] >= self.limits[current_cookie]:
                        current_cookie = self.get_available_cookie()
                        if not current_cookie:
                            logger.info("All cookies exhausted during processing")
                            break
                        logger.info(f"Switched to cookie: {current_cookie}")
                    
                    # Scrape profile
                    success = self.scrape_profile(linkedin_url, current_cookie)
                    
                    if success:
                        self.usage[current_cookie] += 1
                        logger.info(f"Usage: {current_cookie} = {self.usage[current_cookie]}/{self.limits[current_cookie]}")
                    
                    # Small delay between requests
                    await asyncio.sleep(3)
                
                # Log daily progress
                total_used = sum(self.usage.values())
                total_limit = sum(self.limits.values())
                logger.info(f"Daily progress: {total_used}/{total_limit}")
                logger.info(f"Cookie usage: {self.usage}")
                
                # Sleep before next batch
                await asyncio.sleep(10)
                
            except KeyboardInterrupt:
                logger.info("Shutdown requested")
                break
            except Exception as e:
                logger.error(f"Main loop error: {e}")
                await asyncio.sleep(60)
        
        logger.info("LinkedIn Scraper stopped")

async def main():
    scraper = LinkedInScraper()
    await scraper.run()

if __name__ == "__main__":
    asyncio.run(main())
