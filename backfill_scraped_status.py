#!/usr/bin/env python3
"""
Backfill Scraped Status Script
Marks existing scraped profiles as TRUE in subscribers table.
"""

import asyncio
import logging
import sys
import os
from typing import List, Tuple

# Add project path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.connection import db_manager

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("backfill.log")
    ]
)

logger = logging.getLogger(__name__)

class ScrapedStatusBackfill:
    def __init__(self):
        """Initialize the backfill processor."""
        self.stats = {
            'total_scraped_urls': 0,
            'successfully_updated': 0,
            'not_found_in_subscribers': 0,
            'errors': 0
        }
        logger.info("Backfill processor initialized")

    async def get_scraped_urls(self) -> List[str]:
        """
        Get all URLs from linkedin_json_profiles with non-NULL data.
        
        Returns:
            List of LinkedIn URLs that have been scraped
        """
        try:
            query = """
                SELECT linkedin_url 
                FROM linkedin_json_profiles 
                WHERE json_profile IS NOT NULL
                ORDER BY linkedin_url
            """
            
            results = await db_manager.execute_query(query)
            
            if results:
                urls = [row['linkedin_url'] for row in results]
                logger.info(f"Found {len(urls)} scraped URLs in linkedin_json_profiles")
                return urls
            else:
                logger.info("No scraped URLs found in linkedin_json_profiles")
                return []
                
        except Exception as e:
            logger.error(f"Error getting scraped URLs: {e}")
            return []

    async def update_subscriber_status(self, urls: List[str]) -> None:
        """
        Update subscribers table to mark scraped profiles as TRUE.
        
        Args:
            urls: List of LinkedIn URLs to mark as scraped
        """
        self.stats['total_scraped_urls'] = len(urls)
        
        for i, url in enumerate(urls, 1):
            try:
                # Check if URL exists in subscribers table
                check_query = """
                    SELECT id FROM subscribers 
                    WHERE linkedin_profile_url = $1
                """
                
                result = await db_manager.execute_single(check_query, url)
                
                if result:
                    # URL found, update scraped status
                    update_query = """
                        UPDATE subscribers 
                        SET scraped = TRUE 
                        WHERE linkedin_profile_url = $1
                    """
                    
                    success = await db_manager.execute_update(update_query, url)
                    
                    if success:
                        self.stats['successfully_updated'] += 1
                        logger.info(f"[{i}/{len(urls)}] Updated: {url}")
                    else:
                        self.stats['errors'] += 1
                        logger.error(f"[{i}/{len(urls)}] Failed to update: {url}")
                else:
                    # URL not found in subscribers table
                    self.stats['not_found_in_subscribers'] += 1
                    logger.warning(f"[{i}/{len(urls)}] URL not found in subscribers: {url}")
                    # Continue processing - don't let this break the entire process
                    
            except Exception as e:
                self.stats['errors'] += 1
                logger.error(f"[{i}/{len(urls)}] Error processing {url}: {e}")
                # Continue processing - don't let individual errors break everything
                continue

    async def run_backfill(self) -> None:
        """Execute the complete backfill process."""
        logger.info("Starting backfill process")
        
        # Get all scraped URLs
        scraped_urls = await self.get_scraped_urls()
        
        if not scraped_urls:
            logger.info("No URLs to process. Backfill complete.")
            return
        
        # Update subscriber status
        await self.update_subscriber_status(scraped_urls)
        
        # Generate final report
        self.generate_report()
        
        logger.info("Backfill process completed")

    def generate_report(self) -> None:
        """Generate and log comprehensive backfill statistics."""
        logger.info("=" * 50)
        logger.info("BACKFILL REPORT")
        logger.info("=" * 50)
        logger.info(f"Total URLs processed: {self.stats['total_scraped_urls']}")
        logger.info(f"Successfully updated: {self.stats['successfully_updated']}")
        logger.info(f"Not found in subscribers: {self.stats['not_found_in_subscribers']}")
        logger.info(f"Errors encountered: {self.stats['errors']}")
        logger.info("=" * 50)
        
        # Calculate success rate
        if self.stats['total_scraped_urls'] > 0:
            success_rate = (self.stats['successfully_updated'] / self.stats['total_scraped_urls']) * 100
            logger.info(f"Success rate: {success_rate:.1f}%")
        
        # Log any concerns
        if self.stats['not_found_in_subscribers'] > 0:
            logger.warning(f"Found {self.stats['not_found_in_subscribers']} orphaned profiles in linkedin_json_profiles")
            logger.warning("These profiles exist in JSON cache but not in subscribers table")
        
        if self.stats['errors'] > 0:
            logger.error(f"Encountered {self.stats['errors']} errors during processing")
            logger.error("Check logs above for specific error details")

async def main():
    """Main execution function."""
    try:
        logger.info("Starting Scraped Status Backfill")
        
        # Test database connection
        connection_ok = await db_manager.test_connection()
        if not connection_ok:
            logger.error("Database connection failed. Exiting.")
            return
        
        logger.info("Database connection successful")
        
        # Run backfill
        backfill = ScrapedStatusBackfill()
        await backfill.run_backfill()
        
        logger.info("Backfill completed successfully")
        
    except Exception as e:
        logger.error(f"Fatal error during backfill: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
