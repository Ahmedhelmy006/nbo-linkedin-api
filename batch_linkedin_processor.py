"""
Batch LinkedIn Profile Processor

This script processes subscribers in batches to find LinkedIn URLs
using the NBO LinkedIn API, with comprehensive reporting and state management.
"""

import asyncio
import asyncpg
import requests
import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("batch_processor.log")
    ]
)

logger = logging.getLogger(__name__)

@dataclass
class ProcessingStats:
    """Statistics for batch processing."""
    batch_number: int
    total_remaining: int
    processed: int
    skipped: int
    found: int
    not_found: int
    success_percentage: float
    method_counts: Dict[str, int]
    processing_time_seconds: float

@dataclass
class SubscriberRecord:
    """Subscriber record from database."""
    id: int
    email_address: str
    first_name: Optional[str]
    last_name: Optional[str]
    full_name: Optional[str]

class DatabaseManager:
    """Handles database operations for subscriber processing."""
    
    def __init__(self, connection_params: Dict[str, Any]):
        """Initialize database manager with connection parameters."""
        self.connection_params = connection_params
        
    async def get_connection(self):
        """Get database connection."""
        return await asyncpg.connect(**self.connection_params)
    
    async def get_total_unprocessed_count(self) -> int:
        """Get total count of unprocessed active subscribers."""
        query = """
        SELECT COUNT(*) 
        FROM subscribers 
        WHERE status = 'active' 
          AND linkedin_profile_url IS NULL 
          AND (looked_up IS NULL OR looked_up = FALSE)
        """
        
        connection = await self.get_connection()
        try:
            result = await connection.fetchval(query)
            return result or 0
        finally:
            await connection.close()
    
    async def get_batch_records(self, batch_size: int, offset: int) -> List[SubscriberRecord]:
        """Get a batch of subscriber records for processing."""
        query = """
        SELECT id, email_address, first_name, last_name, full_name
        FROM subscribers 
        WHERE status = 'active' 
          AND linkedin_profile_url IS NULL 
          AND (looked_up IS NULL OR looked_up = FALSE)
        ORDER BY id DESC
        LIMIT $1 OFFSET $2
        """
        
        connection = await self.get_connection()
        try:
            rows = await connection.fetch(query, batch_size, offset)
            return [
                SubscriberRecord(
                    id=row['id'],
                    email_address=row['email_address'],
                    first_name=row['first_name'],
                    last_name=row['last_name'],
                    full_name=row['full_name']
                )
                for row in rows
            ]
        finally:
            await connection.close()
    
    async def update_looked_up_status(self, subscriber_id: int, looked_up: bool) -> bool:
        """Update the looked_up status for a subscriber."""
        query = "UPDATE subscribers SET looked_up = $1 WHERE id = $2"
        
        connection = await self.get_connection()
        try:
            await connection.execute(query, looked_up, subscriber_id)
            return True
        except Exception as e:
            logger.error(f"Failed to update looked_up status for ID {subscriber_id}: {e}")
            return False
        finally:
            await connection.close()

class APIClient:
    """Handles API calls to the LinkedIn lookup service."""
    
    def __init__(self, base_url: str, api_key: str):
        """Initialize API client."""
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            'X-API-Key': api_key,
            'Content-Type': 'application/json'
        })
    
    def _prepare_name_fields(self, record: SubscriberRecord) -> Tuple[Optional[str], Optional[str]]:
        """Prepare first_name and last_name for API call."""
        if record.full_name and record.full_name.strip():
            # Use full_name, split into first and last
            name_parts = record.full_name.strip().split()
            if len(name_parts) >= 2:
                return name_parts[0], ' '.join(name_parts[1:])
            else:
                return name_parts[0], None
        elif record.first_name and record.last_name:
            # Use first_name and last_name
            return record.first_name.strip(), record.last_name.strip()
        elif record.first_name:
            # Only first_name available
            return record.first_name.strip(), None
        elif record.last_name:
            # Only last_name available
            return None, record.last_name.strip()
        else:
            # No name available
            return None, None
    
    async def lookup_linkedin_profile(self, record: SubscriberRecord) -> Dict[str, Any]:
        """
        Call the LinkedIn lookup API for a subscriber record.
        
        Returns:
            Dict with API response or error information
        """
        try:
            # Prepare the request payload
            first_name, last_name = self._prepare_name_fields(record)
            
            payload = {
                "email": record.email_address,
                "first_name": first_name,
                "last_name": last_name,
                "location_city": None,
                "location_state": None,
                "location_country": None
            }
            
            # Remove None values
            payload = {k: v for k, v in payload.items() if v is not None}
            
            logger.debug(f"API call for {record.email_address}: {payload}")
            
            # Make the API call
            response = self.session.post(
                f"{self.base_url}/v1/lookup",
                json=payload,
                timeout=350
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"API error for {record.email_address}: {response.status_code} - {response.text}")
                return {
                    "success": False,
                    "error": f"HTTP {response.status_code}: {response.text}",
                    "api_call_failed": True
                }
                
        except Exception as e:
            logger.error(f"API call failed for {record.email_address}: {e}")
            return {
                "success": False,
                "error": str(e),
                "api_call_failed": True
            }

class WebhookReporter:
    """Handles webhook reporting."""
    
    def __init__(self, webhook_url: str):
        """Initialize webhook reporter."""
        self.webhook_url = webhook_url
        self.session = requests.Session()
    
    async def send_batch_report(self, stats: ProcessingStats) -> bool:
        """Send batch statistics to webhook."""
        try:
            payload = {
                "batch_number": stats.batch_number,
                "total_remaining": stats.total_remaining,
                "processed": stats.processed,
                "skipped": stats.skipped,
                "found": stats.found,
                "not_found": stats.not_found,
                "success_percentage": stats.success_percentage,
                "method_breakdown": stats.method_counts,
                "processing_time_seconds": stats.processing_time_seconds,
                "timestamp": datetime.now().isoformat()
            }
            
            response = self.session.post(
                self.webhook_url,
                json=payload,
                timeout=10
            )
            
            if response.status_code == 200:
                logger.info(f"Webhook report sent successfully for batch {stats.batch_number}")
                return True
            else:
                logger.error(f"Webhook failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Webhook error: {e}")
            return False

class ProgressManager:
    """Manages processing progress and state."""
    
    def __init__(self, progress_file: str = "batch_progress.json"):
        """Initialize progress manager."""
        self.progress_file = Path(progress_file)
    
    def load_progress(self) -> Dict[str, Any]:
        """Load processing progress from file."""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to load progress file: {e}")
        
        return {
            "batch_number": 0,
            "total_processed": 0,
            "offset": 0
        }
    
    def save_progress(self, batch_number: int, total_processed: int, offset: int) -> bool:
        """Save processing progress to file."""
        try:
            progress = {
                "batch_number": batch_number,
                "total_processed": total_processed,
                "offset": offset,
                "last_updated": datetime.now().isoformat()
            }
            
            with open(self.progress_file, 'w') as f:
                json.dump(progress, f, indent=2)
            
            return True
        except Exception as e:
            logger.error(f"Failed to save progress: {e}")
            return False

class BatchLinkedInProcessor:
    """Main processor for batch LinkedIn URL lookup."""
    
    def __init__(
        self,
        db_params: Dict[str, Any],
        api_base_url: str,
        api_key: str,
        webhook_url: str,
        batch_size: int = 50,
        batch_delay_minutes: int = 5
    ):
        """Initialize the batch processor."""
        self.db_manager = DatabaseManager(db_params)
        self.api_client = APIClient(api_base_url, api_key)
        self.webhook_reporter = WebhookReporter(webhook_url)
        self.progress_manager = ProgressManager()
        self.batch_size = batch_size
        self.batch_delay_seconds = batch_delay_minutes * 60
        
        logger.info(f"Batch processor initialized: batch_size={batch_size}, delay={batch_delay_minutes}min")
    
    async def process_single_record(self, record: SubscriberRecord) -> Tuple[bool, Dict[str, Any]]:
        """
        Process a single subscriber record.
        
        Returns:
            Tuple of (success, api_response)
        """
        try:
            # Call the API
            api_response = await self.api_client.lookup_linkedin_profile(record)
            
            # Determine if the API call was successful
            api_success = not api_response.get("api_call_failed", False)
            
            # Update the looked_up status
            await self.db_manager.update_looked_up_status(record.id, api_success)
            
            return api_success, api_response
            
        except Exception as e:
            logger.error(f"Failed to process record {record.id}: {e}")
            # Mark as failed
            await self.db_manager.update_looked_up_status(record.id, False)
            return False, {"error": str(e), "api_call_failed": True}
    
    async def process_batch(self, batch_number: int, offset: int) -> ProcessingStats:
        """Process a single batch of records."""
        batch_start_time = time.time()
        
        logger.info(f"Starting batch {batch_number} (offset: {offset})")
        
        # Get total remaining count
        total_remaining = await self.db_manager.get_total_unprocessed_count()
        
        # Get batch records
        records = await self.db_manager.get_batch_records(self.batch_size, offset)
        
        if not records:
            logger.info("No more records to process")
            return ProcessingStats(
                batch_number=batch_number,
                total_remaining=0,
                processed=0,
                skipped=0,
                found=0,
                not_found=0,
                success_percentage=0.0,
                method_counts={},
                processing_time_seconds=time.time() - batch_start_time
            )
        
        # Process each record
        processed = 0
        skipped = 0
        found = 0
        not_found = 0
        method_counts = {}
        
        for record in records:
            logger.info(f"Processing {record.email_address} (ID: {record.id})")
            
            success, api_response = await self.process_single_record(record)
            
            if success:
                processed += 1
                
                # Check if LinkedIn URL was found
                if api_response.get("success", False) and api_response.get("linkedin_url"):
                    found += 1
                else:
                    not_found += 1
                
                # Track method used
                method_used = api_response.get("method_used", "unknown")
                method_counts[method_used] = method_counts.get(method_used, 0) + 1
                
            else:
                skipped += 1
        
        # Calculate statistics
        success_percentage = (found / processed * 100) if processed > 0 else 0.0
        processing_time = time.time() - batch_start_time
        
        stats = ProcessingStats(
            batch_number=batch_number,
            total_remaining=total_remaining,
            processed=processed,
            skipped=skipped,
            found=found,
            not_found=not_found,
            success_percentage=round(success_percentage, 2),
            method_counts=method_counts,
            processing_time_seconds=round(processing_time, 2)
        )
        
        logger.info(f"Batch {batch_number} completed: {processed} processed, {found} found, {skipped} skipped")
        
        return stats
    
    async def run(self) -> None:
        """Run the batch processing."""
        logger.info("Starting batch LinkedIn processing")
        
        # Load progress
        progress = self.progress_manager.load_progress()
        batch_number = progress["batch_number"]
        total_processed = progress["total_processed"]
        offset = progress["offset"]
        
        logger.info(f"Resuming from batch {batch_number + 1}, offset {offset}")
        
        while True:
            batch_number += 1
            
            # Process the batch
            stats = await self.process_batch(batch_number, offset)
            
            # If no records processed, we're done
            if stats.processed == 0 and stats.skipped == 0:
                logger.info("No more records to process. Processing complete!")
                break
            
            # Update totals
            total_processed += stats.processed
            offset += stats.processed + stats.skipped
            
            # Save progress
            self.progress_manager.save_progress(batch_number, total_processed, offset)
            
            # Send webhook report
            await self.webhook_reporter.send_batch_report(stats)
            
            # Check if we're done
            if stats.total_remaining <= (stats.processed + stats.skipped):
                logger.info("All records processed!")
                break
            
            # Wait before next batch
            logger.info(f"Waiting {self.batch_delay_seconds} seconds before next batch...")
            await asyncio.sleep(self.batch_delay_seconds)
        
        logger.info(f"Batch processing completed. Total processed: {total_processed}")


# Example usage and configuration
if __name__ == "__main__":
    # Configuration
    DB_PARAMS = {
        'host': '35.242.246.173',
        'port': 5432,
        'database': 'postgres',
        'user': 'postgres',
        'password': ';lO_if0XU9u2<gN)'  # Replace with actual password
    }
    
    API_BASE_URL = "http://35.246.197.44:8000"
    API_KEY = "B3gn2KwT0"  # Replace with actual API key
    WEBHOOK_URL = "https://hooks.zapier.com/hooks/catch/21008256/2vvu8g7/"
    
    # Create processor
    processor = BatchLinkedInProcessor(
        db_params=DB_PARAMS,
        api_base_url=API_BASE_URL,
        api_key=API_KEY,
        webhook_url=WEBHOOK_URL,
        batch_size=50,
        batch_delay_minutes=5
    )
    
    # Run the processor
    asyncio.run(processor.run())
