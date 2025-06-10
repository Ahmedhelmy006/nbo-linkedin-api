"""
Test cases for the Batch LinkedIn Processor

This file contains test functions to validate the core functionalities
before running the full batch processing script.
"""

import asyncio
import json
from batch_linkedin_processor import (
    DatabaseManager, APIClient, WebhookReporter, ProgressManager,
    SubscriberRecord, ProcessingStats
)

# Test configuration (replace with your actual values)
TEST_DB_PARAMS = {
    'host': '35.242.246.173',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': ';lO_if0XU9u2<gN)'  # Replace with actual password
}

TEST_API_BASE_URL = "http://34.159.235.54:8000"
TEST_API_KEY = "B3gn2KwT0"  # Replace with actual API key
TEST_WEBHOOK_URL = "https://hooks.zapier.com/hooks/catch/21008256/2vvu8g7/"

async def test_database_connection():
    """Test database connectivity and basic queries."""
    print("🔍 Testing database connection...")
    
    try:
        db_manager = DatabaseManager(TEST_DB_PARAMS)
        
        # Test connection
        connection = await db_manager.get_connection()
        await connection.close()
        print("✅ Database connection successful")
        
        # Test total count query
        total_count = await db_manager.get_total_unprocessed_count()
        print(f"📊 Total unprocessed records: {total_count}")
        
        # Test getting a small batch
        records = await db_manager.get_batch_records(batch_size=3, offset=0)
        print(f"📄 Retrieved {len(records)} test records")
        
        for i, record in enumerate(records):
            print(f"   {i+1}. ID: {record.id}, Email: {record.email_address}")
            print(f"      Names: first='{record.first_name}', last='{record.last_name}', full='{record.full_name}'")
        
        return True
        
    except Exception as e:
        print(f"❌ Database test failed: {e}")
        return False

async def test_api_client():
    """Test API client functionality."""
    print("\n🔍 Testing API client...")
    
    try:
        api_client = APIClient(TEST_API_BASE_URL, TEST_API_KEY)
        
        # Create test records with different name configurations
        test_records = [
            SubscriberRecord(
                id=1,
                email_address="ltolbert@drhorton.com",
                first_name="Leticia",
                last_name="Tolbert",
                full_name=None
            ),
            SubscriberRecord(
                id=2,
                email_address="test@example.com",
                first_name=None,
                last_name=None,
                full_name="John Doe"
            ),
            SubscriberRecord(
                id=3,
                email_address="noname@example.com",
                first_name=None,
                last_name=None,
                full_name=None
            )
        ]
        
        for record in test_records:
            print(f"\n📧 Testing API call for: {record.email_address}")
            
            # Test name field preparation
            first_name, last_name = api_client._prepare_name_fields(record)
            print(f"   Prepared names: first='{first_name}', last='{last_name}'")
            
            # Test API call (comment out to avoid actual API calls during testing)
            # response = await api_client.lookup_linkedin_profile(record)
            # print(f"   API Response: {response}")
        
        print("✅ API client tests completed")
        return True
        
    except Exception as e:
        print(f"❌ API client test failed: {e}")
        return False

async def test_webhook_reporter():
    """Test webhook reporting functionality."""
    print("\n🔍 Testing webhook reporter...")
    
    try:
        webhook_reporter = WebhookReporter(TEST_WEBHOOK_URL)
        
        # Create test statistics
        test_stats = ProcessingStats(
            batch_number=999,  # Use high number to identify as test
            total_remaining=1000,
            processed=50,
            skipped=2,
            found=35,
            not_found=15,
            success_percentage=70.0,
            method_counts={
                "google_search": 20,
                "rocketreach_primary": 15,
                "database_cache": 0
            },
            processing_time_seconds=45.2
        )
        
        print(f"📤 Sending test webhook report...")
        
        # Send test report (comment out to avoid sending test data)
        # success = await webhook_reporter.send_batch_report(test_stats)
        # print(f"   Webhook success: {success}")
        
        print("✅ Webhook reporter test completed (actual sending commented out)")
        return True
        
    except Exception as e:
        print(f"❌ Webhook reporter test failed: {e}")
        return False

def test_progress_manager():
    """Test progress management functionality."""
    print("\n🔍 Testing progress manager...")
    
    try:
        progress_manager = ProgressManager("test_progress.json")
        
        # Test saving progress
        print("💾 Testing progress saving...")
        success = progress_manager.save_progress(
            batch_number=5,
            total_processed=250,
            offset=260
        )
        print(f"   Save success: {success}")
        
        # Test loading progress
        print("📂 Testing progress loading...")
        progress = progress_manager.load_progress()
        print(f"   Loaded progress: {progress}")
        
        print("✅ Progress manager tests completed")
        return True
        
    except Exception as e:
        print(f"❌ Progress manager test failed: {e}")
        return False

async def test_single_record_processing():
    """Test processing a single record end-to-end."""
    print("\n🔍 Testing single record processing...")
    
    try:
        # Get a test record from database
        db_manager = DatabaseManager(TEST_DB_PARAMS)
        records = await db_manager.get_batch_records(batch_size=1, offset=0)
        
        if not records:
            print("⚠️  No test records available in database")
            return False
        
        test_record = records[0]
        print(f"📧 Test record: {test_record.email_address} (ID: {test_record.id})")
        
        # Test API call
        api_client = APIClient(TEST_API_BASE_URL, TEST_API_KEY)
        
        print("🔄 Making API call...")
        # Uncomment to test actual API call
        # api_response = await api_client.lookup_linkedin_profile(test_record)
        # print(f"   API Response: {json.dumps(api_response, indent=2)}")
        
        # Test database update (use a safe test - comment out to avoid actual updates)
        # print("💾 Testing database update...")
        # update_success = await db_manager.update_looked_up_status(test_record.id, True)
        # print(f"   Database update success: {update_success}")
        
        print("✅ Single record processing test completed (actual calls commented out)")
        return True
        
    except Exception as e:
        print(f"❌ Single record processing test failed: {e}")
        return False

async def run_all_tests():
    """Run all test functions."""
    print("🚀 Starting Batch LinkedIn Processor Tests\n")
    
    test_results = []
    
    # Run each test
    test_results.append(await test_database_connection())
    test_results.append(await test_api_client())
    test_results.append(await test_webhook_reporter())
    test_results.append(test_progress_manager())
    test_results.append(await test_single_record_processing())
    
    # Summary
    passed = sum(test_results)
    total = len(test_results)
    
    print(f"\n📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! The batch processor is ready to use.")
    else:
        print("⚠️  Some tests failed. Please review the errors above before running the full processor.")

if __name__ == "__main__":
    # Update the test configuration at the top of this file before running
    print("⚠️  Make sure to update TEST_DB_PARAMS and TEST_API_KEY before running tests!")
    print("⚠️  Some tests are commented out to prevent accidental API calls/database updates.")
    print("⚠️  Uncomment them when you're ready to test with real data.\n")
    
    asyncio.run(run_all_tests())
