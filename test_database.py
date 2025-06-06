"""
Quick test script for database connectivity.
Run this to verify database connection works.
"""
import asyncio
import sys
import os

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.connection import db_manager
from database.repositories.subscriber_repository import subscriber_repo

async def test_database():
    """Test database connectivity and basic operations."""
    print("Testing database connectivity...")
    
    try:
        # Test basic connection
        connection_ok = await db_manager.test_connection()
        if connection_ok:
            print("✅ Database connection successful!")
        else:
            print("❌ Database connection failed!")
            return
        
        # Test subscriber stats
        print("\nTesting subscriber repository...")
        stats = await subscriber_repo.get_subscriber_stats()
        print(f"📊 Subscriber stats: {stats}")
        
        # Test checking for a subscriber (you can replace with a real email from your DB)
        test_email = "ltolbert@drhorton.com"  # Replace with an actual email from your subscribers table
        print(f"\nTesting subscriber lookup for: {test_email}")
        
        exists = await subscriber_repo.subscriber_exists(test_email)
        print(f"Subscriber exists: {exists}")
        
        if exists:
            subscriber = await subscriber_repo.get_subscriber_by_email(test_email)
            print(f"Subscriber data: {subscriber}")
            
            linkedin_url = await subscriber_repo.get_linkedin_url(test_email)
            print(f"LinkedIn URL: {linkedin_url}")
        
        print("\n✅ Database tests completed successfully!")
        
    except Exception as e:
        print(f"❌ Database test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_database())
