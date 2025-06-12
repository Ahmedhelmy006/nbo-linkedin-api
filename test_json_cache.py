# test_json_cache.py
"""
Test script for JSON caching functionality.
"""
import asyncio
import sys
import os

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.repositories.linkedin_profile_repository import linkedin_profile_repo

# Sample LinkedIn profile data
SAMPLE_PROFILE_DATA = {
    "id": "test123456",
    "profileId": "ACoAABW_O7kBmWD84tzPcEulqo02vHCEG89test",
    "firstName": "Test",
    "lastName": "User",
    "occupation": "Test Engineer at Test Company",
    "publicIdentifier": "test-user-123",
    "headline": "Test Engineer specializing in cache testing",
    "summary": "This is a test profile for JSON caching functionality.",
    "student": False,
    "industryName": "Information Technology",
    "companyName": "Test Company",
    "jobTitle": "Test Engineer",
    "followersCount": 100,
    "connectionsCount": 50,
    "skills": ["Testing", "JSON", "Caching", "Python"]
}

async def test_json_cache_functionality():
    """Test the JSON caching functionality."""
    print("🔍 Testing JSON Cache Functionality\n")
    
    test_url = "https://www.linkedin.com/in/test-user-123"
    
    try:
        # Test 1: Check cache for non-existent URL
        print("📝 Test 1: Checking cache for non-existent URL")
        cached_data = await linkedin_profile_repo.check_json_cache(test_url)
        if cached_data is None:
            print("✅ Correctly returned None for non-existent cache entry")
        else:
            print("❌ Unexpected: Found data for non-existent URL")
        
        # Test 2: Store JSON profile data
        print("\n📝 Test 2: Storing JSON profile data")
        store_success = await linkedin_profile_repo.store_json_profile(test_url, SAMPLE_PROFILE_DATA)
        if store_success:
            print("✅ Successfully stored JSON profile data")
        else:
            print("❌ Failed to store JSON profile data")
            return False
        
        # Test 3: Retrieve cached data
        print("\n📝 Test 3: Retrieving cached data")
        cached_data = await linkedin_profile_repo.check_json_cache(test_url)
        if cached_data:
            print("✅ Successfully retrieved cached data")
            print(f"   Cached profile ID: {cached_data.get('id')}")
            print(f"   Cached name: {cached_data.get('firstName')} {cached_data.get('lastName')}")
        else:
            print("❌ Failed to retrieve cached data")
            return False
        
        # Test 4: Verify data integrity
        print("\n📝 Test 4: Verifying data integrity")
        if (cached_data.get('id') == SAMPLE_PROFILE_DATA['id'] and 
            cached_data.get('firstName') == SAMPLE_PROFILE_DATA['firstName']):
            print("✅ Data integrity verified - cached data matches original")
        else:
            print("❌ Data integrity failed - cached data doesn't match original")
            return False
        
        # Test 5: Test update (same URL, different data)
        print("\n📝 Test 5: Testing data update")
        updated_data = SAMPLE_PROFILE_DATA.copy()
        updated_data['headline'] = "Updated Test Engineer with new responsibilities"
        updated_data['followersCount'] = 150
        
        update_success = await linkedin_profile_repo.store_json_profile(test_url, updated_data)
        if update_success:
            print("✅ Successfully updated JSON profile data")
        else:
            print("❌ Failed to update JSON profile data")
            return False
        
        # Test 6: Verify updated data
        print("\n📝 Test 6: Verifying updated data")
        updated_cached_data = await linkedin_profile_repo.check_json_cache(test_url)
        if (updated_cached_data and 
            updated_cached_data.get('headline') == updated_data['headline'] and
            updated_cached_data.get('followersCount') == updated_data['followersCount']):
            print("✅ Data update verified - cached data reflects changes")
        else:
            print("❌ Data update failed - cached data doesn't reflect changes")
            return False
        
        # Test 7: Test with different URL
        print("\n📝 Test 7: Testing with different URL")
        different_url = "https://www.linkedin.com/in/another-test-user"
        different_data = SAMPLE_PROFILE_DATA.copy()
        different_data['id'] = "different123"
        different_data['firstName'] = "Different"
        different_data['publicIdentifier'] = "another-test-user"
        
        store_different = await linkedin_profile_repo.store_json_profile(different_url, different_data)
        if store_different:
            print("✅ Successfully stored data for different URL")
        else:
            print("❌ Failed to store data for different URL")
            return False
        
        # Verify both URLs have their own data
        original_data = await linkedin_profile_repo.check_json_cache(test_url)
        different_retrieved = await linkedin_profile_repo.check_json_cache(different_url)
        
        if (original_data.get('firstName') == 'Test' and 
            different_retrieved.get('firstName') == 'Different'):
            print("✅ Multiple URLs correctly maintain separate cache entries")
        else:
            print("❌ Cache collision detected - URLs not properly separated")
            return False
        
        print("\n🎉 All JSON cache functionality tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ JSON cache test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🚀 Testing JSON Cache Functionality")
    print("⚠️  Make sure your database is running and the linkedin_json_profiles table exists!\n")
    
    asyncio.run(test_json_cache_functionality())
    
    print("\n💡 To test the full scraper with caching, try:")
    print("""
# Test scraping a new profile (will be scraped and cached)
$headers = @{ "X-API-Key" = "B3gn2KwT0" }
Invoke-WebRequest -Uri 'http://35.246.197.44:8000/v1/scraper/lkd_scraper?linkedin_url=https://www.linkedin.com/in/new-profile&cookies=main' -Headers $headers

# Test scraping the same profile again (should return cached data instantly)
Invoke-WebRequest -Uri 'http://35.246.197.44:8000/v1/scraper/lkd_scraper?linkedin_url=https://www.linkedin.com/in/new-profile&cookies=main' -Headers $headers
""")
