# test_cookie_functionality.py
"""
Test script for the new cookie-based LinkedIn scraping functionality.
"""
import asyncio
import sys
import os

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.cookie_usage_tracker import cookie_usage_tracker

async def test_cookie_usage_tracker():
    """Test the cookie usage tracker functionality."""
    print("🔍 Testing Cookie Usage Tracker\n")
    
    try:
        # Test checking rate limits for different cookies
        test_cases = [
            ("main", 1),
            ("backup", 5),
            ("personal", 70),
            ("main", 71),  # Should exceed limit
            ("invalid", 1)  # Invalid cookie name
        ]
        
        for cookie_name, requested_count in test_cases:
            print(f"📊 Testing rate limit check: {cookie_name} with {requested_count} requests")
            
            is_allowed, remaining = await cookie_usage_tracker.check_rate_limit(cookie_name, requested_count)
            
            status = "✅ ALLOWED" if is_allowed else "❌ DENIED"
            print(f"   Result: {status}, Remaining: {remaining}")
        
        print("\n🔄 Testing usage increments...")
        
        # Test incrementing usage
        increment_tests = [
            ("main", 5),
            ("backup", 10),
            ("personal", 3)
        ]
        
        for cookie_name, count in increment_tests:
            print(f"📈 Incrementing {cookie_name} usage by {count}")
            remaining = await cookie_usage_tracker.increment_usage(cookie_name, count)
            print(f"   Remaining after increment: {remaining}")
        
        print("\n📊 Getting usage statistics...")
        
        # Test getting stats for individual cookies
        for cookie_name in ["main", "backup", "personal"]:
            stats = cookie_usage_tracker.get_usage_stats(cookie_name)
            print(f"   {cookie_name}: {stats}")
        
        # Test getting stats for all cookies
        all_stats = cookie_usage_tracker.get_usage_stats()
        print(f"\n📈 All cookie stats: {all_stats}")
        
        print("\n✅ Cookie usage tracker tests completed!")
        return True
        
    except Exception as e:
        print(f"❌ Cookie usage tracker test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_cookie_file_loading():
    """Test cookie file loading functionality."""
    print("\n🔍 Testing Cookie File Loading...")
    
    try:
        from services.linkedin_scraper import LinkedInScraper
        scraper = LinkedInScraper()
        
        # Test loading different cookie files
        cookie_names = ["main", "backup", "personal"]
        
        for cookie_name in cookie_names:
            print(f"🍪 Testing {cookie_name} cookie loading...")
            cookies = scraper._load_cookies(cookie_name)
            
            if cookies:
                print(f"   ✅ Successfully loaded {len(cookies)} cookies from {cookie_name}.json")
            else:
                print(f"   ⚠️  No cookies loaded from {cookie_name}.json (file may not exist yet)")
        
        print("✅ Cookie file loading tests completed!")
        return True
        
    except Exception as e:
        print(f"❌ Cookie file loading test failed: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Testing Cookie-Based LinkedIn Scraping Functionality\n")
    print("⚠️  Make sure you have the cookie JSON files in the data/ directory:")
    print("   - data/main.json")
    print("   - data/backup.json") 
    print("   - data/personal.json\n")
    
    # Run tests
    asyncio.run(test_cookie_usage_tracker())
    test_cookie_file_loading()
    
    print("\n🎉 All cookie functionality tests completed!")
    print("\n💡 To test the API endpoints, use these PowerShell commands:")
    print("""
# Test cookie usage stats
$headers = @{ "X-API-Key" = "B3gn2KwT0" }
Invoke-WebRequest -Uri 'http://35.246.197.44:8000/v1/scraper/cookie_usage' -Headers $headers

# Test single profile scraping with cookies
$headers = @{ "X-API-Key" = "B3gn2KwT0" }
Invoke-WebRequest -Uri 'http://35.246.197.44:8000/v1/scraper/lkd_scraper?linkedin_url=https://www.linkedin.com/in/jpark328&cookies=main' -Headers $headers

# Test bulk scraping with cookies (POST)
$headers = @{ "X-API-Key" = "B3gn2KwT0"; "Content-Type" = "application/json" }
$body = @{
    linkedin_urls = @("https://www.linkedin.com/in/jpark328", "https://www.linkedin.com/in/lance-kintz")
    cookies = "backup"
} | ConvertTo-Json
Invoke-WebRequest -Method POST -Uri 'http://35.246.197.44:8000/v1/scraper/lkd_bulk_scraper' -Headers $headers -Body $body
""")
