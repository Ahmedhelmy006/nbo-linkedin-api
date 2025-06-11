# test_linkedin_db_integration.py
"""
Test script for LinkedIn profile database integration.
Run this to verify the database integration works correctly.
"""
import asyncio
import json
import sys
import os

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.repositories.linkedin_profile_repository import linkedin_profile_repo

# Sample LinkedIn profile data (based on your examples)
SAMPLE_PROFILE_DATA = {
    "id": "364854201",
    "profileId": "ACoAABW_O7kBmWD84tzPcEulqo02vHCEG89puio",
    "firstName": "Daniel",
    "lastName": "Schorege",
    "occupation": "AI for Finance I CFO at ControlExpert Group",
    "publicIdentifier": "daniel-schorege-589232a2",
    "trackingId": "AsgT8ogQR6WYu+ryBs3WPg==",
    "pictureUrl": "https://media.licdn.com/dms/image/v2/D4E03AQEEleWU4sVZjw/profile-displayphoto-shrink_100_100/profile-displayphoto-shrink_100_100/0/1715763722326?e=1751500800&v=beta&t=gNBnBdw-3W7pjDlJEnuEvLmD5qj-pvQdHCUxOanfOJM",
    "coverImageUrl": "https://media.licdn.com/dms/image/v2/D4E16AQHyWug3IyaX0w/profile-displaybackgroundimage-shrink_200_800/profile-displaybackgroundimage-shrink_200_800/0/1723897722591?e=1751500800&v=beta&t=75GYkYkmZF5bHl0oo6xNdiaqTfRtUDyq6Mzt7fx_Llw",
    "countryCode": "de",
    "geoUrn": "urn:li:fs_geo:106491352",
    "headline": "AI for Finance I CFO at ControlExpert Group",
    "summary": "My focus as a CFO is simple yet profound: to drive profitable growth in an entrepreneurial way.",
    "student": False,
    "industryName": "Investment Banking",
    "industryUrn": "urn:li:fs_industry:45",
    "geoLocationName": "Düsseldorf, North Rhine-Westphalia",
    "geoCountryName": "Germany",
    "jobTitle": "Group CFO",
    "companyName": "ControlExpert GmbH",
    "companyPublicId": "control€xpert-gmbh",
    "companyLinkedinUrl": "https://www.linkedin.com/company/control€xpert-gmbh",
    "following": False,
    "followable": True,
    "followersCount": 789,
    "connectionsCount": 500,
    "connectionType": "",
    "positions": [
        {
            "title": "Group CFO",
            "description": "Group Finance, Corporate Development und Legal & Compliance in 32 Ländern mit über 1,000 Mitarbeitern",
            "timePeriod": {
                "startDate": {
                    "month": 10,
                    "year": 2020
                }
            },
            "company": {
                "employeeCountRange": {
                    "start": 1001,
                    "end": 5000
                },
                "industries": ["Insurance"],
                "objectUrn": "urn:li:company:5155750",
                "entityUrn": "urn:li:fs_miniCompany:5155750",
                "name": "ControlExpert GmbH",
                "showcase": False,
                "active": True,
                "logo": "https://media.licdn.com/dms/image/v2/D4E0BAQHky9Qkaj9TLA/company-logo_200_200/company-logo_200_200/0/1720009000529/controlxpert_gmbh_logo?e=1751500800&v=beta&t=oEq6tKMkngv1bU-fK93QcNySpOZeA6m8yoo1Kibp2Fg",
                "universalName": "control€xpert-gmbh",
                "dashCompanyUrn": "urn:li:fsd_company:5155750",
                "trackingId": "Fa7bUOjxTViAfM/zPn7mGQ=="
            },
            "companyName": "ControlExpert GmbH"
        }
    ],
    "educations": [
        {
            "degreeName": "Master's Degree",
            "fieldOfStudy": "MSc Investment Analysis",
            "schoolName": "Aston Business School",
            "timePeriod": {
                "endDate": {"year": 2016},
                "startDate": {"year": 2014}
            }
        }
    ],
    "certifications": [],
    "courses": [],
    "honors": [
        {
            "title": "Lloyds TSB Prize",
            "description": "Best student from the MSc Investment Analysis course.",
            "issueDate": {"month": 2, "year": 2016},
            "issuer": "Lloyds Banking Group"
        }
    ],
    "languages": [
        {"name": "English", "proficiency": "FULL_PROFESSIONAL"},
        {"name": "German", "proficiency": "NATIVE_OR_BILINGUAL"}
    ],
    "skills": ["Microsoft Office", "Teamwork", "Team Leadership", "English"],
    "volunteerExperiences": []
}

async def test_date_extraction():
    """Test the date extraction functionality."""
    print("\n🔍 Testing date extraction functionality...")
    
    from database.repositories.linkedin_profile_repository import LinkedInProfileRepository
    repo = LinkedInProfileRepository()
    
    # Test various date formats
    test_dates = [
        {"month": 10, "year": 2020},  # Normal date
        {"year": 2021},               # Year only
        {"month": 13, "year": 2020},  # Invalid month (should be clamped to 12)
        {"month": 0, "year": 2020},   # Invalid month (should be clamped to 1)
        None,                         # None
        {},                           # Empty dict
        {"invalid": "data"}           # Invalid format
    ]
    
    for i, test_date in enumerate(test_dates):
        result = repo._extract_date(test_date)
        print(f"   Test {i+1}: {test_date} -> {result}")
    
    print("✅ Date extraction tests completed!")

async def test_email_mapping_logic():
    """Test the email mapping functionality separately."""
    print("\n🔍 Testing Email Mapping Logic...")
    
    from database.repositories.linkedin_profile_repository import LinkedInProfileRepository
    repo = LinkedInProfileRepository()
    
    # Test profile ID extraction
    test_cases = [
        ("https://www.linkedin.com/in/jpark328", "jpark328"),
        ("https://linkedin.com/in/lance-kintz/", "lance-kintz"),
        ("https://www.linkedin.com/in/manish-jindal-a3b09637?param=value", "manish-jindal-a3b09637"),
        ("https://linkedin.com/in/test-profile/?utm_source=share", "test-profile"),
        ("https://www.linkedin.com/in/simple", "simple"),
        ("invalid-url", None),
        ("https://linkedin.com/in/", None),
    ]
    
    for url, expected in test_cases:
        result = repo._extract_profile_identifier(url)
        status = "✅" if result == expected else "❌"
        print(f"   {status} {url} -> {result} (expected: {expected})")
    
    print("✅ Email mapping logic tests completed!")

async def test_database_integration():
    """Test the LinkedIn profile database integration."""
    print("🔍 Testing LinkedIn Profile Database Integration\n")
    
    try:
        # Test storing the sample profile
        linkedin_url = "https://www.linkedin.com/in/daniel-schorege-589232a2"
        
        print(f"📝 Testing profile storage for: {linkedin_url}")
        print(f"   Profile ID: {SAMPLE_PROFILE_DATA['id']}")
        print(f"   Name: {SAMPLE_PROFILE_DATA['firstName']} {SAMPLE_PROFILE_DATA['lastName']}")
        print(f"   Company: {SAMPLE_PROFILE_DATA['companyName']}")
        
        # Store the profile
        success = await linkedin_profile_repo.store_profile(SAMPLE_PROFILE_DATA, linkedin_url)
        
        if success:
            print("✅ Profile stored successfully in database!")
            
            # Test updating the same profile
            print("\n🔄 Testing profile update...")
            SAMPLE_PROFILE_DATA['headline'] = "Updated: AI for Finance I CFO at ControlExpert Group"
            SAMPLE_PROFILE_DATA['followersCount'] = 800  # Changed value
            
            update_success = await linkedin_profile_repo.store_profile(SAMPLE_PROFILE_DATA, linkedin_url)
            
            if update_success:
                print("✅ Profile updated successfully!")
            else:
                print("❌ Profile update failed!")
                
        else:
            print("❌ Profile storage failed!")
            return False
        
        print("\n📊 Testing email mapping functionality...")
        
        # Test with different LinkedIn URLs to test email mapping
        test_urls = [
            "https://www.linkedin.com/in/jpark328",
            "https://linkedin.com/in/lance-kintz/",
            "https://www.linkedin.com/in/manish-jindal-a3b09637?param=value",
            "https://linkedin.com/in/nonexistent-profile",
        ]
        
        for url in test_urls:
            print(f"\n🔗 Testing URL: {url}")
            
            # Test profile ID extraction
            from database.repositories.linkedin_profile_repository import LinkedInProfileRepository
            repo = LinkedInProfileRepository()
            profile_id = repo._extract_profile_identifier(url)
            print(f"   Extracted profile ID: {profile_id}")
            
            # Note: We can't test actual email mapping without setting up test data
            # in the subscribers table, but the logic is implemented
        
        print("\n🔧 Testing URL validation...")
        
        # Test URL validation
        test_validation_urls = [
            "https://www.linkedin.com/in/valid-profile",  # Valid
            "https://linkedin.com/in/another-valid",      # Valid
            "https://facebook.com/invalid",               # Invalid
            "not-a-url",                                  # Invalid
            "",                                           # Invalid
            None                                          # Invalid
        ]
        
        repo = LinkedInProfileRepository()
        for url in test_validation_urls:
            is_valid = repo._is_valid_linkedin_url(url)
            print(f"   {url} → {'✅ Valid' if is_valid else '❌ Invalid'}")
        
        print("\n🎉 All database integration tests completed!")
        return True
        
    except Exception as e:
        print(f"❌ Database integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("⚠️  Make sure your database is running and properly configured!")
    print("⚠️  Make sure you've run the schema updates to add email_address and linkedin_url columns!")
    print("⚠️  This test will create test data in your profiles table.")
    print("⚠️  You may want to run this on a test database first.\n")
    
    asyncio.run(test_date_extraction())
    asyncio.run(test_email_mapping_logic())
    asyncio.run(test_database_integration())
