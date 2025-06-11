# database/repositories/linkedin_profile_repository.py
"""
LinkedIn Profile repository for database operations.
Handles storing scraped LinkedIn profile data into the database.
"""
import logging
import re
from typing import Dict, Any, Optional, List
from datetime import datetime, date
from database.connection import db_manager

logger = logging.getLogger(__name__)

class LinkedInProfileRepository:
    """
    Repository for LinkedIn profile database operations.
    """
    
    async def store_profile(self, profile_data: Dict[str, Any], linkedin_url: str) -> bool:
        """
        Store LinkedIn profile data into the database.
        
        Args:
            profile_data: Scraped LinkedIn profile data
            linkedin_url: LinkedIn URL from the request
            
        Returns:
            True if successful, False otherwise
        """
        connection = None
        try:
            # Validate LinkedIn URL format first
            if not self._is_valid_linkedin_url(linkedin_url):
                logger.warning(f"Invalid LinkedIn URL format, skipping storage: {linkedin_url}")
                return False
            
            connection = await db_manager.get_connection()
            
            # Start transaction
            async with connection.transaction():
                # Check if profile already exists
                profile_id = profile_data.get('id')
                linkedin_profile_id = profile_data.get('profileId')
                
                if not profile_id:
                    logger.warning("Profile data missing required 'id' field")
                    return False
                
                # Map email address using LinkedIn profile identifier
                email_address = await self._map_email_address(connection, linkedin_url)
                
                # Check for existing profile
                existing_profile = await self._get_existing_profile(connection, profile_id, linkedin_profile_id)
                
                if existing_profile:
                    logger.info(f"Updating existing profile with ID: {profile_id}")
                    # Delete related data before updating
                    await self._delete_related_data(connection, profile_id)
                else:
                    logger.info(f"Inserting new profile with ID: {profile_id}")
                
                # Insert/Update main profile
                await self._upsert_main_profile(connection, profile_data, linkedin_url, email_address)
                
                # Insert related data
                await self._insert_positions(connection, profile_id, profile_data.get('positions', []))
                await self._insert_educations(connection, profile_id, profile_data.get('educations', []))
                await self._insert_certifications(connection, profile_id, profile_data.get('certifications', []))
                await self._insert_courses(connection, profile_id, profile_data.get('courses', []))
                await self._insert_honors(connection, profile_id, profile_data.get('honors', []))
                await self._insert_languages(connection, profile_id, profile_data.get('languages', []))
                await self._insert_skills(connection, profile_id, profile_data.get('skills', []))
                await self._insert_volunteer_experiences(connection, profile_id, profile_data.get('volunteerExperiences', []))
                
                logger.info(f"Successfully stored profile data for ID: {profile_id}, email: {email_address}")
                return True
                
        except Exception as e:
            logger.warning(f"Failed to store profile data: {e}")
            return False
        finally:
            if connection:
                await connection.close()
    
    def _is_valid_linkedin_url(self, url: str) -> bool:
        """
        Validate LinkedIn URL format.
        
        Args:
            url: LinkedIn URL to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not url or not isinstance(url, str):
            return False
        
        # Basic validation for LinkedIn profile URL
        valid_patterns = [
            r'https?://(www\.)?linkedin\.com/in/.+',
            r'https?://linkedin\.com/in/.+'
        ]
        
        return any(re.match(pattern, url.lower()) for pattern in valid_patterns)
    
    def _extract_profile_identifier(self, linkedin_url: str) -> Optional[str]:
        """
        Extract profile identifier from LinkedIn URL.
        
        Args:
            linkedin_url: LinkedIn URL
            
        Returns:
            Profile identifier or None if extraction fails
        """
        try:
            # Remove query parameters and trailing slashes
            clean_url = linkedin_url.split('?')[0].rstrip('/')
            
            # Take whatever comes after the last "/"
            profile_id = clean_url.split('/')[-1]
            
            # Validate that we got something meaningful
            if profile_id and len(profile_id) > 0 and profile_id not in ['in', 'linkedin.com', 'www.linkedin.com']:
                return profile_id
            
            return None
            
        except Exception as e:
            logger.warning(f"Error extracting profile identifier from {linkedin_url}: {e}")
            return None
    
    async def _map_email_address(self, connection, linkedin_url: str) -> str:
        """
        Map LinkedIn profile to email address using subscribers table.
        
        Args:
            connection: Database connection
            linkedin_url: LinkedIn URL from request
            
        Returns:
            Email address if found, otherwise "not_mapped_{profile_id}"
        """
        try:
            # Extract profile identifier
            profile_id = self._extract_profile_identifier(linkedin_url)
            
            if not profile_id:
                logger.warning(f"Could not extract profile ID from URL: {linkedin_url}")
                return "not_mapped_invalid"
            
            # Search in subscribers table using linkedin_profile_url column
            # We'll look for profile URLs that contain our profile_id
            query = """
            SELECT email_address 
            FROM subscribers 
            WHERE LOWER(linkedin_profile_url) LIKE LOWER($1)
            LIMIT 1
            """
            
            # Create search pattern - look for URLs containing the profile ID
            search_pattern = f"%/in/{profile_id}%"
            
            result = await connection.fetchrow(query, search_pattern)
            
            if result and result['email_address']:
                logger.info(f"Found email mapping for profile ID {profile_id}: {result['email_address']}")
                return result['email_address']
            else:
                logger.info(f"No email mapping found for profile ID: {profile_id}")
                return f"not_mapped_{profile_id}"
                
        except Exception as e:
            logger.warning(f"Error mapping email address for {linkedin_url}: {e}")
            profile_id = self._extract_profile_identifier(linkedin_url) or "unknown"
            return f"not_mapped_{profile_id}"
    
    async def _get_existing_profile(self, connection, profile_id: str, linkedin_profile_id: str) -> Optional[Dict]:
        """Check if profile already exists."""
        query = "SELECT id FROM profiles WHERE id = $1 OR profile_id = $2"
        return await connection.fetchrow(query, int(profile_id), linkedin_profile_id)
    
    async def _delete_related_data(self, connection, profile_id: str):
        """Delete all related data for a profile (for updates)."""
        tables = ['positions', 'educations', 'certifications', 'courses', 'honors', 'languages', 'skills', 'volunteer_experiences']
        
        for table in tables:
            query = f"DELETE FROM {table} WHERE profile_id = $1"
            await connection.execute(query, int(profile_id))
    
    async def _upsert_main_profile(self, connection, profile_data: Dict[str, Any], linkedin_url: str, email_address: str):
        """Insert or update main profile data."""
        query = """
        INSERT INTO profiles (
            id, profile_id, first_name, last_name, occupation, public_identifier,
            tracking_id, picture_url, cover_image_url, country_code, geo_urn,
            headline, summary, student, industry_name, industry_urn,
            geo_location_name, geo_country_name, job_title, company_name,
            company_public_id, company_linkedin_url, following, followable,
            followers_count, connections_count, connection_type,
            email_address, linkedin_url, is_aifc_member, created_at, updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
            $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32
        )
        ON CONFLICT (id) DO UPDATE SET
            profile_id = EXCLUDED.profile_id,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            occupation = EXCLUDED.occupation,
            public_identifier = EXCLUDED.public_identifier,
            tracking_id = EXCLUDED.tracking_id,
            picture_url = EXCLUDED.picture_url,
            cover_image_url = EXCLUDED.cover_image_url,
            country_code = EXCLUDED.country_code,
            geo_urn = EXCLUDED.geo_urn,
            headline = EXCLUDED.headline,
            summary = EXCLUDED.summary,
            student = EXCLUDED.student,
            industry_name = EXCLUDED.industry_name,
            industry_urn = EXCLUDED.industry_urn,
            geo_location_name = EXCLUDED.geo_location_name,
            geo_country_name = EXCLUDED.geo_country_name,
            job_title = EXCLUDED.job_title,
            company_name = EXCLUDED.company_name,
            company_public_id = EXCLUDED.company_public_id,
            company_linkedin_url = EXCLUDED.company_linkedin_url,
            following = EXCLUDED.following,
            followable = EXCLUDED.followable,
            followers_count = EXCLUDED.followers_count,
            connections_count = EXCLUDED.connections_count,
            connection_type = EXCLUDED.connection_type,
            email_address = EXCLUDED.email_address,
            linkedin_url = EXCLUDED.linkedin_url,
            is_aifc_member = EXCLUDED.is_aifc_member,
            updated_at = CURRENT_TIMESTAMP
        """
        
        await connection.execute(
            query,
            int(profile_data.get('id')),  # id
            profile_data.get('profileId'),  # profile_id
            profile_data.get('firstName'),  # first_name
            profile_data.get('lastName'),  # last_name
            profile_data.get('occupation'),  # occupation
            profile_data.get('publicIdentifier'),  # public_identifier
            profile_data.get('trackingId'),  # tracking_id
            profile_data.get('pictureUrl'),  # picture_url
            profile_data.get('coverImageUrl'),  # cover_image_url
            profile_data.get('countryCode'),  # country_code
            profile_data.get('geoUrn'),  # geo_urn
            profile_data.get('headline'),  # headline
            profile_data.get('summary'),  # summary
            profile_data.get('student', False),  # student
            profile_data.get('industryName'),  # industry_name
            profile_data.get('industryUrn'),  # industry_urn
            profile_data.get('geoLocationName'),  # geo_location_name
            profile_data.get('geoCountryName'),  # geo_country_name
            profile_data.get('jobTitle'),  # job_title
            profile_data.get('companyName'),  # company_name
            profile_data.get('companyPublicId'),  # company_public_id
            profile_data.get('companyLinkedinUrl'),  # company_linkedin_url
            profile_data.get('following', False),  # following
            profile_data.get('followable', True),  # followable
            profile_data.get('followersCount', 0),  # followers_count
            profile_data.get('connectionsCount', 0),  # connections_count
            profile_data.get('connectionType'),  # connection_type
            email_address,  # email_address (mapped from subscribers)
            linkedin_url,  # linkedin_url (from request)
            False,  # is_aifc_member (False for now)
            datetime.now(),  # created_at
            datetime.now()   # updated_at
        )
    
    async def _insert_positions(self, connection, profile_id: str, positions: List[Dict[str, Any]]):
        """Insert position data."""
        if not positions:
            return
            
        for position in positions:
            # Extract dates
            start_date = self._extract_date(position.get('timePeriod', {}).get('startDate'))
            end_date = self._extract_date(position.get('timePeriod', {}).get('endDate'))
            
            # Extract company info
            company = position.get('company', {})
            employee_count = company.get('employeeCountRange', {})
            
            query = """
            INSERT INTO positions (
                profile_id, title, description, location_name, start_date, end_date,
                company_name, company_employee_count_start, company_employee_count_end,
                company_industry, company_object_urn, company_entity_urn,
                company_showcase, company_active, company_logo_url,
                company_universal_name, company_dash_urn, company_tracking_id, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
            """
            
            await connection.execute(
                query,
                int(profile_id),
                position.get('title'),
                position.get('description'),
                position.get('locationName'),
                start_date,
                end_date,
                position.get('companyName') or company.get('name'),
                employee_count.get('start'),
                employee_count.get('end'),
                ', '.join(company.get('industries', [])) if company.get('industries') else None,
                company.get('objectUrn'),
                company.get('entityUrn'),
                company.get('showcase', False),
                company.get('active', True),
                company.get('logo'),
                company.get('universalName'),
                company.get('dashCompanyUrn'),
                company.get('trackingId'),
                datetime.now()
            )
    
    async def _insert_educations(self, connection, profile_id: str, educations: List[Dict[str, Any]]):
        """Insert education data."""
        if not educations:
            return
            
        for education in educations:
            # Extract dates
            start_date = self._extract_date(education.get('timePeriod', {}).get('startDate'))
            end_date = self._extract_date(education.get('timePeriod', {}).get('endDate'))
            
            query = """
            INSERT INTO educations (
                profile_id, degree_name, field_of_study, school_name,
                start_date, end_date, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            """
            
            await connection.execute(
                query,
                int(profile_id),
                education.get('degreeName'),
                education.get('fieldOfStudy'),
                education.get('schoolName'),
                start_date,
                end_date,
                datetime.now()
            )
    
    async def _insert_certifications(self, connection, profile_id: str, certifications: List[Dict[str, Any]]):
        """Insert certification data."""
        if not certifications:
            return
            
        for cert in certifications:
            # Extract dates
            start_date = self._extract_date(cert.get('timePeriod', {}).get('startDate'))
            end_date = self._extract_date(cert.get('timePeriod', {}).get('endDate'))
            
            query = """
            INSERT INTO certifications (
                profile_id, name, authority, start_date, end_date, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6)
            """
            
            await connection.execute(
                query,
                int(profile_id),
                cert.get('name'),
                cert.get('authority'),
                start_date,
                end_date,
                datetime.now()
            )
    
    async def _insert_courses(self, connection, profile_id: str, courses: List[Dict[str, Any]]):
        """Insert course data."""
        if not courses:
            return
            
        for course in courses:
            query = """
            INSERT INTO courses (profile_id, name, number, created_at)
            VALUES ($1, $2, $3, $4)
            """
            
            await connection.execute(
                query,
                int(profile_id),
                course.get('name'),
                course.get('number'),
                datetime.now()
            )
    
    async def _insert_honors(self, connection, profile_id: str, honors: List[Dict[str, Any]]):
        """Insert honors/awards data."""
        if not honors:
            return
            
        for honor in honors:
            # Extract date
            issue_date = self._extract_date(honor.get('issueDate'))
            
            query = """
            INSERT INTO honors (
                profile_id, title, description, issue_date, issuer, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6)
            """
            
            await connection.execute(
                query,
                int(profile_id),
                honor.get('title'),
                honor.get('description'),
                issue_date,
                honor.get('issuer'),
                datetime.now()
            )
    
    async def _insert_languages(self, connection, profile_id: str, languages: List[Dict[str, Any]]):
        """Insert language data."""
        if not languages:
            return
            
        for language in languages:
            query = """
            INSERT INTO languages (profile_id, name, proficiency, created_at)
            VALUES ($1, $2, $3, $4)
            """
            
            await connection.execute(
                query,
                int(profile_id),
                language.get('name'),
                language.get('proficiency'),
                datetime.now()
            )
    
    async def _insert_skills(self, connection, profile_id: str, skills: List[str]):
        """Insert skills data."""
        if not skills:
            return
            
        for skill in skills:
            if skill:  # Skip empty skills
                query = """
                INSERT INTO skills (profile_id, name, created_at)
                VALUES ($1, $2, $3)
                """
                
                await connection.execute(
                    query,
                    int(profile_id),
                    skill,
                    datetime.now()
                )
    
    async def _insert_volunteer_experiences(self, connection, profile_id: str, volunteer_experiences: List[Dict[str, Any]]):
        """Insert volunteer experience data."""
        if not volunteer_experiences:
            return
            
        for experience in volunteer_experiences:
            # Extract dates
            start_date = self._extract_date(experience.get('timePeriod', {}).get('startDate'))
            end_date = self._extract_date(experience.get('timePeriod', {}).get('endDate'))
            
            query = """
            INSERT INTO volunteer_experiences (
                profile_id, role, organization, description,
                start_date, end_date, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            """
            
            await connection.execute(
                query,
                int(profile_id),
                experience.get('role'),
                experience.get('organization'),
                experience.get('description'),
                start_date,
                end_date,
                datetime.now()
            )
    
    def _extract_date(self, date_obj: Optional[Dict[str, int]]) -> Optional[date]:
        """
        Extract date from LinkedIn date object format.
        
        Args:
            date_obj: Date object like {"month": 10, "year": 2020}
            
        Returns:
            date object or None
        """
        if not date_obj or not isinstance(date_obj, dict):
            return None
            
        year = date_obj.get('year')
        month = date_obj.get('month', 1)  # Default to January if month not specified
        
        if not year:
            return None
            
        try:
            # Ensure month is valid (1-12)
            month = max(1, min(12, month))
            return date(year, month, 1)  # Use first day of the month
        except (ValueError, TypeError):
            logger.warning(f"Invalid date format: {date_obj}")
            return None


# Global repository instance
linkedin_profile_repo = LinkedInProfileRepository()
