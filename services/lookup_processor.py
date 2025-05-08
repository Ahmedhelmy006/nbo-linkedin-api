"""
LinkedIn lookup processor for the NBO Pipeline.

This module provides functionality for orchestrating the LinkedIn
profile lookup process.
"""
import logging
import asyncio
import traceback
from typing import Dict, List, Any, Optional, Tuple

from .name_extractor import NameExtractor
from .google_search import GoogleSearch
logger = logging.getLogger(__name__)

class LinkedInLookupProcessor:
    """
    Orchestrates the LinkedIn profile lookup process.
    """
    
    def __init__(self):
        """Initialize the lookup processor."""
        logger.info("DEBUG: Initializing LinkedInLookupProcessor")
        self.name_extractor = NameExtractor()
        self.google_search = GoogleSearch()
        logger.info("DEBUG: LinkedInLookupProcessor initialized successfully")
    
    async def find_linkedin_profile(
        self,
        subscriber_id: str,
        email: str,
        first_name: Optional[str] = None,
        location_city: Optional[str] = None,
        location_state: Optional[str] = None,
        location_country: Optional[str] = None
    ) -> Optional[str]:
        """
        Find a LinkedIn profile for an email address.
        
        Args:
            subscriber_id: Subscriber ID
            email: Email address
            first_name: Provided first name (if available)
            location_city: City
            location_state: State/province
            location_country: Country
            
        Returns:
            LinkedIn profile URL or None if not found
        """
        logger.info(f"DEBUG: Starting find_linkedin_profile for email={email}")
        
        # Extract the domain and check if it's a work email
        domain = email.split('@')[1] if '@' in email else None
        
        if not domain:
            logger.warning(f"Invalid email format: {(email)}")
            return None
        
        # Extract name from email
        logger.info(f"DEBUG: Extracting name from email with first_name={first_name}")
        try:
            extracted_name, extraction_method = self.name_extractor.extract_name_from_email(email, first_name)
            logger.info(f"DEBUG: Name extraction result: name={extracted_name}, method={extraction_method}")
        except Exception as e:
            logger.error(f"DEBUG: Error during name extraction: {e}")
            logger.error(f"DEBUG: Traceback: {traceback.format_exc()}")
            return None
        
        if not extracted_name:
            logger.warning(f"Could not extract name from {(email)}: {extraction_method}")
            return None
        
        logger.info(f"DEBUG: Extracted name '{extracted_name}' from {(email)} using {extraction_method}")
        
        # Parse first and last name
        name_parts = extracted_name.split()
        if len(name_parts) < 2:
            logger.warning(f"Insufficient name parts extracted for {(email)}: {extracted_name}")
            return None
            
        # Extract first and last name
        extracted_first_name = name_parts[0]
        extracted_last_name = name_parts[-1]
        logger.info(f"DEBUG: Parsed name: first={extracted_first_name}, last={extracted_last_name}")
        
        # Build the search query
        search_components = []
        
        # Add name components
        search_components.append(f"{extracted_first_name} {extracted_last_name}")
        
        # Add company domain
        search_components.append(domain)
        
        # Add location information if available
        if location_state and location_state not in ["None", "null", "N/A", None, ""]:
            search_components.append(str(location_state))
        
        if location_country and location_country not in ["None", "null", "N/A", None, ""]:
            search_components.append(str(location_country))
        
        # Add LinkedIn
        search_components.append("LinkedIn")
        
        # Join with " + " format
        search_query = " + ".join(search_components)
        
        logger.info(f"DEBUG: Built search query: {search_query}")
        
        # Perform the search
        logger.info(f"DEBUG: Calling google_search.google_search with query: {search_query}")
        try:
            search_results = await self.google_search.google_search(search_query)
            logger.info(f"DEBUG: google_search returned {len(search_results) if search_results else 0} results")
            
            # Log first few results for debugging
            if search_results:
                for i, result in enumerate(search_results[:3]):
                    logger.info(f"DEBUG: Search result {i+1}: {result.get('title')} - {result.get('url')}")
            
        except Exception as e:
            logger.error(f"DEBUG: Error during google_search: {e}")
            logger.error(f"DEBUG: Traceback: {traceback.format_exc()}")
            return None
        
        if not search_results:
            logger.warning(f"No search results found for {(email)}")
            return None
        
        # Prepare member info for OpenAI
        member_info = {
            "email": email,
            "first_name": extracted_first_name,
            "last_name": extracted_last_name,
            "state": location_state,
            "country": location_country,
            "company_domain": domain
        }
        
        # Use OpenAI to analyze the search results
        logger.info(f"DEBUG: Calling google_search.query_openai with member_info and search results")
        try:
            linkedin_url = await self.google_search.query_openai(member_info, search_results)
            logger.info(f"DEBUG: query_openai returned: {linkedin_url}")
        except Exception as e:
            logger.error(f"DEBUG: Error during query_openai: {e}")
            logger.error(f"DEBUG: Traceback: {traceback.format_exc()}")
            return None
        
        if linkedin_url:
            logger.info(f"Found LinkedIn profile for {(email)}: {linkedin_url}")
        else:
            logger.info(f"No LinkedIn profile found for {(email)}")
        
        return linkedin_url
