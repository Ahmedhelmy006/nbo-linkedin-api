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
from utils.param_validator import ParamValidator
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
        last_name: Optional[str] = None,
        location_city: Optional[str] = None,
        location_state: Optional[str] = None,
        location_country: Optional[str] = None
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Find a LinkedIn profile for an email address.
        
        Args:
            subscriber_id: Subscriber ID
            email: Email address
            first_name: Provided first name (if available)
            last_name: Provided last name (if available)
            location_city: City
            location_state: State/province
            location_country: Country
            
        Returns:
            Tuple of (LinkedIn profile URL, domain used) or (None, None) if not found
        """
        logger.info(f"DEBUG: Starting find_linkedin_profile for email={email}")
        
        # Validate and sanitize parameters
        if not ParamValidator.validate_email(email):
            logger.warning(f"Invalid email format: {email}")
            return None, None
        
        sanitized_email = email.strip().lower()
        
        # Extract the domain
        domain = sanitized_email.split('@')[1] if '@' in sanitized_email else None
        
        if not domain:
            logger.warning(f"Invalid email format: {email}")
            return None, None
        
        # If first_name and last_name are both provided, use them
        extracted_first_name = None
        extracted_last_name = None
        
        # Sanitize name inputs
        sanitized_first_name = ParamValidator.sanitize_param(first_name)
        sanitized_last_name = ParamValidator.sanitize_param(last_name)
        
        if sanitized_first_name and sanitized_last_name:
            logger.info(f"DEBUG: Using provided first and last name: {sanitized_first_name} {sanitized_last_name}")
            extracted_first_name = sanitized_first_name
            extracted_last_name = sanitized_last_name
        elif sanitized_first_name:
            # Try to extract full name if only first name is provided
            logger.info(f"DEBUG: Extracting name from email with first_name={sanitized_first_name}")
            try:
                extracted_name, extraction_method = self.name_extractor.extract_name_from_email(sanitized_email, sanitized_first_name)
                logger.info(f"DEBUG: Name extraction result: name={extracted_name}, method={extraction_method}")
                
                if extracted_name:
                    # Parse first and last name
                    name_parts = extracted_name.split()
                    if len(name_parts) >= 2:
                        extracted_first_name = name_parts[0]
                        extracted_last_name = name_parts[-1]
                    else:
                        extracted_first_name = sanitized_first_name
                else:
                    extracted_first_name = sanitized_first_name
            except Exception as e:
                logger.error(f"DEBUG: Error during name extraction: {e}")
                logger.error(f"DEBUG: Traceback: {traceback.format_exc()}")
                extracted_first_name = sanitized_first_name
        else:
            # If no name is provided, extract from email
            logger.info(f"DEBUG: Extracting name from email without provided name")
            try:
                extracted_name, extraction_method = self.name_extractor.extract_name_from_email(sanitized_email)
                logger.info(f"DEBUG: Name extraction result: name={extracted_name}, method={extraction_method}")
                
                if extracted_name:
                    # Parse first and last name
                    name_parts = extracted_name.split()
                    if len(name_parts) >= 2:
                        extracted_first_name = name_parts[0]
                        extracted_last_name = name_parts[-1]
                    elif len(name_parts) == 1:
                        extracted_first_name = name_parts[0]
            except Exception as e:
                logger.error(f"DEBUG: Error during name extraction: {e}")
                logger.error(f"DEBUG: Traceback: {traceback.format_exc()}")
        
        # Log the final extracted name components
        logger.info(f"DEBUG: Final extracted name: first={extracted_first_name}, last={extracted_last_name}")
        
        # If we couldn't extract any name, use email username as a fallback
        if not extracted_first_name and not extracted_last_name:
            username = sanitized_email.split('@')[0]
            logger.info(f"DEBUG: Using email username as fallback: {username}")
            extracted_first_name = username
        
        # Prepare search parameters
        search_params = {
            "email": sanitized_email,
            "first_name": extracted_first_name,
            "last_name": extracted_last_name,
            "location_city": ParamValidator.sanitize_param(location_city),
            "location_state": ParamValidator.sanitize_param(location_state),
            "location_country": ParamValidator.sanitize_param(location_country)
        }
        
        # Perform multi-domain search to find LinkedIn profile
        logger.info(f"DEBUG: Calling google_search.find_linkedin_profile_multi_domain with parameters: {search_params}")
        try:
            linkedin_url, domain_used = await self.google_search.find_linkedin_profile_multi_domain(search_params)
            logger.info(f"DEBUG: find_linkedin_profile_multi_domain returned: URL={linkedin_url}, domain={domain_used}")
        except Exception as e:
            logger.error(f"DEBUG: Error during find_linkedin_profile_multi_domain: {e}")
            logger.error(f"DEBUG: Traceback: {traceback.format_exc()}")
            return None, None
        
        if linkedin_url:
            logger.info(f"Found LinkedIn profile for {email}: {linkedin_url} (domain: {domain_used})")
        else:
            logger.info(f"No LinkedIn profile found for {email} across all domains")
        
        return linkedin_url, domain_used
