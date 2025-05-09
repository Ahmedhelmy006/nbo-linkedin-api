# services/query_builder.py
import logging
from typing import List, Dict, Optional, Any
from utils.param_validator import ParamValidator

logger = logging.getLogger(__name__)

class QueryBuilder:
    """
    Builds optimized search queries for LinkedIn profile lookups.
    """
    
    @staticmethod
    def build_search_query(params: Dict[str, Any]) -> str:
        """
        Build a search query based on available parameters.
        
        Args:
            params: Dictionary of search parameters
            
        Returns:
            Optimized search query string
        """
        # Sanitize all parameters first
        sanitized = ParamValidator.validate_request_params(params)
        
        # Extract sanitized components
        email = sanitized.get("email", "")
        first_name = sanitized.get("first_name")
        last_name = sanitized.get("last_name")
        state = sanitized.get("state")
        country = sanitized.get("country")
        
        # Initialize components list
        components = []
        
        # Add name components if available
        if first_name and last_name:
            # Full name available
            components.append(f"{first_name} {last_name}")
        elif first_name:
            # Only first name available
            components.append(first_name)
        elif last_name:
            # Only last name available
            components.append(last_name)
        else:
            # No name available, use email username
            username = email.split('@')[0] if '@' in email else ""
            if username:
                components.append(username)
        
        # Add location components if available
        if state:
            components.append(state)
        if country:
            components.append(country)
        
        # Always add LinkedIn
        components.append("LinkedIn")
        
        # Join with " + " for Google search optimization
        search_query = " + ".join(components)
        
        logger.info(f"Built search query: {search_query}")
        return search_query
    
    @staticmethod
    def build_query_variations(params: Dict[str, Any]) -> List[str]:
        """
        Build multiple search query variations based on parameters.
        
        Args:
            params: Dictionary of search parameters
            
        Returns:
            List of search query variations
        """
        # Start with the primary query
        primary_query = QueryBuilder.build_search_query(params)
        variations = [primary_query]
        
        # Sanitize parameters
        sanitized = ParamValidator.validate_request_params(params)
        
        # Extract sanitized components
        email = sanitized.get("email", "")
        first_name = sanitized.get("first_name")
        last_name = sanitized.get("last_name")
        state = sanitized.get("state")
        country = sanitized.get("country")
        
        # Variation 1: Use "site:linkedin.com" instead of "LinkedIn"
        components = []
        if first_name and last_name:
            components.append(f"{first_name} {last_name}")
        elif first_name:
            components.append(first_name)
        
        if state:
            components.append(state)
        if country:
            components.append(country)
            
        if components:
            site_query = " ".join(components) + " site:linkedin.com"
            variations.append(site_query)
        
        # Variation 2: Just name and LinkedIn
        if first_name and last_name:
            variations.append(f"{first_name} {last_name} LinkedIn")
        
        # Variation 3: Email-based query as last resort
        if email and '@' in email:
            username = email.split('@')[0]
            domain = email.split('@')[1]
            variations.append(f"{username} LinkedIn")
        
        # Return unique variations only
        unique_variations = list(dict.fromkeys(variations))
        logger.info(f"Generated {len(unique_variations)} query variations")
        
        return unique_variations
