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
        
        # Always include the full email address
        if email:
            components.append(email)
        
        # Add name components if available
        if first_name and last_name:
            components.append(first_name)
            components.append(last_name)
        elif first_name:
            components.append(first_name)
        
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
        
        # Variation 1: Email + LinkedIn only
        if email:
            email_only_query = f"{email} + LinkedIn"
            if email_only_query != primary_query:
                variations.append(email_only_query)
        
        # Variation 2: Email + first_name + LinkedIn (if not already covered)
        if email and first_name:
            first_name_query = f"{email} + {first_name} + LinkedIn"
            if first_name_query != primary_query and first_name_query not in variations:
                variations.append(first_name_query)
        
        # Variation 3: Email + names + location + LinkedIn (if not already covered)
        if email and first_name and last_name and (state or country):
            location_components = []
            if state:
                location_components.append(state)
            if country:
                location_components.append(country)
            
            if location_components:
                location_query = f"{email} + {first_name} + {last_name} + {' + '.join(location_components)} + LinkedIn"
                if location_query != primary_query and location_query not in variations:
                    variations.append(location_query)
        
        # Variation 4: Use site:linkedin.com operator
        site_query = f"{email} site:linkedin.com"
        variations.append(site_query)
        
        # Return unique variations only
        unique_variations = list(dict.fromkeys(variations))
        logger.info(f"Generated {len(unique_variations)} query variations")
        
        return unique_variations
