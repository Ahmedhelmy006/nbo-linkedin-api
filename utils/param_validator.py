# utils/param_validator.py
import logging
from typing import Any, List, Optional, Dict, Union

logger = logging.getLogger(__name__)

class ParamValidator:
    """
    Utility class for validating and sanitizing parameters used in LinkedIn lookups.
    """
    
    @staticmethod
    def is_empty_or_invalid(value: Any) -> bool:
        """
        Check if a value is empty, None, or represents a null/invalid placeholder.
        
        Args:
            value: The value to check
            
        Returns:
            True if the value is empty or invalid, False otherwise
        """
        if value is None:
            return True
            
        if isinstance(value, str):
            # Convert to lowercase for case-insensitive comparison
            value_lower = value.lower().strip()
            
            # Check for various forms of null/empty placeholders
            invalid_values = [
                "", "none", "null", "n/a", "#n/a", "undefined", 
                "nil", "na", "-", "unknown", "#null"
            ]
            
            return value_lower in invalid_values or value_lower.isspace()
            
        return False
    
    @staticmethod
    def sanitize_param(value: Any) -> Optional[str]:
        """
        Sanitize a parameter value, returning None if it's invalid.
        
        Args:
            value: The parameter value to sanitize
            
        Returns:
            Sanitized string value or None if invalid
        """
        if ParamValidator.is_empty_or_invalid(value):
            return None
            
        if not isinstance(value, str):
            # Convert non-string values to strings
            value = str(value)
            
        # Trim whitespace and normalize spaces
        return " ".join(value.strip().split())
    
    @staticmethod
    def validate_email(email: str) -> bool:
        """
        Validate if an email address has basic correct format.
        
        Args:
            email: Email address to validate
            
        Returns:
            True if the email has valid format, False otherwise
        """
        if ParamValidator.is_empty_or_invalid(email):
            return False
            
        # Very basic email validation (contains @ and at least one dot after @)
        email = email.strip().lower()
        return '@' in email and '.' in email.split('@')[1]
    
    @staticmethod
    def sanitize_location(city: Optional[str] = None, 
                         state: Optional[str] = None, 
                         country: Optional[str] = None) -> Dict[str, Optional[str]]:
        """
        Sanitize location parameters and return as a dictionary.
        
        Args:
            city: City name
            state: State/province name
            country: Country name
            
        Returns:
            Dictionary of sanitized location parameters
        """
        return {
            "city": ParamValidator.sanitize_param(city),
            "state": ParamValidator.sanitize_param(state),
            "country": ParamValidator.sanitize_param(country)
        }
    
    @staticmethod
    def sanitize_name(first_name: Optional[str] = None, 
                     last_name: Optional[str] = None) -> Dict[str, Optional[str]]:
        """
        Sanitize name parameters and return as a dictionary.
        
        Args:
            first_name: First name
            last_name: Last name
            
        Returns:
            Dictionary of sanitized name parameters
        """
        return {
            "first_name": ParamValidator.sanitize_param(first_name),
            "last_name": ParamValidator.sanitize_param(last_name)
        }
    
    @staticmethod
    def validate_request_params(params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate and sanitize a set of request parameters.
        
        Args:
            params: Dictionary of request parameters
            
        Returns:
            Dictionary of validated and sanitized parameters
        """
        sanitized = {}
        
        # Validate email (required parameter)
        email = params.get("email")
        if not ParamValidator.validate_email(email):
            logger.warning(f"Invalid email format: {email}")
            raise ValueError(f"Invalid email format: {email}")
            
        sanitized["email"] = email.strip().lower()
        
        # Sanitize name parameters
        name_params = ParamValidator.sanitize_name(
            params.get("first_name"), 
            params.get("last_name")
        )
        sanitized.update(name_params)
        
        # Sanitize location parameters
        location_params = ParamValidator.sanitize_location(
            params.get("location_city"),
            params.get("location_state"),
            params.get("location_country")
        )
        sanitized.update(location_params)
        
        return sanitized
