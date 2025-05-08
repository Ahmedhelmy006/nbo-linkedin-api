# services/personal_lookup.py
import time
import logging
from datetime import datetime, timedelta
import random
from typing import Dict, List, Optional, Tuple

# Import the existing functions from rocketreach_requests
from config.rocketreach_requests import (
    get_lkd_profile_devloper_nbo,
    get_lkd_profile_muhammad_helmey_006,
    get_lkd_profile_ahmed_helmey_006,
    get_lkd_profile_ahmed_modelwiz,
    get_lkd_profile_ahmed_helmey_009,
    get_lkd_profile_ichbin
)

# Import utility functions
from utils.email_validator import EmailValidator
from utils.helper import safe_call
from utils.cloud_storage import CloudStorageManager

logger = logging.getLogger(__name__)

class LinkedInProfileLookup:
    """
    Service for looking up LinkedIn profiles based on email addresses.
    Uses cloud storage to coordinate API rate limiting across projects.
    """
    
    def __init__(self):
        """
        Initialize the LinkedIn profile lookup service with cloud storage integration.
        """
        # Initialize cloud storage manager
        self.cloud_storage = CloudStorageManager(
            bucket_name="lookup_status",
            file_path="rocket_reach_requests.json/rocket_reach_requests.json"
        )
        
        # Define rate limits
        self.cooldown_seconds = 10  # No function should be called twice in 10 seconds
        self.max_calls_per_hour = 70  # Maximum 70 calls per hour per function
        
        # Map function names to actual functions
        self.lookup_functions = {
            "get_lkd_profile_devloper_nbo": get_lkd_profile_devloper_nbo,
            "get_lkd_profile_muhammad_helmey_006": get_lkd_profile_muhammad_helmey_006,
            "get_lkd_profile_ahmed_helmey_006": get_lkd_profile_ahmed_helmey_006,
            "get_lkd_profile_ahmed_modelwiz": get_lkd_profile_ahmed_modelwiz,
            "get_lkd_profile_ahmed_helmey_009": get_lkd_profile_ahmed_helmey_009,
            "get_lkd_profile_ichbin": get_lkd_profile_ichbin,
        }
        
        logger.info("LinkedIn profile lookup service initialized with cloud storage integration")
    
    def lookup_by_email(self, email: str) -> Optional[str]:
        """
        Look up a LinkedIn profile using an email address
        
        Args:
            email: Email address to look up
            
        Returns:
            LinkedIn profile URL or None if not found
        """
        # Validate email
        print(f"DEBUG: RocketReach - Looking up profile for email={email}")
        if not EmailValidator.is_valid(email):
            logger.error(f"Invalid email format: {email}")
            raise ValueError(f"Invalid email format: {email}")
        
        # Select the appropriate function to call
        function_name = self._select_available_function()
        if not function_name:
            logger.error("No available lookup functions due to rate limiting")
            raise RuntimeError("Rate limit exceeded for all available lookup functions")
        
        # Record this call in cloud storage
        now = datetime.now()
        self.cloud_storage.update_call_history(function_name, now)
        
        # Call the selected function using safe_call
        logger.info(f"Looking up LinkedIn profile for {email} using {function_name}")
        lookup_function = self.lookup_functions[function_name]
        linkedin_url = safe_call(lookup_function, email)
        
        if linkedin_url:
            print(f"DEBUG: RocketReach - Found LinkedIn profile: {linkedin_url}")
        else:
            print(f"DEBUG: RocketReach - No LinkedIn profile found")
        
        return linkedin_url
    
    def _select_available_function(self) -> Optional[str]:
        """
        Select a function that isn't currently rate-limited
        
        Returns:
            Name of an available function or None if all are rate-limited
        """
        available_functions = []
        
        for func_name in self.lookup_functions.keys():
            if self._can_call_function(func_name):
                available_functions.append(func_name)
        
        if not available_functions:
            return None
        
        # Return a random available function to distribute load
        selected = random.choice(available_functions)
        logger.info(f"Selected function for API call: {selected}")
        return selected
    
    def _can_call_function(self, function_name: str) -> bool:
        """
        Check if a function can be called based on rate limits
        
        Args:
            function_name: Name of the function to check
            
        Returns:
            True if the function can be called, False otherwise
        """
        # Get call history from cloud storage
        call_history = self.cloud_storage.get_call_history(function_name)
        
        # Check cooldown period (last 10 seconds)
        now = datetime.now()
        if call_history and (now - call_history[-1]).total_seconds() < self.cooldown_seconds:
            logger.debug(f"Function {function_name} on cooldown")
            return False
        
        # Check hourly limit
        one_hour_ago = now - timedelta(hours=1)
        recent_calls = [call for call in call_history if call > one_hour_ago]
        if len(recent_calls) >= self.max_calls_per_hour:
            logger.debug(f"Function {function_name} reached hourly limit")
            return False
        
        return True
