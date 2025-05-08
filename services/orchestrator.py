import logging
import time
from typing import Dict, Optional

from services.email_classification.classifier import EmailClassifier
from services.lookup_processor import LinkedInLookupProcessor
from services.personal_lookup import LinkedInProfileLookup

logger = logging.getLogger(__name__)

class LinkedInOrchestrator:
    """
    Orchestrates the LinkedIn profile lookup process by coordinating
    email classification, Google search, and RocketReach fallback.
    """
    
    def __init__(self):
        """Initialize the orchestrator with required services."""
        self.email_classifier = EmailClassifier()
        self.lookup_processor = LinkedInLookupProcessor()
        self.rocketreach_lookup = LinkedInProfileLookup()
    
    async def orchestrate_lookup(
        self,
        email: str,
        full_name: Optional[str] = None,
        location_city: Optional[str] = None,
        location_state: Optional[str] = None,
        location_country: Optional[str] = None
    ) -> Dict[str, any]:
        """
        Orchestrate the LinkedIn lookup process based on email type.
        
        Args:
            email: Email address to look up
            full_name: Optional full name of the person
            location_city: Optional city location
            location_state: Optional state/province location
            location_country: Optional country location
            
        Returns:
            Dictionary with lookup results and metadata
        """
        start_time = time.time()
        
        try:
            # Classify the email
            domain_type, domain = self.email_classifier.classify_email(email)
            
            logger.info(f"Email {email} classified as {domain_type}")
            
            linkedin_url = None
            method_used = None
            error_message = None
            
            # Extract first name from full_name if provided
            first_name = None
            if full_name:
                name_parts = full_name.split()
                first_name = name_parts[0] if name_parts else None
            
            if domain_type == "work":
                # For work emails, try Google search first
                logger.info(f"Attempting Google search for work email: {email}")
                
                try:
                    linkedin_url = await self.lookup_processor.find_linkedin_profile(
                        subscriber_id=email,  # Using email as subscriber_id
                        email=email,
                        first_name=first_name,
                        location_city=location_city,
                        location_state=location_state,
                        location_country=location_country
                    )
                    
                    if linkedin_url:
                        method_used = "google_search"
                    else:
                        # If Google search fails, try RocketReach as fallback
                        logger.info(f"Google search failed, trying RocketReach for work email: {email}")
                        linkedin_url = self.rocketreach_lookup.lookup_by_email(email)
                        method_used = "rocketreach_fallback" if linkedin_url else None
                except Exception as e:
                    logger.error(f"Error during Google search: {e}")
                    # Try RocketReach as fallback on error
                    try:
                        linkedin_url = self.rocketreach_lookup.lookup_by_email(email)
                        method_used = "rocketreach_fallback"
                    except Exception as re:
                        logger.error(f"RocketReach fallback also failed: {re}")
                        error_message = f"Both lookup methods failed: {str(e)}; {str(re)}"
            
            else:  # personal email
                # For personal emails, go directly to RocketReach
                logger.info(f"Attempting RocketReach for personal email: {email}")
                try:
                    linkedin_url = self.rocketreach_lookup.lookup_by_email(email)
                    method_used = "rocketreach_primary"
                except Exception as e:
                    logger.error(f"RocketReach lookup failed: {e}")
                    error_message = f"RocketReach lookup failed: {str(e)}"
            
            # Calculate processing time
            processing_time_ms = int((time.time() - start_time) * 1000)
            
            # Prepare response
            result = {
                "email": email,
                "linkedin_url": linkedin_url,
                "success": linkedin_url is not None,
                "method_used": method_used or "none",
                "domain_type": domain_type,
                "processing_time_ms": processing_time_ms,
                "error_message": error_message
            }
            
            logger.info(f"Lookup completed for {email}: success={result['success']}, method={method_used}, time={processing_time_ms}ms")
            
            return result
            
        except Exception as e:
            # Handle any unexpected exceptions
            processing_time_ms = int((time.time() - start_time) * 1000)
            logger.error(f"Unexpected error during orchestration: {e}")
            
            return {
                "email": email,
                "linkedin_url": None,
                "success": False,
                "method_used": "none",
                "domain_type": "unknown",
                "processing_time_ms": processing_time_ms,
                "error_message": f"Orchestration error: {str(e)}"
            }
