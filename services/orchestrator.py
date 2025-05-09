import logging
import time
from typing import Dict, Optional, Tuple
import traceback

from services.email_classification.classifier import EmailClassifier
from services.lookup_processor import LinkedInLookupProcessor
from services.personal_lookup import LinkedInProfileLookup
from utils.param_validator import ParamValidator

logger = logging.getLogger(__name__)

class LinkedInOrchestrator:
    """
    Orchestrates the LinkedIn profile lookup process by coordinating
    email classification, Google search, and RocketReach fallback.
    """
    
    def __init__(self):
        """Initialize the orchestrator with required services."""
        logger.info("Initializing LinkedInOrchestrator")
        self.email_classifier = EmailClassifier()
        self.lookup_processor = LinkedInLookupProcessor()
        self.rocketreach_lookup = LinkedInProfileLookup()
        logger.info("LinkedInOrchestrator initialized successfully")
    
    async def orchestrate_lookup(
        self,
        email: str,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
        location_city: Optional[str] = None,
        location_state: Optional[str] = None,
        location_country: Optional[str] = None
    ) -> Dict[str, any]:
        """
        Orchestrate the LinkedIn lookup process based on email type.
        
        Args:
            email: Email address to look up
            first_name: Optional first name of the person
            last_name: Optional last name of the person
            location_city: Optional city location
            location_state: Optional state/province location
            location_country: Optional country location
            
        Returns:
            Dictionary with lookup results and metadata
        """
        start_time = time.time()
        
        try:
            # Validate email
            if not ParamValidator.validate_email(email):
                logger.error(f"DEBUG: Invalid email format: {email}")
                return {
                    "email": email,
                    "linkedin_url": None,
                    "success": False,
                    "method_used": "none",
                    "domain_type": "unknown",
                    "google_domain_used": None,
                    "processing_time_ms": int((time.time() - start_time) * 1000),
                    "error_message": f"Invalid email format: {email}"
                }
            
            # Sanitize parameters
            sanitized_email = email.strip().lower()
            
            logger.info(f"DEBUG: Orchestrator - Starting lookup for email={sanitized_email}, first_name={first_name}, last_name={last_name}")
            
            # Classify the email
            domain_type, domain = self.email_classifier.classify_email(sanitized_email)
            logger.info(f"DEBUG: Orchestrator - Email {sanitized_email} classified as {domain_type}")
            
            linkedin_url = None
            method_used = None
            error_message = None
            google_domain_used = None
            
            if domain_type == "work":
                # For work emails, try Google search first
                logger.info(f"DEBUG: Domain type is work, attempting Google search for: {sanitized_email}")
                
                try:
                    logger.info("DEBUG: Starting lookup_processor.find_linkedin_profile")
                    linkedin_url, google_domain_used = await self.lookup_processor.find_linkedin_profile(
                        subscriber_id=sanitized_email,  # Using email as subscriber_id
                        email=sanitized_email,
                        first_name=first_name,
                        last_name=last_name,
                        location_city=location_city,
                        location_state=location_state,
                        location_country=location_country
                    )
                    logger.info(f"DEBUG: find_linkedin_profile returned: {linkedin_url} (domain: {google_domain_used})")
                    
                    if linkedin_url:
                        logger.info("DEBUG: Google search succeeded")
                        method_used = "google_search"
                    else:
                        # If Google search fails, try RocketReach as fallback
                        logger.info(f"DEBUG: Google search returned no results, trying RocketReach fallback")
                        try:
                            linkedin_url = self.rocketreach_lookup.lookup_by_email(sanitized_email)
                            logger.info(f"DEBUG: RocketReach fallback returned: {linkedin_url}")
                            method_used = "rocketreach_fallback" if linkedin_url else None
                        except Exception as rr_e:
                            logger.error(f"DEBUG: RocketReach fallback error: {rr_e}")
                            error_message = f"RocketReach fallback failed: {str(rr_e)}"
                
                except Exception as e:
                    logger.error(f"DEBUG: Error during Google search: {e}")
                    logger.error(f"DEBUG: Traceback: {traceback.format_exc()}")
                    error_message = f"Google search error: {str(e)}"
                    
                    # Try RocketReach as fallback on error
                    try:
                        logger.info("DEBUG: Attempting RocketReach as fallback after Google search error")
                        linkedin_url = self.rocketreach_lookup.lookup_by_email(sanitized_email)
                        logger.info(f"DEBUG: RocketReach fallback returned: {linkedin_url}")
                        method_used = "rocketreach_fallback"
                    except Exception as re:
                        logger.error(f"DEBUG: RocketReach fallback also failed: {re}")
                        error_message = f"Both lookup methods failed: {str(e)}; {str(re)}"
            
            else:  # personal email
                # For personal emails, go directly to RocketReach
                logger.info(f"DEBUG: Domain type is personal, attempting RocketReach for: {sanitized_email}")
                try:
                    linkedin_url = self.rocketreach_lookup.lookup_by_email(sanitized_email)
                    logger.info(f"DEBUG: RocketReach lookup returned: {linkedin_url}")
                    method_used = "rocketreach_primary"
                except Exception as e:
                    logger.error(f"DEBUG: RocketReach lookup failed: {e}")
                    error_message = f"RocketReach lookup failed: {str(e)}"
            
            # Calculate processing time
            processing_time_ms = int((time.time() - start_time) * 1000)
            
            # Prepare response
            result = {
                "email": sanitized_email,
                "linkedin_url": linkedin_url,
                "success": linkedin_url is not None,
                "method_used": method_used or "none",
                "domain_type": domain_type,
                "google_domain_used": google_domain_used,
                "processing_time_ms": processing_time_ms,
                "error_message": error_message
            }
            
            logger.info(f"DEBUG: Lookup completed for {sanitized_email}: success={result['success']}, method={method_used}, domain={google_domain_used}, time={processing_time_ms}ms, error={error_message}")
            
            return result
            
        except Exception as e:
            # Handle any unexpected exceptions
            processing_time_ms = int((time.time() - start_time) * 1000)
            logger.error(f"DEBUG: Unexpected error during orchestration: {e}")
            logger.error(f"DEBUG: Traceback: {traceback.format_exc()}")
            
            return {
                "email": email,
                "linkedin_url": None,
                "success": False,
                "method_used": "none",
                "domain_type": "unknown",
                "google_domain_used": None,
                "processing_time_ms": processing_time_ms,
                "error_message": f"Orchestration error: {str(e)}"
            }
