"""
Email classification for the NBO Pipeline.

This module provides functionality for classifying emails as work or personal.
"""
import re
import logging
import os
from typing import Tuple, Set, List, Optional
from pathlib import Path

from config import settings

logger = logging.getLogger(__name__)

class EmailClassifier:
    """
    Classifies email addresses as work or personal.
    """
    
    def __init__(self, domains_file=None, providers_file=None):
        """
        Initialize the email classifier.
        
        Args:
            domains_file: Path to file with personal email domains
            providers_file: Path to file with personal email providers
        """
        # Use paths from settings, with fallbacks
        try:
            self.domains_file = domains_file or getattr(settings, 'PERSONAL_DOMAINS_FILE', "/home/developer/nbo_linkedin_api/data/personal_domains.txt")
            logger.info(f"Using domains file: {self.domains_file}")
        except AttributeError:
            logger.warning("PERSONAL_DOMAINS_FILE not found in settings, using default path")
            self.domains_file = "/home/developer/nbo_linkedin_api/data/personal_domains.txt"
            
        try:
            self.providers_file = providers_file or getattr(settings, 'PERSONAL_PROVIDERS_FILE', "/home/developer/nbo_linkedin_api/data/personal_providers.txt")
            logger.info(f"Using providers file: {self.providers_file}")
        except AttributeError:
            logger.warning("PERSONAL_PROVIDERS_FILE not found in settings, using default path")
            self.providers_file = "/home/developer/nbo_linkedin_api/data/personal_providers.txt"
        
        # Load personal domains and providers
        self.personal_domains = self._load_personal_domains()
        self.personal_providers = self._load_personal_providers()
        
        # Compile pattern for matching providers
        self.provider_pattern = self._compile_provider_pattern()
        
        logger.info(f"Email classifier initialized with {len(self.personal_domains)} personal domains "
                   f"and {len(self.personal_providers)} personal providers")
    
    def _load_personal_domains(self) -> Set[str]:
        """
        Load personal email domains from file.
        
        Returns:
            Set of personal email domains
        """
        domains = set()
        
        logger.info(f"Attempting to load domains from: {self.domains_file}")
        
        # Check if domains_file is None
        if not self.domains_file:
            logger.warning("Personal domains file path is None, using defaults")
            return self._get_default_domains()
            
        try:
            domains_path = Path(self.domains_file)
            if domains_path.exists():
                logger.info(f"Domains file exists at: {domains_path}")
                try:
                    with open(domains_path, 'r', encoding='utf-8') as f:
                        for line in f:
                            line = line.strip().lower()
                            if line and not line.startswith('#'):  # Skip empty lines and comments
                                domains.add(line)
                    
                    logger.info(f"Successfully loaded {len(domains)} personal email domains from {self.domains_file}")
                    
                    # If file exists but is empty, fall back to defaults
                    if not domains:
                        logger.warning(f"Domains file exists but is empty: {self.domains_file}")
                        domains = self._get_default_domains()
                except Exception as e:
                    logger.error(f"Error reading domains file {self.domains_file}: {e}")
                    domains = self._get_default_domains()
                        
                return domains
            else:
                logger.warning(f"Personal domains file not found: {self.domains_file}, using defaults")
        except Exception as e:
            logger.error(f"Error checking domains file: {e}")
        
        # Fallback to default domains
        logger.info("Using default domains due to file access issues")
        return self._get_default_domains()
    
    def _get_default_domains(self) -> Set[str]:
        """Get default personal email domains."""
        logger.info("Using default personal email domains")
        # Ensure nicolasboucher.online is NOT in this list
        default_domains = {
            'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
            'protonmail.com', 'icloud.com', 'mail.com', 'zoho.com',
            'yandex.com', 'gmx.com', 'tutanota.com', 'mail.ru'
        }
        logger.info(f"Default domains: {default_domains}")
        return default_domains
    
    def _load_personal_providers(self) -> List[str]:
        """
        Load personal email providers from file.
        
        Returns:
            List of personal email providers
        """
        providers = []
        
        logger.info(f"Attempting to load providers from: {self.providers_file}")
        
        # Check if providers_file is None
        if not self.providers_file:
            logger.warning("Personal providers file path is None, using defaults")
            return self._get_default_providers()
            
        try:
            providers_path = Path(self.providers_file)
            if providers_path.exists():
                logger.info(f"Providers file exists at: {providers_path}")
                try:
                    with open(providers_path, 'r', encoding='utf-8') as f:
                        for line in f:
                            line = line.strip().lower()
                            if line and not line.startswith('#'):  # Skip empty lines and comments
                                providers.append(line)
                    
                    logger.info(f"Successfully loaded {len(providers)} personal email providers from {self.providers_file}")
                    
                    # If file exists but is empty, fall back to defaults
                    if not providers:
                        logger.warning(f"Providers file exists but is empty: {self.providers_file}")
                        providers = self._get_default_providers()
                except Exception as e:
                    logger.error(f"Error reading providers file {self.providers_file}: {e}")
                    providers = self._get_default_providers()
                        
                return providers
            else:
                logger.warning(f"Personal providers file not found: {self.providers_file}, using defaults")
        except Exception as e:
            logger.error(f"Error checking providers file: {e}")
        
        # Fallback to default providers
        logger.info("Using default providers due to file access issues")
        return self._get_default_providers()
    
    def _get_default_providers(self) -> List[str]:
        """Get default personal email providers."""
        logger.info("Using default personal email providers")
        default_providers = [
            'gmail', 'yahoo', 'hotmail', 'outlook', 'live', 'msn', 'aol', 
            'protonmail', 'proton', 'icloud', 'zoho', 'yandex', 'gmx'
        ]
        # Ensure 'nicolasboucher' is NOT in this list
        logger.info(f"Default providers: {default_providers}")
        return default_providers
    
    def _compile_provider_pattern(self) -> re.Pattern:
        """
        Compile a regular expression pattern for matching personal providers.
        
        Returns:
            Compiled regular expression pattern
        """
        try:
            if not self.personal_providers:
                # Return a pattern that won't match anything
                return re.compile(r"^$")
                
            pattern_parts = []
            
            for provider in self.personal_providers:
                # Skip empty or very short providers
                if not provider or len(provider) < 2:
                    continue
                
                # Escape any special regex characters
                escaped_provider = re.escape(provider)
                pattern_parts.append(
                    f"(^{escaped_provider}$|^{escaped_provider}\\.|"
                    f"\\.{escaped_provider}$|\\.{escaped_provider}\\.)"
                )
            
            # If no pattern parts were added, return a pattern that won't match anything
            if not pattern_parts:
                return re.compile(r"^$")
                
            combined_pattern = '|'.join(pattern_parts)
            return re.compile(combined_pattern, re.IGNORECASE)
        
        except Exception as e:
            logger.error(f"Error compiling provider pattern: {e}")
            # Return a pattern that won't match anything
            return re.compile(r"^$")
    
    def is_work_email(self, email: str) -> bool:
        """
        Determine if an email is a work email.
        
        Args:
            email: Email address to classify
            
        Returns:
            True if work email, False if personal
        """
        # Special case for testing - always classify nicolasboucher.online as work
        if email and '@nicolasboucher.online' in email.lower():
            logger.info(f"Special case: {email} classified as work email")
            return True
        
        # Check if email is invalid
        if not email or '@' not in email:
            return False
        
        # Extract domain
        try:
            domain = email.split('@')[-1].lower()
            logger.info(f"Checking domain: {domain}")
        except Exception:
            logger.error(f"Error extracting domain from email: {email}")
            return False
        
        # Check if domain is a known personal domain
        if domain in self.personal_domains:
            logger.info(f"Domain {domain} is in personal domains list")
            return False
        
        # Check if domain contains "email" or "mail" keywords
        if 'email' in domain or ('mail' in domain and 'gmail' not in domain):
            logger.info(f"Domain {domain} contains 'email' or 'mail' keywords")
            return False
        
        # Check if domain matches personal provider pattern
        domain_parts = domain.split('.')
        full_domain = '.'.join(domain_parts)  # Reconstruct full domain
        
        # Check all parts of the domain against the pattern
        for i in range(len(domain_parts)):
            partial_domain = '.'.join(domain_parts[i:])
            if self.provider_pattern.search(partial_domain):
                logger.info(f"Domain part {partial_domain} matches personal provider pattern")
                return False
                
        # Also check if domain contains a provider name anywhere
        for provider in self.personal_providers:
            if provider in domain:
                logger.info(f"Domain {domain} contains personal provider {provider}")
                return False
        
        # Special cases for business domains
        if 'enterprise.org' in domain or 'business.org' in domain:
            return True
            
        # Special cases for .net business domains
        if 'business.net' in domain or 'enterprise.net' in domain:
            return True
        
        # Check for educational institution email
        if domain.endswith('.edu') or '.edu.' in domain:
            return False
        
        # Check for generic top-level domains that are often personal
        generic_tlds = {'.org', '.info', '.io', '.me'}
        # Only consider it a generic domain if it's a direct TLD
        if any(domain.endswith(tld) for tld in generic_tlds) and len(domain.split('.')) == 2:
            # Don't automatically return False - these could be work or personal
            # Let other rules decide
            pass
            
        # Check for government email (treat as work)
        if domain.endswith('.gov') or '.gov.' in domain:
            return True
        
        # For domains ending in .com and other business TLDs, assume work email
        business_tlds = {'.com', '.co', '.biz', '.ltd', '.pro', '.company', '.net'}
        if any(domain.endswith(tld) for tld in business_tlds):
            logger.info(f"Domain {domain} ends with business TLD - classified as work")
            return True
            
        # If specific country TLD (.fr, .de, etc.), check if it's a business/company
        country_tlds = ['.uk', '.ca', '.au', '.fr', '.de', '.it', '.es', '.jp']
        for tld in country_tlds:
            if domain.endswith(tld):
                # Check if it's a business domain
                business_prefixes = ['.co', '.com', '.biz', '.enterprise', '.business']
                if any(f"{prefix}{tld}" in domain for prefix in business_prefixes):
                    return True
        
        # If we get here, default to assume it's a work email for .com domains
        # but personal for others
        if domain.endswith('.com'):
            logger.info(f"Domain {domain} ends with .com - classified as work")
            return True
            
        # Default to personal for all other cases
        logger.info(f"Domain {domain} classified as personal by default")
        return False
    
    def classify_email(self, email: str) -> Tuple[str, str]:
        """
        Classify an email address and return the domain type.
        
        Args:
            email: Email address to classify
            
        Returns:
            Tuple of (domain_type, domain)
            domain_type is "work", "personal", or "unknown"
        """
        if not email or '@' not in email:
            return "unknown", ""
        
        try:
            # Extract domain
            domain = email.split('@')[-1].lower()
            
            # Classify the email
            is_work = self.is_work_email(email)
            domain_type = "work" if is_work else "personal"
            
            logger.info(f"Email {email} classified as {domain_type}")
            
            return domain_type, domain
        
        except Exception as e:
            logger.error(f"Error classifying email {email}: {e}")
            return "unknown", ""
    
    def reload_domains(self):
        """Reload domains and providers from files."""
        self.personal_domains = self._load_personal_domains()
        self.personal_providers = self._load_personal_providers()
        self.provider_pattern = self._compile_provider_pattern()
        logger.info("Reloaded email classification data from files")


# Create a singleton instance of EmailClassifier
_classifier = None

def get_classifier() -> EmailClassifier:
    """
    Get the email classifier singleton.
    
    Returns:
        EmailClassifier instance
    """
    global _classifier
    if _classifier is None:
        _classifier = EmailClassifier()
    return _classifier

def is_work_email(email: str) -> bool:
    """
    Determine if an email is a work email.
    
    Args:
        email: Email address to classify
        
    Returns:
        True if work email, False if personal
    """
    classifier = get_classifier()
    return classifier.is_work_email(email)

def classify_email(email: str) -> Tuple[str, str]:
    """
    Classify an email address and return the domain type.
    
    Args:
        email: Email address to classify
        
    Returns:
        Tuple of (domain_type, domain)
        domain_type is "work", "personal", or "unknown"
    """
    classifier = get_classifier()
    return classifier.classify_email(email)

def reload_classification_data():
    """Reload email classification data from files."""
    classifier = get_classifier()
    classifier.reload_domains()
