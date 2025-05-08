"""
Name extractor for the NBO Pipeline using OpenAI API.

This module provides functionality for extracting names from email addresses
for LinkedIn lookup by leveraging OpenAI's language models.
"""
import re
import logging
import os
import json
import requests
from typing import Tuple, Optional, List, Dict
from config import settings
from config.api_keys import OPENAI_API_KEY
from config.headers import get_openai_headers
from utils.helpers import mask_email

logger = logging.getLogger(__name__)

class NameExtractor:
    """
    Extracts names from email addresses using OpenAI API.
    """
    
    def __init__(self, openai_api_key=None):
        """Initialize the name extractor."""
        self.api_key = openai_api_key or OPENAI_API_KEY
        
        if not self.api_key:
            logger.warning("OpenAI API key not set. Name extraction may not work correctly.")
        
        # For cases when API might be unavailable or rate limited
        self.non_personal = {
            'admin', 'info', 'contact', 'hello', 'sales', 'support',
            'marketing', 'help', 'webmaster', 'noreply', 'no-reply',
            'team', 'office', 'billing', 'mail', 'postmaster', 'jobs',
            'career', 'hr', 'service', 'services'
        }
    
    def extract_name_from_email(self, email: str, given_name: Optional[str] = None) -> Tuple[Optional[str], str]:
        """
        Extract a name from an email address using OpenAI API.
        
        Args:
            email: Email address
            given_name: Provided first name (if available)
            
        Returns:
            Tuple of (extracted name, method)
        """
        logger.info(f"Extracting name from email: {email}, given_name: {given_name}")
        
        # If given_name contains a full name (first and last), use it directly
        if given_name and ' ' in given_name:
            logger.info(f"Using provided full name: {given_name}")
            return given_name, "User provided full name"
            
        if not isinstance(email, str) or '@' not in email:
            return None, "Invalid email"
        
        # Get username part
        username = email.split('@')[0].lower()
        
        # Quick filter for non-personal emails
        if username in self.non_personal:
            return None, "Non-personal email"
        
        # For obviously invalid or random usernames, don't bother calling the API
        if len(username) < 2 or re.match(r'^[0-9]+$', username):
            return None, "Invalid username"
        
        # Special case for a given first name
        if given_name:
            # Create a simple fake last name from email domain if needed
            domain_parts = email.split('@')[1].split('.')
            if domain_parts and len(domain_parts) > 0:
                # Get first part of domain before the dot
                domain = domain_parts[0].capitalize()
                if domain and len(domain) > 2:
                    full_name = f"{given_name} {domain}"
                    logger.info(f"Created name from given first name and domain: {full_name}")
                    return full_name, "Combined first name and domain"
        
        # Try to use OpenAI to extract the name
        try:
            logger.info("Attempting to extract name with OpenAI")
            extracted_name = self._call_openai_api(email, given_name)
            
            if extracted_name:
                logger.info(f"Successfully extracted name with OpenAI: {extracted_name}")
                return extracted_name, "OpenAI extraction"
            else:
                logger.info("OpenAI failed to extract a name, using fallback")
                # If OpenAI fails, immediately try the fallback
                fallback_name, fallback_method = self._basic_fallback_extraction(email, given_name)
                if fallback_name:
                    logger.info(f"Fallback extracted name: {fallback_name} using {fallback_method}")
                    return fallback_name, f"Fallback: {fallback_method}"
                logger.warning("Both OpenAI and fallback failed to extract a name")
                return None, "No name detected by OpenAI"
        
        except Exception as e:
            logger.error(f"Error calling OpenAI API: {e}")
            
            # Fallback to basic extraction in case of API failure
            logger.info("Using fallback extraction method due to OpenAI error")
            return self._basic_fallback_extraction(email, given_name)
    
    def _call_openai_api(self, email: str, given_name: Optional[str] = None) -> Optional[str]:
        """
        Call OpenAI API to extract a name from an email address.
        
        Args:
            email: Email address
            given_name: Provided first name (if available)
            
        Returns:
            Extracted name or None if not detected
        """
        # Use the API key from configuration/environment
        api_key = self.api_key or OPENAI_API_KEY
        
        if not api_key:
            logger.error("OpenAI API key not available. Set it as an environment variable: OPENAI_API_KEY")
            return None
        
        # If given_name is already provided and is a full name, use it
        if given_name and ' ' in given_name:
            logger.info(f"Using provided full name from given_name: {given_name}")
            return given_name
        
        # Define the model to use (with fallback)
        try:
            model = getattr(settings, 'OPENAI_MODEL', 'gpt-4o')
            logger.info(f"Using OpenAI model: {model}")
        except AttributeError:
            logger.warning("OPENAI_MODEL not found in settings, using default gpt-4o")
            model = 'gpt-4o'
        
        prompt = (
""" You will be given two inputs: an email and and a user entered name. 
                    These emails and names are user given. So they are messy. That's why You will be given these two inputs and you job is to return the first name and the last name from these inputs. 
                    There are a couple patterns that I expect you to break down. First pattern is: mlindholm@hlcsweden.com, Marko This is the input. I expect you to provide the name and the name only which is Marko Lindholm.
                    Why? Because the first letter in the email is referencing the user first name. Another example: ngeorges@pinnacleclimate.com, Nick ===> Nick Georges 
                    There are some straight forward cases like: steve.desalvo@kzf.com, Steve 
                    I expect the output to be Steve Desalvo 
                    And some Random cases like: igspam@wevalueprivacy.com, MindYourBusiness So I expect this to be None qzmlnhgzwwuzgdhgv@poplk.com, ko This one too is None
                    Note, your output will be used in a python code, so the output should be strict. If none then it's None, not none. 
                    same for returned names, first letter should be capital and there is a space between the first and last name."""
        )
        
        # If given_name is None, set it to empty string for the API call
        if given_name is None:
            given_name = ""
        
        try:
            # Use the get_openai_headers utility function which handles the API key correctly
            logger.info("Getting OpenAI headers")
            headers = get_openai_headers(api_key)
            
            data = {
                "model": model,
                "messages": [
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": f"{email}, {given_name}"}
                ],
                "temperature": 0.2,  # Low temperature for consistent results
                "max_tokens": 50     # We just need the name, so limit tokens
            }
            
            logger.info("Sending request to OpenAI API")
            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers,
                json=data,
                timeout=10  # Set a reasonable timeout
            )
            
            if response.status_code == 200:
                logger.info("OpenAI API request successful")
                response_data = response.json()
                extracted_text = response_data["choices"][0]["message"]["content"].strip()
                logger.info(f"OpenAI returned: {extracted_text}")
                
                # Check if result is None
                if extracted_text.lower() == "none":
                    logger.info("OpenAI returned 'None', no name extracted")
                    return None
                
                # Return the extracted name with proper capitalization
                name = self._format_name(extracted_text)
                logger.info(f"Formatted extracted name: {name}")
                return name
            else:
                logger.error(f"OpenAI API error: {response.status_code} - {response.text}")
                return None
        
        except Exception as e:
            logger.error(f"Error in OpenAI API call: {e}")
            return None
    
    def _format_name(self, name: str) -> str:
        """
        Format a name with proper capitalization.
        
        Args:
            name: Name to format
            
        Returns:
            Formatted name
        """
        # Split the name into parts
        parts = name.split()
        
        # Format each part with proper capitalization
        formatted_parts = []
        for part in parts:
            if not part:
                continue
            formatted_part = part[0].upper() + part[1:].lower() if len(part) > 1 else part.upper()
            formatted_parts.append(formatted_part)
        
        # Join the parts back together
        return " ".join(formatted_parts)
    
    def _basic_fallback_extraction(self, email: str, given_name: Optional[str] = None) -> Tuple[Optional[str], str]:
        """
        Fallback method for basic name extraction when API is unavailable.
        
        Args:
            email: Email address
            given_name: Provided first name (if available)
            
        Returns:
            Tuple of (extracted name, method)
        """
        logger.info(f"Using basic fallback extraction for {email} with given_name {given_name}")
        
        # Get username part
        username = email.split('@')[0].lower()
        
        # Get domain for potential last name
        domain_parts = email.split('@')[1].split('.') if '@' in email else []
        domain = domain_parts[0].capitalize() if domain_parts else None
        
        # If given_name is provided and not empty, use it
        if given_name:
            # If it's a full name, use it directly
            if ' ' in given_name:
                logger.info(f"Using provided full name in fallback: {given_name}")
                return given_name, "Used provided full name"
            else:
                # If just a first name is provided, try to create a last name
                if domain and len(domain) > 2:
                    # Use domain as last name
                    full_name = f"{given_name} {domain}"
                    logger.info(f"Created name from given first name and domain: {full_name}")
                    return full_name, "Combined first name and domain"
                else:
                    # Either use the given name directly
                    logger.info(f"Using provided first name only: {given_name}")
                    return f"{given_name} User", "Used provided first name"
        
        # Special case for simple first.last@domain.com format
        if '.' in username:
            parts = username.split('.')
            if len(parts) == 2 and all(len(part) > 1 for part in parts):
                formatted_name = f"{parts[0].capitalize()} {parts[1].capitalize()}"
                logger.info(f"Extracted name from first.last format: {formatted_name}")
                return formatted_name, "First.Last format"
        
        # Check for structured email with separators
        if '.' in username or '_' in username or '-' in username:
            # Replace separators with spaces
            clean_username = re.sub(r'[._-]', ' ', username)
            # Strip out numbers
            clean_username = re.sub(r'[0-9]', '', clean_username).strip()
            
            # Format the parts
            parts = clean_username.split()
            if parts and len(parts) >= 2:
                formatted_parts = []
                for part in parts:
                    if len(part) == 1:
                        formatted_parts.append(part.upper())
                    else:
                        formatted_parts.append(part.capitalize())
                
                full_name = " ".join(formatted_parts)
                logger.info(f"Extracted name from structured email: {full_name}")
                return full_name, "Structured email fallback"
            elif parts and len(parts) == 1 and domain:
                # If only one part but we have a domain, use domain as last name
                full_name = f"{parts[0].capitalize()} {domain}"
                logger.info(f"Created name from username part and domain: {full_name}")
                return full_name, "Username and domain fallback"
        
        # For special case of first initial + last name format (jsmith@domain.com)
        if len(username) > 1 and username[0].isalpha() and username[1:].isalpha():
            # Check if it might be first initial + last name
            potential_last = username[1:].capitalize()
            first_initial = username[0].upper()
            if len(potential_last) > 2:  # Reasonable last name length
                full_name = f"{first_initial}. {potential_last}"
                logger.info(f"Extracted name from first initial + last name format: {full_name}")
                return full_name, "First initial + last name fallback"
        
        # Last resort: if domain is available, create a fake name from the username and domain
        if domain and len(domain) > 2 and len(username) > 2:
            full_name = f"{username.capitalize()} {domain}"
            logger.info(f"Created fallback name from username and domain: {full_name}")
            return full_name, "Username + domain fallback"
        
        # Fallback for single-word usernames
        if len(username) >= 3:
            # Just use the username as a first name and "User" as last name
            full_name = f"{username.capitalize()} User"
            logger.info(f"Created name from username only: {full_name}")
            return full_name, "Single word username fallback"
        
        logger.warning(f"Could not extract name for {email} using any fallback method")
        return None, "No name detected in fallback"
