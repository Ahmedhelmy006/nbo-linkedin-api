"""
Google search component for the NBO Pipeline.

This module provides functionality for searching Google for LinkedIn profiles.
"""
import os
import re
import time
import json
import logging
import asyncio
import requests
import urllib.parse
from typing import List, Dict, Any, Optional, Tuple
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# Import settings with fallback values
from config import settings

# Initialize fallback values for settings
GOOGLE_SEARCH_HEADLESS = True
GOOGLE_SEARCH_MAX_RESULTS = 10
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
BROWSER_ARGS = ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]

# Try to get values from settings, use fallbacks if not available
try:
    GOOGLE_SEARCH_HEADLESS = getattr(settings, 'GOOGLE_SEARCH_HEADLESS', GOOGLE_SEARCH_HEADLESS)
except AttributeError:
    logging.warning("GOOGLE_SEARCH_HEADLESS not found in settings, using default value")

try:
    GOOGLE_SEARCH_MAX_RESULTS = getattr(settings, 'GOOGLE_SEARCH_MAX_RESULTS', GOOGLE_SEARCH_MAX_RESULTS)
except AttributeError:
    logging.warning("GOOGLE_SEARCH_MAX_RESULTS not found in settings, using default value")

try:
    USER_AGENT = getattr(settings, 'USER_AGENT', USER_AGENT)
except AttributeError:
    logging.warning("USER_AGENT not found in settings, using default value")

try:
    BROWSER_ARGS = getattr(settings, 'BROWSER_ARGS', BROWSER_ARGS)
except AttributeError:
    logging.warning("BROWSER_ARGS not found in settings, using default value")

from config.headers import get_google_search_headers, get_openai_headers
from config.api_keys import OPENAI_API_KEY
from .query_builder import QueryBuilder

logger = logging.getLogger(__name__)

class GoogleSearch:
    """
    Performs Google searches to find LinkedIn profiles.
    """
    
    def __init__(self, headless=None, max_results=None):
        """
        Initialize the Google search component.
        
        Args:
            headless: Whether to run the browser in headless mode
            max_results: Maximum number of search results to retrieve
        """
        self.headless = headless if headless is not None else GOOGLE_SEARCH_HEADLESS
        self.max_results = max_results or GOOGLE_SEARCH_MAX_RESULTS
        self.headers = get_google_search_headers()
        
        # Alternative selectors for search results
        self.result_selectors = [
            "div.g", 
            "div[data-sokoban-container]", 
            "div[data-hveid]",
            "div.tF2Cxc",
            "div.yuRUbf"
        ]
        
        # Define supported Google domains
        self.google_domains = [
            "google.com",       # Default (US)
            "google.de",        # Germany
            "google.com.eg",    # Egypt
            "google.com.au"     # Australia
        ]
    
    def is_linkedin_url(self, url: str) -> bool:
        """
        Comprehensive check for LinkedIn URLs.
        Captures various LinkedIn URL formats.
        
        Args:
            url: URL to check
            
        Returns:
            True if URL is a LinkedIn URL, False otherwise
        """
        linkedin_domains = [
            'linkedin.com/in/',     # Standard profile URLs
            'linkedin.com/company/', # Company pages
            'linkedin.com/posts/',   # Posts
            'linkedin.com/pulse/',   # Article links
            'linkedin.com/groups/',  # Group pages
            'eg.linkedin.com/in/',   # Country-specific profile URLs
            'linkedin.com/feed/',    # Feed links
            'linkedin.com/mwlite/'   # Mobile web links
        ]
        
        # Convert URL to lowercase for case-insensitive matching
        url_lower = url.lower()
        
        # Check if any LinkedIn domain is in the URL
        return any(domain in url_lower for domain in linkedin_domains)
    
    async def extract_search_results(self, page, max_results=None) -> List[Dict[str, str]]:
        """
        Extract search results from Google search page.
        
        Args:
            page: Playwright page object
            max_results: Maximum number of results to extract
            
        Returns:
            List of search results with title, url, and snippet
        """
        max_results = max_results or self.max_results
        results = []
        
        try:
            # Get the page HTML content for BeautifulSoup parsing
            html_content = await page.content()
            
            # Use BeautifulSoup for more reliable parsing
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for Google search result containers
            search_result_containers = []
            for selector in self.result_selectors:
                containers = soup.select(selector)
                if containers:
                    search_result_containers.extend(containers)
                    break
            
            if search_result_containers:
                logger.info(f"Found {len(search_result_containers)} search result containers")
                
                # Process each container
                for container in search_result_containers:
                    try:
                        # Find the link
                        link = container.select_one('a[href^="http"]')
                        if not link or not link.get('href'):
                            continue
                            
                        url = link.get('href')
                        
                        # Skip Google's own links and other non-result links
                        if (url.startswith('https://www.google.com') or
                            url.startswith('https://accounts.google.com') or
                            url.startswith('https://support.google.com') or
                            'webcache' in url or
                            'translate.google' in url):
                            continue
                        
                        # Extract title - try different selectors
                        title_elem = container.select_one('h3')
                        if not title_elem:
                            # Try other potential title containers
                            title_elem = container.select_one('div.vvjwJb, div.LC20lb')
                        
                        title = title_elem.get_text().strip() if title_elem else link.get_text().strip()
                        
                        # Extract snippet - try different selectors for Google snippets
                        snippet = ""
                        # Try the common snippet containers
                        snippet_elem = container.select_one('div.VwiC3b, span.aCOpRe, div.s, div[data-content-feature="1"]')
                        if snippet_elem:
                            snippet = snippet_elem.get_text().strip()
                        
                        # If no snippet found, try getting text from the container that's not in the link or title
                        if not snippet:
                            # Get all text in the container
                            container_text = container.get_text().strip()
                            
                            # Remove the title from the text
                            if title and title in container_text:
                                remaining_text = container_text.replace(title, '', 1).strip()
                                if remaining_text:
                                    snippet = remaining_text
                        
                        # Add to results if we have needed info
                        if url and title:
                            results.append({
                                "title": title,
                                "url": url,
                                "snippet": snippet
                            })
                    except Exception as e:
                        logger.error(f"Error processing search result container: {e}")
                        continue
            else:
                logger.warning("No standard result containers found, using fallback approach")
                
                # Extract all links
                links = soup.select('a[href^="http"]')
                logger.info(f"Found {len(links)} links on the page")
                
                for link in links:
                    try:
                        url = link.get('href')
                        
                        # Skip non-result links
                        if (url and 
                            not url.startswith('https://www.google.com') and
                            not url.startswith('https://accounts.google.com') and
                            not url.startswith('https://support.google.com') and
                            'webcache' not in url and
                            'translate.google' not in url):
                            
                            # Try to get the title from the link text
                            title = link.get_text().strip()
                            
                            # Skip empty titles
                            if not title:
                                continue
                                
                            # Try to find a snippet - look at parent elements
                            snippet = ""
                            parent = link.parent
                            
                            # Look for text in parent elements
                            for _ in range(3):  # Check up to 3 levels up
                                if not parent:
                                    break
                                    
                                # Get text that's not in links
                                text_nodes = []
                                for child in parent.children:
                                    if child.name != 'a' and child.string and child.string.strip():
                                        text_nodes.append(child.string.strip())
                                
                                if text_nodes:
                                    snippet = ' '.join(text_nodes)
                                    break
                                
                                parent = parent.parent
                            
                            # Add to potential results
                            results.append({
                                "title": title,
                                "url": url,
                                "snippet": snippet
                            })
                    except Exception as e:
                        logger.error(f"Error processing link: {e}")
                        continue
            
            # Deduplicate results by URL
            unique_urls = set()
            filtered_results = []
            for result in results:
                if result["url"] not in unique_urls:
                    unique_urls.add(result["url"])
                    filtered_results.append(result)
            
            # Prioritize LinkedIn results
            linkedin_results = [r for r in filtered_results if self.is_linkedin_url(r["url"])]
            other_results = [r for r in filtered_results if not self.is_linkedin_url(r["url"])]
            
            # Combine and limit to max_results
            final_results = linkedin_results + other_results
            final_results = final_results[:max_results]
            
            logger.info(f"After filtering, found {len(final_results)} results ({len(linkedin_results)} LinkedIn profiles)")
            
            return final_results
                
        except Exception as e:
            logger.error(f"Error during result extraction: {e}")
            return []
    
    async def google_search(self, query: str, domain: str = "google.com") -> List[Dict[str, str]]:
        """
        Perform Google search for LinkedIn profiles using a specific Google domain.
        
        Args:
            query: Search query
            domain: Google domain to use (default: google.com)
            
        Returns:
            List of search results
        """
        # Encode the search query for URL
        encoded_query = urllib.parse.quote(query)
        search_url = f'https://www.{domain}/search?q={encoded_query}'
        
        logger.info(f"Searching on {domain}: {search_url}")
        
        async with async_playwright() as p:
            # Launch the browser
            browser = await p.chromium.launch(
                headless=self.headless,
                args=BROWSER_ARGS
            )
            
            try:
                # Create context with minimal options to avoid detection
                context = await browser.new_context(
                    viewport={'width': 1200, 'height': 800},
                    user_agent=USER_AGENT,
                    java_script_enabled=True,
                )
                
                # Create new page
                page = await context.new_page()
                
                # Simple anti-detection measures
                await page.evaluate("""
                () => {
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                }
                """)
                
                # Navigate to the search URL
                await page.goto(search_url, wait_until='networkidle')
                
                # Let the page settle
                await asyncio.sleep(3)
                
                # Extract search results
                results = await self.extract_search_results(page)
                
                return results
                
            except Exception as e:
                logger.error(f"Error during search on {domain}: {e}")
                return []
            
            finally:
                # Close the browser
                await browser.close()
    
    async def multi_domain_search(self, query: str) -> Tuple[List[Dict[str, str]], str]:
        """
        Perform search across multiple Google domains until a LinkedIn profile is found.
        
        Args:
            query: Search query
            
        Returns:
            Tuple of (search results, domain used)
        """
        linkedin_found = False
        domain_used = None
        results = []
        
        # Try each domain in sequence
        for domain in self.google_domains:
            logger.info(f"Trying search on {domain} with query: {query}")
            
            try:
                # Perform search on this domain
                domain_results = await self.google_search(query, domain)
                
                # Check if any LinkedIn profiles were found
                linkedin_results = [r for r in domain_results if self.is_linkedin_url(r["url"])]
                
                if linkedin_results:
                    logger.info(f"Found {len(linkedin_results)} LinkedIn profiles on {domain}")
                    results = domain_results
                    domain_used = domain
                    linkedin_found = True
                    break
                else:
                    logger.info(f"No LinkedIn profiles found on {domain}, trying next domain")
            
            except Exception as e:
                logger.error(f"Error during search on {domain}: {e}")
                # Continue to next domain on error
        
        if not linkedin_found:
            # If no LinkedIn profiles found on any domain, return results from first domain that returned any results
            for domain in self.google_domains:
                try:
                    results = await self.google_search(query, domain)
                    if results:
                        domain_used = domain
                        logger.info(f"No LinkedIn profiles found on any domain. Using results from {domain}")
                        break
                except Exception:
                    continue
        
        return results, domain_used
    
    async def find_linkedin_profile_multi_domain(self, search_params: Dict[str, Any]) -> Tuple[Optional[str], str]:
        """
        Find LinkedIn profile by trying multiple search strategies across multiple Google domains.
        
        First tries searching with the full email, then falls back to name extraction if needed.
        
        Args:
            search_params: Dictionary of search parameters
            
        Returns:
            Tuple of (LinkedIn profile URL, domain used) or (None, None) if not found
        """
        # PHASE 1: Try with full email address first
        logger.info(f"PHASE 1: Attempting search with full email address")
        
        # Create email-only query
        email = search_params.get("email", "")
        email_query = f"{email} + LinkedIn"
        
        logger.info(f"Trying email-only query: {email_query}")
        
        # Try the email-only query across all domains
        results, domain = await self.multi_domain_search(email_query)
        
        # Check if any LinkedIn profiles were found
        linkedin_results = [r for r in results if self.is_linkedin_url(r["url"])]
        
        if linkedin_results:
            logger.info(f"PHASE 1 SUCCESS: Found LinkedIn profile with email-only query")
            
            # Use OpenAI to select the best match
            member_info = {
                "email": email,
                "first_name": search_params.get("first_name", ""),
                "last_name": search_params.get("last_name", ""),
                "state": search_params.get("location_state", ""),
                "country": search_params.get("location_country", "")
            }
            
            linkedin_url = await self.query_openai(member_info, linkedin_results)
            
            if linkedin_url:
                return linkedin_url, domain
        
        # PHASE 2: If no results from full email, try with extracted name
        logger.info(f"PHASE 1 FAILED: No LinkedIn profiles found with email-only query, moving to PHASE 2")
        
        # Generate query variations using all available parameters
        query_variations = QueryBuilder.build_query_variations(search_params)
        
        linkedin_url = None
        domain_used = None
        
        # Try each query variation
        for query in query_variations:
            # Skip the email-only query since we already tried it
            if query == email_query:
                continue
                
            logger.info(f"Trying query variation: {query}")
            
            # Search across multiple domains
            results, domain = await self.multi_domain_search(query)
            
            # Filter for LinkedIn profiles
            linkedin_results = [r for r in results if self.is_linkedin_url(r["url"])]
            
            if linkedin_results:
                # Query OpenAI to pick the best match
                member_info = {
                    "email": search_params.get("email", ""),
                    "first_name": search_params.get("first_name", ""),
                    "last_name": search_params.get("last_name", ""),
                    "state": search_params.get("location_state", ""),
                    "country": search_params.get("location_country", "")
                }
                
                linkedin_url = await self.query_openai(member_info, linkedin_results)
                
                if linkedin_url:
                    domain_used = domain
                    logger.info(f"PHASE 2 SUCCESS: Found LinkedIn profile: {linkedin_url} on {domain} with query: {query}")
                    break
        
        if not linkedin_url:
            logger.info("Both search phases failed. No LinkedIn profile found.")
        
        return linkedin_url, domain_used
    
    async def query_openai(self, member_info: Dict[str, str], search_results: List[Dict[str, str]]) -> Optional[str]:
        """
        Query OpenAI to find the most relevant LinkedIn profile.
        
        Args:
            member_info: Information about the member
            search_results: Google search results
            
        Returns:
            LinkedIn profile URL or None if not found
        """
        if not OPENAI_API_KEY:
            logger.error("OpenAI API key not set. Cannot query OpenAI.")
            return None
        
        # If no search results, return None
        if not search_results or len(search_results) == 0:
            logger.info("No search results to analyze")
            return None
        
        # System prompt for GPT
        system_prompt = (
            "You are a helpful assistant that identifies the correct LinkedIn profile URL for a person based on their information "
            "and search results. Given a person's information (email, name, location) and search results, find the most likely "
            "LinkedIn profile URL for that person. Only respond with the full URL if you're confident it's correct, or 'null' "
            "Most likely the result is in the first 3-5 results, the person we're looking for."
        )
        
        # Clean member_info by removing null/empty values
        clean_member_info = {}
        for key, value in member_info.items():
            if value is not None and value != "":
                clean_member_info[key] = value
        
        # Create message payload with the context and search results
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps({
                "member_info": clean_member_info,
                "search_results": search_results
            }, indent=2)}
        ]
        
        # Call OpenAI API
        try:
            headers = get_openai_headers(OPENAI_API_KEY)
            
            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers,
                json={
                    "model": "gpt-4o",
                    "messages": messages
                },
                timeout=30
            )
            
            # Check if the request was successful
            response.raise_for_status()
            
            # Extract the assistant's message
            result = response.json()
            linkedin_url = result["choices"][0]["message"]["content"].strip()
            
            # If the result is "null", convert to None
            if linkedin_url.lower() == "null":
                return None
                
            # Validate that it's a LinkedIn URL
            if "linkedin.com/in/" in linkedin_url.lower():
                return linkedin_url
            else:
                logger.warning(f"OpenAI returned a non-LinkedIn URL: {linkedin_url}")
                return None
                
        except Exception as e:
            logger.error(f"Error calling OpenAI API: {e}")
            return None
