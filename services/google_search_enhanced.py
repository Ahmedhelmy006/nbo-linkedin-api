# services/google_search_enhanced.py
"""
Enhanced Google search component with anti-detection measures.
"""
import os
import re
import time
import json
import logging
import asyncio
import random
import urllib.parse
from typing import List, Dict, Any, Optional, Tuple
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

class EnhancedGoogleSearch:
    """
    Enhanced Google search with anti-detection measures.
    """
    
    def __init__(self, headless=True, max_results=10):
        """Initialize the enhanced Google search component."""
        self.headless = headless
        self.max_results = max_results
        
        # Rotating user agents (real browser fingerprints)
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/121.0"
        ]
        
        # Browser arguments for stealth
        self.browser_args = [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-accelerated-2d-canvas",
            "--no-first-run",
            "--no-zygote",
            "--disable-gpu",
            "--disable-features=VizDisplayCompositor",
            "--disable-background-networking",
            "--disable-background-timer-throttling",
            "--disable-renderer-backgrounding",
            "--disable-backgrounding-occluded-windows",
            "--disable-client-side-phishing-detection",
            "--disable-crash-reporter",
            "--disable-extensions",
            "--disable-features=TranslateUI",
            "--disable-ipc-flooding-protection",
            "--disable-web-security",
            "--disable-features=VizDisplayCompositor"
        ]
        
        # Alternative search engines/domains
        self.search_engines = [
            "www.google.com",
            "www.google.co.uk", 
            "www.google.ca",
            "www.google.com.au",
            "www.google.de",
            "www.bing.com",
            "duckduckgo.com"
        ]
        
        # Request rate limiting
        self.min_delay = 2  # Minimum delay between requests
        self.max_delay = 8  # Maximum delay between requests
        self.last_request_time = 0
        
    async def _wait_between_requests(self):
        """Add random delay between requests."""
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        
        delay = random.uniform(self.min_delay, self.max_delay)
        if elapsed < delay:
            wait_time = delay - elapsed
            logger.info(f"Rate limiting: waiting {wait_time:.2f} seconds")
            await asyncio.sleep(wait_time)
        
        self.last_request_time = time.time()
    
    async def _setup_stealth_browser(self, playwright_instance):
        """Set up browser with stealth configuration."""
        # Random user agent
        user_agent = random.choice(self.user_agents)
        
        # Launch browser with stealth args
        browser = await playwright_instance.chromium.launch(
            headless=self.headless,
            args=self.browser_args
        )
        
        # Create context with realistic settings
        context = await browser.new_context(
            viewport={
                'width': random.randint(1200, 1920),
                'height': random.randint(800, 1080)
            },
            user_agent=user_agent,
            java_script_enabled=True,
            permissions=['geolocation'],
            geolocation={'latitude': 30.0444, 'longitude': 31.2357},  # Cairo coordinates
            locale='en-US',
            timezone_id='Africa/Cairo'
        )
        
        # Add realistic headers
        await context.set_extra_http_headers({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0'
        })
        
        return browser, context
    
    async def _inject_stealth_scripts(self, page):
        """Inject anti-detection JavaScript."""
        stealth_scripts = [
            # Override webdriver property
            """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            """,
            
            # Override plugins
            """
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            """,
            
            # Override languages
            """
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });
            """,
            
            # Override Chrome runtime
            """
            if (navigator.userAgent.includes('Chrome')) {
                window.chrome = {
                    runtime: {}
                };
            }
            """,
            
            # Override permissions
            """
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
            """,
            
            # Mouse movements simulation
            """
            function simulateMouseMovement() {
                const event = new MouseEvent('mousemove', {
                    clientX: Math.random() * window.innerWidth,
                    clientY: Math.random() * window.innerHeight
                });
                document.dispatchEvent(event);
            }
            setInterval(simulateMouseMovement, 1000 + Math.random() * 2000);
            """
        ]
        
        for script in stealth_scripts:
            try:
                await page.evaluate(script)
            except Exception as e:
                logger.warning(f"Failed to inject stealth script: {e}")
    
    async def _simulate_human_behavior(self, page):
        """Simulate human-like behavior on the page."""
        try:
            # Random scroll
            scroll_amount = random.randint(100, 500)
            await page.evaluate(f"window.scrollBy(0, {scroll_amount})")
            await asyncio.sleep(random.uniform(0.5, 1.5))
            
            # Random mouse movement
            await page.mouse.move(
                random.randint(100, 800),
                random.randint(100, 600)
            )
            await asyncio.sleep(random.uniform(0.3, 0.8))
            
            # Sometimes click somewhere random (but safe)
            if random.random() < 0.3:
                await page.mouse.click(
                    random.randint(100, 400),
                    random.randint(100, 300)
                )
                await asyncio.sleep(random.uniform(0.2, 0.5))
                
        except Exception as e:
            logger.warning(f"Error simulating human behavior: {e}")
    
    async def _check_for_captcha(self, page) -> bool:
        """Check if the page contains a CAPTCHA."""
        try:
            # Check for common CAPTCHA indicators
            captcha_selectors = [
                '#captcha-form',
                '.g-recaptcha',
                '[data-sitekey]',
                'iframe[src*="recaptcha"]',
                'div[class*="captcha"]',
                'form[action*="captcha"]'
            ]
            
            for selector in captcha_selectors:
                element = await page.query_selector(selector)
                if element:
                    logger.warning(f"CAPTCHA detected: {selector}")
                    return True
            
            # Check page content for CAPTCHA text
            content = await page.content()
            captcha_texts = [
                "unusual traffic",
                "not a robot", 
                "captcha",
                "verify you're human",
                "automated requests"
            ]
            
            content_lower = content.lower()
            for text in captcha_texts:
                if text in content_lower:
                    logger.warning(f"CAPTCHA text detected: {text}")
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking for CAPTCHA: {e}")
            return True  # Assume CAPTCHA on error for safety
    
    async def google_search_stealth(self, query: str, domain: str = "www.google.com") -> List[Dict[str, str]]:
        """
        Perform Google search with enhanced stealth measures.
        """
        await self._wait_between_requests()
        
        # Encode the search query
        encoded_query = urllib.parse.quote(query)
        
        # Build search URL based on search engine
        if "bing.com" in domain:
            search_url = f'https://{domain}/search?q={encoded_query}'
        elif "duckduckgo.com" in domain:
            search_url = f'https://{domain}/?q={encoded_query}'
        else:  # Google variants
            search_url = f'https://{domain}/search?q={encoded_query}'
        
        logger.info(f"Stealth search on {domain}: {search_url}")
        
        async with async_playwright() as p:
            browser, context = await self._setup_stealth_browser(p)
            
            try:
                page = await context.new_page()
                
                # Inject stealth scripts
                await self._inject_stealth_scripts(page)
                
                # Navigate with random delay
                await asyncio.sleep(random.uniform(1, 3))
                
                # Navigate to search URL
                response = await page.goto(
                    search_url, 
                    wait_until='networkidle',
                    timeout=30000
                )
                
                # Check response status
                if response and response.status != 200:
                    logger.warning(f"Non-200 response: {response.status}")
                
                # Wait for page to load
                await asyncio.sleep(random.uniform(2, 4))
                
                # Check for CAPTCHA
                if await self._check_for_captcha(page):
                    logger.error("CAPTCHA detected, aborting search")
                    return []
                
                # Simulate human behavior
                await self._simulate_human_behavior(page)
                
                # Save page for debugging
                await self._save_page_html(page, search_url, "stealth_search")
                
                # Extract results based on search engine
                if "bing.com" in domain:
                    results = await self._extract_bing_results(page)
                elif "duckduckgo.com" in domain:
                    results = await self._extract_duckduckgo_results(page)
                else:
                    results = await self._extract_google_results(page)
                
                return results
                
            except Exception as e:
                logger.error(f"Error during stealth search on {domain}: {e}")
                return []
            
            finally:
                await browser.close()
    
    async def _extract_google_results(self, page) -> List[Dict[str, str]]:
        """Extract results from Google search page."""
        try:
            html_content = await page.content()
            soup = BeautifulSoup(html_content, 'html.parser')
            
            results = []
            
            # Google result selectors
            result_selectors = [
                "div.g",
                "div[data-hveid]",
                "div.tF2Cxc",
                "div.yuRUbf"
            ]
            
            search_containers = []
            for selector in result_selectors:
                containers = soup.select(selector)
                if containers:
                    search_containers.extend(containers)
                    break
            
            for container in search_containers[:self.max_results]:
                try:
                    # Find link
                    link = container.select_one('a[href^="http"]')
                    if not link:
                        continue
                    
                    url = link.get('href')
                    if not url or self._is_google_internal_link(url):
                        continue
                    
                    # Extract title
                    title_elem = container.select_one('h3')
                    title = title_elem.get_text().strip() if title_elem else ""
                    
                    # Extract snippet
                    snippet_elem = container.select_one('div.VwiC3b, span.aCOpRe, div.s')
                    snippet = snippet_elem.get_text().strip() if snippet_elem else ""
                    
                    if url and title:
                        results.append({
                            "title": title,
                            "url": url,
                            "snippet": snippet
                        })
                        
                except Exception as e:
                    logger.error(f"Error processing Google result: {e}")
                    continue
            
            return results
            
        except Exception as e:
            logger.error(f"Error extracting Google results: {e}")
            return []
    
    async def _extract_bing_results(self, page) -> List[Dict[str, str]]:
        """Extract results from Bing search page."""
        try:
            html_content = await page.content()
            soup = BeautifulSoup(html_content, 'html.parser')
            
            results = []
            containers = soup.select('li.b_algo')[:self.max_results]
            
            for container in containers:
                try:
                    link = container.select_one('h2 a')
                    if not link:
                        continue
                    
                    url = link.get('href')
                    title = link.get_text().strip()
                    
                    snippet_elem = container.select_one('p, div.b_caption p')
                    snippet = snippet_elem.get_text().strip() if snippet_elem else ""
                    
                    if url and title:
                        results.append({
                            "title": title,
                            "url": url,
                            "snippet": snippet
                        })
                        
                except Exception as e:
                    logger.error(f"Error processing Bing result: {e}")
                    continue
            
            return results
            
        except Exception as e:
            logger.error(f"Error extracting Bing results: {e}")
            return []
    
    async def _extract_duckduckgo_results(self, page) -> List[Dict[str, str]]:
        """Extract results from DuckDuckGo search page."""
        try:
            html_content = await page.content()
            soup = BeautifulSoup(html_content, 'html.parser')
            
            results = []
            containers = soup.select('article[data-testid="result"]')[:self.max_results]
            
            for container in containers:
                try:
                    link = container.select_one('h2 a')
                    if not link:
                        continue
                    
                    url = link.get('href')
                    title = link.get_text().strip()
                    
                    snippet_elem = container.select_one('[data-result="snippet"]')
                    snippet = snippet_elem.get_text().strip() if snippet_elem else ""
                    
                    if url and title:
                        results.append({
                            "title": title,
                            "url": url,
                            "snippet": snippet
                        })
                        
                except Exception as e:
                    logger.error(f"Error processing DuckDuckGo result: {e}")
                    continue
            
            return results
            
        except Exception as e:
            logger.error(f"Error extracting DuckDuckGo results: {e}")
            return []
    
    def _is_google_internal_link(self, url: str) -> bool:
        """Check if URL is a Google internal link."""
        internal_patterns = [
            'google.com/url',
            'google.com/search',
            'accounts.google.com',
            'support.google.com',
            'webcache',
            'translate.google'
        ]
        return any(pattern in url for pattern in internal_patterns)
    
    def is_linkedin_url(self, url: str) -> bool:
        """Check if URL is a LinkedIn URL."""
        linkedin_patterns = [
            'linkedin.com/in/',
            'linkedin.com/company/',
            'linkedin.com/posts/',
            'linkedin.com/pulse/',
            'eg.linkedin.com/in/'
        ]
        return any(pattern in url.lower() for pattern in linkedin_patterns)
    
    async def multi_engine_search(self, query: str) -> Tuple[List[Dict[str, str]], str]:
        """
        Search across multiple engines until LinkedIn profiles are found.
        """
        linkedin_found = False
        engine_used = None
        results = []
        
        # Randomize search engine order
        search_engines = self.search_engines.copy()
        random.shuffle(search_engines)
        
        for engine in search_engines:
            logger.info(f"Trying search on {engine}")
            
            try:
                engine_results = await self.google_search_stealth(query, engine)
                
                # Check for LinkedIn results
                linkedin_results = [r for r in engine_results if self.is_linkedin_url(r["url"])]
                
                if linkedin_results:
                    logger.info(f"Found {len(linkedin_results)} LinkedIn profiles on {engine}")
                    results = engine_results
                    engine_used = engine
                    linkedin_found = True
                    break
                else:
                    logger.info(f"No LinkedIn profiles found on {engine}")
                    
                # Add delay between different search engines
                await asyncio.sleep(random.uniform(3, 7))
                
            except Exception as e:
                logger.error(f"Error searching on {engine}: {e}")
                continue
        
        return results, engine_used
    
    async def _save_page_html(self, page, url: str, page_type: str = "page") -> str:
        """Save HTML content for debugging."""
        try:
            logs_dir = Path("logs/pages")
            logs_dir.mkdir(parents=True, exist_ok=True)
            
            html_content = await page.content()
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
            safe_url = re.sub(r'[^\w\-_.]', '_', url)[:50]
            filename = f"{page_type}_{timestamp}_{safe_url}.html"
            filepath = logs_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            logger.info(f"Saved HTML to: {filepath}")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"Error saving HTML: {e}")
            return None
