"""HTTP headers for various API requests."""

def get_openai_headers(api_key=None):
    """
    Get headers for OpenAI API requests.
    
    Args:
        api_key: Optional API key override
        
    Returns:
        Dictionary of headers
    """
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    return headers

def get_google_search_headers():
    """
    Get headers for Google search requests.
    
    Returns:
        Dictionary of headers
    """
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0"
    }
