import os
from pathlib import Path
from pydantic_settings import BaseSettings
from functools import lru_cache
from typing import Optional, List

# Get the base directory of the project
BASE_DIR = Path(__file__).resolve().parent.parent

class Settings(BaseSettings):
    # API Settings
    API_KEY: str
    DEBUG: bool = False
    
    # Database Settings
    DB_HOST: str
    DB_PORT: int
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str
    CLOUD_BUCKET_NAME: str = "lookup_status"  

    # API Keys
    OPENAI_API_KEY: str
    
    # Application Settings
    LOG_LEVEL: str = "INFO"
    
    # Email Classification Settings 
    PERSONAL_DOMAINS_FILE: str = os.path.join(BASE_DIR, "data", "personal_domains_providers.txt")
    PERSONAL_PROVIDERS_FILE: str = os.path.join(BASE_DIR, "data", "personal_providers.txt")
    
    # Google Search Settings
    GOOGLE_SEARCH_HEADLESS: bool = True
    GOOGLE_SEARCH_MAX_RESULTS: int = 10
    BROWSER_ARGS: List[str] = ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
    USER_AGENT: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    
    # OpenAI Settings
    OPENAI_MODEL: str = "gpt-4o"
    
    class Config:
        env_file = ".env"

@lru_cache()
def get_settings():
    return Settings()

settings = get_settings()
