"""
Global configuration settings for the scraper project.
"""
import os
from pathlib import Path
from typing import Dict, List, Optional
from pydantic import BaseSettings, Field

class Settings(BaseSettings):
    """Application settings with environment variable support."""
    
    # Project paths
    PROJECT_ROOT: Path = Path(__file__).parent.parent
    DATA_DIR: Path = PROJECT_ROOT / "data"
    RAW_DATA_DIR: Path = DATA_DIR / "raw" 
    PROCESSED_DATA_DIR: Path = DATA_DIR / "processed"
    LOGS_DIR: Path = DATA_DIR / "logs"
    CONFIG_DIR: Path = PROJECT_ROOT / "config"
    
    # API Configuration
    API_BASE_URL: str = "https://api.example.com"
    API_KEY: Optional[str] = Field(None, env="API_KEY")
    API_SECRET: Optional[str] = Field(None, env="API_SECRET")
    BEARER_TOKEN: Optional[str] = Field(None, env="BEARER_TOKEN")
    
    # Request Configuration
    REQUEST_TIMEOUT: int = 30
    MAX_RETRIES: int = 3
    RETRY_DELAY: float = 1.0
    MAX_RETRY_DELAY: float = 60.0
    BACKOFF_FACTOR: float = 2.0
    
    # Rate Limiting
    REQUESTS_PER_MINUTE: int = 60
    REQUESTS_PER_HOUR: int = 1000
    CONCURRENT_REQUESTS: int = 10
    
    # Proxy Configuration
    USE_PROXY: bool = True
    PROXY_ROTATION: bool = True
    PROXY_TIMEOUT: int = 10
    
    # User Agent Configuration
    ROTATE_USER_AGENTS: bool = True
    
    # Cache Configuration
    ENABLE_CACHE: bool = True
    CACHE_TTL: int = 3600  # 1 hour
    MAX_CACHE_SIZE: int = 1000
    
    # Storage Configuration
    DEFAULT_OUTPUT_FORMAT: str = "json"  # json, csv, sqlite
    SQLITE_DB_PATH: Path = DATA_DIR / "scraped_data.db"
    
    # Logging Configuration
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
    LOG_DATE_FORMAT: str = "%H:%M:%S"
    MAX_LOG_SIZE: int = 10 * 1024 * 1024  # 10MB
    LOG_BACKUP_COUNT: int = 5
    
    # Scheduler Configuration
    SCHEDULE_ENABLED: bool = False
    SCHEDULE_INTERVAL_MINUTES: int = 10
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.create_directories()
    
    def create_directories(self):
        """Create necessary directories if they don't exist."""
        directories = [
            self.DATA_DIR,
            self.RAW_DATA_DIR, 
            self.PROCESSED_DATA_DIR,
            self.LOGS_DIR
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)

# Global settings instance
settings = Settings()

# Headers template
DEFAULT_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# Common endpoints (example for Twitter-like API)
API_ENDPOINTS = {
    "user_timeline": "/2/users/{user_id}/tweets",
    "user_info": "/2/users/{user_id}",
    "search_tweets": "/2/tweets/search/recent",
    "tweet_details": "/2/tweets/{tweet_id}",
}