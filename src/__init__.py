"""
Web Scraper Project

A modular, scalable, and efficient web scraping framework
with proxy rotation, user-agent management, caching, and elegant logging.
"""

__version__ = "1.0.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"
__description__ = "Advanced web scraping framework with modern async features"

from .scraper import AsyncScraper
from .parser import DataParser
from .storage import DataStorage
from .cache import CacheManager
from .logger import setup_logger, get_dashboard

__all__ = [
    "AsyncScraper",
    "DataParser", 
    "DataStorage",
    "CacheManager",
    "setup_logger",
    "get_dashboard"
]