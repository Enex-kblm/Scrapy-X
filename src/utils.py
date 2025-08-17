"""
Utility functions for the scraper.
"""
import asyncio
import random
from typing import Union, Any
from datetime import datetime, timedelta

async def random_delay(min_seconds: float = 1.0, max_seconds: float = 3.0):
    """
    Add random delay between requests.
    
    Args:
        min_seconds: Minimum delay in seconds
        max_seconds: Maximum delay in seconds
    """
    delay = random.uniform(min_seconds, max_seconds)
    await asyncio.sleep(delay)

def exponential_backoff(attempt: int, base_delay: float = 1.0, max_delay: float = 60.0) -> float:
    """
    Calculate exponential backoff delay.
    
    Args:
        attempt: Current attempt number (starting from 0)
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds
    
    Returns:
        Delay in seconds
    """
    delay = min(base_delay * (2 ** attempt), max_delay)
    # Add some jitter
    jitter = random.uniform(0.1, 0.3) * delay
    return delay + jitter

def is_rate_limited(status_code: int) -> bool:
    """
    Check if response indicates rate limiting.
    
    Args:
        status_code: HTTP status code
    
    Returns:
        True if rate limited
    """
    return status_code in [429, 420, 503]

def is_client_error(status_code: int) -> bool:
    """
    Check if response is a client error that shouldn't be retried.
    
    Args:
        status_code: HTTP status code
    
    Returns:
        True if client error
    """
    return 400 <= status_code < 500 and status_code not in [429, 420]

def is_server_error(status_code: int) -> bool:
    """
    Check if response is a server error that can be retried.
    
    Args:
        status_code: HTTP status code
    
    Returns:
        True if server error
    """
    return 500 <= status_code < 600

def sanitize_filename(filename: str) -> str:
    """
    Sanitize filename for safe storage.
    
    Args:
        filename: Original filename
    
    Returns:
        Sanitized filename
    """
    # Replace invalid characters
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    
    # Remove leading/trailing spaces and dots
    filename = filename.strip(' .')
    
    # Limit length
    if len(filename) > 255:
        filename = filename[:255]
    
    return filename

def format_size(size_bytes: int) -> str:
    """
    Format file size in human readable format.
    
    Args:
        size_bytes: Size in bytes
    
    Returns:
        Formatted size string
    """
    if size_bytes == 0:
        return "0B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    
    return f"{size_bytes:.1f}{size_names[i]}"

def generate_timestamp() -> str:
    """
    Generate timestamp string for filenames.
    
    Returns:
        Timestamp string in format YYYYMMDD_HHMMSS
    """
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def parse_retry_after(retry_after: Union[str, int, None]) -> int:
    """
    Parse Retry-After header value.
    
    Args:
        retry_after: Retry-After header value
    
    Returns:
        Seconds to wait
    """
    if not retry_after:
        return 60  # Default 1 minute
    
    try:
        # Try as number (seconds)
        return int(retry_after)
    except ValueError:
        # Try as date
        try:
            retry_date = datetime.strptime(retry_after, "%a, %d %b %Y %H:%M:%S GMT")
            delta = retry_date - datetime.utcnow()
            return max(int(delta.total_seconds()), 0)
        except ValueError:
            return 60  # Default 1 minute

def chunk_list(lst: list, chunk_size: int) -> list:
    """
    Split list into chunks of specified size.
    
    Args:
        lst: List to chunk
        chunk_size: Size of each chunk
    
    Returns:
        List of chunks
    """
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

def safe_get(dictionary: dict, key: str, default: Any = None) -> Any:
    """
    Safely get value from nested dictionary.
    
    Args:
        dictionary: Dictionary to search
        key: Key path (e.g., "user.profile.name")
        default: Default value if key not found
    
    Returns:
        Value or default
    """
    try:
        keys = key.split('.')
        value = dictionary
        for k in keys:
            value = value[k]
        return value
    except (KeyError, TypeError):
        return default

class RateLimiter:
    """Simple rate limiter for API requests."""
    
    def __init__(self, max_requests: int, time_window: int):
        """
        Initialize rate limiter.
        
        Args:
            max_requests: Maximum requests in time window
            time_window: Time window in seconds
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
    
    async def acquire(self):
        """Acquire rate limit token."""
        now = datetime.now()
        
        # Remove old requests outside time window
        cutoff = now - timedelta(seconds=self.time_window)
        self.requests = [req_time for req_time in self.requests if req_time > cutoff]
        
        # Check if we can make a request
        if len(self.requests) >= self.max_requests:
            # Calculate wait time
            oldest_request = min(self.requests)
            wait_time = (oldest_request + timedelta(seconds=self.time_window) - now).total_seconds()
            if wait_time > 0:
                await asyncio.sleep(wait_time)
        
        # Add current request
        self.requests.append(now)

# Global rate limiter instances
request_rate_limiter = RateLimiter(
    max_requests=60,  # 60 requests
    time_window=60    # per minute
)

hourly_rate_limiter = RateLimiter(
    max_requests=1000,  # 1000 requests  
    time_window=3600    # per hour
)