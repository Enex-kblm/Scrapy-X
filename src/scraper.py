"""
Core async scraping functionality with retry, rate limiting, and proxy support.
"""
import asyncio
import time
from typing import Dict, List, Optional, Any, Union, Callable
from datetime import datetime
import httpx
from tenacity import (
    retry, stop_after_attempt, wait_exponential, 
    retry_if_exception_type, before_sleep_log
)
import logging

from config.settings import settings, DEFAULT_HEADERS
from .logger import get_logger
from .proxy_manager import proxy_manager
from .user_agent_manager import user_agent_manager
from .cache import cache_manager
from .utils import (
    random_delay, is_rate_limited, is_client_error, 
    is_server_error, parse_retry_after, request_rate_limiter,
    hourly_rate_limiter
)

class AsyncScraper:
    """
    Advanced async web scraper with proxy rotation, caching, and retry logic.
    """
    
    def __init__(self, 
                 base_url: Optional[str] = None,
                 custom_headers: Optional[Dict[str, str]] = None):
        """
        Initialize scraper.
        
        Args:
            base_url: Base URL for API requests
            custom_headers: Custom headers to merge with defaults
        """
        self.logger = get_logger()
        self.base_url = base_url or settings.API_BASE_URL
        self.session: Optional[httpx.AsyncClient] = None
        
        # Prepare headers
        self.headers = DEFAULT_HEADERS.copy()
        if custom_headers:
            self.headers.update(custom_headers)
        
        # Request statistics
        self.stats = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "cached_requests": 0,
            "retry_requests": 0
        }
        
        self.logger.info("AsyncScraper initialized")
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.start_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close_session()
    
    async def start_session(self):
        """Start HTTP session."""
        if self.session is None:
            # Get proxy configuration
            proxy_config = proxy_manager.get_proxy()
            
            # Configure client
            self.session = httpx.AsyncClient(
                timeout=httpx.Timeout(settings.REQUEST_TIMEOUT),
                limits=httpx.Limits(
                    max_connections=settings.CONCURRENT_REQUESTS,
                    max_keepalive_connections=10
                ),
                proxies=proxy_config,
                follow_redirects=True
            )
            
            self.logger.debug("HTTP session started")
    
    async def close_session(self):
        """Close HTTP session."""
        if self.session:
            await self.session.aclose()
            self.session = None
            self.logger.debug("HTTP session closed")
    
    def _prepare_headers(self, custom_headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """Prepare request headers with User-Agent rotation."""
        headers = self.headers.copy()
        
        # Add rotated User-Agent
        headers["User-Agent"] = user_agent_manager.get_user_agent()
        
        # Add API authentication if available
        if settings.API_KEY:
            headers["X-API-Key"] = settings.API_KEY
        if settings.BEARER_TOKEN:
            headers["Authorization"] = f"Bearer {settings.BEARER_TOKEN}"
        
        # Merge custom headers
        if custom_headers:
            headers.update(custom_headers)
        
        return headers
    
    def _create_cache_key(self, url: str, params: Optional[Dict] = None, 
                         method: str = "GET") -> str:
        """Create cache key for request."""
        cache_data = {
            "url": url,
            "method": method,
            "params": params or {}
        }
        return str(cache_data)
    
    @retry(
        stop=stop_after_attempt(lambda self: settings.MAX_RETRIES),
        wait=wait_exponential(
            multiplier=settings.RETRY_DELAY,
            max=settings.MAX_RETRY_DELAY
        ),
        retry=retry_if_exception_type((
            httpx.TimeoutException,
            httpx.ConnectError,
            httpx.RemoteProtocolError
        )),
        before_sleep=before_sleep_log(logging.getLogger(), logging.WARNING)
    )
    async def _make_request(self, 
                          method: str,
                          url: str, 
                          headers: Dict[str, str],
                          **kwargs) -> httpx.Response:
        """Make HTTP request with retry logic."""
        if not self.session:
            await self.start_session()
        
        start_time = time.time()
        
        try:
            response = await self.session.request(
                method=method,
                url=url,
                headers=headers,
                **kwargs
            )
            
            end_time = time.time()
            response_time = end_time - start_time
            
            # Record proxy performance
            proxy_config = proxy_manager.get_proxy()
            if response.status_code < 400:
                proxy_manager.record_proxy_result(proxy_config, True, response_time)
            else:
                proxy_manager.record_proxy_result(proxy_config, False)
            
            # Handle rate limiting
            if is_rate_limited(response.status_code):
                retry_after = parse_retry_after(response.headers.get('retry-after'))
                self.logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                await asyncio.sleep(retry_after)
                raise httpx.HTTPStatusError(
                    f"Rate limited: {response.status_code}",
                    request=response.request,
                    response=response
                )
            
            # Don't retry client errors (except rate limiting)
            if is_client_error(response.status_code):
                self.logger.error(f"Client error {response.status_code}: {url}")
                return response
            
            # Retry server errors
            if is_server_error(response.status_code):
                raise httpx.HTTPStatusError(
                    f"Server error: {response.status_code}",
                    request=response.request,
                    response=response
                )
            
            return response
            
        except Exception as e:
            # Record proxy failure
            proxy_config = proxy_manager.get_proxy()
            proxy_manager.record_proxy_result(proxy_config, False)
            raise e
    
    async def get(self, 
                  endpoint: str,
                  params: Optional[Dict[str, Any]] = None,
                  headers: Optional[Dict[str, str]] = None,
                  use_cache: bool = True,
                  **kwargs) -> Optional[Dict[str, Any]]:
        """
        Perform GET request.
        
        Args:
            endpoint: API endpoint or full URL
            params: Query parameters
            headers: Custom headers
            use_cache: Whether to use caching
            **kwargs: Additional httpx parameters
        
        Returns:
            Response data or None if failed
        """
        return await self._request(
            method="GET",
            endpoint=endpoint,
            params=params,
            headers=headers,
            use_cache=use_cache,
            **kwargs
        )
    
    async def post(self,
                   endpoint: str,
                   data: Optional[Dict[str, Any]] = None,
                   json: Optional[Dict[str, Any]] = None,
                   headers: Optional[Dict[str, str]] = None,
                   **kwargs) -> Optional[Dict[str, Any]]:
        """
        Perform POST request.
        
        Args:
            endpoint: API endpoint or full URL
            data: Form data
            json: JSON data
            headers: Custom headers
            **kwargs: Additional httpx parameters
        
        Returns:
            Response data or None if failed
        """
        return await self._request(
            method="POST",
            endpoint=endpoint,
            data=data,
            json=json,
            headers=headers,
            use_cache=False,  # Don't cache POST requests
            **kwargs
        )
    
    async def _request(self,
                      method: str,
                      endpoint: str,
                      params: Optional[Dict[str, Any]] = None,
                      data: Optional[Dict[str, Any]] = None,
                      json: Optional[Dict[str, Any]] = None,
                      headers: Optional[Dict[str, str]] = None,
                      use_cache: bool = True,
                      **kwargs) -> Optional[Dict[str, Any]]:
        """
        Internal request method with caching and rate limiting.
        
        Args:
            method: HTTP method
            endpoint: API endpoint or full URL
            params: Query parameters
            data: Form data
            json: JSON data
            headers: Custom headers
            use_cache: Whether to use caching
            **kwargs: Additional httpx parameters
        
        Returns:
            Response data or None if failed
        """
        # Build full URL
        if endpoint.startswith('http'):
            url = endpoint
        else:
            url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        
        # Check cache first
        cache_key = self._create_cache_key(url, params, method)
        if use_cache and method == "GET":
            cached_data = cache_manager.get(cache_key)
            if cached_data is not None:
                self.stats["cached_requests"] += 1
                self.logger.stats.add_request(success=True, cached=True)
                self.logger.debug(f"Cache hit: {endpoint}")
                return cached_data
        
        # Rate limiting
        await request_rate_limiter.acquire()
        await hourly_rate_limiter.acquire()
        
        # Add random delay
        await random_delay()
        
        try:
            # Prepare headers
            request_headers = self._prepare_headers(headers)
            
            self.stats["total_requests"] += 1
            
            # Make request
            response = await self._make_request(
                method=method,
                url=url,
                headers=request_headers,
                params=params,
                data=data,
                json=json,
                **kwargs
            )
            
            if response.status_code >= 400:
                self.stats["failed_requests"] += 1
                self.logger.stats.add_request(success=False)
                self.logger.error(f"Request failed: {response.status_code} {url}")
                return None
            
            # Parse response
            try:
                response_data = response.json()
            except Exception:
                # If not JSON, return text content
                response_data = {"content": response.text}
            
            # Cache successful GET requests
            if use_cache and method == "GET" and response.status_code == 200:
                cache_manager.set(cache_key, response_data)
            
            self.stats["successful_requests"] += 1
            self.logger.stats.add_request(success=True)
            self.logger.debug(f"Request successful: {endpoint}")
            
            return response_data
            
        except Exception as e:
            self.stats["failed_requests"] += 1
            self.logger.stats.add_request(success=False)
            self.logger.error(f"Request failed: {endpoint} - {e}")
            return None
    
    async def scrape_multiple(self,
                            requests: List[Dict[str, Any]],
                            max_concurrent: Optional[int] = None) -> List[Optional[Dict[str, Any]]]:
        """
        Scrape multiple endpoints concurrently.
        
        Args:
            requests: List of request configurations
                Each dict should contain: endpoint, method, params, headers, etc.
            max_concurrent: Maximum concurrent requests (defaults to settings)
        
        Returns:
            List of response data (same order as requests)
        """
        max_concurrent = max_concurrent or settings.CONCURRENT_REQUESTS
        
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def scrape_with_semaphore(request_config: Dict[str, Any]):
            async with semaphore:
                method = request_config.pop("method", "GET")
                if method.upper() == "GET":
                    return await self.get(**request_config)
                elif method.upper() == "POST":
                    return await self.post(**request_config)
                else:
                    return await self._request(method=method.upper(), **request_config)
        
        tasks = [scrape_with_semaphore(req.copy()) for req in requests]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Convert exceptions to None
        processed_results = []
        for result in results:
            if isinstance(result, Exception):
                self.logger.error(f"Task failed: {result}")
                processed_results.append(None)
            else:
                processed_results.append(result)
        
        return processed_results
    
    async def scrape_paginated(self,
                              endpoint: str,
                              params: Optional[Dict[str, Any]] = None,
                              page_param: str = "page",
                              per_page_param: str = "per_page",
                              per_page: int = 100,
                              max_pages: Optional[int] = None,
                              extract_data_func: Optional[Callable] = None) -> List[Any]:
        """
        Scrape paginated data.
        
        Args:
            endpoint: API endpoint
            params: Base query parameters
            page_param: Page parameter name
            per_page_param: Per page parameter name
            per_page: Items per page
            max_pages: Maximum pages to scrape
            extract_data_func: Function to extract data from response
        
        Returns:
            List of all scraped items
        """
        all_data = []
        page = 1
        params = params or {}
        
        while True:
            if max_pages and page > max_pages:
                break
            
            # Set pagination parameters
            current_params = params.copy()
            current_params[page_param] = page
            current_params[per_page_param] = per_page
            
            # Make request
            response_data = await self.get(endpoint, params=current_params)
            
            if not response_data:
                break
            
            # Extract data
            if extract_data_func:
                page_data = extract_data_func(response_data)
            else:
                # Try common data keys
                page_data = (
                    response_data.get("data") or
                    response_data.get("results") or
                    response_data.get("items") or
                    [response_data]
                )
            
            if not page_data:
                break
            
            all_data.extend(page_data)
            
            # Check if there's more data
            if len(page_data) < per_page:
                break
            
            page += 1
            self.logger.info(f"Scraped page {page-1}, total items: {len(all_data)}")
        
        self.logger.info(f"Pagination complete. Total items: {len(all_data)}")
        return all_data
    
    def get_stats(self) -> Dict[str, Any]:
        """Get scraping statistics."""
        return {
            **self.stats,
            "success_rate": (
                (self.stats["successful_requests"] / self.stats["total_requests"] * 100)
                if self.stats["total_requests"] > 0 else 0
            ),
            "cache_hit_rate": (
                (self.stats["cached_requests"] / self.stats["total_requests"] * 100)
                if self.stats["total_requests"] > 0 else 0
            )
        }