"""
Proxy rotation and management system.
"""
import random
import asyncio
from typing import List, Optional, Dict, Any
from pathlib import Path
import httpx
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from config.settings import settings
from .logger import get_logger

@dataclass
class ProxyInfo:
    """Information about a proxy server."""
    url: str
    protocol: str = field(init=False)
    host: str = field(init=False)
    port: int = field(init=False)
    is_working: bool = True
    last_used: Optional[datetime] = None
    success_count: int = 0
    failure_count: int = 0
    response_time: float = 0.0
    last_checked: Optional[datetime] = None
    
    def __post_init__(self):
        """Parse proxy URL components."""
        if "://" in self.url:
            self.protocol, rest = self.url.split("://", 1)
            if ":" in rest:
                self.host, port_str = rest.split(":", 1)
                self.port = int(port_str)
            else:
                self.host = rest
                self.port = 8080 if self.protocol == "http" else 1080
        else:
            # Assume http if no protocol specified
            self.protocol = "http"
            if ":" in self.url:
                self.host, port_str = self.url.split(":", 1)
                self.port = int(port_str)
            else:
                self.host = self.url
                self.port = 8080
            self.url = f"{self.protocol}://{self.host}:{self.port}"
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage."""
        total = self.success_count + self.failure_count
        if total == 0:
            return 100.0
        return (self.success_count / total) * 100.0
    
    def record_success(self, response_time: float):
        """Record successful request."""
        self.success_count += 1
        self.response_time = response_time
        self.last_used = datetime.now()
        self.is_working = True
    
    def record_failure(self):
        """Record failed request."""
        self.failure_count += 1
        self.last_used = datetime.now()
        # Mark as not working if failure rate is too high
        if self.failure_count > 5 and self.success_rate < 20:
            self.is_working = False

class ProxyManager:
    """Manages proxy rotation and health checking."""
    
    def __init__(self):
        self.logger = get_logger()
        self.proxies: List[ProxyInfo] = []
        self.current_proxy_index = 0
        self._load_proxies()
        
        # Health check settings
        self.health_check_interval = 300  # 5 minutes
        self.health_check_url = "http://httpbin.org/ip"
        self.last_health_check = None
        
        # Start health check task if proxies are loaded
        if self.proxies:
            asyncio.create_task(self._periodic_health_check())
    
    def _load_proxies(self):
        """Load proxies from configuration file."""
        proxy_file = settings.CONFIG_DIR / "proxies.txt"
        
        if not proxy_file.exists():
            self.logger.warning(f"Proxy file not found: {proxy_file}")
            return
        
        try:
            with open(proxy_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            for line in lines:
                line = line.strip()
                # Skip empty lines and comments
                if not line or line.startswith('#'):
                    continue
                
                try:
                    proxy_info = ProxyInfo(url=line)
                    self.proxies.append(proxy_info)
                except ValueError as e:
                    self.logger.warning(f"Invalid proxy format: {line} - {e}")
            
            self.logger.info(f"Loaded {len(self.proxies)} proxies")
            
        except Exception as e:
            self.logger.error(f"Failed to load proxies: {e}")
    
    def get_proxy(self) -> Optional[Dict[str, str]]:
        """Get next available proxy."""
        if not self.proxies or not settings.USE_PROXY:
            return None
        
        # Filter working proxies
        working_proxies = [p for p in self.proxies if p.is_working]
        
        if not working_proxies:
            self.logger.warning("No working proxies available")
            return None
        
        # Choose proxy based on rotation strategy
        if settings.PROXY_ROTATION:
            # Round-robin with preference for better performing proxies
            proxy = self._select_best_proxy(working_proxies)
        else:
            # Use first working proxy
            proxy = working_proxies[0]
        
        # Update stats
        self.logger.stats.set_current_proxy(f"{proxy.host}:{proxy.port}")
        
        # Return proxy configuration for httpx
        return {
            "http://": proxy.url,
            "https://": proxy.url
        }
    
    def _select_best_proxy(self, working_proxies: List[ProxyInfo]) -> ProxyInfo:
        """Select best proxy based on performance metrics."""
        # Sort by success rate and response time
        sorted_proxies = sorted(
            working_proxies,
            key=lambda p: (p.success_rate, -p.response_time),
            reverse=True
        )
        
        # Use weighted random selection favoring better proxies
        weights = []
        for i, proxy in enumerate(sorted_proxies):
            # Higher weight for better proxies
            weight = max(1, int(proxy.success_rate / 10))
            weights.append(weight)
        
        return random.choices(sorted_proxies, weights=weights)[0]
    
    def record_proxy_result(self, proxy_dict: Optional[Dict[str, str]], 
                          success: bool, response_time: float = 0.0):
        """Record the result of using a proxy."""
        if not proxy_dict:
            return
        
        # Extract proxy URL from dict
        proxy_url = list(proxy_dict.values())[0] if proxy_dict else None
        if not proxy_url:
            return
        
        # Find matching proxy info
        for proxy_info in self.proxies:
            if proxy_info.url == proxy_url:
                if success:
                    proxy_info.record_success(response_time)
                    self.logger.debug(f"Proxy success: {proxy_info.host}:{proxy_info.port}")
                else:
                    proxy_info.record_failure()
                    self.logger.warning(f"Proxy failed: {proxy_info.host}:{proxy_info.port}")
                break
    
    async def _check_proxy_health(self, proxy_info: ProxyInfo) -> bool:
        """Check if a proxy is working."""
        try:
            start_time = asyncio.get_event_loop().time()
            
            async with httpx.AsyncClient(
                proxies={
                    "http://": proxy_info.url,
                    "https://": proxy_info.url
                },
                timeout=settings.PROXY_TIMEOUT
            ) as client:
                response = await client.get(self.health_check_url)
                
                end_time = asyncio.get_event_loop().time()
                response_time = end_time - start_time
                
                if response.status_code == 200:
                    proxy_info.record_success(response_time)
                    proxy_info.last_checked = datetime.now()
                    return True
                else:
                    proxy_info.record_failure()
                    return False
        
        except Exception as e:
            self.logger.debug(f"Proxy health check failed for {proxy_info.url}: {e}")
            proxy_info.record_failure()
            proxy_info.last_checked = datetime.now()
            return False
    
    async def _periodic_health_check(self):
        """Periodically check proxy health."""
        while True:
            try:
                await asyncio.sleep(self.health_check_interval)
                await self.health_check_all()
            except Exception as e:
                self.logger.error(f"Error in periodic health check: {e}")
    
    async def health_check_all(self):
        """Check health of all proxies."""
        if not self.proxies:
            return
        
        self.logger.info("Starting proxy health check...")
        
        # Check all proxies concurrently
        tasks = [
            self._check_proxy_health(proxy_info) 
            for proxy_info in self.proxies
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        working_count = sum(1 for result in results if result is True)
        total_count = len(self.proxies)
        
        self.logger.info(f"Proxy health check complete: {working_count}/{total_count} working")
        self.last_health_check = datetime.now()
    
    def get_proxy_stats(self) -> Dict[str, Any]:
        """Get proxy statistics."""
        if not self.proxies:
            return {"total": 0, "working": 0, "average_success_rate": 0.0}
        
        working_proxies = [p for p in self.proxies if p.is_working]
        total_success_rate = sum(p.success_rate for p in self.proxies)
        
        return {
            "total": len(self.proxies),
            "working": len(working_proxies),
            "average_success_rate": total_success_rate / len(self.proxies),
            "last_health_check": self.last_health_check,
            "proxies": [
                {
                    "url": p.url,
                    "is_working": p.is_working,
                    "success_rate": p.success_rate,
                    "response_time": p.response_time,
                    "last_used": p.last_used
                }
                for p in self.proxies
            ]
        }

# Global proxy manager instance
proxy_manager = ProxyManager()