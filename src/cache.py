"""
Simple caching system for avoiding duplicate requests.
"""
import hashlib
import json
import pickle
import time
from pathlib import Path
from typing import Any, Optional, Dict, Union
from datetime import datetime, timedelta

from config.settings import settings
from .logger import get_logger

class CacheManager:
    """Simple file-based caching system."""
    
    def __init__(self, cache_dir: Optional[Path] = None):
        self.logger = get_logger()
        self.cache_dir = cache_dir or (settings.DATA_DIR / "cache")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # In-memory cache for frequently accessed items
        self._memory_cache: Dict[str, dict] = {}
        self._memory_cache_size = 0
        self.max_memory_cache_size = 100  # MB
        
        self.logger.debug(f"Cache initialized at: {self.cache_dir}")
    
    def _get_cache_key(self, key: Union[str, dict]) -> str:
        """Generate cache key from input."""
        if isinstance(key, dict):
            # Sort dict to ensure consistent keys
            key_str = json.dumps(key, sort_keys=True)
        else:
            key_str = str(key)
        
        # Create hash for filename
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def _get_cache_file(self, cache_key: str) -> Path:
        """Get cache file path."""
        return self.cache_dir / f"{cache_key}.cache"
    
    def _is_expired(self, timestamp: float) -> bool:
        """Check if cache entry is expired."""
        return time.time() - timestamp > settings.CACHE_TTL
    
    def set(self, key: Union[str, dict], value: Any, ttl: Optional[int] = None) -> bool:
        """
        Store value in cache.
        
        Args:
            key: Cache key (string or dict)
            value: Value to cache
            ttl: Time to live in seconds (defaults to settings.CACHE_TTL)
        
        Returns:
            True if successful
        """
        if not settings.ENABLE_CACHE:
            return False
        
        try:
            cache_key = self._get_cache_key(key)
            timestamp = time.time()
            ttl = ttl or settings.CACHE_TTL
            
            cache_entry = {
                "value": value,
                "timestamp": timestamp,
                "ttl": ttl,
                "expires_at": timestamp + ttl
            }
            
            # Store in memory cache if not too large
            serialized = pickle.dumps(cache_entry)
            size_mb = len(serialized) / (1024 * 1024)
            
            if size_mb < 10 and self._memory_cache_size + size_mb < self.max_memory_cache_size:
                self._memory_cache[cache_key] = cache_entry
                self._memory_cache_size += size_mb
            
            # Store in file
            cache_file = self._get_cache_file(cache_key)
            with open(cache_file, 'wb') as f:
                pickle.dump(cache_entry, f)
            
            self.logger.debug(f"Cached key: {cache_key[:10]}... (size: {size_mb:.2f}MB)")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to cache key {key}: {e}")
            return False
    
    def get(self, key: Union[str, dict]) -> Optional[Any]:
        """
        Retrieve value from cache.
        
        Args:
            key: Cache key (string or dict)
        
        Returns:
            Cached value or None if not found/expired
        """
        if not settings.ENABLE_CACHE:
            return None
        
        try:
            cache_key = self._get_cache_key(key)
            
            # Try memory cache first
            if cache_key in self._memory_cache:
                cache_entry = self._memory_cache[cache_key]
                if not self._is_expired(cache_entry["timestamp"]):
                    self.logger.debug(f"Cache hit (memory): {cache_key[:10]}...")
                    return cache_entry["value"]
                else:
                    # Remove expired entry
                    del self._memory_cache[cache_key]
            
            # Try file cache
            cache_file = self._get_cache_file(cache_key)
            if not cache_file.exists():
                return None
            
            with open(cache_file, 'rb') as f:
                cache_entry = pickle.load(f)
            
            if self._is_expired(cache_entry["timestamp"]):
                # Remove expired file
                cache_file.unlink()
                self.logger.debug(f"Cache expired: {cache_key[:10]}...")
                return None
            
            self.logger.debug(f"Cache hit (file): {cache_key[:10]}...")
            
            # Add to memory cache if there's space
            serialized = pickle.dumps(cache_entry)
            size_mb = len(serialized) / (1024 * 1024)
            if size_mb < 10 and self._memory_cache_size + size_mb < self.max_memory_cache_size:
                self._memory_cache[cache_key] = cache_entry
                self._memory_cache_size += size_mb
            
            return cache_entry["value"]
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve cache for key {key}: {e}")
            return None
    
    def exists(self, key: Union[str, dict]) -> bool:
        """
        Check if key exists in cache and is not expired.
        
        Args:
            key: Cache key (string or dict)
        
        Returns:
            True if key exists and not expired
        """
        return self.get(key) is not None
    
    def delete(self, key: Union[str, dict]) -> bool:
        """
        Delete key from cache.
        
        Args:
            key: Cache key (string or dict)
        
        Returns:
            True if successful
        """
        try:
            cache_key = self._get_cache_key(key)
            
            # Remove from memory cache
            if cache_key in self._memory_cache:
                del self._memory_cache[cache_key]
            
            # Remove from file cache
            cache_file = self._get_cache_file(cache_key)
            if cache_file.exists():
                cache_file.unlink()
                self.logger.debug(f"Deleted cache: {cache_key[:10]}...")
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to delete cache for key {key}: {e}")
            return False
    
    def clear(self) -> bool:
        """
        Clear all cache entries.
        
        Returns:
            True if successful
        """
        try:
            # Clear memory cache
            self._memory_cache.clear()
            self._memory_cache_size = 0
            
            # Clear file cache
            for cache_file in self.cache_dir.glob("*.cache"):
                cache_file.unlink()
            
            self.logger.info("Cache cleared")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to clear cache: {e}")
            return False
    
    def cleanup_expired(self) -> int:
        """
        Remove expired cache entries.
        
        Returns:
            Number of entries removed
        """
        removed_count = 0
        
        try:
            # Clean memory cache
            expired_keys = []
            for cache_key, cache_entry in self._memory_cache.items():
                if self._is_expired(cache_entry["timestamp"]):
                    expired_keys.append(cache_key)
            
            for cache_key in expired_keys:
                del self._memory_cache[cache_key]
                removed_count += 1
            
            # Clean file cache
            for cache_file in self.cache_dir.glob("*.cache"):
                try:
                    with open(cache_file, 'rb') as f:
                        cache_entry = pickle.load(f)
                    
                    if self._is_expired(cache_entry["timestamp"]):
                        cache_file.unlink()
                        removed_count += 1
                        
                except Exception:
                    # Remove corrupted cache files
                    cache_file.unlink()
                    removed_count += 1
            
            if removed_count > 0:
                self.logger.info(f"Removed {removed_count} expired cache entries")
            
            return removed_count
            
        except Exception as e:
            self.logger.error(f"Failed to cleanup cache: {e}")
            return 0
    
    def get_stats(self) -> dict:
        """Get cache statistics."""
        try:
            file_count = len(list(self.cache_dir.glob("*.cache")))
            memory_count = len(self._memory_cache)
            
            total_size = 0
            for cache_file in self.cache_dir.glob("*.cache"):
                total_size += cache_file.stat().st_size
            
            return {
                "enabled": settings.ENABLE_CACHE,
                "file_entries": file_count,
                "memory_entries": memory_count,
                "total_size_mb": total_size / (1024 * 1024),
                "memory_size_mb": self._memory_cache_size,
                "ttl_seconds": settings.CACHE_TTL,
                "cache_dir": str(self.cache_dir)
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get cache stats: {e}")
            return {"error": str(e)}

# Global cache manager instance
cache_manager = CacheManager()