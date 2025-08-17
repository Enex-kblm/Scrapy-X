"""
Unit tests for the scraper module.
"""
import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock
import httpx

from src.scraper import AsyncScraper
from src.cache import cache_manager
from config.settings import settings

@pytest.fixture
def scraper():
    """Create scraper instance for testing."""
    return AsyncScraper(base_url="https://api.test.com")

@pytest.fixture
def mock_response():
    """Create mock HTTP response."""
    response = Mock()
    response.status_code = 200
    response.json.return_value = {"data": [{"id": "123", "text": "Test tweet"}]}
    response.headers = {}
    response.text = '{"data": [{"id": "123", "text": "Test tweet"}]}'
    return response

class TestAsyncScraper:
    """Test AsyncScraper functionality."""
    
    @pytest.mark.asyncio
    async def test_scraper_initialization(self, scraper):
        """Test scraper initialization."""
        assert scraper.base_url == "https://api.test.com"
        assert scraper.session is None
        assert scraper.stats["total_requests"] == 0
    
    @pytest.mark.asyncio
    async def test_session_management(self, scraper):
        """Test HTTP session management."""
        await scraper.start_session()
        assert scraper.session is not None
        assert isinstance(scraper.session, httpx.AsyncClient)
        
        await scraper.close_session()
        assert scraper.session is None
    
    @pytest.mark.asyncio
    async def test_context_manager(self, scraper):
        """Test async context manager."""
        async with scraper as s:
            assert s.session is not None
        
        assert scraper.session is None
    
    @pytest.mark.asyncio
    @patch('src.scraper.httpx.AsyncClient.request')
    async def test_successful_get_request(self, mock_request, scraper, mock_response):
        """Test successful GET request."""
        mock_request.return_value = mock_response
        
        async with scraper:
            result = await scraper.get("test/endpoint")
        
        assert result is not None
        assert result["data"][0]["id"] == "123"
        assert scraper.stats["successful_requests"] == 1
        assert scraper.stats["total_requests"] == 1
    
    @pytest.mark.asyncio
    @patch('src.scraper.httpx.AsyncClient.request')
    async def test_failed_request(self, mock_request, scraper):
        """Test failed request handling."""
        # Mock 404 response
        mock_response = Mock()
        mock_response.status_code = 404
        mock_request.return_value = mock_response
        
        async with scraper:
            result = await scraper.get("nonexistent/endpoint")
        
        assert result is None
        assert scraper.stats["failed_requests"] == 1
    
    @pytest.mark.asyncio
    @patch('src.scraper.httpx.AsyncClient.request')
    async def test_rate_limiting_handling(self, mock_request, scraper):
        """Test rate limiting response handling."""
        # Mock 429 response
        mock_response = Mock()
        mock_response.status_code = 429
        mock_response.headers = {"retry-after": "60"}
        mock_request.return_value = mock_response
        
        async with scraper:
            with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
                result = await scraper.get("rate/limited/endpoint")
                
                # Should have slept for retry-after time
                mock_sleep.assert_called_with(60)
    
    @pytest.mark.asyncio
    @patch('src.scraper.httpx.AsyncClient.request')
    async def test_caching_functionality(self, mock_request, scraper, mock_response):
        """Test request caching."""
        mock_request.return_value = mock_response
        
        # Clear cache first
        cache_manager.clear()
        
        async with scraper:
            # First request should hit API
            result1 = await scraper.get("cacheable/endpoint", use_cache=True)
            
            # Second request should hit cache
            result2 = await scraper.get("cacheable/endpoint", use_cache=True)
        
        assert result1 == result2
        # Should only have made one actual request
        assert mock_request.call_count == 1
        assert scraper.stats["cached_requests"] > 0
    
    @pytest.mark.asyncio
    @patch('src.scraper.httpx.AsyncClient.request')
    async def test_multiple_concurrent_requests(self, mock_request, scraper, mock_response):
        """Test concurrent request handling."""
        mock_request.return_value = mock_response
        
        requests = [
            {"endpoint": "test/1", "params": {"id": 1}},
            {"endpoint": "test/2", "params": {"id": 2}},
            {"endpoint": "test/3", "params": {"id": 3}}
        ]
        
        async with scraper:
            results = await scraper.scrape_multiple(requests, max_concurrent=2)
        
        assert len(results) == 3
        assert all(result is not None for result in results)
        assert mock_request.call_count == 3
    
    @pytest.mark.asyncio
    @patch('src.scraper.httpx.AsyncClient.request')
    async def test_paginated_scraping(self, mock_request, scraper):
        """Test paginated data scraping."""
        # Mock responses for multiple pages
        page1_response = Mock()
        page1_response.status_code = 200
        page1_response.json.return_value = {"data": [{"id": f"item_{i}"} for i in range(100)]}
        
        page2_response = Mock()
        page2_response.status_code = 200
        page2_response.json.return_value = {"data": [{"id": f"item_{i}"} for i in range(100, 150)]}
        
        mock_request.side_effect = [page1_response, page2_response]
        
        async with scraper:
            results = await scraper.scrape_paginated(
                "paginated/endpoint",
                per_page=100,
                max_pages=2
            )
        
        assert len(results) == 150
        assert results[0]["id"] == "item_0"
        assert results[-1]["id"] == "item_149"
    
    @pytest.mark.asyncio
    async def test_headers_preparation(self, scraper):
        """Test request headers preparation."""
        with patch('src.user_agent_manager.user_agent_manager.get_user_agent') as mock_ua:
            mock_ua.return_value = "TestAgent/1.0"
            
            headers = scraper._prepare_headers({"Custom-Header": "value"})
            
            assert "User-Agent" in headers
            assert headers["User-Agent"] == "TestAgent/1.0"
            assert headers["Custom-Header"] == "value"
    
    def test_cache_key_generation(self, scraper):
        """Test cache key generation."""
        key1 = scraper._create_cache_key("http://test.com", {"param": "value"})
        key2 = scraper._create_cache_key("http://test.com", {"param": "value"})
        key3 = scraper._create_cache_key("http://test.com", {"param": "different"})
        
        assert key1 == key2  # Same parameters should generate same key
        assert key1 != key3  # Different parameters should generate different key
    
    def test_stats_tracking(self, scraper):
        """Test statistics tracking."""
        initial_stats = scraper.get_stats()
        
        # Manually update stats
        scraper.stats["total_requests"] = 10
        scraper.stats["successful_requests"] = 8
        scraper.stats["cached_requests"] = 2
        
        stats = scraper.get_stats()
        
        assert stats["total_requests"] == 10
        assert stats["successful_requests"] == 8
        assert stats["success_rate"] == 80.0
        assert stats["cache_hit_rate"] == 20.0

if __name__ == "__main__":
    pytest.main([__file__])