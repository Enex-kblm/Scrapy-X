"""
Unit tests for the parser module.
"""
import pytest
from datetime import datetime
from unittest.mock import Mock

from src.parser import DataParser

@pytest.fixture
def parser():
    """Create parser instance for testing."""
    return DataParser()

@pytest.fixture
def sample_tweet_data():
    """Sample tweet data for testing."""
    return {
        "id": "123456789",
        "text": "This is a test tweet with #hashtag and @mention https://example.com",
        "created_at": "2024-01-15T10:30:00Z",
        "author_id": "987654321",
        "public_metrics": {
            "retweet_count": 5,
            "like_count": 15,
            "reply_count": 2,
            "quote_count": 1
        },
        "lang": "en"
    }

@pytest.fixture
def sample_user_data():
    """Sample user data for testing."""
    return {
        "id": "987654321",
        "username": "testuser",
        "name": "Test User",
        "description": "A test user account for testing purposes",
        "created_at": "2020-01-01T00:00:00Z",
        "public_metrics": {
            "followers_count": 1000,
            "following_count": 500,
            "tweet_count": 250,
            "listed_count": 10
        },
        "verified": False,
        "protected": False
    }

class TestDataParser:
    """Test DataParser functionality."""
    
    def test_clean_text(self, parser):
        """Test text cleaning functionality."""
        # Test whitespace normalization
        text = "  This   has    extra   spaces  "
        cleaned = parser.clean_text(text)
        assert cleaned == "This has extra spaces"
        
        # Test control character removal
        text = "Text\x00with\x1fcontrol\x7fchars"
        cleaned = parser.clean_text(text)
        assert "\x00" not in cleaned
        assert "\x1f" not in cleaned
        assert "\x7f" not in cleaned
        
        # Test None handling
        assert parser.clean_text(None) == ""
        
        # Test non-string handling
        assert parser.clean_text(123) == "123"
    
    def test_extract_urls(self, parser):
        """Test URL extraction."""
        text = "Check out https://example.com and http://test.org for more info"
        urls = parser.extract_urls(text)
        
        assert len(urls) == 2
        assert "https://example.com" in urls
        assert "http://test.org" in urls
    
    def test_extract_hashtags(self, parser):
        """Test hashtag extraction."""
        text = "This tweet has #AI #MachineLearning and #Python hashtags"
        hashtags = parser.extract_hashtags(text)
        
        assert len(hashtags) == 3
        assert "#ai" in hashtags  # Should be lowercase
        assert "#machinelearning" in hashtags
        assert "#python" in hashtags
    
    def test_extract_mentions(self, parser):
        """Test mention extraction."""
        text = "Hello @user1 and @user2, how are you?"
        mentions = parser.extract_mentions(text)
        
        assert len(mentions) == 2
        assert "@user1" in mentions
        assert "@user2" in mentions
    
    def test_parse_html(self, parser):
        """Test HTML parsing."""
        html = "<div><p>This is <strong>bold</strong> text</p><script>alert('test')</script></div>"
        text = parser.parse_html(html)
        
        assert "This is bold text" in text
        assert "alert" not in text  # Script should be removed
        assert "<" not in text  # HTML tags should be removed
    
    def test_parse_date(self, parser):
        """Test date parsing."""
        # Test ISO format
        date_str = "2024-01-15T10:30:00Z"
        parsed_date = parser.parse_date(date_str)
        assert isinstance(parsed_date, datetime)
        assert parsed_date.year == 2024
        assert parsed_date.month == 1
        assert parsed_date.day == 15
        
        # Test alternative format
        date_str = "2024-01-15"
        parsed_date = parser.parse_date(date_str)
        assert isinstance(parsed_date, datetime)
        
        # Test invalid format
        parsed_date = parser.parse_date("invalid-date")
        assert parsed_date is None
    
    def test_extract_numbers(self, parser):
        """Test number extraction."""
        text = "The price is $19.99 and quantity is 5"
        numbers = parser.extract_numbers(text)
        
        assert 19.99 in numbers
        assert 5.0 in numbers
        
        # Test negative numbers
        text = "Temperature is -5.2 degrees"
        numbers = parser.extract_numbers(text)
        assert -5.2 in numbers
    
    def test_normalize_data(self, parser):
        """Test data normalization."""
        raw_data = {
            "id": 123,
            "text": "  Test text  ",
            "count": "42",
            "score": "3.14",
            "active": "true",
            "created": "2024-01-15"
        }
        
        schema = {
            "id": {"type": "str"},
            "text": {"type": "str"},
            "count": {"type": "int"},
            "score": {"type": "float"},
            "active": {"type": "bool"},
            "created": {"type": "date"},
            "missing": {"type": "str", "default": "N/A"}
        }
        
        normalized = parser.normalize_data(raw_data, schema)
        
        assert normalized["id"] == "123"
        assert normalized["text"] == "Test text"
        assert normalized["count"] == 42
        assert normalized["score"] == 3.14
        assert normalized["active"] is True
        assert isinstance(normalized["created"], datetime)
        assert normalized["missing"] == "N/A"
    
    def test_parse_twitter_data(self, parser, sample_tweet_data):
        """Test Twitter data parsing."""
        parsed = parser.parse_twitter_data(sample_tweet_data)
        
        assert parsed["id"] == "123456789"
        assert "hashtag" in parsed["hashtags"][0]
        assert "mention" in parsed["mentions"][0]
        assert len(parsed["urls"]) == 1
        assert parsed["retweet_count"] == 5
        assert parsed["like_count"] == 15
        assert parsed["text_length"] > 0
    
    def test_parse_user_data(self, parser, sample_user_data):
        """Test user data parsing."""
        parsed = parser.parse_user_data(sample_user_data)
        
        assert parsed["id"] == "987654321"
        assert parsed["username"] == "testuser"
        assert parsed["followers_count"] == 1000
        assert parsed["following_count"] == 500
        assert parsed["engagement_ratio"] == 0.25  # tweet_count / followers_count
    
    def test_batch_parse(self, parser, sample_tweet_data):
        """Test batch parsing."""
        data_list = [sample_tweet_data.copy() for _ in range(3)]
        
        # Modify IDs to make them unique
        for i, item in enumerate(data_list):
            item["id"] = f"12345678{i}"
        
        parsed_list = parser.batch_parse(data_list, parser.parse_twitter_data)
        
        assert len(parsed_list) == 3
        assert all("hashtags" in item for item in parsed_list)
        assert all("mentions" in item for item in parsed_list)
    
    def test_validate_data(self, parser, sample_tweet_data):
        """Test data validation."""
        required_fields = ["id", "text", "created_at"]
        
        # Valid data
        assert parser.validate_data(sample_tweet_data, required_fields) is True
        
        # Invalid data (missing field)
        invalid_data = sample_tweet_data.copy()
        del invalid_data["text"]
        assert parser.validate_data(invalid_data, required_fields) is False
    
    def test_filter_data(self, parser):
        """Test data filtering."""
        data_list = [
            {"lang": "en", "score": 5.0},
            {"lang": "es", "score": 3.0},
            {"lang": "en", "score": 4.5},
            {"lang": "fr", "score": 2.0}
        ]
        
        # Filter by language
        filtered = parser.filter_data(data_list, {"lang": "en"})
        assert len(filtered) == 2
        assert all(item["lang"] == "en" for item in filtered)
        
        # Filter by callable
        filtered = parser.filter_data(data_list, {"score": lambda x: x >= 4.0})
        assert len(filtered) == 2
        assert all(item["score"] >= 4.0 for item in filtered)
    
    def test_aggregate_data(self, parser):
        """Test data aggregation."""
        data_list = [
            {"category": "A", "value": 10, "count": 1},
            {"category": "A", "value": 20, "count": 1},
            {"category": "B", "value": 15, "count": 1},
            {"category": "B", "value": 25, "count": 1}
        ]
        
        aggregated = parser.aggregate_data(
            data_list,
            group_by="category",
            aggregations={"value": "sum", "count": "count"}
        )
        
        assert len(aggregated) == 2
        assert aggregated["A"]["value_sum"] == 30
        assert aggregated["A"]["count"] == 2
        assert aggregated["B"]["value_sum"] == 40
        assert aggregated["B"]["count"] == 2

if __name__ == "__main__":
    pytest.main([__file__])