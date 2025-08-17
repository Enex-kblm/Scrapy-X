"""
Data parsing and cleaning utilities.
"""
import re
from typing import Dict, List, Any, Optional, Union, Callable
from datetime import datetime
from bs4 import BeautifulSoup
import json

from .logger import get_logger

class DataParser:
    """Modular data parser for various data formats."""
    
    def __init__(self):
        self.logger = get_logger()
    
    def clean_text(self, text: str) -> str:
        """
        Clean and normalize text data.
        
        Args:
            text: Raw text
        
        Returns:
            Cleaned text
        """
        if not isinstance(text, str):
            return str(text) if text is not None else ""
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove control characters
        text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text)
        
        # Strip leading/trailing whitespace
        text = text.strip()
        
        return text
    
    def extract_urls(self, text: str) -> List[str]:
        """
        Extract URLs from text.
        
        Args:
            text: Text containing URLs
        
        Returns:
            List of URLs
        """
        url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
        urls = re.findall(url_pattern, text)
        return list(set(urls))  # Remove duplicates
    
    def extract_hashtags(self, text: str) -> List[str]:
        """
        Extract hashtags from text.
        
        Args:
            text: Text containing hashtags
        
        Returns:
            List of hashtags
        """
        hashtag_pattern = r'#\w+'
        hashtags = re.findall(hashtag_pattern, text)
        return [tag.lower() for tag in hashtags]
    
    def extract_mentions(self, text: str) -> List[str]:
        """
        Extract mentions from text.
        
        Args:
            text: Text containing mentions
        
        Returns:
            List of mentions
        """
        mention_pattern = r'@\w+'
        mentions = re.findall(mention_pattern, text)
        return [mention.lower() for mention in mentions]
    
    def parse_html(self, html: str) -> str:
        """
        Parse HTML and extract text content.
        
        Args:
            html: HTML string
        
        Returns:
            Extracted text
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            
            # Get text
            text = soup.get_text()
            
            # Clean text
            return self.clean_text(text)
            
        except Exception as e:
            self.logger.error(f"HTML parsing failed: {e}")
            return ""
    
    def parse_date(self, date_str: str, formats: Optional[List[str]] = None) -> Optional[datetime]:
        """
        Parse date string into datetime object.
        
        Args:
            date_str: Date string
            formats: List of date formats to try
        
        Returns:
            Datetime object or None if parsing failed
        """
        if not date_str:
            return None
        
        # Default date formats
        default_formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%d",
            "%m/%d/%Y",
            "%d/%m/%Y",
            "%Y%m%d",
            "%b %d, %Y",
            "%B %d, %Y"
        ]
        
        formats = formats or default_formats
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        self.logger.warning(f"Could not parse date: {date_str}")
        return None
    
    def extract_numbers(self, text: str) -> List[float]:
        """
        Extract numbers from text.
        
        Args:
            text: Text containing numbers
        
        Returns:
            List of numbers
        """
        # Match integers and floats
        number_pattern = r'-?\d+\.?\d*'
        numbers = re.findall(number_pattern, text)
        
        try:
            return [float(num) for num in numbers if num]
        except ValueError:
            return []
    
    def normalize_data(self, data: Dict[str, Any], schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize data according to schema.
        
        Args:
            data: Raw data
            schema: Data schema with field types and transformations
        
        Returns:
            Normalized data
        """
        normalized = {}
        
        for field, config in schema.items():
            if field not in data:
                # Use default value if specified
                if "default" in config:
                    normalized[field] = config["default"]
                continue
            
            value = data[field]
            field_type = config.get("type", "str")
            
            try:
                if field_type == "str":
                    normalized[field] = self.clean_text(str(value))
                elif field_type == "int":
                    normalized[field] = int(value) if value else 0
                elif field_type == "float":
                    normalized[field] = float(value) if value else 0.0
                elif field_type == "bool":
                    normalized[field] = bool(value)
                elif field_type == "date":
                    normalized[field] = self.parse_date(str(value))
                elif field_type == "list":
                    normalized[field] = value if isinstance(value, list) else [value]
                elif field_type == "json":
                    normalized[field] = value if isinstance(value, (dict, list)) else json.loads(str(value))
                else:
                    normalized[field] = value
                
                # Apply custom transformation if specified
                if "transform" in config and callable(config["transform"]):
                    normalized[field] = config["transform"](normalized[field])
                    
            except Exception as e:
                self.logger.warning(f"Failed to normalize field {field}: {e}")
                normalized[field] = config.get("default")
        
        return normalized
    
    def parse_twitter_data(self, tweet_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse Twitter/X data into standardized format.
        
        Args:
            tweet_data: Raw tweet data from API
        
        Returns:
            Parsed tweet data
        """
        schema = {
            "id": {"type": "str"},
            "text": {"type": "str", "transform": self.clean_text},
            "created_at": {"type": "date"},
            "author_id": {"type": "str"},
            "public_metrics": {"type": "json", "default": {}},
            "lang": {"type": "str", "default": "en"},
            "reply_settings": {"type": "str", "default": "everyone"}
        }
        
        parsed = self.normalize_data(tweet_data, schema)
        
        # Extract additional features
        if "text" in parsed and parsed["text"]:
            parsed["hashtags"] = self.extract_hashtags(parsed["text"])
            parsed["mentions"] = self.extract_mentions(parsed["text"])
            parsed["urls"] = self.extract_urls(parsed["text"])
            parsed["text_length"] = len(parsed["text"])
        
        # Extract metrics
        metrics = parsed.get("public_metrics", {})
        parsed["retweet_count"] = metrics.get("retweet_count", 0)
        parsed["like_count"] = metrics.get("like_count", 0)
        parsed["reply_count"] = metrics.get("reply_count", 0)
        parsed["quote_count"] = metrics.get("quote_count", 0)
        
        return parsed
    
    def parse_user_data(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse user data into standardized format.
        
        Args:
            user_data: Raw user data from API
        
        Returns:
            Parsed user data
        """
        schema = {
            "id": {"type": "str"},
            "username": {"type": "str"},
            "name": {"type": "str", "transform": self.clean_text},
            "description": {"type": "str", "transform": self.clean_text},
            "created_at": {"type": "date"},
            "public_metrics": {"type": "json", "default": {}},
            "verified": {"type": "bool", "default": False},
            "protected": {"type": "bool", "default": False},
            "location": {"type": "str", "default": ""},
            "url": {"type": "str", "default": ""}
        }
        
        parsed = self.normalize_data(user_data, schema)
        
        # Extract metrics
        metrics = parsed.get("public_metrics", {})
        parsed["followers_count"] = metrics.get("followers_count", 0)
        parsed["following_count"] = metrics.get("following_count", 0)
        parsed["tweet_count"] = metrics.get("tweet_count", 0)
        parsed["listed_count"] = metrics.get("listed_count", 0)
        
        # Calculate engagement metrics
        if parsed["followers_count"] > 0:
            parsed["engagement_ratio"] = parsed["tweet_count"] / parsed["followers_count"]
        else:
            parsed["engagement_ratio"] = 0.0
        
        return parsed
    
    def batch_parse(self, 
                   data_list: List[Dict[str, Any]], 
                   parser_func: Callable) -> List[Dict[str, Any]]:
        """
        Parse multiple items using specified parser function.
        
        Args:
            data_list: List of raw data items
            parser_func: Parser function to apply
        
        Returns:
            List of parsed data
        """
        parsed_data = []
        
        for i, item in enumerate(data_list):
            try:
                parsed_item = parser_func(item)
                parsed_data.append(parsed_item)
            except Exception as e:
                self.logger.error(f"Failed to parse item {i}: {e}")
                continue
        
        self.logger.info(f"Parsed {len(parsed_data)}/{len(data_list)} items")
        return parsed_data
    
    def validate_data(self, data: Dict[str, Any], required_fields: List[str]) -> bool:
        """
        Validate that data contains required fields.
        
        Args:
            data: Data to validate
            required_fields: List of required field names
        
        Returns:
            True if valid
        """
        missing_fields = []
        
        for field in required_fields:
            if field not in data or data[field] is None:
                missing_fields.append(field)
        
        if missing_fields:
            self.logger.warning(f"Missing required fields: {missing_fields}")
            return False
        
        return True
    
    def filter_data(self, 
                   data_list: List[Dict[str, Any]], 
                   filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Filter data based on criteria.
        
        Args:
            data_list: List of data items
            filters: Filter criteria (field: value or field: callable)
        
        Returns:
            Filtered data list
        """
        filtered_data = []
        
        for item in data_list:
            include = True
            
            for field, criteria in filters.items():
                if field not in item:
                    include = False
                    break
                
                if callable(criteria):
                    if not criteria(item[field]):
                        include = False
                        break
                else:
                    if item[field] != criteria:
                        include = False
                        break
            
            if include:
                filtered_data.append(item)
        
        self.logger.info(f"Filtered {len(filtered_data)}/{len(data_list)} items")
        return filtered_data
    
    def aggregate_data(self, 
                      data_list: List[Dict[str, Any]], 
                      group_by: str,
                      aggregations: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
        """
        Aggregate data by grouping field.
        
        Args:
            data_list: List of data items
            group_by: Field to group by
            aggregations: Aggregation functions (field: function_name)
                         Supported functions: sum, avg, count, min, max
        
        Returns:
            Aggregated data grouped by field
        """
        groups = {}
        
        # Group data
        for item in data_list:
            if group_by not in item:
                continue
            
            group_key = str(item[group_by])
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(item)
        
        # Calculate aggregations
        results = {}
        for group_key, group_items in groups.items():
            group_result = {"count": len(group_items)}
            
            for field, func_name in aggregations.items():
                values = [item.get(field, 0) for item in group_items if field in item]
                
                if not values:
                    group_result[f"{field}_{func_name}"] = 0
                    continue
                
                if func_name == "sum":
                    group_result[f"{field}_{func_name}"] = sum(values)
                elif func_name == "avg":
                    group_result[f"{field}_{func_name}"] = sum(values) / len(values)
                elif func_name == "min":
                    group_result[f"{field}_{func_name}"] = min(values)
                elif func_name == "max":
                    group_result[f"{field}_{func_name}"] = max(values)
                elif func_name == "count":
                    group_result[f"{field}_{func_name}"] = len([v for v in values if v])
            
            results[group_key] = group_result
        
        self.logger.info(f"Aggregated data into {len(results)} groups")
        return results

# Global parser instance
data_parser = DataParser()