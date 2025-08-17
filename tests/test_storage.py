"""
Unit tests for the storage module.
"""
import pytest
import json
import csv
import sqlite3
from pathlib import Path
from unittest.mock import Mock, patch
import tempfile
import shutil

from src.storage import DataStorage

@pytest.fixture
def storage():
    """Create storage instance for testing."""
    return DataStorage()

@pytest.fixture
def sample_data():
    """Sample data for testing."""
    return [
        {
            "id": "123",
            "text": "Test tweet 1",
            "created_at": "2024-01-15T10:30:00Z",
            "metrics": {"likes": 5, "retweets": 2}
        },
        {
            "id": "456",
            "text": "Test tweet 2",
            "created_at": "2024-01-15T11:00:00Z",
            "metrics": {"likes": 10, "retweets": 3}
        }
    ]

@pytest.fixture
def temp_dir():
    """Create temporary directory for testing."""
    temp_dir = Path(tempfile.mkdtemp())
    yield temp_dir
    shutil.rmtree(temp_dir)

class TestDataStorage:
    """Test DataStorage functionality."""
    
    def test_save_and_load_json(self, storage, sample_data, temp_dir):
        """Test JSON save and load functionality."""
        # Save data
        file_path = storage.save_json(
            sample_data, 
            filename="test_data.json",
            directory=temp_dir
        )
        
        assert file_path.exists()
        assert file_path.suffix == ".json"
        
        # Load data
        loaded_data = storage.load_json(file_path)
        
        assert len(loaded_data) == 2
        assert loaded_data[0]["id"] == "123"
        assert loaded_data[1]["id"] == "456"
        assert loaded_data == sample_data
    
    def test_save_json_pretty_formatting(self, storage, sample_data, temp_dir):
        """Test JSON pretty formatting."""
        file_path = storage.save_json(
            sample_data,
            filename="pretty.json",
            directory=temp_dir,
            pretty=True
        )
        
        # Check if file is formatted with indentation
        with open(file_path, 'r') as f:
            content = f.read()
            assert "  " in content  # Should have indentation
    
    def test_save_json_single_object(self, storage, temp_dir):
        """Test saving single object to JSON."""
        single_object = {"id": "123", "text": "Single tweet"}
        
        file_path = storage.save_json(
            single_object,
            filename="single.json",
            directory=temp_dir
        )
        
        loaded_data = storage.load_json(file_path)
        assert loaded_data == single_object
    
    def test_save_and_load_csv(self, storage, sample_data, temp_dir):
        """Test CSV save and load functionality."""
        # Save data
        file_path = storage.save_csv(
            sample_data,
            filename="test_data.csv",
            directory=temp_dir
        )
        
        assert file_path.exists()
        assert file_path.suffix == ".csv"
        
        # Load data
        loaded_data = storage.load_csv(file_path)
        
        assert len(loaded_data) == 2
        assert loaded_data[0]["id"] == "123"
        
        # Note: CSV loading converts nested objects back from JSON strings
        assert isinstance(loaded_data[0]["metrics"], dict)
    
    def test_csv_nested_objects_handling(self, storage, temp_dir):
        """Test CSV handling of nested objects."""
        data = [
            {
                "id": "123",
                "nested": {"key": "value", "number": 42},
                "list": [1, 2, 3]
            }
        ]
        
        file_path = storage.save_csv(data, filename="nested.csv", directory=temp_dir)
        loaded_data = storage.load_csv(file_path)
        
        assert loaded_data[0]["nested"]["key"] == "value"
        assert loaded_data[0]["nested"]["number"] == 42
        assert loaded_data[0]["list"] == [1, 2, 3]
    
    def test_save_and_load_sqlite(self, storage, sample_data, temp_dir):
        """Test SQLite save and load functionality."""
        db_path = temp_dir / "test.db"
        
        # Save data
        result_path = storage.save_sqlite(
            sample_data,
            table_name="tweets",
            db_path=db_path
        )
        
        assert result_path.exists()
        assert result_path == db_path
        
        # Load data
        loaded_data = storage.load_sqlite("tweets", db_path=db_path)
        
        assert len(loaded_data) == 2
        assert loaded_data[0]["id"] == "123"
    
    def test_sqlite_custom_query(self, storage, sample_data, temp_dir):
        """Test SQLite with custom query."""
        db_path = temp_dir / "test.db"
        
        # Save data
        storage.save_sqlite(sample_data, "tweets", db_path=db_path)
        
        # Load with custom query
        loaded_data = storage.load_sqlite(
            "tweets",
            db_path=db_path,
            query="SELECT * FROM tweets WHERE id = '123'"
        )
        
        assert len(loaded_data) == 1
        assert loaded_data[0]["id"] == "123"
    
    def test_sqlite_append_mode(self, storage, sample_data, temp_dir):
        """Test SQLite append mode."""
        db_path = temp_dir / "test.db"
        
        # Save initial data
        storage.save_sqlite(sample_data[:1], "tweets", db_path=db_path)
        
        # Append more data
        storage.save_sqlite(sample_data[1:], "tweets", db_path=db_path, if_exists="append")
        
        # Load all data
        loaded_data = storage.load_sqlite("tweets", db_path=db_path)
        
        assert len(loaded_data) == 2
    
    def test_save_auto_format_detection(self, storage, sample_data, temp_dir):
        """Test automatic format detection."""
        # JSON file
        json_path = storage.save_auto(
            sample_data,
            filename="auto_test.json",
            directory=temp_dir
        )
        assert json_path.suffix == ".json"
        
        # CSV file
        csv_path = storage.save_auto(
            sample_data,
            filename="auto_test.csv",
            directory=temp_dir
        )
        assert csv_path.suffix == ".csv"
        
        # SQLite file
        sqlite_path = storage.save_auto(
            sample_data,
            filename="auto_test.db",
            directory=temp_dir
        )
        assert sqlite_path.suffix == ".db"
    
    def test_save_auto_explicit_format(self, storage, sample_data, temp_dir):
        """Test save_auto with explicit format."""
        file_path = storage.save_auto(
            sample_data,
            format="json",
            filename="explicit_format_test",
            directory=temp_dir
        )
        
        assert file_path.suffix == ".json"
    
    def test_empty_data_handling(self, storage, temp_dir):
        """Test handling of empty data."""
        # CSV and SQLite should raise error for empty data
        with pytest.raises(ValueError):
            storage.save_csv([], filename="empty.csv", directory=temp_dir)
        
        with pytest.raises(ValueError):
            storage.save_sqlite([], "empty_table", db_path=temp_dir / "empty.db")
        
        # JSON should handle empty list
        file_path = storage.save_json([], filename="empty.json", directory=temp_dir)
        loaded_data = storage.load_json(file_path)
        assert loaded_data == []
    
    def test_filename_sanitization(self, storage, sample_data, temp_dir):
        """Test filename sanitization."""
        unsafe_filename = "test<file>name:with|invalid?chars*.json"
        
        file_path = storage.save_json(
            sample_data,
            filename=unsafe_filename,
            directory=temp_dir
        )
        
        # Check that invalid characters are replaced
        assert "<" not in file_path.name
        assert ">" not in file_path.name
        assert ":" not in file_path.name
        assert "|" not in file_path.name
        assert "?" not in file_path.name
        assert "*" not in file_path.name
    
    def test_auto_filename_generation(self, storage, sample_data, temp_dir):
        """Test automatic filename generation."""
        file_path = storage.save_json(sample_data, directory=temp_dir)
        
        # Should generate filename with timestamp
        assert "scraped_data_" in file_path.name
        assert file_path.suffix == ".json"
    
    def test_file_extension_handling(self, storage, sample_data, temp_dir):
        """Test file extension handling."""
        # Should add extension if missing
        file_path = storage.save_json(
            sample_data,
            filename="test_no_extension",
            directory=temp_dir
        )
        assert file_path.suffix == ".json"
        
        # Should not double-add extension
        file_path = storage.save_json(
            sample_data,
            filename="test_with_extension.json",
            directory=temp_dir
        )
        assert file_path.name.count(".json") == 1
    
    @patch('src.storage.settings')
    def test_get_storage_stats(self, mock_settings, storage, sample_data, temp_dir):
        """Test storage statistics."""
        # Mock settings to use temp directory
        mock_settings.RAW_DATA_DIR = temp_dir
        mock_settings.PROCESSED_DATA_DIR = temp_dir / "processed"
        mock_settings.PROCESSED_DATA_DIR.mkdir(exist_ok=True)
        
        # Create some test files
        storage.save_json(sample_data, filename="test1.json", directory=temp_dir)
        storage.save_csv(sample_data, filename="test2.csv", directory=temp_dir)
        
        stats = storage.get_storage_stats()
        
        assert stats["raw_data_files"] >= 2
        assert stats["formats"]["json"] >= 1
        assert stats["formats"]["csv"] >= 1
        assert stats["total_size_mb"] > 0
    
    def test_error_handling(self, storage, temp_dir):
        """Test error handling in storage operations."""
        # Test loading non-existent file
        with pytest.raises(FileNotFoundError):
            storage.load_json(temp_dir / "nonexistent.json")
        
        # Test loading from non-existent database
        with pytest.raises(FileNotFoundError):
            storage.load_sqlite("table", db_path=temp_dir / "nonexistent.db")

if __name__ == "__main__":
    pytest.main([__file__])