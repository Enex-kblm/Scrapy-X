"""
Data storage utilities for JSON, CSV, and SQLite.
"""
import json
import csv
import sqlite3
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
import pandas as pd

from config.settings import settings
from .logger import get_logger
from .utils import generate_timestamp, sanitize_filename

class DataStorage:
    """Handle data storage in multiple formats."""
    
    def __init__(self):
        self.logger = get_logger()
    
    def save_json(self, 
                  data: Union[Dict[str, Any], List[Dict[str, Any]]],
                  filename: Optional[str] = None,
                  directory: Optional[Path] = None,
                  pretty: bool = True) -> Path:
        """
        Save data to JSON file.
        
        Args:
            data: Data to save
            filename: Custom filename (auto-generated if None)
            directory: Target directory (defaults to RAW_DATA_DIR)
            pretty: Whether to format JSON with indentation
        
        Returns:
            Path to saved file
        """
        directory = directory or settings.RAW_DATA_DIR
        
        if filename is None:
            timestamp = generate_timestamp()
            filename = f"scraped_data_{timestamp}.json"
        
        filename = sanitize_filename(filename)
        if not filename.endswith('.json'):
            filename += '.json'
        
        file_path = directory / filename
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                if pretty:
                    json.dump(data, f, indent=2, ensure_ascii=False, default=str)
                else:
                    json.dump(data, f, ensure_ascii=False, default=str)
            
            item_count = len(data) if isinstance(data, list) else 1
            self.logger.success(f"Saved {item_count} items to JSON: {file_path}")
            self.logger.stats.saved_items += item_count
            
            return file_path
            
        except Exception as e:
            self.logger.error(f"Failed to save JSON: {e}")
            raise
    
    def load_json(self, file_path: Path) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Load data from JSON file.
        
        Args:
            file_path: Path to JSON file
        
        Returns:
            Loaded data
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            item_count = len(data) if isinstance(data, list) else 1
            self.logger.info(f"Loaded {item_count} items from JSON: {file_path}")
            
            return data
            
        except Exception as e:
            self.logger.error(f"Failed to load JSON: {e}")
            raise
    
    def save_csv(self,
                 data: List[Dict[str, Any]],
                 filename: Optional[str] = None,
                 directory: Optional[Path] = None,
                 fieldnames: Optional[List[str]] = None) -> Path:
        """
        Save data to CSV file.
        
        Args:
            data: List of dictionaries to save
            filename: Custom filename (auto-generated if None)
            directory: Target directory (defaults to PROCESSED_DATA_DIR)
            fieldnames: Column names (auto-detected if None)
        
        Returns:
            Path to saved file
        """
        if not data:
            raise ValueError("No data to save")
        
        directory = directory or settings.PROCESSED_DATA_DIR
        
        if filename is None:
            timestamp = generate_timestamp()
            filename = f"scraped_data_{timestamp}.csv"
        
        filename = sanitize_filename(filename)
        if not filename.endswith('.csv'):
            filename += '.csv'
        
        file_path = directory / filename
        
        # Auto-detect fieldnames if not provided
        if fieldnames is None:
            fieldnames = set()
            for item in data:
                fieldnames.update(item.keys())
            fieldnames = sorted(list(fieldnames))
        
        try:
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                for item in data:
                    # Handle nested objects by converting to JSON strings
                    row = {}
                    for field in fieldnames:
                        value = item.get(field, '')
                        if isinstance(value, (dict, list)):
                            row[field] = json.dumps(value, default=str)
                        else:
                            row[field] = str(value) if value is not None else ''
                    writer.writerow(row)
            
            self.logger.success(f"Saved {len(data)} items to CSV: {file_path}")
            self.logger.stats.saved_items += len(data)
            
            return file_path
            
        except Exception as e:
            self.logger.error(f"Failed to save CSV: {e}")
            raise
    
    def load_csv(self, file_path: Path) -> List[Dict[str, Any]]:
        """
        Load data from CSV file.
        
        Args:
            file_path: Path to CSV file
        
        Returns:
            List of dictionaries
        """
        try:
            data = []
            
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Try to parse JSON strings back to objects
                    parsed_row = {}
                    for key, value in row.items():
                        try:
                            # Try to parse as JSON
                            parsed_value = json.loads(value)
                            parsed_row[key] = parsed_value
                        except (json.JSONDecodeError, TypeError):
                            # Keep as string
                            parsed_row[key] = value
                    data.append(parsed_row)
            
            self.logger.info(f"Loaded {len(data)} items from CSV: {file_path}")
            return data
            
        except Exception as e:
            self.logger.error(f"Failed to load CSV: {e}")
            raise
    
    def save_sqlite(self,
                    data: List[Dict[str, Any]],
                    table_name: str,
                    db_path: Optional[Path] = None,
                    if_exists: str = "append") -> Path:
        """
        Save data to SQLite database.
        
        Args:
            data: List of dictionaries to save
            table_name: Name of the table
            db_path: Path to database file (defaults to settings.SQLITE_DB_PATH)
            if_exists: What to do if table exists ('append', 'replace', 'fail')
        
        Returns:
            Path to database file
        """
        if not data:
            raise ValueError("No data to save")
        
        db_path = db_path or settings.SQLITE_DB_PATH
        
        try:
            # Use pandas for easier SQLite operations
            df = pd.DataFrame(data)
            
            # Handle datetime columns
            for col in df.columns:
                if df[col].dtype == 'object':
                    # Try to convert datetime strings
                    try:
                        df[col] = pd.to_datetime(df[col], errors='ignore')
                    except:
                        pass
            
            # Connect to database
            with sqlite3.connect(db_path) as conn:
                df.to_sql(table_name, conn, if_exists=if_exists, index=False)
            
            self.logger.success(f"Saved {len(data)} items to SQLite table '{table_name}': {db_path}")
            self.logger.stats.saved_items += len(data)
            
            return db_path
            
        except Exception as e:
            self.logger.error(f"Failed to save to SQLite: {e}")
            raise
    
    def load_sqlite(self,
                    table_name: str,
                    db_path: Optional[Path] = None,
                    query: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Load data from SQLite database.
        
        Args:
            table_name: Name of the table
            db_path: Path to database file (defaults to settings.SQLITE_DB_PATH)
            query: Custom SQL query (defaults to SELECT * FROM table_name)
        
        Returns:
            List of dictionaries
        """
        db_path = db_path or settings.SQLITE_DB_PATH
        
        if not db_path.exists():
            raise FileNotFoundError(f"Database file not found: {db_path}")
        
        try:
            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row  # Enable column access by name
                
                if query is None:
                    query = f"SELECT * FROM {table_name}"
                
                cursor = conn.execute(query)
                rows = cursor.fetchall()
                
                # Convert to list of dictionaries
                data = [dict(row) for row in rows]
            
            self.logger.info(f"Loaded {len(data)} items from SQLite table '{table_name}'")
            return data
            
        except Exception as e:
            self.logger.error(f"Failed to load from SQLite: {e}")
            raise
    
    def save_auto(self, 
                  data: Union[Dict[str, Any], List[Dict[str, Any]]],
                  format: Optional[str] = None,
                  filename: Optional[str] = None,
                  **kwargs) -> Path:
        """
        Save data using automatic format detection.
        
        Args:
            data: Data to save
            format: Output format ('json', 'csv', 'sqlite') or auto-detect from filename
            filename: Output filename
            **kwargs: Additional arguments for specific save methods
        
        Returns:
            Path to saved file
        """
        # Ensure data is a list for CSV and SQLite
        if format in ('csv', 'sqlite') and not isinstance(data, list):
            data = [data]
        
        # Auto-detect format
        if format is None:
            if filename:
                if filename.endswith('.json'):
                    format = 'json'
                elif filename.endswith('.csv'):
                    format = 'csv'
                elif filename.endswith('.db') or filename.endswith('.sqlite'):
                    format = 'sqlite'
            
            if format is None:
                format = settings.DEFAULT_OUTPUT_FORMAT
        
        # Save using appropriate method
        if format == 'json':
            return self.save_json(data, filename, **kwargs)
        elif format == 'csv':
            return self.save_csv(data, filename, **kwargs)
        elif format == 'sqlite':
            table_name = kwargs.pop('table_name', 'scraped_data')
            return self.save_sqlite(data, table_name, **kwargs)
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        stats = {
            "raw_data_files": 0,
            "processed_data_files": 0,
            "total_size_mb": 0.0,
            "formats": {"json": 0, "csv": 0, "sqlite": 0}
        }
        
        try:
            # Count raw data files
            if settings.RAW_DATA_DIR.exists():
                raw_files = list(settings.RAW_DATA_DIR.glob("*"))
                stats["raw_data_files"] = len(raw_files)
                
                for file_path in raw_files:
                    if file_path.is_file():
                        stats["total_size_mb"] += file_path.stat().st_size / (1024 * 1024)
                        
                        suffix = file_path.suffix.lower()
                        if suffix == ".json":
                            stats["formats"]["json"] += 1
                        elif suffix == ".csv":
                            stats["formats"]["csv"] += 1
                        elif suffix in [".db", ".sqlite"]:
                            stats["formats"]["sqlite"] += 1
            
            # Count processed data files
            if settings.PROCESSED_DATA_DIR.exists():
                processed_files = list(settings.PROCESSED_DATA_DIR.glob("*"))
                stats["processed_data_files"] = len(processed_files)
                
                for file_path in processed_files:
                    if file_path.is_file():
                        stats["total_size_mb"] += file_path.stat().st_size / (1024 * 1024)
            
        except Exception as e:
            self.logger.error(f"Failed to get storage stats: {e}")
        
        return stats

# Global storage instance
data_storage = DataStorage()