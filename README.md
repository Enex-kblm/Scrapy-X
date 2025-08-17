# Advanced Web Scraper Project

A modular, scalable, and efficient web scraping framework with proxy rotation, user-agent management, caching, and elegant logging. Built with modern async Python for high-performance data extraction.

## 🚀 Features

### Core Functionality
- **Async HTTP Requests**: Built with `httpx` for high-performance concurrent scraping
- **Proxy Rotation**: Automatic proxy rotation with health checking and failover
- **User-Agent Management**: Smart user-agent rotation to avoid detection
- **Intelligent Caching**: File-based caching system to avoid duplicate requests
- **Retry Logic**: Exponential backoff with configurable retry strategies
- **Rate Limiting**: Built-in rate limiting to respect API limits

### Data Processing
- **Modular Parser**: Clean, extensible parsing system for multiple data formats
- **Data Normalization**: Automatic data cleaning and standardization
- **Multiple Storage Formats**: JSON, CSV, and SQLite output support
- **Batch Processing**: Efficient batch parsing and processing

### Monitoring & Management
- **Elegant Logging**: Rich console output with real-time dashboard
- **Task Scheduling**: Built-in scheduler for periodic scraping
- **Statistics Tracking**: Comprehensive metrics and performance monitoring
- **Error Handling**: Robust error handling with detailed logging

### Production Ready
- **Unit Tests**: Comprehensive test suite with pytest
- **Configuration Management**: Environment-based configuration
- **Documentation**: Complete API documentation and examples
- **Scalable Architecture**: Clean separation of concerns with SOLID principles

## 📁 Project Structure

```
scraper-project/
│
├── config/
│   ├── settings.py          # Global configuration
│   ├── proxies.txt          # Proxy server list
│   └── user_agents.txt      # User-Agent rotation list
│
├── data/
│   ├── raw/                 # Raw scraped data
│   ├── processed/           # Cleaned and processed data
│   └── logs/                # Application logs
│
├── src/
│   ├── __init__.py
│   ├── main.py              # Main entry point
│   ├── scraper.py           # Core async scraping engine
│   ├── parser.py            # Data parsing and cleaning
│   ├── proxy_manager.py     # Proxy rotation management
│   ├── user_agent_manager.py # User-agent rotation
│   ├── storage.py           # Data storage (JSON/CSV/SQLite)
│   ├── cache.py             # Caching system
│   ├── scheduler.py         # Task scheduling
│   ├── utils.py             # Utility functions
│   └── logger.py            # Elegant logging system
│
├── tests/
│   ├── test_scraper.py      # Scraper tests
│   ├── test_parser.py       # Parser tests
│   └── test_storage.py      # Storage tests
│
├── notebooks/
│   └── analysis.ipynb       # Data analysis examples
│
├── requirements.txt         # Python dependencies
├── README.md               # This file
└── .env.example            # Environment variables template
```

## 🛠️ Installation

### Prerequisites
- Python 3.8 or higher
- pip package manager

### Setup

1. **Clone or download the project files**

2. **Install dependencies**:
```bash
pip install -r requirements.txt
```

3. **Set up environment variables**:
```bash
cp .env.example .env
# Edit .env file with your API keys and configuration
```

4. **Configure proxy list** (optional):
   - Edit `config/proxies.txt` with your proxy servers
   - Format: `protocol://ip:port` (one per line)

5. **Configure user agents** (optional):
   - Edit `config/user_agents.txt` with user agent strings
   - The system includes defaults if file is empty

## 🚦 Quick Start

### Basic Usage

```bash
# Run example scraping
python -m src.main

# Scrape with specific parameters
python -m src.main --query "#AI #MachineLearning" --max-items 100 --format json

# Run with interactive dashboard
python -m src.main --mode interactive

# Schedule periodic scraping
python -m src.main --mode schedule
```

### Command Line Options

```bash
python -m src.main [OPTIONS]

Options:
  --mode {scrape,interactive,schedule}  Running mode (default: scrape)
  --format {json,csv,sqlite}           Output format
  --query TEXT                         Search query for scraping
  --user-id TEXT                       User ID to scrape
  --max-items INTEGER                  Maximum items to scrape (default: 100)
  --no-cache                          Disable caching
  --clear-cache                       Clear cache before starting
  --help                              Show help message
```

## 💻 Usage Examples

### Basic Scraping

```python
import asyncio
from src.scraper import AsyncScraper
from src.parser import data_parser
from src.storage import data_storage

async def basic_scraping():
    async with AsyncScraper() as scraper:
        # Scrape data
        data = await scraper.get("api/endpoint")
        
        # Parse data
        parsed_data = data_parser.parse_twitter_data(data)
        
        # Save results
        data_storage.save_auto(parsed_data, format="json")

# Run the scraper
asyncio.run(basic_scraping())
```

### Advanced Configuration

```python
from config.settings import settings
from src.main import ScrapingPipeline

# Customize settings
settings.CONCURRENT_REQUESTS = 20
settings.REQUEST_TIMEOUT = 60
settings.ENABLE_CACHE = True

# Run pipeline
pipeline = ScrapingPipeline()
asyncio.run(pipeline.run_example_scraping())
```

### Scheduled Scraping

```python
from src.scheduler import task_scheduler, schedule_periodic_scrape

async def my_scraping_task():
    # Your scraping logic here
    pass

# Schedule task every 30 minutes
schedule_periodic_scrape(
    func=my_scraping_task,
    interval_minutes=30,
    name="custom_scraper"
)

# Start scheduler
task_scheduler.start()
```

## ⚙️ Configuration

### Environment Variables (.env)

```bash
# API Configuration
API_KEY=your_api_key_here
API_SECRET=your_api_secret_here
BEARER_TOKEN=your_bearer_token_here

# Rate Limiting
REQUESTS_PER_MINUTE=60
REQUESTS_PER_HOUR=1000

# Proxy Settings
USE_PROXY=true
PROXY_ROTATION=true

# Cache Settings
ENABLE_CACHE=true
CACHE_TTL=3600

# Logging
LOG_LEVEL=INFO

# Scheduling
SCHEDULE_ENABLED=false
SCHEDULE_INTERVAL_MINUTES=10
```

### Settings Configuration

Key settings in `config/settings.py`:

- **Request Configuration**: Timeout, retries, rate limiting
- **Proxy Management**: Proxy rotation, health checking
- **Caching**: TTL, cache size, cleanup intervals
- **Storage**: Default formats, file paths
- **Logging**: Log levels, file rotation
- **Scheduling**: Intervals, task management

## 📊 Dashboard and Monitoring

The scraper includes a real-time dashboard showing:

- **Request Statistics**: Total, successful, failed requests
- **Success Rates**: Request and cache hit rates
- **Current Status**: Active proxy, user agent
- **Performance Metrics**: Response times, error rates
- **Recent Errors**: Latest error messages with timestamps

### Dashboard Example

```
┏━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Metric                 ┃ Value                  ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━┩
│ Total Requests         │ 1,245                  │
│ Successful             │ 1,198                  │
│ Failed                 │ 47                     │
│ Success Rate           │ 96.2%                  │
│ Items Saved            │ 1,150                  │
│ Runtime                │ 0:15:42                │
│ Current Proxy          │ 192.168.1.100:8080    │
└────────────────────────┴────────────────────────┘
```

## 🧪 Testing

Run the test suite:

```bash
# Run all tests
python -m pytest tests/

# Run specific test file
python -m pytest tests/test_scraper.py

# Run with coverage
python -m pytest --cov=src tests/

# Run with verbose output
python -m pytest -v tests/
```

### Test Coverage

The test suite covers:
- HTTP request handling and error cases
- Proxy rotation and failover
- Caching functionality
- Data parsing and validation
- Storage operations
- Configuration management

## 📈 Data Analysis

Use the included Jupyter notebook for data analysis:

```bash
# Start Jupyter
jupyter notebook notebooks/analysis.ipynb
```

The analysis notebook includes:
- Data loading and cleaning
- Engagement metrics analysis
- Temporal pattern analysis
- Language and content analysis
- Hashtag trend analysis
- Correlation analysis
- Visualization examples

## 🔧 Extending the Framework

### Adding New Data Sources

1. **Create API Configuration**:
```python
# In config/settings.py
NEW_API_ENDPOINTS = {
    "user_posts": "/api/v1/users/{user_id}/posts",
    "search": "/api/v1/search"
}
```

2. **Add Parser Methods**:
```python
# In src/parser.py
def parse_new_api_data(self, data):
    schema = {
        "id": {"type": "str"},
        "content": {"type": "str", "transform": self.clean_text},
        # ... more fields
    }
    return self.normalize_data(data, schema)
```

3. **Implement Scraping Logic**:
```python
# In your scraper code
async def scrape_new_api(self, query):
    endpoint = NEW_API_ENDPOINTS["search"]
    data = await self.scraper.get(endpoint, params={"q": query})
    return data_parser.parse_new_api_data(data)
```

### Custom Storage Backends

Extend the storage system:

```python
from src.storage import DataStorage

class CustomStorage(DataStorage):
    def save_to_database(self, data, connection_string):
        # Implement custom database storage
        pass
    
    def save_to_cloud(self, data, cloud_config):
        # Implement cloud storage
        pass
```

## 🚀 Performance Optimization

### Scaling Tips

1. **Increase Concurrency**:
```python
settings.CONCURRENT_REQUESTS = 50  # Adjust based on target server capacity
```

2. **Optimize Caching**:
```python
settings.CACHE_TTL = 7200  # 2 hours
settings.MAX_CACHE_SIZE = 5000  # More items in memory
```

3. **Tune Rate Limiting**:
```python
settings.REQUESTS_PER_MINUTE = 120
settings.REQUESTS_PER_HOUR = 5000
```

4. **Use Multiple Proxy Pools**:
   - Configure high-quality proxy services
   - Implement proxy rotation strategies
   - Monitor proxy health and performance

### Memory Management

- Enable cache cleanup for long-running processes
- Use batch processing for large datasets
- Implement pagination for large result sets
- Monitor memory usage during scraping

## 🛡️ Best Practices

### Ethical Scraping

1. **Respect robots.txt**: Always check and follow robots.txt guidelines
2. **Rate Limiting**: Don't overwhelm target servers
3. **User Agents**: Use realistic, rotating user agents
4. **API Terms**: Follow API terms of service and rate limits
5. **Data Privacy**: Handle personal data responsibly

### Production Deployment

1. **Environment Management**: Use proper environment configurations
2. **Monitoring**: Implement comprehensive monitoring and alerting
3. **Error Recovery**: Design for graceful failure and recovery
4. **Data Backup**: Regular backup of scraped data
5. **Security**: Secure API keys and sensitive configuration

### Code Quality

1. **Error Handling**: Comprehensive exception handling
2. **Logging**: Detailed logging for debugging and monitoring
3. **Testing**: Maintain high test coverage
4. **Documentation**: Keep documentation up to date
5. **Code Review**: Regular code review and refactoring

## 🔍 Troubleshooting

### Common Issues

1. **Proxy Connection Errors**:
   - Check proxy server status
   - Verify proxy credentials
   - Test proxy connectivity manually

2. **Rate Limiting**:
   - Reduce request frequency
   - Implement longer delays
   - Use more diverse proxy pool

3. **Authentication Errors**:
   - Verify API keys and tokens
   - Check token expiration
   - Ensure proper headers

4. **Memory Issues**:
   - Reduce concurrent requests
   - Enable cache cleanup
   - Process data in smaller batches

5. **Data Quality Issues**:
   - Review parser logic
   - Add data validation
   - Implement error handling

### Debug Mode

Enable debug logging:

```python
import logging
from src.logger import setup_logger

# Enable debug logging
logger = setup_logger()
logger.logger.setLevel(logging.DEBUG)
```

## 📄 License

This project is provided as-is for educational and development purposes. Please ensure you comply with the terms of service of any APIs or websites you scrape.

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request with detailed description

## 📞 Support

For issues and questions:

1. Check the troubleshooting section
2. Review the test cases for usage examples
3. Check the configuration documentation
4. Create an issue with detailed information

## 🎯 Roadmap

Future enhancements:
- [ ] GraphQL API support
- [ ] Distributed scraping with Redis
- [ ] Machine learning for content classification
- [ ] Real-time streaming data support
- [ ] Advanced anti-detection measures
- [ ] Integration with popular data science tools
- [ ] Web UI for monitoring and control
- [ ] Plugin system for custom extensions

---

**Happy Scraping! 🕷️**