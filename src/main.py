"""
Main entry point for the web scraper project.
"""
import asyncio
import argparse
from typing import Dict, Any, List
from pathlib import Path

from config.settings import settings
from .logger import setup_logger, get_logger
from .scraper import AsyncScraper
from .parser import data_parser
from .storage import data_storage
from .cache import cache_manager
from .scheduler import task_scheduler, schedule_periodic_scrape
from .proxy_manager import proxy_manager
from .user_agent_manager import user_agent_manager

class ScrapingPipeline:
    """Main scraping pipeline orchestrator."""
    
    def __init__(self):
        self.logger = setup_logger()
        self.logger.show_startup_banner()
        
        # Initialize components
        self.scraper = AsyncScraper()
        self.results = []
        
        # Display system status
        self._show_system_status()
    
    def _show_system_status(self):
        """Display system configuration status."""
        proxy_stats = proxy_manager.get_proxy_stats()
        ua_stats = user_agent_manager.get_stats()
        cache_stats = cache_manager.get_stats()
        
        self.logger.info(f"Proxy Manager: {proxy_stats['working']}/{proxy_stats['total']} working")
        self.logger.info(f"User Agents: {ua_stats['total_user_agents']} loaded")
        self.logger.info(f"Cache: {'Enabled' if cache_stats['enabled'] else 'Disabled'}")
        self.logger.info(f"Output Format: {settings.DEFAULT_OUTPUT_FORMAT}")
    
    async def scrape_twitter_user_timeline(self, user_id: str, max_tweets: int = 100) -> List[Dict[str, Any]]:
        """
        Example: Scrape Twitter user timeline.
        
        Args:
            user_id: Twitter user ID
            max_tweets: Maximum number of tweets to scrape
        
        Returns:
            List of parsed tweet data
        """
        self.logger.info(f"Scraping timeline for user: {user_id}")
        
        async with self.scraper:
            # Example endpoint (replace with actual API)
            endpoint = f"2/users/{user_id}/tweets"
            
            tweets_data = await self.scraper.scrape_paginated(
                endpoint=endpoint,
                params={
                    "tweet.fields": "created_at,author_id,public_metrics,lang",
                    "user.fields": "id,name,username,created_at,description,public_metrics"
                },
                per_page=min(max_tweets, 100),
                max_pages=max_tweets // 100 + 1
            )
            
            if tweets_data:
                # Parse tweet data
                parsed_tweets = data_parser.batch_parse(tweets_data, data_parser.parse_twitter_data)
                
                # Filter out invalid tweets
                valid_tweets = [
                    tweet for tweet in parsed_tweets 
                    if data_parser.validate_data(tweet, ["id", "text", "created_at"])
                ]
                
                self.logger.info(f"Successfully scraped {len(valid_tweets)} tweets")
                return valid_tweets
            
            return []
    
    async def scrape_twitter_search(self, query: str, max_tweets: int = 100) -> List[Dict[str, Any]]:
        """
        Example: Search Twitter for tweets matching query.
        
        Args:
            query: Search query
            max_tweets: Maximum number of tweets to scrape
        
        Returns:
            List of parsed tweet data
        """
        self.logger.info(f"Searching Twitter for: {query}")
        
        async with self.scraper:
            endpoint = "2/tweets/search/recent"
            
            search_results = await self.scraper.scrape_paginated(
                endpoint=endpoint,
                params={
                    "query": query,
                    "tweet.fields": "created_at,author_id,public_metrics,lang",
                    "user.fields": "id,name,username"
                },
                per_page=min(max_tweets, 100),
                max_pages=max_tweets // 100 + 1
            )
            
            if search_results:
                parsed_tweets = data_parser.batch_parse(search_results, data_parser.parse_twitter_data)
                
                # Apply filters (example: English tweets only)
                filtered_tweets = data_parser.filter_data(
                    parsed_tweets,
                    {"lang": "en"}
                )
                
                self.logger.info(f"Found {len(filtered_tweets)} tweets matching query")
                return filtered_tweets
            
            return []
    
    async def run_example_scraping(self):
        """Run example scraping tasks."""
        self.logger.info("Starting example scraping tasks...")
        
        # Example 1: Scrape user timeline (replace with actual user ID)
        user_tweets = await self.scrape_twitter_user_timeline("123456789", max_tweets=50)
        if user_tweets:
            self.results.extend(user_tweets)
        
        # Example 2: Search for tweets
        search_tweets = await self.scrape_twitter_search("#AI #MachineLearning", max_tweets=30)
        if search_tweets:
            self.results.extend(search_tweets)
        
        # Example 3: Concurrent scraping of multiple users
        user_ids = ["123456789", "987654321", "555666777"]  # Replace with actual IDs
        
        requests = [
            {
                "endpoint": f"2/users/{user_id}/tweets",
                "params": {"tweet.fields": "created_at,author_id,public_metrics"},
                "method": "GET"
            }
            for user_id in user_ids
        ]
        
        async with self.scraper:
            concurrent_results = await self.scraper.scrape_multiple(requests)
            
            for i, result in enumerate(concurrent_results):
                if result and isinstance(result, dict) and "data" in result:
                    user_tweets = data_parser.batch_parse(
                        result["data"], 
                        data_parser.parse_twitter_data
                    )
                    self.results.extend(user_tweets)
        
        self.logger.info(f"Total scraped items: {len(self.results)}")
    
    async def save_results(self, format: str = None):
        """Save scraping results."""
        if not self.results:
            self.logger.warning("No results to save")
            return
        
        format = format or settings.DEFAULT_OUTPUT_FORMAT
        
        try:
            # Save raw data
            raw_file = data_storage.save_auto(
                self.results,
                format=format,
                filename=f"scraped_data_{format}"
            )
            
            # Analyze and save aggregated data
            if len(self.results) > 0:
                # Example aggregation: group by language
                aggregated = data_parser.aggregate_data(
                    self.results,
                    group_by="lang",
                    aggregations={
                        "like_count": "sum",
                        "retweet_count": "sum",
                        "text_length": "avg"
                    }
                )
                
                # Save aggregated data
                agg_file = data_storage.save_auto(
                    [{"language": k, **v} for k, v in aggregated.items()],
                    format=format,
                    filename=f"aggregated_data_{format}",
                    directory=settings.PROCESSED_DATA_DIR
                )
                
                self.logger.success(f"Saved aggregated data: {agg_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to save results: {e}")
    
    def setup_scheduled_scraping(self):
        """Setup scheduled scraping tasks."""
        if not settings.SCHEDULE_ENABLED:
            self.logger.info("Scheduled scraping is disabled")
            return
        
        # Schedule periodic scraping
        async def periodic_scrape():
            await self.run_example_scraping()
            await self.save_results()
        
        schedule_periodic_scrape(
            func=periodic_scrape,
            interval_minutes=settings.SCHEDULE_INTERVAL_MINUTES,
            name="main_scraper"
        )
        
        # Start scheduler
        task_scheduler.start()
        self.logger.info("Scheduled scraping enabled")
    
    async def run_interactive_mode(self):
        """Run interactive mode with dashboard."""
        self.logger.info("Starting interactive mode (press Ctrl+C to exit)")
        
        try:
            while True:
                # Show dashboard
                self.logger.show_dashboard()
                
                # Wait before next update
                await asyncio.sleep(5)
                
        except KeyboardInterrupt:
            self.logger.info("Interactive mode stopped")
    
    async def cleanup(self):
        """Cleanup resources."""
        self.logger.info("Cleaning up resources...")
        
        # Stop scheduler
        task_scheduler.stop()
        
        # Cleanup cache
        cache_manager.cleanup_expired()
        
        # Close scraper session
        if hasattr(self.scraper, 'close_session'):
            await self.scraper.close_session()
        
        self.logger.info("Cleanup complete")

async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Advanced Web Scraper")
    parser.add_argument("--mode", choices=["scrape", "interactive", "schedule"], 
                       default="scrape", help="Running mode")
    parser.add_argument("--format", choices=["json", "csv", "sqlite"], 
                       help="Output format (overrides config)")
    parser.add_argument("--query", help="Search query for scraping")
    parser.add_argument("--user-id", help="User ID to scrape")
    parser.add_argument("--max-items", type=int, default=100, 
                       help="Maximum items to scrape")
    parser.add_argument("--no-cache", action="store_true", 
                       help="Disable caching")
    parser.add_argument("--clear-cache", action="store_true", 
                       help="Clear cache before starting")
    
    args = parser.parse_args()
    
    # Initialize pipeline
    pipeline = ScrapingPipeline()
    
    # Apply command line overrides
    if args.format:
        settings.DEFAULT_OUTPUT_FORMAT = args.format
    
    if args.no_cache:
        settings.ENABLE_CACHE = False
    
    if args.clear_cache:
        cache_manager.clear()
        pipeline.logger.info("Cache cleared")
    
    try:
        if args.mode == "scrape":
            # Run scraping once
            if args.query:
                results = await pipeline.scrape_twitter_search(args.query, args.max_items)
                pipeline.results.extend(results)
            elif args.user_id:
                results = await pipeline.scrape_twitter_user_timeline(args.user_id, args.max_items)
                pipeline.results.extend(results)
            else:
                # Run example scraping
                await pipeline.run_example_scraping()
            
            # Save results
            await pipeline.save_results(args.format)
            
        elif args.mode == "schedule":
            # Setup and run scheduled scraping
            pipeline.setup_scheduled_scraping()
            await pipeline.run_interactive_mode()
            
        elif args.mode == "interactive":
            # Run interactive dashboard
            await pipeline.run_interactive_mode()
        
    except KeyboardInterrupt:
        pipeline.logger.info("Scraping interrupted by user")
    
    except Exception as e:
        pipeline.logger.error(f"Scraping failed: {e}")
        raise
    
    finally:
        await pipeline.cleanup()

if __name__ == "__main__":
    # Set up asyncio event loop policy for Windows compatibility
    if hasattr(asyncio, 'WindowsProactorEventLoopPolicy'):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    # Run main function
    asyncio.run(main())