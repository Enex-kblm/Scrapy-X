"""
Elegant logging system with Rich console integration.
"""
import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.logging import RichHandler
from rich.text import Text

from config.settings import settings

class ScrapingStats:
    """Track scraping statistics for dashboard."""
    
    def __init__(self):
        self.reset()
    
    def reset(self):
        """Reset all statistics."""
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.cached_requests = 0
        self.saved_items = 0
        self.current_proxy = "None"
        self.current_user_agent = "Default"
        self.start_time = datetime.now()
        self.errors = []
    
    def add_request(self, success: bool = True, cached: bool = False):
        """Add a request to statistics."""
        self.total_requests += 1
        if cached:
            self.cached_requests += 1
        elif success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1
    
    def add_saved_item(self):
        """Add a saved item to statistics."""
        self.saved_items += 1
    
    def add_error(self, error: str):
        """Add an error to the error list."""
        self.errors.append({
            "time": datetime.now(),
            "error": error
        })
        # Keep only last 10 errors
        if len(self.errors) > 10:
            self.errors = self.errors[-10:]
    
    def set_current_proxy(self, proxy: str):
        """Set current proxy."""
        self.current_proxy = proxy
    
    def set_current_user_agent(self, user_agent: str):
        """Set current user agent (truncated for display)."""
        self.current_user_agent = user_agent[:50] + "..." if len(user_agent) > 50 else user_agent

class ScrapingLogger:
    """Enhanced logger with Rich integration."""
    
    def __init__(self, name: str = "scraper"):
        self.name = name
        self.console = Console()
        self.stats = ScrapingStats()
        self._setup_logger()
    
    def _setup_logger(self):
        """Setup logger with both file and console handlers."""
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(getattr(logging, settings.LOG_LEVEL))
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # File handler with rotation
        log_file = settings.LOGS_DIR / "scraper.log"
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=settings.MAX_LOG_SIZE,
            backupCount=settings.LOG_BACKUP_COUNT
        )
        file_formatter = logging.Formatter(
            settings.LOG_FORMAT,
            datefmt=settings.LOG_DATE_FORMAT
        )
        file_handler.setFormatter(file_formatter)
        
        # Rich console handler
        console_handler = RichHandler(
            console=self.console,
            show_time=True,
            show_path=False,
            markup=True
        )
        console_handler.setLevel(getattr(logging, settings.LOG_LEVEL))
        
        # Add handlers
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # Prevent propagation to root logger
        self.logger.propagate = False
    
    def info(self, message: str, **kwargs):
        """Log info message."""
        self.logger.info(message, **kwargs)
    
    def error(self, message: str, **kwargs):
        """Log error message and add to stats."""
        self.logger.error(message, **kwargs)
        self.stats.add_error(message)
    
    def warning(self, message: str, **kwargs):
        """Log warning message."""
        self.logger.warning(message, **kwargs)
    
    def debug(self, message: str, **kwargs):
        """Log debug message."""
        self.logger.debug(message, **kwargs)
    
    def success(self, message: str):
        """Log success message with green color."""
        self.console.print(f"[green]✓[/green] {message}")
        self.logger.info(message)
    
    def failure(self, message: str):
        """Log failure message with red color."""
        self.console.print(f"[red]✗[/red] {message}")
        self.logger.error(message)
    
    def show_dashboard(self):
        """Display real-time dashboard."""
        # Statistics table
        table = Table(title="Scraping Dashboard", show_header=True, header_style="bold magenta")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="white")
        
        # Calculate runtime
        runtime = datetime.now() - self.stats.start_time
        runtime_str = str(runtime).split('.')[0]  # Remove microseconds
        
        # Success rate
        success_rate = 0
        if self.stats.total_requests > 0:
            success_rate = (self.stats.successful_requests / self.stats.total_requests) * 100
        
        # Add rows
        metrics = [
            ("Total Requests", str(self.stats.total_requests)),
            ("Successful", f"[green]{self.stats.successful_requests}[/green]"),
            ("Failed", f"[red]{self.stats.failed_requests}[/red]"),
            ("Cached", f"[yellow]{self.stats.cached_requests}[/yellow]"),
            ("Success Rate", f"[green]{success_rate:.1f}%[/green]"),
            ("Items Saved", f"[blue]{self.stats.saved_items}[/blue]"),
            ("Runtime", runtime_str),
            ("Current Proxy", self.stats.current_proxy),
            ("Current User-Agent", self.stats.current_user_agent),
        ]
        
        for metric, value in metrics:
            table.add_row(metric, value)
        
        # Recent errors panel
        error_text = ""
        if self.stats.errors:
            error_text = "\n".join([
                f"[red]{error['time'].strftime('%H:%M:%S')}[/red] {error['error']}"
                for error in self.stats.errors[-5:]  # Show last 5 errors
            ])
        else:
            error_text = "[green]No recent errors[/green]"
        
        error_panel = Panel(
            error_text,
            title="Recent Errors",
            border_style="red"
        )
        
        # Clear console and display dashboard
        self.console.clear()
        self.console.print(table)
        self.console.print(error_panel)
    
    def show_startup_banner(self):
        """Show elegant startup banner."""
        banner = Panel.fit(
            "[bold blue]🕷️  Advanced Web Scraper[/bold blue]\n"
            f"[dim]Version 1.0.0 | Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}[/dim]",
            border_style="blue"
        )
        self.console.print(banner)
        self.console.print()

# Global logger instance
_global_logger: Optional[ScrapingLogger] = None

def setup_logger(name: str = "scraper") -> ScrapingLogger:
    """Setup and return global logger instance."""
    global _global_logger
    if _global_logger is None:
        _global_logger = ScrapingLogger(name)
    return _global_logger

def get_logger() -> ScrapingLogger:
    """Get the global logger instance."""
    if _global_logger is None:
        return setup_logger()
    return _global_logger

def get_dashboard() -> ScrapingStats:
    """Get the global stats instance."""
    logger = get_logger()
    return logger.stats