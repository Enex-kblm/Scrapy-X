"""
Task scheduling system for periodic scraping.
"""
import asyncio
from typing import Dict, List, Any, Optional, Callable, Awaitable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
import schedule
import time
from threading import Thread

from config.settings import settings
from .logger import get_logger

@dataclass
class ScheduledTask:
    """Represents a scheduled scraping task."""
    name: str
    func: Callable[[], Awaitable[Any]]
    interval_minutes: int
    enabled: bool = True
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    run_count: int = 0
    error_count: int = 0
    max_retries: int = 3
    timeout_seconds: int = 300  # 5 minutes
    
    def __post_init__(self):
        """Calculate next run time."""
        if self.next_run is None:
            self.next_run = datetime.now() + timedelta(minutes=self.interval_minutes)

class TaskScheduler:
    """Manages scheduled scraping tasks."""
    
    def __init__(self):
        self.logger = get_logger()
        self.tasks: Dict[str, ScheduledTask] = {}
        self.running = False
        self.scheduler_thread: Optional[Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
    
    def add_task(self, 
                 name: str,
                 func: Callable[[], Awaitable[Any]],
                 interval_minutes: int,
                 enabled: bool = True,
                 max_retries: int = 3,
                 timeout_seconds: int = 300) -> None:
        """
        Add a scheduled task.
        
        Args:
            name: Unique task name
            func: Async function to execute
            interval_minutes: Interval between executions in minutes
            enabled: Whether task is enabled
            max_retries: Maximum retry attempts on failure
            timeout_seconds: Task timeout in seconds
        """
        task = ScheduledTask(
            name=name,
            func=func,
            interval_minutes=interval_minutes,
            enabled=enabled,
            max_retries=max_retries,
            timeout_seconds=timeout_seconds
        )
        
        self.tasks[name] = task
        self.logger.info(f"Added scheduled task: {name} (every {interval_minutes} minutes)")
    
    def remove_task(self, name: str) -> bool:
        """
        Remove a scheduled task.
        
        Args:
            name: Task name to remove
        
        Returns:
            True if task was removed
        """
        if name in self.tasks:
            del self.tasks[name]
            self.logger.info(f"Removed scheduled task: {name}")
            return True
        return False
    
    def enable_task(self, name: str) -> bool:
        """
        Enable a scheduled task.
        
        Args:
            name: Task name to enable
        
        Returns:
            True if task was enabled
        """
        if name in self.tasks:
            self.tasks[name].enabled = True
            self.logger.info(f"Enabled task: {name}")
            return True
        return False
    
    def disable_task(self, name: str) -> bool:
        """
        Disable a scheduled task.
        
        Args:
            name: Task name to disable
        
        Returns:
            True if task was disabled
        """
        if name in self.tasks:
            self.tasks[name].enabled = False
            self.logger.info(f"Disabled task: {name}")
            return True
        return False
    
    async def _execute_task(self, task: ScheduledTask) -> bool:
        """
        Execute a single task with error handling and timeout.
        
        Args:
            task: Task to execute
        
        Returns:
            True if task succeeded
        """
        try:
            self.logger.info(f"Executing scheduled task: {task.name}")
            
            # Execute with timeout
            await asyncio.wait_for(
                task.func(),
                timeout=task.timeout_seconds
            )
            
            # Update task statistics
            task.last_run = datetime.now()
            task.run_count += 1
            task.next_run = task.last_run + timedelta(minutes=task.interval_minutes)
            
            self.logger.success(f"Task completed: {task.name}")
            return True
            
        except asyncio.TimeoutError:
            task.error_count += 1
            self.logger.error(f"Task timed out: {task.name} (after {task.timeout_seconds}s)")
            return False
            
        except Exception as e:
            task.error_count += 1
            self.logger.error(f"Task failed: {task.name} - {e}")
            return False
    
    async def _run_due_tasks(self):
        """Run all tasks that are due for execution."""
        now = datetime.now()
        
        for task in self.tasks.values():
            if not task.enabled:
                continue
            
            if task.next_run and now >= task.next_run:
                success = await self._execute_task(task)
                
                # If task failed and has retries left, retry after 1 minute
                if not success and task.error_count < task.max_retries:
                    task.next_run = now + timedelta(minutes=1)
                    self.logger.warning(f"Will retry task {task.name} in 1 minute")
                elif not success:
                    # Disable task after max retries
                    task.enabled = False
                    self.logger.error(f"Task {task.name} disabled after {task.max_retries} failures")
    
    async def _scheduler_loop(self):
        """Main scheduler event loop."""
        self.logger.info("Scheduler started")
        
        while self.running:
            try:
                await self._run_due_tasks()
                
                # Sleep for 1 minute before checking again
                await asyncio.sleep(60)
                
            except Exception as e:
                self.logger.error(f"Scheduler error: {e}")
                await asyncio.sleep(60)  # Continue after error
        
        self.logger.info("Scheduler stopped")
    
    def start(self):
        """Start the task scheduler."""
        if self.running:
            self.logger.warning("Scheduler is already running")
            return
        
        if not settings.SCHEDULE_ENABLED:
            self.logger.info("Scheduling is disabled in settings")
            return
        
        self.running = True
        
        # Start scheduler in separate thread with new event loop
        def run_scheduler():
            # Create new event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop
            
            try:
                loop.run_until_complete(self._scheduler_loop())
            finally:
                loop.close()
        
        self.scheduler_thread = Thread(target=run_scheduler, daemon=True)
        self.scheduler_thread.start()
        
        self.logger.info("Task scheduler started")
    
    def stop(self):
        """Stop the task scheduler."""
        if not self.running:
            return
        
        self.running = False
        
        # Wait for thread to finish
        if self.scheduler_thread and self.scheduler_thread.is_alive():
            self.scheduler_thread.join(timeout=5)
        
        self.logger.info("Task scheduler stopped")
    
    def get_task_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all scheduled tasks."""
        status = {}
        
        for name, task in self.tasks.items():
            status[name] = {
                "enabled": task.enabled,
                "interval_minutes": task.interval_minutes,
                "last_run": task.last_run.isoformat() if task.last_run else None,
                "next_run": task.next_run.isoformat() if task.next_run else None,
                "run_count": task.run_count,
                "error_count": task.error_count,
                "success_rate": (
                    ((task.run_count - task.error_count) / task.run_count * 100)
                    if task.run_count > 0 else 0
                )
            }
        
        return status
    
    def run_task_now(self, name: str) -> bool:
        """
        Run a specific task immediately.
        
        Args:
            name: Task name to run
        
        Returns:
            True if task exists and was queued for execution
        """
        if name not in self.tasks:
            return False
        
        task = self.tasks[name]
        
        # Schedule for immediate execution
        task.next_run = datetime.now()
        
        self.logger.info(f"Task {name} queued for immediate execution")
        return True

# Global scheduler instance
task_scheduler = TaskScheduler()

# Convenience functions for common scheduling patterns
def schedule_daily_scrape(func: Callable[[], Awaitable[Any]], 
                         name: str = "daily_scrape",
                         hour: int = 9) -> None:
    """Schedule a daily scraping task."""
    # Convert to minutes from midnight
    interval_minutes = 24 * 60  # 24 hours
    
    task_scheduler.add_task(
        name=name,
        func=func,
        interval_minutes=interval_minutes
    )

def schedule_hourly_scrape(func: Callable[[], Awaitable[Any]], 
                          name: str = "hourly_scrape") -> None:
    """Schedule an hourly scraping task."""
    task_scheduler.add_task(
        name=name,
        func=func,
        interval_minutes=60
    )

def schedule_periodic_scrape(func: Callable[[], Awaitable[Any]], 
                           interval_minutes: int,
                           name: str = "periodic_scrape") -> None:
    """Schedule a periodic scraping task."""
    task_scheduler.add_task(
        name=name,
        func=func,
        interval_minutes=interval_minutes
    )