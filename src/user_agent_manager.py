"""
User-Agent rotation and management system.
"""
import random
from typing import List
from pathlib import Path

from config.settings import settings
from .logger import get_logger

class UserAgentManager:
    """Manages User-Agent rotation."""
    
    def __init__(self):
        self.logger = get_logger()
        self.user_agents: List[str] = []
        self.current_index = 0
        self._load_user_agents()
    
    def _load_user_agents(self):
        """Load User-Agent strings from configuration file."""
        ua_file = settings.CONFIG_DIR / "user_agents.txt"
        
        if not ua_file.exists():
            self.logger.warning(f"User agents file not found: {ua_file}")
            # Use default user agents
            self._use_default_user_agents()
            return
        
        try:
            with open(ua_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            for line in lines:
                line = line.strip()
                # Skip empty lines and comments
                if not line or line.startswith('#'):
                    continue
                self.user_agents.append(line)
            
            if not self.user_agents:
                self._use_default_user_agents()
            else:
                self.logger.info(f"Loaded {len(self.user_agents)} user agents")
            
        except Exception as e:
            self.logger.error(f"Failed to load user agents: {e}")
            self._use_default_user_agents()
    
    def _use_default_user_agents(self):
        """Use default User-Agent strings as fallback."""
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0"
        ]
        self.logger.info(f"Using {len(self.user_agents)} default user agents")
    
    def get_user_agent(self) -> str:
        """Get a User-Agent string."""
        if not self.user_agents:
            return "Mozilla/5.0 (compatible; ScraperBot/1.0)"
        
        if not settings.ROTATE_USER_AGENTS:
            # Always use first User-Agent
            user_agent = self.user_agents[0]
        else:
            # Random selection for better distribution
            user_agent = random.choice(self.user_agents)
        
        # Update stats
        self.logger.stats.set_current_user_agent(user_agent)
        
        return user_agent
    
    def get_random_user_agent(self) -> str:
        """Get a random User-Agent string regardless of rotation setting."""
        if not self.user_agents:
            return "Mozilla/5.0 (compatible; ScraperBot/1.0)"
        
        return random.choice(self.user_agents)
    
    def get_user_agent_by_browser(self, browser: str) -> str:
        """
        Get User-Agent by browser type.
        
        Args:
            browser: Browser type ('chrome', 'firefox', 'safari', 'edge')
        
        Returns:
            User-Agent string
        """
        browser_agents = {
            'chrome': [ua for ua in self.user_agents if 'Chrome' in ua and 'Firefox' not in ua],
            'firefox': [ua for ua in self.user_agents if 'Firefox' in ua],
            'safari': [ua for ua in self.user_agents if 'Safari' in ua and 'Chrome' not in ua],
            'edge': [ua for ua in self.user_agents if 'Edg' in ua]
        }
        
        agents = browser_agents.get(browser.lower(), [])
        if not agents:
            return self.get_user_agent()
        
        return random.choice(agents)
    
    def get_user_agent_by_os(self, os_type: str) -> str:
        """
        Get User-Agent by operating system.
        
        Args:
            os_type: OS type ('windows', 'macos', 'linux')
        
        Returns:
            User-Agent string
        """
        os_agents = {
            'windows': [ua for ua in self.user_agents if 'Windows NT' in ua],
            'macos': [ua for ua in self.user_agents if 'Macintosh' in ua],
            'linux': [ua for ua in self.user_agents if 'Linux' in ua and 'Android' not in ua]
        }
        
        agents = os_agents.get(os_type.lower(), [])
        if not agents:
            return self.get_user_agent()
        
        return random.choice(agents)
    
    def get_mobile_user_agent(self) -> str:
        """Get a mobile User-Agent string."""
        mobile_agents = [
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (iPad; CPU OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (Linux; Android 14; SM-G998B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
            "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
        ]
        
        return random.choice(mobile_agents)
    
    def add_user_agent(self, user_agent: str):
        """Add a new User-Agent string."""
        if user_agent and user_agent not in self.user_agents:
            self.user_agents.append(user_agent)
            self.logger.debug(f"Added new user agent: {user_agent[:50]}...")
    
    def remove_user_agent(self, user_agent: str):
        """Remove a User-Agent string."""
        if user_agent in self.user_agents:
            self.user_agents.remove(user_agent)
            self.logger.debug(f"Removed user agent: {user_agent[:50]}...")
    
    def get_stats(self) -> dict:
        """Get User-Agent statistics."""
        return {
            "total_user_agents": len(self.user_agents),
            "rotation_enabled": settings.ROTATE_USER_AGENTS,
            "current_user_agent": self.logger.stats.current_user_agent
        }

# Global user agent manager instance
user_agent_manager = UserAgentManager()