"""
Base scraper class and HTTP fetching utilities.

This is the abstract base class that all scrapers inherit from.
It handles the boring HTTP stuff so you can focus on parsing.

If you're making a new scraper, extend this class and implement scrape().
Don't reinvent the wheel, you beautiful idiot.
"""
from abc import ABC, abstractmethod
import logging
import requests
from requests import Response
from requests.exceptions import RequestException, Timeout, HTTPError 
from typing import Any


logger = logging.getLogger(__name__)  


class Scraper(ABC):
    """
    Abstract base class for all scrapers.
    
    Provides a static fetch_content() method for HTTP requests with
    proper error handling, timeouts, and headers.
    
    Subclasses must implement scrape() - that's where your logic goes.
    """
    
    @abstractmethod
    def scrape(self) -> Any:
        """
        Main scrape method - implement this in your subclass.
        
        Returns whatever data structure makes sense for your scraper.
        """
        pass
    
    @staticmethod
    def fetch_content(url=None, timeout: int = 30) -> Response:
        """
        Fetch content from a URL with proper error handling.
        
        This is the workhorse method for HTTP requests. It:
        - Sets a fake User-Agent (because websites are paranoid)
        - Handles timeouts gracefully
        - Raises proper exceptions on HTTP errors
        - Forces UTF-8 encoding (because encoding bugs are the worst)
        
        Args:
            url: The URL to fetch. Can't be empty, obviously.
            timeout: How long to wait before giving up (seconds).
                    Default is 30s. Set higher for slow servers.
        
        Returns:
            requests.Response object with the content
            
        Raises:
            ValueError: If URL is empty (you had one job)
            Timeout: If the server is slower than a sloth
            HTTPError: If the server returns an error status
            RequestException: If something else goes wrong
        """
        # Validate timeout - can't have negative or zero timeouts
        checked_timeout = timeout if timeout and timeout > 0 else 30
        
        # Fake User-Agent to avoid bot detection
        # Websites discriminate against bots. It's 2024 and we still do this.
        DEFAULT_HEADERS = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        } 
        
        if not url:
            raise ValueError("URL cannot be empty.")
        
        logger.debug(f"Fetching URL: {url}")
        
        try:
            response = requests.get(url=url, headers=DEFAULT_HEADERS, timeout=checked_timeout)
            response.raise_for_status()  # Raises HTTPError for 4xx/5xx status codes
            response.encoding = 'utf-8'  # Force UTF-8 because encoding bugs suck
            logger.debug(f"Successfully fetched {len(response.content)} bytes")
            return response
        except Timeout:
            logger.error(f"Request timed out after {checked_timeout}s: {url}")
            raise
        except HTTPError as e:
            logger.error(f"HTTP error {e.response.status_code}: {url}")
            raise
        except RequestException as e:
            logger.error(f"Request failed: {e}")
            raise


