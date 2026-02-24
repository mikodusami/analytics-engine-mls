from abc import ABC, abstractmethod
import logging
import requests
from requests import Response
from requests.exceptions import RequestException, Timeout, HTTPError 
from typing import Any


logger = logging.getLogger(__name__)  

class Scraper(ABC):
    
    @abstractmethod
    def scrape(self) -> Any:
        pass
    
    @staticmethod
    def fetch_content(url=None, timeout: int = 30) -> Response:
        checked_timeout = timeout if timeout and timeout > 0 else 30
        DEFAULT_HEADERS = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        } 
        if not url:
            raise ValueError("URL cannot be empty.")
        logger.debug(f"Fetching URL: {url}")
        try:
            response = requests.get(url=url, headers=DEFAULT_HEADERS, timeout=checked_timeout)
            response.raise_for_status()
            # ensure utf-8 encoding
            response.encoding = 'utf-8'
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


