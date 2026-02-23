from abc import ABC, abstractmethod
from abc import ABC, abstractmethod
from typing import Any
import logging
import requests
from requests import Response
from requests.exceptions import RequestException, Timeout, HTTPError 

logger = logging.getLogger(__name__)  

class Scraper(ABC):
    
    def scrape(self):
        pass

class HTTPWebScraper(Scraper):
    DEFAULT_TIMEOUT = 30
    DEFAULT_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    } 

    def __init__(self, url: str, timeout: int = DEFAULT_TIMEOUT):
        super().__init__()
        if not url:
            raise ValueError("URL cannot be empty.")
        self.url = url
        self.timeout = timeout
    
    def fetch_content(self) -> Response:
        logger.debug(f"Fetching URL: {self.url}")
        try:
            response = requests.get(url=self.url, headers=self.DEFAULT_HEADERS, timeout=self.timeout)
            response.raise_for_status()
            # ensure utf-8 encoding
            response.encoding = 'utf-8'
            logger.debug(f"Successfully fetched {len(response.content)} bytes")
            return response
        except Timeout:
            logger.error(f"Request timed out after {self.timeout}s: {self.url}")
            raise
        except HTTPError as e:
            logger.error(f"HTTP error {e.response.status_code}: {self.url}")
            raise
        except RequestException as e:
            logger.error(f"Request failed: {e}")
            raise 