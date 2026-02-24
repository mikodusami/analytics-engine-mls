"""
Base Playwright scraper for JavaScript-rendered pages.
"""
import logging
from abc import ABC, abstractmethod
from typing import Any, Optional
from playwright.sync_api import sync_playwright, Page, Browser, BrowserContext

logger = logging.getLogger(__name__)


class PlaywrightScraper(ABC):
    """Base class for scrapers that need JavaScript rendering."""
    
    BASE_URL = ""
    
    def __init__(self, headless: bool = True, timeout: int = 60000):
        self.headless = headless
        self.timeout = timeout
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None
        self._playwright = None
    
    def __enter__(self):
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
    
    def start(self) -> None:
        """Start browser session."""
        logger.info("Starting Playwright browser...")
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(headless=self.headless)
        self._context = self._browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        )
        self._page = self._context.new_page()
        self._page.set_default_timeout(self.timeout)
        logger.info("Browser started")
    
    def stop(self) -> None:
        """Stop browser session."""
        if self._context:
            self._context.close()
        if self._browser:
            self._browser.close()
        if self._playwright:
            self._playwright.stop()
        logger.info("Browser stopped")
    
    @property
    def page(self) -> Page:
        if not self._page:
            raise RuntimeError("Browser not started. Call start() first or use context manager.")
        return self._page
    
    def navigate(self, url: str, wait_selector: Optional[str] = None) -> str:
        """Navigate to URL and return page HTML."""
        logger.debug(f"Navigating to: {url}")
        self.page.goto(url, wait_until="domcontentloaded")
        # Give dynamic content time to load
        self.page.wait_for_load_state("load")
        self.page.wait_for_timeout(1000)  # Wait 1s for JS rendering
        if wait_selector:
            try:
                self.page.wait_for_selector(wait_selector, timeout=self.timeout)
            except Exception:
                # Selector not found, continue anyway
                logger.debug(f"Selector {wait_selector} not found, continuing")
        return self.page.content()
    
    def get_html(self) -> str:
        """Get current page HTML."""
        return self.page.content()
    
    @abstractmethod
    def scrape(self) -> Any:
        """Main scrape method to be implemented by subclasses."""
        pass
