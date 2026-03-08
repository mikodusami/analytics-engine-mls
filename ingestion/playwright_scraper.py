"""
Base Playwright scraper for JavaScript-rendered pages.

Because some websites are a**holes and render everything with JavaScript,
we need a real browser to scrape them. Enter Playwright - the nuclear option
for web scraping.

This is the base class that handles all the browser bulls**t so the
actual scrapers can focus on parsing HTML like civilized code.
"""
import logging
from abc import ABC, abstractmethod
from typing import Any, Optional
from playwright.sync_api import sync_playwright, Page, Browser, BrowserContext

logger = logging.getLogger(__name__)


class PlaywrightScraper(ABC):
    """
    Base class for scrapers that need JavaScript rendering.
    
    If you're using this, it means requests + BeautifulSoup wasn't enough
    and you had to bring out the big guns. My condolences.
    
    Usage:
        with MyPlaywrightScraper() as scraper:
            data = scraper.scrape()
    
    Or if you hate context managers for some reason:
        scraper = MyPlaywrightScraper()
        scraper.start()
        data = scraper.scrape()
        scraper.stop()  # Don't forget this or you'll have zombie browsers
    """
    
    # Override this in subclasses, you lazy b*stard
    BASE_URL = ""
    
    def __init__(self, headless: bool = True, timeout: int = 60000):
        """
        Initialize the scraper.
        
        Args:
            headless: Run browser without GUI. Set to False if you want to
                     watch the browser do its thing (useful for debugging,
                     or if you're just bored)
            timeout: How long to wait for sh*t to load (in milliseconds).
                    Default is 60 seconds because some sites are slow af.
        """
        self.headless = headless
        self.timeout = timeout
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None
        self._playwright = None
    
    def __enter__(self):
        """Context manager entry - starts the browser."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - stops the browser. Always. No exceptions."""
        self.stop()
    
    def start(self) -> None:
        """
        Start browser session.
        
        This fires up a Chromium browser instance. It's like opening Chrome
        but without all the tabs you forgot to close from last week.
        """
        logger.info("Starting Playwright browser...")
        self._playwright = sync_playwright().start()
        
        # Launch Chromium - we use this because it's the most compatible
        # Firefox and WebKit are for hipsters
        self._browser = self._playwright.chromium.launch(headless=self.headless)
        
        # Create a browser context with a fake user agent
        # Because websites discriminate against bots. Rude.
        self._context = self._browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        )
        
        self._page = self._context.new_page()
        self._page.set_default_timeout(self.timeout)
        logger.info("Browser started")
    
    def stop(self) -> None:
        """
        Stop browser session.
        
        ALWAYS call this when you're done, or you'll have browser processes
        hanging around like that one coworker who won't leave after 5pm.
        """
        if self._context:
            self._context.close()
        if self._browser:
            self._browser.close()
        if self._playwright:
            self._playwright.stop()
        logger.info("Browser stopped")
    
    @property
    def page(self) -> Page:
        """
        Get the current page object.
        
        Raises an error if you forgot to start the browser first.
        Because I'm not your babysitter.
        """
        if not self._page:
            raise RuntimeError("Browser not started. Call start() first or use context manager.")
        return self._page
    
    def navigate(self, url: str, wait_selector: Optional[str] = None) -> str:
        """
        Navigate to URL and return page HTML.
        
        This is the main method for loading pages. It:
        1. Goes to the URL
        2. Waits for the DOM to load
        3. Waits an extra second for JS to do its thing
        4. Optionally waits for a specific element
        5. Returns the HTML
        
        Args:
            url: The URL to navigate to. Duh.
            wait_selector: Optional CSS selector to wait for. If the element
                          doesn't appear, we just move on. Life's too short.
        
        Returns:
            The page HTML as a string. Do whatever you want with it.
        """
        logger.debug(f"Navigating to: {url}")
        self.page.goto(url, wait_until="domcontentloaded")
        
        # Give dynamic content time to load
        # Because JavaScript is slow and we have to wait for it like peasants
        self.page.wait_for_load_state("load")
        self.page.wait_for_timeout(1000)  # Wait 1s for JS rendering
        
        if wait_selector:
            try:
                self.page.wait_for_selector(wait_selector, timeout=self.timeout)
            except Exception:
                # Selector not found, continue anyway
                # Sometimes elements just don't show up. It happens.
                logger.debug(f"Selector {wait_selector} not found, continuing")
        
        return self.page.content()
    
    def get_html(self) -> str:
        """
        Get current page HTML.
        
        Just returns whatever's on the page right now.
        No navigation, no waiting, no bulls**t.
        """
        return self.page.content()
    
    @abstractmethod
    def scrape(self) -> Any:
        """
        Main scrape method to be implemented by subclasses.
        
        This is where you put your actual scraping logic.
        If you don't implement this, Python will yell at you.
        And so will I.
        """
        pass
