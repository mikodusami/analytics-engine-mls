"""
Salary scraper for MLSPA salary guide data.

This scraper handles the MLSPA (MLS Players Association) salary guide page.
They publish salary data going back to the early 2000s in various formats
(CSV for recent years, PDF for older years).

The scraper discovers all available years, then routes each to the
appropriate parser based on file format. It's like a traffic cop for data.
"""
from dataclasses import dataclass
from ingestion.scrapers import Scraper
from ingestion.parsers import SalaryParser
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional
from ingestion.csv_salary_parser import CSVSalaryParser
from ingestion.pdf_salary_parser import PDFSalaryParser
import logging
import re

logger = logging.getLogger(__name__)


@dataclass
class SalarySource:
    """
    Represents a single salary data source.
    
    Each year's data has a URL and format (csv or pdf).
    This dataclass keeps track of what we found during discovery.
    """
    year: int      # Year of the salary data (e.g., 2024)
    url: str       # Direct URL to the file
    format: str    # File format: "csv" or "pdf"


class SalaryScraper(Scraper):
    """
    Scraper for MLS player salary data across all available years.
    
    This is the main entry point for salary data. It:
    1. Discovers all available years from the MLSPA salary guide page
    2. Downloads each year's data (CSV or PDF)
    3. Routes to the appropriate parser based on format
    4. Returns raw rows for the transformer to clean up
    
    Usage:
        scraper = SalaryScraper()
        sources = scraper.discover_sources()  # Find what's available
        rows = scraper.scrape_year(2024)      # Get specific year
        # or
        all_data = scraper.scrape()           # Get everything
    """ 
    
    # The holy grail - MLSPA's salary guide page
    # If this URL changes, we're f**ked
    MLS_SALARY_URL = "https://mlsplayers.org/resources/salary-guide"  

    def __init__(self):
        self._sources: Dict[int, SalarySource] = {}  # year -> SalarySource
        self._records_by_year: Dict[int, List[Dict[str, Any]]] = {}  # year -> rows
        
        # Parser registry - maps format to parser instance
        # Add new parsers here if MLSPA starts using new formats
        self._parsers: Dict[str, SalaryParser] = {
            'csv': CSVSalaryParser(),
            'pdf': PDFSalaryParser()
        }
    
    @property
    def sources(self) -> Dict[int, SalarySource]:
        """Get discovered sources. Call discover_sources() first."""
        return self._sources
    
    @property
    def records_by_year(self) -> Dict[int, List[List[str]]]:
        """Get scraped records by year. Call scrape() or scrape_year() first."""
        return self._records_by_year

    def scrape(self) -> Dict[int, List[List[str]]]:
        """
        Scrape all discovered sources.
        
        This is the "do everything" method. Discovers sources, then
        scrapes each year in reverse chronological order (newest first).
        
        Returns:
            Dict mapping year -> list of rows
        """
        self.discover_sources()
        for year in sorted(self._sources.keys(), reverse=True):
            self.scrape_year(year)
        logger.info(f"Total: {len(self._records_by_year)} years scraped")
        return self._records_by_year

    def discover_sources(self) -> Dict[int, SalarySource]:
        """
        Discover available salary sources from the MLSPA page.
        
        Parses the salary guide page to find all download links.
        Each link is analyzed to extract the year and format.
        
        Returns:
            Dict mapping year -> SalarySource
        """
        logger.info(f"Discovering salary sources from {self.MLS_SALARY_URL}")
        response = Scraper.fetch_content(url=self.MLS_SALARY_URL)
        soup = BeautifulSoup(response.text, "html.parser")

        # The salary links are in a div with class "salaryGuides"
        # If MLSPA changes their HTML structure, this will break
        guides_div = soup.find("div", class_="salaryGuides")
        if guides_div:
            for link in guides_div.find_all("a", href=True):
                self._process_link(link)
        else:
            logger.warning("salaryGuides div not found, searching all links")
        
        logger.info(f"Discovered {len(self._sources)} sources: {sorted(self._sources.keys())}")
        return self._sources

    def scrape_year(self, year: int) -> List[List[str]]:
        """
        Scrape salary data for a specific year.
        
        Downloads the file and routes to the appropriate parser.
        Results are cached in _records_by_year.
        
        Args:
            year: The year to scrape (e.g., 2024)
            
        Returns:
            List of rows (each row is a list of strings)
        """
        if year not in self._sources:
            logger.warning(f"No source found for year {year}")
            return []

        source = self._sources[year]
        logger.info(f"Scraping {year} ({source.format}) from {source.url}")
        
        try:
            # Download the file
            response = Scraper.fetch_content(url=source.url)
            
            # Get the right parser for this format
            parser = self._parsers.get(source.format)
            if not parser:
                logger.error(f"No parser for format: {source.format}")
                return []
            
            # Parse and cache the results
            rows = parser.parse(response.content)
            self._records_by_year[year] = rows
            
            logger.info(f"Year {year}: parsed {len(rows)} rows")
            return rows
        except Exception as e:
            logger.error(f"Failed to scrape {year}: {e}")
            return [] 
            
    # =========================================================================
    # PRIVATE METHODS - The boring implementation details
    # =========================================================================

    def _process_link(self, link) -> None:
        """
        Process a single link from the salary guide page.
        
        Extracts year and format, adds to _sources if valid.
        """
        logger.debug(f"Processing Link: {link}")
        href = link['href']
        text = link.get_text(strip=True)
        
        # Try to extract year from link text first, then URL
        year = self._extract_year(text) or self._extract_year(href)
        if not year:
            return
        
        # Detect format from URL extension
        format = self._detect_format(url=href)
        if format:
            self._sources[year] = SalarySource(year=year, url=href, format=format)
            logger.debug(f"Found {year} ({format}): {href}")
    
    def _extract_year(self, text: str) -> Optional[int]:
        """
        Extract a year from text using regex.
        
        Looks for 4-digit years between 2000-2050.
        """
        logger.debug(f"Extracting Year From: {text}")
        match = re.search(r"20([0-4]\d|50)", text)
        return int(match.group()) if match else None
    
    def _detect_format(self, url: str) -> Optional[str]:
        """
        Detect content format from URL extension.
        
        Returns "csv", "pdf", or None if unknown.
        """
        logger.debug(f"Detecting Format Of {url}")
        url_lower = url.lower()
        if ".csv" in url_lower:
            return "csv"
        elif ".pdf" in url_lower:
            return "pdf"
        logger.debug(f"Format Of {url} Not Found")
        return None

