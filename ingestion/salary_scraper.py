from dataclasses import dataclass
from ingestion.scrapers import Scraper
from ingestion.parsers import SalaryParser
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional
from ingestion.csv_salary_parser import CSVSalaryParser
import logging
import re

logger = logging.getLogger(__name__)

@dataclass
class SalarySource:
    year: int # year of the salary source
    url: str # the url of the salary source
    format: str # the format (html, csv, pdf)

class SalaryScraper(Scraper):
    """
    Scraper for MLS player salary data across all available years.
    
    Discovers historical links on the salary guide page and routes
    each to the appropriate parser based on content type.
    Returns raw dicts preserving original column names.
    """ 
    MLS_SALARY_URL = "https://mlsplayers.org/resources/salary-guide"  

    def __init__(self):
        self._sources: Dict[int, SalarySource] = {}
        self._records_by_year: Dict[int, List[Dict[str, Any]]] = {}
        self._parsers: Dict[str, SalaryParser] = {
            'csv': CSVSalaryParser(),
            'pdf': None
        }
        
    
    @property
    def sources(self) -> Dict[int, SalarySource]:
        return self._sources
    
    @property
    def records_by_year(self) -> Dict[int, List[Dict[str, Any]]]:
        return self._records_by_year

    def scrape(self) -> List[Dict[str, Any]]:
        all_records = []
        self.discover_sources()
        for year in sorted(self._sources.keys(), reverse=True):
            records = self.scrape_year(year)
            all_records.extend(records)

    def discover_sources(self) -> Dict[int, SalarySource]:
        logger.info(f"Discovering salary sources from {self.url}")
        response = self.fetch_content()
        soup = BeautifulSoup(response.text, "html.parser")

        # find the salary guides
        guides_div = soup.find("div", class_="salaryGuides")
        if guides_div:
            for link in guides_div.find_all("a", href=True):
                self._process_link(link)
        else:
            logger.warning("salaryGuides div not found, searching all links")
        
        logger.info(f"Discovered {len(self._sources)} sources: {sorted(self._sources.keys())}")
        return self._sources

    def scrape_year(self, year: int) -> List[Dict[str, Any]]:
        if year not in self._sources:
            logger.warning(f"No source found for year {year}")
            return []

        source = self._sources[year]
        logger.info(f"Scraping {year} ({source.format}) from {source.url}")
        try:
            response = self.fetch_content(url=self.MLS_SALARY_URL)
            parser = self._parsers.get(source.format)
            if not parser:
                logger.error(f"No parser for format: {source.format}")
                return []
            records = parser.parse(response.content)
            # adding a column for the year
            for record in records:
                record["_year"] = year 
            
            logger.info(f"Year {year}: parsed {len(records)} records")
            return records
        except Exception as e:
            logger.error(f"Failed to scrape {year}: {e}")
            return [] 
            
    # PRIVATE     

    def _process_link(self, link) -> None:
        logger.debug(f"Processing Link: {link}")
        href = link['href']
        text = link.get_text(strip=True)
        year = self._extract_year(text) or self._extract_year(href)
        if not year:
            return
        format = self._detect_format()
        if format:
            self._sources[year] = SalarySource(year=year, url=href, format=format)
            logger.debug(f"Found {year} ({format}): {href}")
    
    def _extract_year(self, text: str) -> Optional[int]:
        logger.debug(f"Extracting Year From: {text}")
        match = re.search(r"20([0-4]\d|50)", text)
        return int(match.group()) if match else None
    
    def _detect_format(self, url: str) -> Optional[str]:
        logger.debug(f"Detecting Format Of {url}")
        """Detect content format from URL."""
        url_lower = url.lower()
        if ".csv" in url_lower:
            return "csv"
        elif ".pdf" in url_lower:
            return "pdf"
        logger.debug(f"Format Of {url} Not Found")
        return None