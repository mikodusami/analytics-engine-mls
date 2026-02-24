from dataclasses import dataclass
from scrapers import HTTPWebScraper
from parsers import SalaryParser
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional
from csv_salary_parser import CSVSalaryParser

@dataclass
class SalarySource:
    year: int # year of the salary source
    url: str # the url of the salary source
    format: str # the format (html, csv, pdf)

class SalaryScraper(HTTPWebScraper):
    """
    Scraper for MLS player salary data across all available years.
    
    Discovers historical links on the salary guide page and routes
    each to the appropriate parser based on content type.
    Returns raw dicts preserving original column names.
    """ 
    MLS_SALARY_URL = "https://mlsplayers.org/resources/salary-guide"  

    def __init__(self, url: str = None):
        super().__init__(url=url or self.MLS_SALARY_URL)
        self._sources: Dict[int, SalarySource] = {}
        self._records_by_year: Dict[int, List[Dict[str, Any]]] = {}
        self._parsers: Dict[str, SalaryParser] = {
            'csv': CSVSalaryParser()
        }
    
    @property
    def sources(self) -> Dict[int, SalarySource]:
        return self._sources
    
    @property
    def records_by_year(self) -> Dict[int, List[Dict[str, Any]]]:
        return self._records_by_year

