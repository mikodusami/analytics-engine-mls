from dataclasses import dataclass
from scrapers import HTTPWebScraper
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional


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

 