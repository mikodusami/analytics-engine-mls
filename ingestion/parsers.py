import logging
from abc import ABC, abstractmethod
import io
import csv
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class Parser(ABC):
    @abstractmethod
    def parse(self, content: bytes) -> List[Dict[str, Any]]:
        """
        parses raw content into records
        returns list of dicts with original column names preserved
        """
        pass


class CSVSalaryParser(Parser):

    def parse(self, content: bytes) -> List[Dict[str, Any]]:
        
        # try decoding the content into our preferred encoding

        # loading the content into a format we can work with

        # clean each row and append the cleaned row to our records

        # return our records
        pass
    
    def _clean(self, value: str) -> str:
        return value.strip() if value else ""