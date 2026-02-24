from parsers import Parser
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class CSVSalaryParser(Parser):

    def parse(self, content: bytes) -> List[Dict[str, Any]]:
        
        # try decoding the content into our preferred encoding
        try: 
            text = content.decode('utf-8')
        except UnicodeDecodeError:
            logger.debug(f"Decoding Error: {UnicodeDecodeError}")
            text = content.decode('latin-1')

        # loading the content into a format we can work with

        # clean each row and append the cleaned row to our records

        # return our records
        pass
    
    def _clean(self, value: str) -> str:
        return value.strip() if value else ""