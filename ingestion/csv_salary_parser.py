from ingestion.parsers import Parser
from typing import List, Dict, Any
import logging
import csv
import io

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
        reader = csv.DictReader(io.StringIO(text))
        records = []
        if not reader.fieldnames:
            logger.warning("csv has no headers")
            return []
        logger.debug(f"CSV columns: {reader.fieldnames}") 

        # clean each row and append the cleaned row to our records
        for row in reader:
            cleaned = {k.strip(): self._clean(value=v) for k, v in row.items() if k}
            if any(cleaned.values()):
                records.append(cleaned)

        # return our records
        logger.info(f"CSV parser: {len(records)} records parsed")
        return records
    
    def _clean(self, value: str) -> str:
        return value.strip() if value else ""