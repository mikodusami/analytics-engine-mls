from ingestion.parsers import SalaryParser
from typing import List
import logging
import csv
import io

logger = logging.getLogger(__name__)


class CSVSalaryParser(SalaryParser):
    """Extracts raw row data from CSV salary documents."""

    def parse(self, content: bytes) -> List[List[str]]:
        """
        Parse CSV content and return all rows as lists of strings.
        
        Args:
            content: Raw CSV bytes
            
        Returns:
            List of rows, where each row is a list of string values
        """
        # try decoding the content into our preferred encoding
        try: 
            text = content.decode('utf-8')
        except UnicodeDecodeError:
            logger.debug(f"Decoding Error: {UnicodeDecodeError}")
            text = content.decode('latin-1')

        reader = csv.reader(io.StringIO(text))
        rows = []
        
        for row in reader:
            # Strip whitespace from each cell
            cleaned_row = [cell.strip() for cell in row]
            rows.append(cleaned_row)

        logger.info(f"CSV parser: {len(rows)} rows parsed")
        return rows