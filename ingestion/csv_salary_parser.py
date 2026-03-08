"""
CSV parser for salary documents.

Handles CSV files from MLSPA. These are the easy ones - nice clean
comma-separated values. No PDF bulls**t to deal with.

Recent years (2020+) tend to be CSV format. Thank god.
"""
from ingestion.parsers import SalaryParser
from typing import List
import logging
import csv
import io

logger = logging.getLogger(__name__)


class CSVSalaryParser(SalaryParser):
    """
    Extracts raw row data from CSV salary documents.
    
    This is the simple parser. CSV is a civilized format that doesn't
    require black magic to parse. Just read the rows and strip whitespace.
    """

    def parse(self, content: bytes) -> List[List[str]]:
        """
        Parse CSV content and return all rows as lists of strings.
        
        Handles encoding issues gracefully - tries UTF-8 first,
        falls back to Latin-1 if that fails (some older files are weird).
        
        Args:
            content: Raw CSV bytes
            
        Returns:
            List of rows, where each row is a list of string values
        """
        # Try UTF-8 first (the civilized encoding)
        # Fall back to Latin-1 for older files that don't know better
        try: 
            text = content.decode('utf-8')
        except UnicodeDecodeError:
            logger.debug(f"UTF-8 decode failed, falling back to Latin-1")
            text = content.decode('latin-1')

        # Use Python's csv module to handle quoting, escaping, etc.
        reader = csv.reader(io.StringIO(text))
        rows = []
        
        for row in reader:
            # Strip whitespace from each cell
            # Some CSVs have trailing spaces that mess things up
            cleaned_row = [cell.strip() for cell in row]
            rows.append(cleaned_row)

        logger.info(f"CSV parser: {len(rows)} rows parsed")
        return rows