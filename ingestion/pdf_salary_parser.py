from ingestion.parsers import SalaryParser
from typing import List
import logging
import io
from pypdf import PdfReader

logger = logging.getLogger(__name__)


class PDFSalaryParser(SalaryParser):
    """Extracts raw row data from PDF salary documents."""
    
    def parse(self, content: bytes) -> List[List[str]]:
        """
        Parse PDF content and return all rows as lists of strings.
        
        Args:
            content: Raw PDF bytes
            
        Returns:
            List of rows, where each row is a list of whitespace-split tokens
        """
        stream = io.BytesIO(content)
        pdf_reader = PdfReader(stream=stream)

        num_pages = len(pdf_reader.pages)
        logger.debug(f"Found {num_pages} pages in PDF.")

        rows = []
        for page_number in range(num_pages):
            current_page = pdf_reader.pages[page_number]
            extracted_text = current_page.extract_text(extraction_mode="layout")

            for line in extracted_text.split("\n"):
                tokens = line.split()
                rows.append(tokens)
        
        return rows