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
        Tries plain mode first, falls back to layout if text looks fragmented.
        
        Args:
            content: Raw PDF bytes
            
        Returns:
            List of rows, where each row is a list of whitespace-split tokens
        """
        stream = io.BytesIO(content)
        pdf_reader = PdfReader(stream=stream)

        num_pages = len(pdf_reader.pages)
        logger.debug(f"Found {num_pages} pages in PDF.")

        # Try plain mode first, check if it's fragmented
        mode = self._detect_best_mode(pdf_reader)
        logger.debug(f"Using extraction mode: {mode}")

        rows = []
        for page_number in range(num_pages):
            current_page = pdf_reader.pages[page_number]
            extracted_text = current_page.extract_text(extraction_mode=mode)

            for line in extracted_text.split("\n"):
                tokens = line.split()
                rows.append(tokens)
        
        return rows
    
    def _detect_best_mode(self, pdf_reader: PdfReader) -> str:
        """
        Detect whether plain or layout mode produces better results.
        Checks first page for fragmented text patterns.
        """
        if not pdf_reader.pages:
            return "plain"
        
        first_page = pdf_reader.pages[0]
        
        # Try layout mode and check for fragmentation
        layout_text = first_page.extract_text(extraction_mode="layout")
        layout_tokens = layout_text.split()
        
        # Count single/double char tokens - high count = fragmented
        short_tokens = sum(1 for t in layout_tokens[:50] if len(t) <= 2)
        
        # If more than 40% of first 50 tokens are very short, text is fragmented
        if len(layout_tokens) >= 10 and short_tokens / min(len(layout_tokens), 50) > 0.4:
            logger.debug(f"Layout mode fragmented ({short_tokens} short tokens), using plain")
            return "plain"
        
        return "layout"