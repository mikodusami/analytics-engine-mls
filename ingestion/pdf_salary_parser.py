"""
PDF parser for salary documents.

This is where the pain lives. Older salary data (pre-2020) is in PDF format,
and extracting text from PDFs is like pulling teeth. With pliers. Blindfolded.

We use pypdf to extract text, but PDFs are notoriously inconsistent.
Some have nice layout, some have fragmented text. We try to detect which
mode works best and use that.

If you're debugging this, I'm sorry. May god have mercy on your soul.
"""
from ingestion.parsers import SalaryParser
from typing import List
import logging
import io
from pypdf import PdfReader

logger = logging.getLogger(__name__)


class PDFSalaryParser(SalaryParser):
    """
    Extracts raw row data from PDF salary documents.
    
    PDFs are a nightmare. The text extraction can produce:
    - Nice clean lines (if we're lucky)
    - Fragmented garbage (if we're not)
    
    We try "layout" mode first (preserves spacing), but fall back to
    "plain" mode if the text looks fragmented.
    """
    
    def parse(self, content: bytes) -> List[List[str]]:
        """
        Parse PDF content and return all rows as lists of strings.
        
        Tries to auto-detect the best extraction mode based on how
        fragmented the text looks. Because PDFs are inconsistent a**holes.
        
        Args:
            content: Raw PDF bytes
            
        Returns:
            List of rows, where each row is a list of whitespace-split tokens
        """
        # Wrap bytes in a file-like object for pypdf
        stream = io.BytesIO(content)
        pdf_reader = PdfReader(stream=stream)

        num_pages = len(pdf_reader.pages)
        logger.debug(f"Found {num_pages} pages in PDF.")

        # Auto-detect best extraction mode
        mode = self._detect_best_mode(pdf_reader)
        logger.debug(f"Using extraction mode: {mode}")

        rows = []
        for page_number in range(num_pages):
            current_page = pdf_reader.pages[page_number]
            extracted_text = current_page.extract_text(extraction_mode=mode)

            # Split by newlines, then split each line by whitespace
            for line in extracted_text.split("\n"):
                tokens = line.split()
                rows.append(tokens)
        
        return rows
    
    def _detect_best_mode(self, pdf_reader: PdfReader) -> str:
        """
        Detect whether plain or layout mode produces better results.
        
        Layout mode preserves spacing (good for tables), but some PDFs
        produce fragmented text with lots of single-character tokens.
        
        We check the first page for fragmentation patterns:
        - If >40% of tokens are 1-2 characters, text is fragmented
        - Use plain mode for fragmented text, layout for clean text
        
        Args:
            pdf_reader: The PdfReader object
            
        Returns:
            "plain" or "layout"
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