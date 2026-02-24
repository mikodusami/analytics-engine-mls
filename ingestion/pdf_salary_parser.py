from ingestion.parsers import SalaryParser
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class PDFSalaryParser(SalaryParser):
    def parse(self, content: bytes) -> List[Dict[str, Any]]:
        pass