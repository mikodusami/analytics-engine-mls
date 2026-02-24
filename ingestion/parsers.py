import logging
from abc import ABC, abstractmethod
from typing import List

logger = logging.getLogger(__name__)


class SalaryParser(ABC):
    @abstractmethod
    def parse(self, content: bytes) -> List[List[str]]:
        """
        Parse raw content into rows.
        
        Args:
            content: Raw bytes from source
            
        Returns:
            List of rows, where each row is a list of string tokens
        """
        pass

