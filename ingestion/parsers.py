"""
Abstract base class for salary parsers.

Different years of salary data come in different formats (CSV, PDF, etc.).
Each format needs its own parser, but they all implement this interface.

This is the "Strategy Pattern" in action. Look at you, learning design patterns.
"""
import logging
from abc import ABC, abstractmethod
from typing import List

logger = logging.getLogger(__name__)


class SalaryParser(ABC):
    """
    Abstract base class for salary document parsers.
    
    Implement parse() to handle a specific format (CSV, PDF, etc.).
    The method takes raw bytes and returns a list of rows.
    
    Each row is a list of strings - we don't do any cleaning here,
    that's the transformer's job. Separation of concerns, baby.
    """
    
    @abstractmethod
    def parse(self, content: bytes) -> List[List[str]]:
        """
        Parse raw content into rows.
        
        Args:
            content: Raw bytes from the source (PDF, CSV, whatever)
            
        Returns:
            List of rows, where each row is a list of string tokens.
            No cleaning, no normalization - just raw data.
        """
        pass

