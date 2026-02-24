import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class Parser(ABC):
    @abstractmethod
    def parse(self, content: bytes) -> List[Dict[str, Any]]:
        """
        parses raw content into records
        returns list of dicts with original column names preserved
        """
        pass

