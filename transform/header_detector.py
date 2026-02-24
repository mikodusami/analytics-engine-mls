"""
Detects header rows and maps columns to canonical field names.
"""
import re
from typing import Optional
import logging

logger = logging.getLogger(__name__)

# Keywords that indicate a header row (check in full cell text, not just tokens)
HEADER_KEYWORDS = {"club", "team", "last", "first", "name", "salary", "position", "pos", "base", "guaranteed", "comp", "compensation"}

# Keywords that indicate a title/metadata row (not a header)
EXCLUDE_KEYWORDS = {"mls", "player", "salaries", "alphabetical", "guide", "list", "as", "of", "fall", "winter", "spring", "summer", "source"}


def find_header_row(rows: list[list[str]]) -> int:
    """
    Find the index of the header row.
    Returns -1 if not found.
    """
    for pos, row in enumerate(rows):
        if not row:
            continue
        
        # Join all cells and check for keywords in the full text
        row_text = " ".join(cell.lower() for cell in row)
        row_lower = [cell.lower() for cell in row]
        
        # Count header keywords (check both individual cells and full text)
        header_matches = 0
        for keyword in HEADER_KEYWORDS:
            if keyword in row_lower or keyword in row_text:
                header_matches += 1
        
        # Count exclude keywords
        exclude_matches = sum(1 for keyword in EXCLUDE_KEYWORDS if keyword in row_text)
        
        # If we have many header keywords (5+), it's likely a header even with some excludes
        # If we have 3-4 header keywords, only accept if no excludes
        if header_matches >= 5:
            return pos
        elif header_matches >= 3 and exclude_matches == 0:
            return pos
    
    return -1


def detect_column_order(header_row: list[str]) -> dict[str, int]:
    """
    Analyze header row and return mapping of field -> column index.
    Handles both single-word and multi-word headers like 'First Name', 'Team Name'.
    """
    mapping = {}
    
    for i, h in enumerate(header_row):
        h_lower = h.lower().strip()
        
        # Club/Team
        if h_lower in ("club", "team", "team name"):
            mapping["club"] = i
        
        # Names
        elif h_lower in ("last", "last name"):
            mapping["last_name"] = i
        elif h_lower in ("first", "first name"):
            mapping["first_name"] = i
        
        # Position
        elif h_lower in ("pos", "position"):
            mapping["position"] = i
        
        # Salaries - check for various patterns
        elif "base" in h_lower and "salary" in h_lower:
            mapping["base_salary"] = i
        elif h_lower == "base":
            mapping["base_salary"] = i
        elif "guaranteed" in h_lower:
            mapping["guaranteed_comp"] = i
        elif h_lower in ("compensation", "comp", "comp."):
            if "guaranteed_comp" not in mapping:
                mapping["guaranteed_comp"] = i
    
    logger.debug(f"Column mapping: {mapping}")
    return mapping
