"""
Detects header rows and maps columns to canonical field names.
"""
import re
from typing import Optional
import logging

logger = logging.getLogger(__name__)

# Keywords that indicate a header row
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
        
        row_lower = [cell.lower() for cell in row]
        
        # Count header keywords
        header_matches = sum(1 for keyword in HEADER_KEYWORDS if keyword in row_lower)
        
        # Count exclude keywords
        exclude_matches = sum(1 for keyword in EXCLUDE_KEYWORDS if keyword in row_lower)
        
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
    Handles multi-token headers like ['First', 'Name'].
    """
    header_lower = [h.lower() for h in header_row]
    header_text = " ".join(header_lower)
    
    mapping = {}
    
    # Detect club/team position
    for i, h in enumerate(header_lower):
        if h in ("club", "team"):
            mapping["club"] = i
            break
    
    # Detect name columns - look for patterns
    for i, h in enumerate(header_lower):
        if h == "last":
            mapping["last_name"] = i
        elif h == "first":
            mapping["first_name"] = i
    
    # Detect position
    for i, h in enumerate(header_lower):
        if h in ("pos", "position"):
            mapping["position"] = i
            break
    
    # Detect salary columns - trickier due to multi-token headers
    # Look for "base" followed by "salary" or just "salary"
    for i, h in enumerate(header_lower):
        if h == "base" and "base_salary" not in mapping:
            mapping["base_salary"] = i
        elif h == "guaranteed" or h.startswith("guar"):
            mapping["guaranteed_comp"] = i
        elif h in ("compensation", "comp", "comp."):
            if "guaranteed_comp" not in mapping:
                mapping["guaranteed_comp"] = i
    
    logger.debug(f"Column mapping: {mapping}")
    return mapping
