"""
Header detection and column mapping for salary data.

Different years of salary data have different column orders and names.
Some have "Club" first, some have "Last Name" first. Some say "Base Salary",
others say "Base". It's a mess.

This module figures out where the header row is and maps columns to
our canonical field names so the transformer knows what's what.
"""
import re
from typing import Optional
import logging

logger = logging.getLogger(__name__)

# Keywords that indicate a header row
# If a row has several of these, it's probably the header
HEADER_KEYWORDS = {
    "club", "team", "last", "first", "name", "salary", 
    "position", "pos", "base", "guaranteed", "comp", "compensation"
}

# Keywords that indicate a title/metadata row (NOT a header)
# These appear in title rows like "MLS Player Salaries - Fall 2024"
EXCLUDE_KEYWORDS = {
    "mls", "player", "salaries", "alphabetical", "guide", "list", 
    "as", "of", "fall", "winter", "spring", "summer", "source"
}


def find_header_row(rows: list[list[str]]) -> int:
    """
    Find the index of the header row.
    
    Scans through rows looking for one that has enough header keywords
    but not too many exclude keywords (which would indicate a title row).
    
    Args:
        rows: List of rows from the parser
        
    Returns:
        Index of header row, or -1 if not found
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
        
        # Count exclude keywords (title row indicators)
        exclude_matches = sum(1 for keyword in EXCLUDE_KEYWORDS if keyword in row_text)
        
        # Decision logic:
        # - 5+ header keywords = definitely a header (even with some excludes)
        # - 3-4 header keywords = header only if no excludes
        if header_matches >= 5:
            return pos
        elif header_matches >= 3 and exclude_matches == 0:
            return pos
    
    return -1


def detect_column_order(header_row: list[str]) -> dict[str, int]:
    """
    Analyze header row and return mapping of field -> column index.
    
    Handles various header formats:
    - "Club" or "Team" or "Team Name" -> club
    - "Last" or "Last Name" -> last_name
    - "First" or "First Name" -> first_name
    - "Pos" or "Position" -> position
    - "Base Salary" or just "Base" -> base_salary
    - "Guaranteed Compensation" or "Guaranteed" -> guaranteed_comp
    
    Args:
        header_row: List of header cell values
        
    Returns:
        Dict mapping field name to column index
    """
    mapping = {}
    logger.debug(f"printing header rows: {header_row}")
    index = 0
    for i, h in enumerate(header_row):
        h_lower = h.lower().strip()
        
        # Club/Team
        if h_lower in ("club", "team", "team name"):
            mapping["club"] = index
            index += 1
        
        # Names
        elif h_lower in ("last", "last name"):
            mapping["last_name"] = index
            index += 1
        elif h_lower in ("first", "first name"):
            mapping["first_name"] = index
            index += 1
        
        # Position
        elif h_lower in ("pos", "position"):
            mapping["position"] = index
            index += 1
        
        # Salaries - check for various patterns
        elif "base" in h_lower and "salary" in h_lower:
            mapping["base_salary"] = index
            index += 1
        elif h_lower == "base":
            mapping["base_salary"] = index
            index += 1
        elif i > 0 and header_row[i - 1].lower().strip() == 'cy' and h_lower == "salary":
            mapping["base_salary"] = index
            index += 1
        elif "guaranteed" in h_lower:
            mapping["guaranteed_comp"] = index
            index += 1
        elif h_lower in ("compensation", "comp", "comp."):
            # Only use this if we don't already have guaranteed_comp
            if "guaranteed_comp" not in mapping:
                mapping["guaranteed_comp"] = index
                index += 1
        
        # specific check for 2019
        
    
    logger.debug(f"Column mapping: {mapping}")
    return mapping
