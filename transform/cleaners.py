"""
Utility functions for cleaning raw values.

These are the little helper functions that clean up the messy data
from the parsers. Salaries with dollar signs, names with quotes,
positions in random cases - we fix all that sh*t here.
"""
import re
from typing import Optional


def clean_salary(value: str) -> Optional[float]:
    """
    Convert salary string to float.
    
    Handles all the weird formats MLSPA uses:
    - $1,234.56 (normal)
    - 1234.56$ (European style, because why not)
    - $1,234 (no cents)
    - 1234 (bare number)
    
    Args:
        value: Raw salary string
        
    Returns:
        Float value or None if parsing failed
    """
    if not value:
        return None
    # Remove $ and commas, handle trailing $
    cleaned = re.sub(r'[$,]', '', value.strip())
    try:
        return float(cleaned)
    except ValueError:
        return None


def clean_position(value: str) -> str:
    """
    Normalize position codes.
    
    Just uppercases and strips whitespace. Positions are usually
    short codes like "M", "F", "GK", "D", "M-F", etc.
    
    Args:
        value: Raw position string
        
    Returns:
        Cleaned position string (uppercase)
    """
    return value.strip().upper() if value else ""


def clean_name(value: str) -> str:
    """
    Clean player name.
    
    Removes surrounding quotes (some PDFs have these for some reason)
    and strips whitespace.
    
    Args:
        value: Raw name string
        
    Returns:
        Cleaned name string
    """
    if not value:
        return ""
    # Remove surrounding quotes (single or double)
    cleaned = value.strip().strip('"').strip("'")
    return cleaned
