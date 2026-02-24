"""
Utility functions for cleaning raw values.
"""
import re
from typing import Optional


def clean_salary(value: str) -> Optional[float]:
    """
    Convert salary string to float.
    Handles: $1,234.56, 1234.56$, $1,234, 1234
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
    """Normalize position codes."""
    return value.strip().upper() if value else ""


def clean_name(value: str) -> str:
    """Clean player name, remove quotes."""
    if not value:
        return ""
    # Remove surrounding quotes
    cleaned = value.strip().strip('"').strip("'")
    return cleaned
