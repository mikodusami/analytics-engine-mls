"""
Canonical schema for salary records.

This is THE schema. All transformers output records matching this structure.
If you want to add a new field, add it here first.

Keep it simple. Keep it consistent. Don't be a hero.
"""
from dataclasses import dataclass
from typing import Optional


@dataclass
class SalaryRecord:
    """
    Normalized salary record.
    
    This is what comes out of the transformer and goes into storage.
    Every salary record, regardless of source format (CSV, PDF), ends up
    looking like this.
    
    Fields:
        year: The year of the salary data (e.g., 2024)
        club: Normalized club name (e.g., "Inter Miami CF")
        last_name: Player's last name
        first_name: Player's first name
        position: Position code (e.g., "M", "F", "GK", "D")
        base_salary: Base salary as raw string (e.g., "$500,000")
        guaranteed_comp: Total guaranteed compensation as raw string
    """
    year: int
    club: str
    last_name: str
    first_name: str
    position: str
    base_salary: str  # Raw string, not cleaned
    guaranteed_comp: str  # Raw string, not cleaned
    
    def to_dict(self) -> dict:
        """Convert to dict for CSV/database storage."""
        return {
            "year": self.year,
            "club": self.club,
            "last_name": self.last_name,
            "first_name": self.first_name,
            "position": self.position,
            "base_salary": self.base_salary,
            "guaranteed_comp": self.guaranteed_comp,
        }
