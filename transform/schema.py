"""
Canonical schema for salary records.
All transformers output records matching this schema.
"""
from dataclasses import dataclass
from typing import Optional


@dataclass
class SalaryRecord:
    """Normalized salary record."""
    year: int
    club: str
    last_name: str
    first_name: str
    position: str
    base_salary: float
    guaranteed_comp: float
    
    def to_dict(self) -> dict:
        return {
            "year": self.year,
            "club": self.club,
            "last_name": self.last_name,
            "first_name": self.first_name,
            "position": self.position,
            "base_salary": self.base_salary,
            "guaranteed_comp": self.guaranteed_comp,
        }
