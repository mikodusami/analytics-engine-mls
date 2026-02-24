"""
CSV writer for salary records.
"""
import csv
import logging
from pathlib import Path
from typing import List
from transform.schema import SalaryRecord

logger = logging.getLogger(__name__)


class CSVWriter:
    """Writes SalaryRecords to CSV files."""
    
    FIELDNAMES = ["year", "club", "last_name", "first_name", "position", "base_salary", "guaranteed_comp"]
    
    def __init__(self, output_dir: str = "output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def write_all(self, records: List[SalaryRecord], filename: str = "salaries.csv") -> Path:
        """Write all records to a single CSV file."""
        filepath = self.output_dir / filename
        
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.FIELDNAMES)
            writer.writeheader()
            for record in records:
                writer.writerow(record.to_dict())
        
        logger.info(f"Wrote {len(records)} records to {filepath}")
        return filepath
    
    def write_by_year(self, records: List[SalaryRecord]) -> List[Path]:
        """Write records to separate CSV files by year."""
        # Group by year
        by_year: dict[int, List[SalaryRecord]] = {}
        for record in records:
            by_year.setdefault(record.year, []).append(record)
        
        paths = []
        for year, year_records in sorted(by_year.items()):
            filepath = self.output_dir / f"salaries_{year}.csv"
            
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.FIELDNAMES)
                writer.writeheader()
                for record in year_records:
                    writer.writerow(record.to_dict())
            
            logger.info(f"Wrote {len(year_records)} records to {filepath}")
            paths.append(filepath)
        
        return paths
