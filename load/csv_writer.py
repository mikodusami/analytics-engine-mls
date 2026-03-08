"""
CSV writer for salary records.

This is the original writer for the salary ETL pipeline.
Writes SalaryRecord objects to CSV files.
"""
import csv
import logging
from pathlib import Path
from typing import List
from transform.schema import SalaryRecord

logger = logging.getLogger(__name__)


class CSVWriter:
    """
    Writes SalaryRecords to CSV files.
    
    Can write all records to one file or split by year.
    
    Usage:
        writer = CSVWriter(output_dir="output")
        writer.write_all(records, "salaries.csv")
        # or
        writer.write_by_year(records)  # Creates salaries_2024.csv, etc.
    """
    
    # Column order for the CSV - matches SalaryRecord.to_dict() keys
    FIELDNAMES = ["year", "club", "last_name", "first_name", "position", "base_salary", "guaranteed_comp"]
    
    def __init__(self, output_dir: str = "output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def write_all(self, records: List[SalaryRecord], filename: str = "salaries.csv") -> Path:
        """
        Write all records to a single CSV file.
        
        Args:
            records: List of SalaryRecord objects
            filename: Output filename
            
        Returns:
            Path to the written file
        """
        filepath = self.output_dir / filename
        
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.FIELDNAMES)
            writer.writeheader()
            for record in records:
                writer.writerow(record.to_dict())
        
        logger.info(f"Wrote {len(records)} records to {filepath}")
        return filepath
    
    def write_by_year(self, records: List[SalaryRecord]) -> List[Path]:
        """
        Write records to separate CSV files by year.
        
        Creates files like: salaries_2024.csv, salaries_2023.csv, etc.
        
        Args:
            records: List of SalaryRecord objects
            
        Returns:
            List of paths to written files
        """
        # Group records by year
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
