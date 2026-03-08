"""
Parquet file storage for salary data.

Parquet is a columnar storage format that's perfect for analytics:
- Compressed (smaller files than CSV)
- Fast to read specific columns
- Works great with pandas, spark, duckdb, etc.
- Preserves data types (no more "is this a string or int?" BS)

If you're doing any serious data analysis, use parquet over CSV.
Your future self will thank you.
"""
import logging
from pathlib import Path
from typing import List
import pandas as pd
from transform.schema import SalaryRecord

logger = logging.getLogger(__name__)


class ParquetStorage:
    """
    Parquet file storage for efficient analytics.
    
    Saves SalaryRecords to parquet format with optimized dtypes.
    Uses snappy compression for a good balance of speed and size.
    
    Usage:
        storage = ParquetStorage("output")
        storage.save(records)
        df = storage.load()
    """
    
    def __init__(self, output_dir: str = "output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def save(self, records: List[SalaryRecord], filename: str = "salaries.parquet") -> Path:
        """
        Save records to Parquet file.
        
        Optimizes dtypes for storage efficiency:
        - year: int16 (years don't need 64 bits)
        - club, position: category (repeated strings = use categories)
        - salaries: float32 (we don't need double precision for money)
        
        Args:
            records: List of SalaryRecord objects
            filename: Output filename
            
        Returns:
            Path to the written file
        """
        # Convert records to DataFrame
        df = pd.DataFrame([r.to_dict() for r in records])
        
        # Optimize dtypes for storage efficiency
        df["year"] = df["year"].astype("int16")  # Years fit in 16 bits
        df["club"] = df["club"].astype("category")  # Repeated strings
        df["position"] = df["position"].astype("category")  # Repeated strings
        df["base_salary"] = df["base_salary"].astype("float32")  # Don't need double
        df["guaranteed_comp"] = df["guaranteed_comp"].astype("float32")
        
        filepath = self.output_dir / filename
        # Use snappy compression - fast and good compression ratio
        df.to_parquet(filepath, index=False, compression="snappy")
        
        logger.info(f"Saved {len(records)} records to {filepath}")
        return filepath
    
    def load(self, filename: str = "salaries.parquet") -> pd.DataFrame:
        """
        Load Parquet file into DataFrame.
        
        Args:
            filename: Parquet file to load
            
        Returns:
            pandas DataFrame with the data
        """
        filepath = self.output_dir / filename
        return pd.read_parquet(filepath)
