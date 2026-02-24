"""
Parquet file storage for salary data.
"""
import logging
from pathlib import Path
from typing import List
import pandas as pd
from transform.schema import SalaryRecord

logger = logging.getLogger(__name__)


class ParquetStorage:
    """Parquet file storage for efficient analytics."""
    
    def __init__(self, output_dir: str = "output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def save(self, records: List[SalaryRecord], filename: str = "salaries.parquet") -> Path:
        """Save records to Parquet file."""
        df = pd.DataFrame([r.to_dict() for r in records])
        
        # Optimize dtypes
        df["year"] = df["year"].astype("int16")
        df["club"] = df["club"].astype("category")
        df["position"] = df["position"].astype("category")
        df["base_salary"] = df["base_salary"].astype("float32")
        df["guaranteed_comp"] = df["guaranteed_comp"].astype("float32")
        
        filepath = self.output_dir / filename
        df.to_parquet(filepath, index=False, compression="snappy")
        
        logger.info(f"Saved {len(records)} records to {filepath}")
        return filepath
    
    def load(self, filename: str = "salaries.parquet") -> pd.DataFrame:
        """Load Parquet file into DataFrame."""
        filepath = self.output_dir / filename
        return pd.read_parquet(filepath)
