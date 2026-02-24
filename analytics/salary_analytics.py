"""
Salary analytics and insights.
"""
import pandas as pd
import logging
from pathlib import Path
from typing import Optional, Dict, List, Any

logger = logging.getLogger(__name__)


class SalaryAnalytics:
    """Analytics engine for MLS salary data."""
    
    def __init__(self, data_source: str = "output/salaries.parquet"):
        """
        Initialize with data source.
        Accepts parquet file path or CSV file path.
        """
        path = Path(data_source)
        if path.suffix == ".parquet":
            self.df = pd.read_parquet(path)
        else:
            self.df = pd.read_csv(path)
        
        logger.info(f"Loaded {len(self.df)} records from {data_source}")
    
    # === Salary Trends ===
    
    def salary_trends_by_year(self) -> pd.DataFrame:
        """Get salary statistics by year."""
        return self.df.groupby("year").agg({
            "base_salary": ["mean", "median", "min", "max", "sum"],
            "guaranteed_comp": ["mean", "median", "min", "max", "sum"],
            "last_name": "count"
        }).round(2)
    
    def salary_growth_rate(self) -> pd.DataFrame:
        """Calculate year-over-year salary growth."""
        yearly = self.df.groupby("year")["base_salary"].mean()
        growth = yearly.pct_change() * 100
        return pd.DataFrame({
            "avg_salary": yearly,
            "growth_pct": growth.round(2)
        })
    
    # === Team Analysis ===
    
    def team_spending(self, year: Optional[int] = None) -> pd.DataFrame:
        """Get total team spending."""
        df = self.df if year is None else self.df[self.df["year"] == year]
        return df.groupby("club").agg({
            "base_salary": "sum",
            "guaranteed_comp": "sum",
            "last_name": "count"
        }).rename(columns={"last_name": "player_count"}).sort_values("guaranteed_comp", ascending=False)

    def team_spending_over_time(self, club: str) -> pd.DataFrame:
        """Get spending history for a specific team."""
        return self.df[self.df["club"] == club].groupby("year").agg({
            "base_salary": "sum",
            "guaranteed_comp": "sum",
            "last_name": "count"
        }).rename(columns={"last_name": "player_count"})
    
    def team_comparison(self, year: int) -> pd.DataFrame:
        """Compare all teams for a specific year."""
        df = self.df[self.df["year"] == year]
        return df.groupby("club").agg({
            "base_salary": ["sum", "mean", "max"],
            "guaranteed_comp": ["sum", "mean", "max"],
            "last_name": "count"
        }).sort_values(("guaranteed_comp", "sum"), ascending=False)
    
    # === Top Earners ===
    
    def top_earners(self, year: Optional[int] = None, n: int = 10) -> pd.DataFrame:
        """Get top earners by guaranteed compensation."""
        df = self.df if year is None else self.df[self.df["year"] == year]
        return df.nlargest(n, "guaranteed_comp")[
            ["year", "club", "first_name", "last_name", "position", "base_salary", "guaranteed_comp"]
        ]
    
    def top_earners_by_position(self, position: str, year: Optional[int] = None, n: int = 10) -> pd.DataFrame:
        """Get top earners for a specific position."""
        df = self.df if year is None else self.df[self.df["year"] == year]
        df = df[df["position"].str.contains(position, case=False, na=False)]
        return df.nlargest(n, "guaranteed_comp")[
            ["year", "club", "first_name", "last_name", "position", "base_salary", "guaranteed_comp"]
        ]
    
    def top_earners_by_year(self, n: int = 1) -> pd.DataFrame:
        """Get top n earners for each year."""
        return self.df.groupby("year").apply(
            lambda x: x.nlargest(n, "guaranteed_comp")
        ).reset_index(drop=True)[
            ["year", "club", "first_name", "last_name", "position", "guaranteed_comp"]
        ]
    
    # === Salary Distribution ===
    
    def salary_distribution(self, year: Optional[int] = None) -> Dict[str, float]:
        """Get salary distribution statistics."""
        df = self.df if year is None else self.df[self.df["year"] == year]
        salary = df["guaranteed_comp"]
        return {
            "count": len(salary),
            "mean": salary.mean(),
            "median": salary.median(),
            "std": salary.std(),
            "min": salary.min(),
            "max": salary.max(),
            "q25": salary.quantile(0.25),
            "q75": salary.quantile(0.75),
            "q90": salary.quantile(0.90),
            "q99": salary.quantile(0.99),
        }
    
    def salary_percentiles(self, year: Optional[int] = None) -> pd.Series:
        """Get salary at various percentiles."""
        df = self.df if year is None else self.df[self.df["year"] == year]
        return df["guaranteed_comp"].quantile([0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99])
    
    def position_salary_comparison(self, year: Optional[int] = None) -> pd.DataFrame:
        """Compare salaries across positions."""
        df = self.df if year is None else self.df[self.df["year"] == year]
        # Filter out empty positions
        df = df[df["position"].notna() & (df["position"] != "")]
        return df.groupby("position").agg({
            "guaranteed_comp": ["mean", "median", "min", "max", "count"]
        }).sort_values(("guaranteed_comp", "mean"), ascending=False)
