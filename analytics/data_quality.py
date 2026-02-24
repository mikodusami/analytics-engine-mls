"""
Data quality checks and validation.
"""
import pandas as pd
import logging
from typing import Dict, List, Any

logger = logging.getLogger(__name__)


class DataQualityChecker:
    """Validates and reports on data quality issues."""
    
    def __init__(self, df: pd.DataFrame):
        self.df = df
    
    def run_all_checks(self) -> Dict[str, Any]:
        """Run all data quality checks."""
        return {
            "missing_values": self.check_missing_values(),
            "salary_outliers": self.check_salary_outliers(),
            "invalid_positions": self.check_invalid_positions(),
            "duplicate_players": self.check_duplicates(),
            "club_variations": self.check_club_variations(),
            "summary": self.summary()
        }
    
    def check_missing_values(self) -> Dict[str, int]:
        """Check for missing values in each column."""
        missing = self.df.isnull().sum()
        empty_strings = (self.df == "").sum()
        return {
            "null_values": missing.to_dict(),
            "empty_strings": empty_strings.to_dict()
        }
    
    def check_salary_outliers(self) -> Dict[str, Any]:
        """Identify potential salary outliers."""
        salary = self.df["guaranteed_comp"]
        q1, q3 = salary.quantile([0.25, 0.75])
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        
        outliers = self.df[(salary < lower_bound) | (salary > upper_bound)]
        
        return {
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
            "outlier_count": len(outliers),
            "zero_salary_count": len(self.df[salary == 0]),
            "negative_salary_count": len(self.df[salary < 0]),
        }
    
    def check_invalid_positions(self) -> Dict[str, Any]:
        """Check for missing or unusual positions."""
        positions = self.df["position"]
        missing = positions.isna() | (positions == "")
        
        # Get position distribution
        pos_counts = positions.value_counts()
        
        return {
            "missing_position_count": missing.sum(),
            "missing_position_pct": (missing.sum() / len(self.df) * 100),
            "unique_positions": len(pos_counts),
            "position_distribution": pos_counts.head(20).to_dict()
        }

    def check_duplicates(self) -> Dict[str, Any]:
        """Check for potential duplicate player entries within same year."""
        # Group by year, last_name, first_name
        dupes = self.df.groupby(["year", "last_name", "first_name"]).size()
        dupes = dupes[dupes > 1]
        
        return {
            "duplicate_count": len(dupes),
            "duplicates": dupes.head(20).to_dict() if len(dupes) > 0 else {}
        }
    
    def check_club_variations(self) -> Dict[str, Any]:
        """Check for club name variations that might be the same team."""
        clubs = self.df["club"].unique()
        
        # Look for similar names
        variations = []
        clubs_lower = {c.lower(): c for c in clubs}
        
        return {
            "unique_clubs": len(clubs),
            "clubs": sorted(clubs)
        }
    
    def summary(self) -> Dict[str, Any]:
        """Get overall data quality summary."""
        total = len(self.df)
        complete = self.df.dropna().shape[0]
        
        return {
            "total_records": total,
            "complete_records": complete,
            "completeness_pct": (complete / total * 100) if total > 0 else 0,
            "years_covered": sorted(self.df["year"].unique().tolist()),
            "year_count": self.df["year"].nunique(),
            "club_count": self.df["club"].nunique(),
        }
    
    def print_report(self):
        """Print a formatted data quality report."""
        results = self.run_all_checks()
        
        print("\n" + "="*60)
        print("DATA QUALITY REPORT")
        print("="*60)
        
        print(f"\n📊 Summary:")
        print(f"   Total records: {results['summary']['total_records']:,}")
        print(f"   Years covered: {results['summary']['year_count']} ({min(results['summary']['years_covered'])}-{max(results['summary']['years_covered'])})")
        print(f"   Unique clubs: {results['summary']['club_count']}")
        
        print(f"\n⚠️  Missing Values:")
        for col, count in results['missing_values']['empty_strings'].items():
            if count > 0:
                print(f"   {col}: {count} empty")
        
        print(f"\n💰 Salary Outliers:")
        print(f"   Outlier count: {results['salary_outliers']['outlier_count']}")
        print(f"   Zero salaries: {results['salary_outliers']['zero_salary_count']}")
        
        print(f"\n🏃 Positions:")
        print(f"   Missing: {results['invalid_positions']['missing_position_count']} ({results['invalid_positions']['missing_position_pct']:.1f}%)")
        print(f"   Unique positions: {results['invalid_positions']['unique_positions']}")
        
        print(f"\n👥 Duplicates:")
        print(f"   Potential duplicates: {results['duplicate_players']['duplicate_count']}")
        
        print("\n" + "="*60)
