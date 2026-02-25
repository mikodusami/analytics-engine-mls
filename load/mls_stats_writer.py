"""
CSV/Parquet writer for MLS player stats data.
"""
import csv
import logging
from pathlib import Path
from typing import List, Dict, Any, Set
from transform.mls_stats_schema import MLSPlayerStats

logger = logging.getLogger(__name__)


class MLSStatsWriter:
    """Writes MLS player stats data to files."""
    
    def __init__(self, output_dir: str = "output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def write_stats(self, stats: List[MLSPlayerStats], filename: str = "mls_player_stats.csv") -> Path:
        """Write stats records to CSV."""
        if not stats:
            logger.warning("No stats to write")
            return None
        
        filepath = self.output_dir / filename
        
        # Collect all possible fields
        all_stat_fields: Set[str] = set()
        all_profile_fields: Set[str] = set()
        
        for stat in stats:
            all_stat_fields.update(stat.stats.keys())
            all_profile_fields.update(stat.profile_details.keys())
        
        base_fields = [
            "team_name", "team_slug", "season", "stat_type",
            "player_name", "player_url", "club"
        ]
        stat_fields = [f"stat_{f}" for f in sorted(all_stat_fields)]
        profile_fields = [f"profile_{f}" for f in sorted(all_profile_fields)]
        fieldnames = base_fields + stat_fields + profile_fields
        
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for stat in stats:
                writer.writerow(stat.to_dict())
        
        logger.info(f"Wrote {len(stats)} stats records to {filepath}")
        return filepath
    
    def write_stats_by_team(self, stats: List[MLSPlayerStats], prefix: str = "mls_stats") -> List[Path]:
        """Write stats to separate files by team."""
        if not stats:
            logger.warning("No stats to write")
            return []
        
        # Group by team
        by_team: Dict[str, List[MLSPlayerStats]] = {}
        for stat in stats:
            by_team.setdefault(stat.team_slug, []).append(stat)
        
        paths = []
        for team_slug, team_stats in by_team.items():
            filename = f"{prefix}_{team_slug}.csv"
            path = self.write_stats(team_stats, filename)
            if path:
                paths.append(path)
        
        return paths
    
    def write_stats_parquet(self, stats: List[MLSPlayerStats], filename: str = "mls_player_stats.parquet") -> Path:
        """Write stats records to Parquet."""
        try:
            import pandas as pd
        except ImportError:
            logger.error("pandas required for parquet output")
            return None
        
        if not stats:
            logger.warning("No stats to write")
            return None
        
        filepath = self.output_dir / filename
        df = pd.DataFrame([s.to_dict() for s in stats])
        df.to_parquet(filepath, index=False)
        
        logger.info(f"Wrote {len(stats)} stats records to {filepath}")
        return filepath
