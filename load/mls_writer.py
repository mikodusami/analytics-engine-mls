"""
CSV/Parquet writer for MLS roster and player data.
"""
import csv
import logging
from pathlib import Path
from typing import List
from transform.mls_schema import MLSPlayer, MLSTeam

logger = logging.getLogger(__name__)


class MLSWriter:
    """Writes MLS player and team data to files."""
    
    def __init__(self, output_dir: str = "output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def write_players(self, players: List[MLSPlayer], filename: str = "mls_rosters.csv") -> Path:
        """Write player records to CSV."""
        if not players:
            logger.warning("No players to write")
            return None
        
        filepath = self.output_dir / filename
        
        # Collect all possible fields from profile_details
        all_fields = set()
        base_fields = ["team_name", "team_slug", "player_name", "player_url", 
                       "jersey_number", "position", "roster_category", 
                       "player_category", "player_status"]
        
        for player in players:
            all_fields.update(player.profile_details.keys())
        
        fieldnames = base_fields + sorted(all_fields)
        
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for player in players:
                writer.writerow(player.to_dict())
        
        logger.info(f"Wrote {len(players)} players to {filepath}")
        return filepath
    
    def write_teams(self, teams: List[MLSTeam], filename: str = "mls_teams.csv") -> Path:
        """Write team records to CSV."""
        if not teams:
            logger.warning("No teams to write")
            return None
        
        filepath = self.output_dir / filename
        fieldnames = ["name", "slug", "roster_url", "stats_url"]
        
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for team in teams:
                writer.writerow(team.to_dict())
        
        logger.info(f"Wrote {len(teams)} teams to {filepath}")
        return filepath
    
    def write_players_parquet(self, players: List[MLSPlayer], filename: str = "mls_rosters.parquet") -> Path:
        """Write player records to Parquet."""
        try:
            import pandas as pd
        except ImportError:
            logger.error("pandas required for parquet output")
            return None
        
        if not players:
            logger.warning("No players to write")
            return None
        
        filepath = self.output_dir / filename
        df = pd.DataFrame([p.to_dict() for p in players])
        df.to_parquet(filepath, index=False)
        
        logger.info(f"Wrote {len(players)} players to {filepath}")
        return filepath
