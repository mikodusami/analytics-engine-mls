"""
CSV/Parquet writer for MLS roster and player data.
"""
import csv
import logging
from pathlib import Path
from typing import List, Dict, Any, Union
from transform.mls_schema import MLSPlayer

logger = logging.getLogger(__name__)


class MLSWriter:
    """Writes MLS player and team data to files."""
    
    def __init__(self, output_dir: str = "output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def write_players(self, players: List[MLSPlayer], filename: str = "mls_rosters.csv") -> Path:
        """Write MLSPlayer records to CSV."""
        if not players:
            logger.warning("No players to write")
            return None
        
        filepath = self.output_dir / filename
        
        # Collect all possible fields from profile_details
        all_profile_fields = set()
        for player in players:
            all_profile_fields.update(player.profile_details.keys())
        
        base_fields = [
            "team_name", "team_slug", "player_name", "player_url",
            "jersey_number", "position", "roster_category",
            "player_category", "player_status",
            "player_image_thumb", "player_image", "team_logo"
        ]
        profile_fields = [f"profile_{f}" for f in sorted(all_profile_fields)]
        fieldnames = base_fields + profile_fields
        
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for player in players:
                writer.writerow(player.to_dict())
        
        logger.info(f"Wrote {len(players)} players to {filepath}")
        return filepath
    
    def write_teams_raw(self, teams: List[Dict[str, str]], filename: str = "mls_teams.csv") -> Path:
        """Write raw team dicts to CSV."""
        if not teams:
            logger.warning("No teams to write")
            return None
        
        filepath = self.output_dir / filename
        fieldnames = ["name", "slug", "roster_url", "stats_url"]
        
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for team in teams:
                writer.writerow(team)
        
        logger.info(f"Wrote {len(teams)} teams to {filepath}")
        return filepath
    
    def write_players_parquet(self, players: List[MLSPlayer], filename: str = "mls_rosters.parquet") -> Path:
        """Write MLSPlayer records to Parquet."""
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
