"""
CSV/Parquet writer for MLS roster and player data.

This is the "L" in ETL - the Load phase. Takes our nice clean MLSPlayer
objects and writes them to files. Supports CSV and Parquet formats.

CSV is human-readable but slow for large datasets.
Parquet is binary but fast and compressed. Use parquet for analytics.
"""
import csv
import logging
from pathlib import Path
from typing import List, Dict, Any, Union
from transform.mls_schema import MLSPlayer

logger = logging.getLogger(__name__)


class MLSWriter:
    """
    Writes MLS player and team data to files.
    
    Handles both CSV and Parquet output formats. Creates the output
    directory if it doesn't exist.
    
    Usage:
        writer = MLSWriter(output_dir="output")
        writer.write_players(players, "mls_rosters.csv")
        writer.write_players_parquet(players, "mls_rosters.parquet")
    """
    
    def __init__(self, output_dir: str = "output"):
        self.output_dir = Path(output_dir)
        # Create output directory if it doesn't exist
        # parents=True creates parent dirs, exist_ok=True doesn't error if exists
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def write_players(self, players: List[MLSPlayer], filename: str = "mls_rosters.csv") -> Path:
        """
        Write MLSPlayer records to CSV.
        
        Dynamically builds fieldnames from all players' profile_details
        so we don't miss any columns. Different players might have different
        profile fields depending on what was available on their page.
        
        Args:
            players: List of MLSPlayer objects
            filename: Output filename
            
        Returns:
            Path to the written file, or None if no players
        """
        if not players:
            logger.warning("No players to write")
            return None
        
        filepath = self.output_dir / filename
        
        # Collect all possible fields from profile_details across all players
        # This ensures we have columns for every field that appears in any player
        all_profile_fields = set()
        for player in players:
            all_profile_fields.update(player.profile_details.keys())
        
        # Build fieldnames: base fields first, then sorted profile fields
        base_fields = [
            "team_name", "team_slug", "player_name", "player_url",
            "jersey_number", "position", "roster_category",
            "player_category", "player_status",
            "player_image_thumb", "player_image", "team_logo"
        ]
        profile_fields = [f"profile_{f}" for f in sorted(all_profile_fields)]
        fieldnames = base_fields + profile_fields
        
        # Write the CSV
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            # extrasaction="ignore" means ignore dict keys not in fieldnames
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for player in players:
                writer.writerow(player.to_dict())
        
        logger.info(f"Wrote {len(players)} players to {filepath}")
        return filepath
    
    def write_teams_raw(self, teams: List[Dict[str, str]], filename: str = "mls_teams.csv") -> Path:
        """
        Write raw team dicts to CSV.
        
        This is for the team list, not player data. Simple 4-column CSV.
        
        Args:
            teams: List of team dicts from scraper
            filename: Output filename
            
        Returns:
            Path to the written file, or None if no teams
        """
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
        """
        Write MLSPlayer records to Parquet.
        
        Parquet is a columnar format that's great for analytics:
        - Compressed (smaller files)
        - Fast to read specific columns
        - Works great with pandas, spark, etc.
        
        Requires pandas to be installed.
        
        Args:
            players: List of MLSPlayer objects
            filename: Output filename
            
        Returns:
            Path to the written file, or None if no players/pandas not installed
        """
        try:
            import pandas as pd
        except ImportError:
            logger.error("pandas required for parquet output - pip install pandas")
            return None
        
        if not players:
            logger.warning("No players to write")
            return None
        
        filepath = self.output_dir / filename
        # Convert to DataFrame and write to parquet
        df = pd.DataFrame([p.to_dict() for p in players])
        df.to_parquet(filepath, index=False)
        
        logger.info(f"Wrote {len(players)} players to {filepath}")
        return filepath
