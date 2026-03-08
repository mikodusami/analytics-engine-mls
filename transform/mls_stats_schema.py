"""
Schema for MLS player stats data.

This is the dataclass for stats records. Each record represents one player's
stats for one team, one season, and one stat type.

So if a player has stats for 5 stat types in 2024, that's 5 records.
Multiply by 30 seasons and you get why the CSV files are huge.
"""
from dataclasses import dataclass, field
from typing import Optional, Dict, Any


@dataclass
class MLSPlayerStats:
    """
    Player stats record for a specific team, season, and stat type.
    
    This is the output of the stats transformer. One record per player
    per season per stat type.
    
    Fields:
        team_name: Human-readable team name
        team_slug: URL-friendly team ID
        season: Year (e.g., 2024)
        stat_type: One of: general, passing, attacking, defending, goalkeeping
        player_name: Player's display name
        player_url: Full URL to player's profile page
        club: Club abbreviation from stats table (e.g., "MIA", "ATL")
        stats: Dict of stat values, prefixed with stat type
               (e.g., {"general_games_played": "10", "general_goals": "5"})
        profile_details: Dict of profile data if fetched (height, weight, etc.)
    """
    team_name: str
    team_slug: str
    season: int
    stat_type: str  # general, passing, attacking, defending, goalkeeping
    player_name: str
    player_url: str
    club: str  # Club abbreviation from stats table
    stats: Dict[str, Any] = field(default_factory=dict)  # All stat columns
    profile_details: Dict[str, Any] = field(default_factory=dict)  # From player profile
    
    def to_dict(self) -> dict:
        """
        Convert to flat dict for CSV/storage.
        
        Flattens stats with "stat_" prefix and profile_details with "profile_" prefix.
        This makes the CSV columns look like:
        - stat_general_games_played
        - stat_general_goals
        - profile_height
        - profile_weight
        """
        base = {
            "team_name": self.team_name,
            "team_slug": self.team_slug,
            "season": self.season,
            "stat_type": self.stat_type,
            "player_name": self.player_name,
            "player_url": self.player_url,
            "club": self.club,
        }
        # Add all stats with "stat_" prefix
        for key, value in self.stats.items():
            base[f"stat_{key}"] = value
        # Add profile details with "profile_" prefix
        for key, value in self.profile_details.items():
            base[f"profile_{key}"] = value
        return base
