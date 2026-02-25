"""
Schema for MLS player stats data.
"""
from dataclasses import dataclass, field
from typing import Optional, Dict, Any


@dataclass
class MLSPlayerStats:
    """Player stats record for a specific team, season, and stat type."""
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
        """Convert to flat dict for CSV/storage."""
        base = {
            "team_name": self.team_name,
            "team_slug": self.team_slug,
            "season": self.season,
            "stat_type": self.stat_type,
            "player_name": self.player_name,
            "player_url": self.player_url,
            "club": self.club,
        }
        # Add all stats
        for key, value in self.stats.items():
            base[f"stat_{key}"] = value
        # Add profile details
        for key, value in self.profile_details.items():
            base[f"profile_{key}"] = value
        return base
