"""
Schema for MLS roster and player data.
"""
from dataclasses import dataclass, field
from typing import Optional, Dict, Any


@dataclass
class MLSPlayer:
    """Normalized player record."""
    team_name: str
    team_slug: str
    player_name: str
    player_url: str
    jersey_number: Optional[str] = None
    position: Optional[str] = None
    roster_category: Optional[str] = None
    player_category: Optional[str] = None
    player_status: Optional[str] = None
    profile_details: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        """Convert to flat dict for CSV/storage."""
        base = {
            "team_name": self.team_name,
            "team_slug": self.team_slug,
            "player_name": self.player_name,
            "player_url": self.player_url,
            "jersey_number": self.jersey_number,
            "position": self.position,
            "roster_category": self.roster_category,
            "player_category": self.player_category,
            "player_status": self.player_status,
        }
        # Flatten profile details into the dict
        for key, value in self.profile_details.items():
            base[f"profile_{key}"] = value
        return base


@dataclass
class MLSTeam:
    """Team info."""
    name: str
    slug: str
    roster_url: str
    stats_url: str
    
    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "slug": self.slug,
            "roster_url": self.roster_url,
            "stats_url": self.stats_url,
        }
