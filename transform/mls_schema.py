"""
Schema for MLS roster and player data.
"""
from dataclasses import dataclass, field
from typing import Optional, Dict, Any


@dataclass
class MLSPlayer:
    """Player record from roster page."""
    team_name: str
    team_slug: str
    player_name: str
    player_url: str
    jersey_number: Optional[str] = None
    position: Optional[str] = None
    roster_category: Optional[str] = None
    player_category: Optional[str] = None
    player_status: Optional[str] = None
    # Profile details (populated after visiting player page)
    profile_details: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        return {
            "team_name": self.team_name,
            "team_slug": self.team_slug,
            "player_name": self.player_name,
            "player_url": self.player_url,
            "jersey_number": self.jersey_number,
            "position": self.position,
            "roster_category": self.roster_category,
            "player_category": self.player_category,
            "player_status": self.player_status,
            **self.profile_details,
        }


@dataclass
class MLSTeam:
    """Team info from players page."""
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
