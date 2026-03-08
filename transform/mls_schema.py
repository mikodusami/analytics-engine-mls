"""
Schema for MLS roster and player data.

These dataclasses define the structure of our cleaned/transformed data.
The scrapers produce raw dicts, the transformers convert them to these
dataclasses, and the writers serialize them to CSV/Parquet.

Why dataclasses? Because dicts are chaos and I like type hints.
"""
from dataclasses import dataclass, field
from typing import Optional, Dict, Any


@dataclass
class MLSPlayer:
    """
    Normalized player record from roster scraping.
    
    This is what comes out of the transformer. All the messy raw data
    has been cleaned up and organized into this nice structure.
    
    Fields:
        team_name: Human-readable team name (e.g., "Inter Miami Cf")
        team_slug: URL-friendly team ID (e.g., "inter-miami-cf")
        player_name: Player's display name
        player_url: Full URL to player's profile page
        jersey_number: Jersey number (as string because some are weird)
        position: Player position (GK, D, M, F, etc.)
        roster_category: Senior Roster, Supplemental, etc.
        player_category: Domestic, International, etc.
        player_status: Active, Injured, etc.
        player_image_thumb: Small image URL from roster table
        player_image: Large image URL from profile page
        team_logo: Team logo URL from profile page
        profile_details: Dict of extra profile data (height, weight, DOB, etc.)
    """
    team_name: str
    team_slug: str
    player_name: str
    player_url: str
    jersey_number: Optional[str] = None
    position: Optional[str] = None
    roster_category: Optional[str] = None
    player_category: Optional[str] = None
    player_status: Optional[str] = None
    player_image_thumb: Optional[str] = None
    player_image: Optional[str] = None
    team_logo: Optional[str] = None
    profile_details: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        """
        Convert to flat dict for CSV/storage.
        
        Flattens profile_details into the main dict with "profile_" prefix.
        This makes it easier to write to CSV (no nested structures).
        """
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
            "player_image_thumb": self.player_image_thumb,
            "player_image": self.player_image,
            "team_logo": self.team_logo,
        }
        # Flatten profile details into the dict
        for key, value in self.profile_details.items():
            base[f"profile_{key}"] = value
        return base


@dataclass
class MLSTeam:
    """
    Team info.
    
    Basic team data extracted from the players page.
    """
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
