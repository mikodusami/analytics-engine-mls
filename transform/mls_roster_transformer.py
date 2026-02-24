"""
Transformer for MLS roster data.
Takes raw scraped dicts and produces normalized MLSPlayer records.
"""
import logging
import re
from typing import List, Dict, Any, Optional
from transform.mls_schema import MLSPlayer

logger = logging.getLogger(__name__)


class MLSRosterTransformer:
    """Transforms raw roster data into normalized MLSPlayer records."""
    
    def transform(self, raw_players: List[Dict[str, Any]]) -> List[MLSPlayer]:
        """Transform raw player dicts into MLSPlayer records."""
        records = []
        
        for raw in raw_players:
            record = self._transform_player(raw)
            if record:
                records.append(record)
        
        logger.info(f"Transformed {len(records)} player records")
        return records
    
    def _transform_player(self, raw: Dict[str, Any]) -> Optional[MLSPlayer]:
        """Transform a single raw player dict into MLSPlayer."""
        try:
            # Extract and clean basic fields
            player_name = self._clean_name(raw.get("player_name", ""))
            if not player_name:
                return None
            
            # Prefer profile data over roster data where available
            jersey = raw.get("jersey_number_profile") or raw.get("jersey_number")
            position = raw.get("profile_position") or raw.get("position")
            roster_category = raw.get("profile_roster_category") or raw.get("roster_category")
            player_category = raw.get("profile_player_category") or raw.get("player_category")
            
            # Build profile details dict from all profile_ prefixed fields
            profile_details = {}
            for key, value in raw.items():
                if key.startswith("profile_") and value:
                    clean_key = key[8:]  # Remove "profile_" prefix
                    profile_details[clean_key] = self._clean_value(value)
            
            # Add any extra fields not in profile
            if raw.get("full_name"):
                profile_details["full_name"] = raw["full_name"]
            if raw.get("club_slug"):
                profile_details["club_slug"] = raw["club_slug"]
            
            return MLSPlayer(
                team_name=raw.get("team_name", ""),
                team_slug=raw.get("team_slug", ""),
                player_name=player_name,
                player_url=raw.get("player_url", ""),
                jersey_number=self._clean_jersey(jersey),
                position=self._clean_position(position),
                roster_category=self._clean_value(roster_category),
                player_category=self._clean_value(player_category),
                player_status=self._clean_value(raw.get("player_status")),
                profile_details=profile_details,
            )
            
        except Exception as e:
            logger.debug(f"Failed to transform player: {e}")
            return None
    
    def _clean_name(self, name: str) -> str:
        """Clean player name."""
        if not name:
            return ""
        # Remove extra whitespace
        return re.sub(r"\s+", " ", name).strip()
    
    def _clean_jersey(self, jersey: Optional[str]) -> Optional[str]:
        """Clean jersey number."""
        if not jersey:
            return None
        # Extract just the number
        match = re.search(r"(\d+)", str(jersey))
        return match.group(1) if match else None
    
    def _clean_position(self, position: Optional[str]) -> Optional[str]:
        """Clean position string."""
        if not position:
            return None
        return position.strip()
    
    def _clean_value(self, value: Optional[str]) -> Optional[str]:
        """Clean generic string value."""
        if not value:
            return None
        cleaned = re.sub(r"\s+", " ", str(value)).strip()
        return cleaned if cleaned else None
