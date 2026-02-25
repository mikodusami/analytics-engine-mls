"""
Transformer for MLS player stats data.
"""
import logging
import re
from typing import List, Dict, Any, Optional
from transform.mls_stats_schema import MLSPlayerStats

logger = logging.getLogger(__name__)


class MLSStatsTransformer:
    """Transforms raw stats data into normalized MLSPlayerStats records."""
    
    def transform(self, raw_stats: List[Dict[str, Any]]) -> List[MLSPlayerStats]:
        """Transform raw stats dicts into MLSPlayerStats records."""
        records = []
        
        for raw in raw_stats:
            record = self._transform_stat(raw)
            if record:
                records.append(record)
        
        logger.info(f"Transformed {len(records)} stats records")
        return records
    
    def _transform_stat(self, raw: Dict[str, Any]) -> Optional[MLSPlayerStats]:
        """Transform a single raw stat dict into MLSPlayerStats."""
        try:
            player_name = self._clean_name(raw.get("player_name", ""))
            if not player_name:
                return None
            
            # Clean stats values
            stats = {}
            raw_stats = raw.get("stats", {})
            for key, value in raw_stats.items():
                clean_key = self._normalize_key(key)
                stats[clean_key] = self._clean_stat_value(value)
            
            # Get club from stats
            club = stats.pop("club", "") or ""
            
            # Clean profile details
            profile_details = {}
            raw_profile = raw.get("profile_details", {})
            for key, value in raw_profile.items():
                profile_details[key] = self._clean_value(value)
            
            # Add player image if present
            if raw.get("player_image"):
                profile_details["player_image_thumb"] = raw["player_image"]
            
            return MLSPlayerStats(
                team_name=raw.get("team_name", ""),
                team_slug=raw.get("team_slug", ""),
                season=raw.get("season", 0),
                stat_type=raw.get("stat_type", ""),
                player_name=player_name,
                player_url=raw.get("player_url", ""),
                club=club,
                stats=stats,
                profile_details=profile_details,
            )
            
        except Exception as e:
            logger.debug(f"Failed to transform stat: {e}")
            return None
    
    def _clean_name(self, name: str) -> str:
        """Clean player name."""
        if not name:
            return ""
        return re.sub(r"\s+", " ", name).strip()
    
    def _normalize_key(self, text: str) -> str:
        """Normalize text to a valid dict key."""
        if not text:
            return ""
        key = re.sub(r"[^\w\s]", "", text.lower())
        key = re.sub(r"\s+", "_", key.strip())
        return key
    
    def _clean_stat_value(self, value: Optional[str]) -> Optional[str]:
        """Clean stat value."""
        if value is None:
            return None
        cleaned = str(value).strip()
        return cleaned if cleaned else None
    
    def _clean_value(self, value: Optional[str]) -> Optional[str]:
        """Clean generic string value."""
        if not value:
            return None
        cleaned = re.sub(r"\s+", " ", str(value)).strip()
        return cleaned if cleaned else None
