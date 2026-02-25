"""
MLS Stats Scraper - Extracts team player stats by season from mlssoccer.com
"""
import logging
import re
from typing import List, Dict, Optional, Any
from bs4 import BeautifulSoup
from ingestion.playwright_scraper import PlaywrightScraper

logger = logging.getLogger(__name__)


class MLSStatsScraper(PlaywrightScraper):
    """
    Scrapes MLS team player stats by season and stat type.
    
    Flow:
    1. Visit /players/ to get all teams
    2. For each team, visit stats page
    3. For each season, iterate through stat types (general, passing, etc.)
    4. For each player, optionally fetch profile details
    """
    
    BASE_URL = "https://www.mlssoccer.com"
    PLAYERS_URL = f"{BASE_URL}/players/"
    
    STAT_TYPES = [
        "STATS_GENERAL",
        "STATS_PASSING",
        "STATS_ATTACKING",
        "STATS_DEFENDING",
        "STATS_GOALKEEPING",
    ]
    
    def __init__(self, headless: bool = True, timeout: int = 60000, fetch_profiles: bool = True):
        super().__init__(headless=headless, timeout=timeout)
        self._teams: List[Dict[str, str]] = []
        self._stats: List[Dict[str, Any]] = []
        self._fetch_profiles = fetch_profiles
        self._profile_cache: Dict[str, Dict[str, Any]] = {}  # Cache profiles to avoid re-fetching
    
    @property
    def teams(self) -> List[Dict[str, str]]:
        return self._teams
    
    @property
    def stats(self) -> List[Dict[str, Any]]:
        return self._stats
    
    def scrape(self, seasons: Optional[List[int]] = None) -> List[Dict[str, Any]]:
        """Run full scrape for all teams and specified seasons."""
        self.discover_teams()
        for team in self._teams:
            self.scrape_team_stats(team, seasons=seasons)
        logger.info(f"Total stats records scraped: {len(self._stats)}")
        return self._stats
    
    def discover_teams(self) -> List[Dict[str, str]]:
        """Discover all teams from the players page."""
        logger.info(f"Discovering teams from {self.PLAYERS_URL}")
        html = self.navigate(self.PLAYERS_URL)
        soup = BeautifulSoup(html, "html.parser")
        
        # Find all stats links like /clubs/atlanta-united/stats/
        all_links = soup.find_all("a", href=lambda x: x and "/stats" in x)
        
        seen_slugs = set()
        for link in all_links:
            href = link.get("href", "")
            match = re.search(r"/clubs/([^/]+)/stats", href)
            if match:
                team_slug = match.group(1)
                if team_slug not in seen_slugs:
                    seen_slugs.add(team_slug)
                    self._teams.append({
                        "name": team_slug.replace("-", " ").title(),
                        "slug": team_slug,
                        "stats_url": f"{self.BASE_URL}/clubs/{team_slug}/stats/",
                    })
        
        logger.info(f"Discovered {len(self._teams)} teams")
        return self._teams

    def scrape_team_stats(self, team: Dict[str, str], seasons: Optional[List[int]] = None) -> List[Dict[str, Any]]:
        """Scrape stats for a single team across seasons and stat types."""
        logger.info(f"Scraping stats for {team['name']}: {team['stats_url']}")
        
        # Navigate to team stats page
        self.navigate(team["stats_url"])
        self.page.wait_for_timeout(2000)
        
        # Get available seasons from dropdown
        available_seasons = self._get_available_seasons()
        if not available_seasons:
            logger.warning(f"No seasons found for {team['name']}")
            return []
        
        # Filter to requested seasons
        if seasons:
            available_seasons = [s for s in available_seasons if s in seasons]
        
        logger.info(f"  Processing {len(available_seasons)} seasons: {available_seasons[:5]}...")
        
        team_stats = []
        
        for season in available_seasons:
            # Select season
            self._select_season(season)
            
            for stat_type in self.STAT_TYPES:
                # Select stat type
                self._select_stat_type(stat_type)
                
                # Parse stats table (without fetching profiles yet)
                stats = self._parse_stats_table(team, season, stat_type)
                team_stats.extend(stats)
                self._stats.extend(stats)
        
        # Now fetch profiles for all unique players (if enabled)
        if self._fetch_profiles:
            unique_urls = set(s.get("player_url") for s in team_stats if s.get("player_url"))
            logger.info(f"  Fetching {len(unique_urls)} player profiles...")
            
            for player_url in unique_urls:
                if player_url not in self._profile_cache:
                    profile = self._fetch_player_profile(player_url)
                    self._profile_cache[player_url] = profile
            
            # Update stats with profile data
            for stat in team_stats:
                player_url = stat.get("player_url")
                if player_url and player_url in self._profile_cache:
                    stat["profile_details"] = self._profile_cache[player_url]
        
        logger.info(f"  {team['name']}: {len(team_stats)} stat records")
        return team_stats
    
    def _get_available_seasons(self) -> List[int]:
        """Get list of available seasons from dropdown."""
        try:
            html = self.page.content()
            soup = BeautifulSoup(html, "html.parser")
            
            # Find season dropdown (first select without 'mobile' class)
            selects = soup.find_all("select", class_="mls-o-buttons__dropdown-button")
            for sel in selects:
                if "mobile" not in sel.get("class", []):
                    options = sel.find_all("option")
                    seasons = []
                    for opt in options:
                        try:
                            seasons.append(int(opt.get("value")))
                        except (ValueError, TypeError):
                            pass
                    return seasons
            return []
        except Exception as e:
            logger.debug(f"Failed to get seasons: {e}")
            return []
    
    def _select_season(self, season: int) -> None:
        """Select a season from the dropdown."""
        try:
            season_select = self.page.locator("select.mls-o-buttons__dropdown-button").first
            season_select.select_option(str(season))
            self.page.wait_for_timeout(1500)
        except Exception as e:
            logger.debug(f"Failed to select season {season}: {e}")
    
    def _select_stat_type(self, stat_type: str) -> None:
        """Select a stat type from the dropdown."""
        try:
            stat_select = self.page.locator("select.mobile").first
            stat_select.select_option(stat_type)
            self.page.wait_for_timeout(1500)
        except Exception as e:
            logger.debug(f"Failed to select stat type {stat_type}: {e}")

    def _parse_stats_table(self, team: Dict[str, str], season: int, stat_type: str) -> List[Dict[str, Any]]:
        """Parse the stats table for current selection."""
        try:
            html = self.page.content()
            soup = BeautifulSoup(html, "html.parser")
            
            table = soup.select_one("table.mls-o-table, table")
            if not table:
                return []
            
            # Get headers (skip first which is stat type name)
            headers = []
            for th in table.find_all("th"):
                text = th.get_text(strip=True).lower()
                if text and text not in ["general", "passing", "attacking", "defending", "goalkeeping"]:
                    headers.append(text)
            
            if not headers:
                return []
            
            # Parse data rows
            stats_records = []
            rows = table.find_all("tr")
            
            for row in rows:
                cells = row.find_all("td")
                if not cells or len(cells) < 3:
                    continue
                
                # Find player link
                player_link = row.select_one("a[href*='/players/']")
                if not player_link:
                    continue
                
                href = player_link.get("href", "")
                player_url = href if href.startswith("http") else f"{self.BASE_URL}{href}"
                
                # Get player name
                name_elem = player_link.select_one(".short-name, .mls-o-table__player-name")
                player_name = name_elem.get_text(strip=True) if name_elem else player_link.get_text(strip=True)
                
                # Get player image thumbnail
                player_image = None
                img = player_link.select_one("img")
                if img:
                    player_image = img.get("src")
                
                # Map cell values to headers
                cell_texts = [c.get_text(strip=True) for c in cells]
                
                stats_data = {}
                for i, header in enumerate(headers):
                    if i < len(cell_texts):
                        # Skip player column (already extracted)
                        if header == "player":
                            continue
                        stats_data[header] = cell_texts[i] if cell_texts[i] else None
                
                record = {
                    "team_name": team["name"],
                    "team_slug": team["slug"],
                    "season": season,
                    "stat_type": stat_type.replace("STATS_", "").lower(),
                    "player_name": player_name,
                    "player_url": player_url,
                    "player_image": player_image,
                    "stats": stats_data,
                }
                
                # Fetch player profile if enabled
                # Note: profiles are fetched after all stats are collected
                # to avoid navigating away from stats page
                
                stats_records.append(record)
            
            return stats_records
            
        except Exception as e:
            logger.debug(f"Failed to parse stats table: {e}")
            return []
    
    def _fetch_player_profile(self, player_url: str) -> Dict[str, Any]:
        """Fetch player profile details."""
        try:
            html = self.navigate(player_url)
            soup = BeautifulSoup(html, "html.parser")
            
            details = {}
            
            # Parse masthead for images
            masthead = soup.select_one(".mls-o-masthead")
            if masthead:
                player_img = masthead.select_one(".mls-o-masthead__branded-image img")
                if player_img:
                    details["player_image_large"] = player_img.get("src")
                    details["full_name"] = player_img.get("alt", "").strip()
                
                club_logo = masthead.select_one(".mls-o-masthead__club-logo img")
                if club_logo:
                    details["team_logo"] = club_logo.get("src")
            
            # Parse player details section
            details_section = soup.select_one(".mls-l-module--player-status-details")
            if details_section:
                info_items = details_section.select(".mls-l-module--player-status-details__info")
                for item in info_items:
                    label_elem = item.select_one("h3")
                    value_elem = item.select_one("span")
                    if label_elem and value_elem:
                        label = label_elem.get_text(strip=True)
                        value = value_elem.get_text(" ", strip=True)
                        key = re.sub(r"[^\w\s]", "", label.lower())
                        key = re.sub(r"\s+", "_", key.strip())
                        if key and value:
                            details[key] = value
            
            return details
            
        except Exception as e:
            logger.debug(f"Failed to fetch profile {player_url}: {e}")
            return {}
