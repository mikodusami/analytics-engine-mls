"""
MLS Roster Scraper - Extracts team rosters and player details from mlssoccer.com
"""
import logging
import re
from typing import List, Dict, Optional, Any
from bs4 import BeautifulSoup
from ingestion.playwright_scraper import PlaywrightScraper

logger = logging.getLogger(__name__)


class MLSRosterScraper(PlaywrightScraper):
    """
    Scrapes MLS team rosters and player profile details.
    
    Flow:
    1. Visit /players/ to get all teams
    2. For each team, visit roster page
    3. For each player on roster, visit profile page
    4. Collect all player details as raw dicts
    """
    
    BASE_URL = "https://www.mlssoccer.com"
    PLAYERS_URL = f"{BASE_URL}/players/"
    
    def __init__(self, headless: bool = True, timeout: int = 60000):
        super().__init__(headless=headless, timeout=timeout)
        self._teams: List[Dict[str, str]] = []
        self._players: List[Dict[str, Any]] = []
    
    @property
    def teams(self) -> List[Dict[str, str]]:
        return self._teams
    
    @property
    def players(self) -> List[Dict[str, Any]]:
        return self._players
    
    def scrape(self) -> List[Dict[str, Any]]:
        """Run full scrape: teams -> rosters -> player profiles."""
        self.discover_teams()
        for team in self._teams:
            self.scrape_team_roster(team)
        logger.info(f"Total players scraped: {len(self._players)}")
        return self._players
    
    def discover_teams(self) -> List[Dict[str, str]]:
        """Discover all teams from the players page."""
        logger.info(f"Discovering teams from {self.PLAYERS_URL}")
        html = self.navigate(self.PLAYERS_URL)
        soup = BeautifulSoup(html, "html.parser")
        
        # Find all roster links like /clubs/inter-miami-cf/roster/
        all_links = soup.find_all("a", href=lambda x: x and "/roster" in x)
        
        seen_slugs = set()
        for link in all_links:
            href = link.get("href", "")
            match = re.search(r"/clubs/([^/]+)/roster", href)
            if match:
                team_slug = match.group(1)
                if team_slug not in seen_slugs:
                    seen_slugs.add(team_slug)
                    self._teams.append({
                        "name": team_slug.replace("-", " ").title(),
                        "slug": team_slug,
                        "roster_url": f"{self.BASE_URL}/clubs/{team_slug}/roster/",
                        "stats_url": f"{self.BASE_URL}/clubs/{team_slug}/stats/",
                    })
        
        logger.info(f"Discovered {len(self._teams)} teams")
        return self._teams

    def scrape_team_roster(self, team: Dict[str, str]) -> List[Dict[str, Any]]:
        """Scrape roster for a single team."""
        logger.info(f"Scraping roster for {team['name']}: {team['roster_url']}")
        html = self.navigate(team["roster_url"])
        soup = BeautifulSoup(html, "html.parser")
        
        # Find all tables (roster page has multiple tables for different roster categories)
        tables = soup.find_all("table")
        if not tables:
            logger.warning(f"No roster tables found for {team['name']}")
            return []
        
        team_players = []
        
        for table in tables:
            # Get headers to understand column order
            headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
            if not headers or "player" not in headers:
                continue
            
            # Parse each row
            rows = table.find_all("tr")[1:]  # Skip header row
            for row in rows:
                cells = row.find_all("td")
                if not cells:
                    continue
                
                player_data = self._parse_roster_row(row, cells, headers, team)
                if player_data:
                    # Fetch player profile details
                    self._fetch_player_profile(player_data)
                    team_players.append(player_data)
                    self._players.append(player_data)
        
        logger.info(f"  {team['name']}: {len(team_players)} players")
        return team_players
    
    def _parse_roster_row(self, row, cells, headers: List[str], team: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """
        Parse a roster table row.
        
        Headers: ['player', 'jersey #', 'position', 'roster category', 'player category', 'player status']
        """
        try:
            # Find player link
            player_link = row.select_one("a.mls-o-table__href, a[href*='/players/']")
            if not player_link:
                return None
            
            href = player_link.get("href", "")
            player_url = href if href.startswith("http") else f"{self.BASE_URL}{href}"
            
            # Get player name from the link
            name_elem = player_link.select_one(".short-name, .mls-o-table__player-name")
            player_name = name_elem.get_text(strip=True) if name_elem else player_link.get_text(strip=True)
            
            # Get cell texts
            cell_texts = [c.get_text(strip=True) for c in cells]
            
            # Map columns based on headers
            player_data = {
                "team_name": team["name"],
                "team_slug": team["slug"],
                "player_name": player_name,
                "player_url": player_url,
            }
            
            # Map each header to its value
            header_map = {
                "jersey #": "jersey_number",
                "position": "position",
                "roster category": "roster_category",
                "player category": "player_category",
                "player status": "player_status",
            }
            
            for i, header in enumerate(headers):
                if header in header_map and i < len(cell_texts):
                    player_data[header_map[header]] = cell_texts[i] or None
            
            return player_data
            
        except Exception as e:
            logger.debug(f"Failed to parse roster row: {e}")
            return None

    def _fetch_player_profile(self, player_data: Dict[str, Any]) -> None:
        """Fetch and parse player profile page for additional details."""
        player_url = player_data.get("player_url")
        if not player_url:
            return
        
        try:
            logger.debug(f"Fetching profile: {player_url}")
            html = self.navigate(player_url)
            soup = BeautifulSoup(html, "html.parser")
            
            # Parse masthead (header with jersey #, position, club)
            masthead = soup.select_one(".mls-o-masthead")
            if masthead:
                self._parse_masthead(masthead, player_data)
            
            # Parse player details section
            details_section = soup.select_one(".mls-l-module--player-status-details")
            if details_section:
                self._parse_player_details(details_section, player_data)
            
        except Exception as e:
            logger.warning(f"Failed to fetch profile for {player_data.get('player_name')}: {e}")
    
    def _parse_masthead(self, masthead, player_data: Dict[str, Any]) -> None:
        """
        Parse masthead section.
        Contains: Player image, jersey #, position, club name/logo
        Example text: "Lionel Messi#10 • Midfielder •Inter Miami CFSenior"
        """
        try:
            # Get full name from image alt
            img = masthead.select_one("img[alt]")
            if img:
                player_data["full_name"] = img.get("alt", "").strip()
            
            # Get club link/name
            club_link = masthead.select_one("a.mls-o-masthead__club-logo")
            if club_link:
                club_href = club_link.get("href", "")
                match = re.search(r"/clubs/([^/]+)/", club_href)
                if match:
                    player_data["club_slug"] = match.group(1)
            
            # Parse the info text (contains jersey #, position)
            info_wrapper = masthead.select_one(".mls-o-masthead__info-wrapper")
            if info_wrapper:
                text = info_wrapper.get_text(" ", strip=True)
                # Extract jersey number (e.g., "#10")
                jersey_match = re.search(r"#(\d+)", text)
                if jersey_match:
                    player_data["jersey_number_profile"] = jersey_match.group(1)
                
        except Exception as e:
            logger.debug(f"Failed to parse masthead: {e}")
    
    def _parse_player_details(self, section, player_data: Dict[str, Any]) -> None:
        """
        Parse player details section.
        Structure:
        <div class="mls-l-module--player-status-details__info">
            <h3>Name</h3>
            <span>Lionel Messi</span>
        </div>
        """
        try:
            info_items = section.select(".mls-l-module--player-status-details__info")
            
            for item in info_items:
                label_elem = item.select_one("h3")
                value_elem = item.select_one("span")
                
                if label_elem and value_elem:
                    label = label_elem.get_text(strip=True)
                    value = value_elem.get_text(" ", strip=True)
                    
                    # Normalize key
                    key = self._normalize_key(label)
                    if key and value:
                        player_data[f"profile_{key}"] = value
                        
        except Exception as e:
            logger.debug(f"Failed to parse player details: {e}")
    
    def _normalize_key(self, text: str) -> str:
        """Normalize text to a valid dict key."""
        if not text:
            return ""
        key = re.sub(r"[^\w\s]", "", text.lower())
        key = re.sub(r"\s+", "_", key.strip())
        return key
