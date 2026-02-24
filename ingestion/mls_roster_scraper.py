"""
MLS Roster Scraper - Extracts team rosters and player details from mlssoccer.com
"""
import logging
import re
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from ingestion.playwright_scraper import PlaywrightScraper
from transform.mls_schema import MLSPlayer, MLSTeam

logger = logging.getLogger(__name__)


class MLSRosterScraper(PlaywrightScraper):
    """
    Scrapes MLS team rosters and player profile details.
    
    Flow:
    1. Visit /players/ to get all teams
    2. For each team, visit roster page
    3. For each player on roster, visit profile page
    4. Collect all player details
    """
    
    BASE_URL = "https://www.mlssoccer.com"
    PLAYERS_URL = f"{BASE_URL}/players/"
    
    def __init__(self, headless: bool = True, timeout: int = 30000):
        super().__init__(headless=headless, timeout=timeout)
        self._teams: List[MLSTeam] = []
        self._players: List[MLSPlayer] = []
    
    @property
    def teams(self) -> List[MLSTeam]:
        return self._teams
    
    @property
    def players(self) -> List[MLSPlayer]:
        return self._players
    
    def scrape(self) -> List[MLSPlayer]:
        """Run full scrape: teams -> rosters -> player profiles."""
        self.discover_teams()
        for team in self._teams:
            self.scrape_team_roster(team)
        logger.info(f"Total players scraped: {len(self._players)}")
        return self._players
    
    def discover_teams(self) -> List[MLSTeam]:
        """Discover all teams from the players page."""
        logger.info(f"Discovering teams from {self.PLAYERS_URL}")
        html = self.navigate(self.PLAYERS_URL)
        soup = BeautifulSoup(html, "html.parser")
        
        # Find all links that contain /clubs/ and /roster/
        all_links = soup.find_all("a", href=True)
        
        seen_slugs = set()
        for link in all_links:
            href = link.get("href", "")
            # Look for roster links like /clubs/inter-miami-cf/roster/
            match = re.search(r"/clubs/([^/]+)/roster", href)
            if match:
                team_slug = match.group(1)
                if team_slug not in seen_slugs:
                    seen_slugs.add(team_slug)
                    team = MLSTeam(
                        name=team_slug.replace("-", " ").title(),
                        slug=team_slug,
                        roster_url=f"{self.BASE_URL}/clubs/{team_slug}/roster/",
                        stats_url=f"{self.BASE_URL}/clubs/{team_slug}/stats/",
                    )
                    self._teams.append(team)
        
        logger.info(f"Discovered {len(self._teams)} teams")
        return self._teams

    def scrape_team_roster(self, team: MLSTeam) -> List[MLSPlayer]:
        """Scrape roster for a single team."""
        logger.info(f"Scraping roster for {team.name}: {team.roster_url}")
        html = self.navigate(team.roster_url)
        soup = BeautifulSoup(html, "html.parser")
        
        # Find roster table - try multiple selectors
        table = soup.select_one("table, .mls-o-table, [class*='roster']")
        if not table:
            logger.warning(f"No roster table found for {team.name}")
            return []
        
        # Parse each player row
        rows = table.select("tbody tr, tr[class*='row']")
        if not rows:
            rows = table.find_all("tr")[1:]  # Skip header row
        
        team_players = []
        
        for row in rows:
            player = self._parse_roster_row(row, team)
            if player:
                # Fetch player profile details
                self._fetch_player_profile(player)
                team_players.append(player)
                self._players.append(player)
        
        logger.info(f"  {team.name}: {len(team_players)} players")
        return team_players
    
    def _parse_team_item(self, item) -> Optional[MLSTeam]:
        """Parse team item to extract team info and links."""
        try:
            # Find roster and stats links
            links = item.find_all("a", href=True)
            roster_url = None
            stats_url = None
            team_name = None
            team_slug = None
            
            for link in links:
                href = link.get("href", "")
                if "/roster" in href:
                    roster_url = self._make_absolute_url(href)
                    # Extract team slug from URL
                    match = re.search(r"/clubs/([^/]+)/roster", href)
                    if match:
                        team_slug = match.group(1)
                elif "/stats" in href:
                    stats_url = self._make_absolute_url(href)
            
            # Get team name from image alt or link text
            img = item.find("img")
            if img:
                team_name = img.get("alt", "").strip()
            if not team_name:
                team_name = item.get_text(strip=True).split("\n")[0]
            
            # Clean up team name
            if team_name:
                team_name = re.sub(r"\s+", " ", team_name).strip()
            
            if roster_url and team_slug:
                return MLSTeam(
                    name=team_name or team_slug.replace("-", " ").title(),
                    slug=team_slug,
                    roster_url=roster_url,
                    stats_url=stats_url or "",
                )
        except Exception as e:
            logger.debug(f"Failed to parse team item: {e}")
        return None
    
    def _parse_roster_row(self, row, team: MLSTeam) -> Optional[MLSPlayer]:
        """Parse a roster table row into a player record."""
        try:
            # Find player link
            player_link = row.select_one("a.mls-o-table__href, a[href*='/players/']")
            if not player_link:
                return None
            
            player_url = self._make_absolute_url(player_link.get("href", ""))
            
            # Get player name
            name_elem = row.select_one(".mls-o-table__player-name, .short-name")
            player_name = name_elem.get_text(strip=True) if name_elem else ""
            
            if not player_name:
                # Try getting from link text
                player_name = player_link.get_text(strip=True)
            
            # Parse table cells for other data
            cells = row.find_all(["td", "div"])
            jersey_number = None
            position = None
            roster_category = None
            player_category = None
            player_status = None
            
            # Column order: Player, Jersey #, Position, Roster Category, Player Category, Player Status
            cell_texts = [c.get_text(strip=True) for c in cells]
            
            # Find jersey number (usually a number)
            for i, text in enumerate(cell_texts):
                if text.isdigit() and len(text) <= 3:
                    jersey_number = text
                    break
            
            # Find position (short code like GK, D, M, F, etc.)
            position_patterns = ["GK", "D", "M", "F", "DF", "MF", "FW"]
            for text in cell_texts:
                if text.upper() in position_patterns or re.match(r"^[A-Z]{1,3}$", text):
                    position = text
                    break
            
            # Try to get categories from specific cells
            if len(cell_texts) >= 6:
                roster_category = cell_texts[3] if cell_texts[3] else None
                player_category = cell_texts[4] if cell_texts[4] else None
                player_status = cell_texts[5] if cell_texts[5] else None
            
            return MLSPlayer(
                team_name=team.name,
                team_slug=team.slug,
                player_name=player_name,
                player_url=player_url,
                jersey_number=jersey_number,
                position=position,
                roster_category=roster_category,
                player_category=player_category,
                player_status=player_status,
            )
        except Exception as e:
            logger.debug(f"Failed to parse roster row: {e}")
        return None

    def _fetch_player_profile(self, player: MLSPlayer) -> None:
        """Fetch and parse player profile page for additional details."""
        if not player.player_url:
            return
        
        try:
            logger.debug(f"Fetching profile: {player.player_url}")
            html = self.navigate(player.player_url)
            soup = BeautifulSoup(html, "html.parser")
            
            details = {}
            
            # Parse header info (next to player image)
            header = soup.select_one(".mls-c-player-header, .player-header")
            if header:
                details.update(self._parse_player_header(header))
            
            # Parse player details section
            details_section = soup.select_one(".mls-c-player-details, .player-details, [class*='player-bio']")
            if details_section:
                details.update(self._parse_player_details(details_section))
            
            # Parse any stats shown on profile
            stats_section = soup.select_one(".mls-c-player-stats, .player-stats")
            if stats_section:
                details.update(self._parse_player_stats(stats_section))
            
            # Get full name if available
            full_name = soup.select_one("h1, .player-name, .mls-c-player-header__name")
            if full_name:
                details["full_name"] = full_name.get_text(strip=True)
            
            player.profile_details = details
            
        except Exception as e:
            logger.warning(f"Failed to fetch profile for {player.player_name}: {e}")
    
    def _parse_player_header(self, header) -> Dict[str, str]:
        """Parse player header section (info next to image)."""
        details = {}
        
        # Look for labeled items
        items = header.select(".mls-c-player-header__item, [class*='info-item'], dt, dd")
        
        current_label = None
        for item in items:
            text = item.get_text(strip=True)
            if item.name == "dt" or "label" in item.get("class", []):
                current_label = self._normalize_key(text)
            elif current_label:
                details[current_label] = text
                current_label = None
            elif ":" in text:
                parts = text.split(":", 1)
                key = self._normalize_key(parts[0])
                details[key] = parts[1].strip()
        
        # Look for club logo/name
        club_elem = header.select_one(".club-name, [class*='club'], img[alt*='logo']")
        if club_elem:
            if club_elem.name == "img":
                details["club_from_profile"] = club_elem.get("alt", "").replace("logo", "").strip()
            else:
                details["club_from_profile"] = club_elem.get_text(strip=True)
        
        return details
    
    def _parse_player_details(self, section) -> Dict[str, str]:
        """Parse player details section."""
        details = {}
        
        # Try definition list format
        dts = section.find_all("dt")
        dds = section.find_all("dd")
        for dt, dd in zip(dts, dds):
            key = self._normalize_key(dt.get_text(strip=True))
            details[key] = dd.get_text(strip=True)
        
        # Try labeled div format
        items = section.select("[class*='detail'], [class*='info']")
        for item in items:
            label = item.select_one("[class*='label'], span:first-child")
            value = item.select_one("[class*='value'], span:last-child")
            if label and value:
                key = self._normalize_key(label.get_text(strip=True))
                details[key] = value.get_text(strip=True)
        
        # Try text with colon format
        for elem in section.find_all(["p", "div", "span"]):
            text = elem.get_text(strip=True)
            if ":" in text and len(text) < 100:
                parts = text.split(":", 1)
                key = self._normalize_key(parts[0])
                if key and len(key) < 30:
                    details[key] = parts[1].strip()
        
        return details
    
    def _parse_player_stats(self, section) -> Dict[str, str]:
        """Parse player stats section."""
        stats = {}
        
        # Look for stat items
        items = section.select("[class*='stat'], [class*='item']")
        for item in items:
            label = item.select_one("[class*='label'], [class*='name']")
            value = item.select_one("[class*='value'], [class*='number']")
            if label and value:
                key = "stat_" + self._normalize_key(label.get_text(strip=True))
                stats[key] = value.get_text(strip=True)
        
        return stats
    
    def _normalize_key(self, text: str) -> str:
        """Normalize text to a valid dict key."""
        if not text:
            return ""
        # Remove special chars, lowercase, replace spaces with underscore
        key = re.sub(r"[^\w\s]", "", text.lower())
        key = re.sub(r"\s+", "_", key.strip())
        return key
    
    def _make_absolute_url(self, url: str) -> str:
        """Convert relative URL to absolute."""
        if not url:
            return ""
        if url.startswith("http"):
            return url
        if url.startswith("/"):
            return f"{self.BASE_URL}{url}"
        return f"{self.BASE_URL}/{url}"
