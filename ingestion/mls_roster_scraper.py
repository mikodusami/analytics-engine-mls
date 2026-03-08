"""
MLS Roster Scraper - Extracts team rosters and player details from mlssoccer.com

This scraper is a beautiful disaster that somehow works. It crawls through
mlssoccer.com like a drunk spider, grabbing every piece of player data it can find.

The site is a JavaScript nightmare, so we use Playwright to render the pages
like a real browser. Because f**k your simple HTTP requests.

Author: Someone who spent way too many hours debugging CSS selectors
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
    
    This class inherits from PlaywrightScraper which handles all the browser
    bulls**t. We just focus on parsing the HTML and extracting data.
    
    Flow (the grand plan that somehow works):
    1. Visit /players/ to get all teams (they list every team's roster link)
    2. For each team, visit their roster page (/clubs/<team>/roster/)
    3. For each player on roster, visit their profile page for extra details
    4. Collect all player details as raw dicts for the transformer to clean up
    
    Usage:
        with MLSRosterScraper() as scraper:
            scraper.discover_teams()
            for team in scraper.teams:
                scraper.scrape_team_roster(team)
            players = scraper.players  # All the good sh*t is here
    """
    
    # Base URLs - if MLS changes these, we're f**ked
    BASE_URL = "https://www.mlssoccer.com"
    PLAYERS_URL = f"{BASE_URL}/players/"
    
    def __init__(self, headless: bool = True, timeout: int = 60000):
        # Call parent constructor to set up Playwright browser
        super().__init__(headless=headless, timeout=timeout)
        self._teams: List[Dict[str, str]] = []  # List of team dicts
        self._players: List[Dict[str, Any]] = []  # List of player dicts (the gold)
    
    @property
    def teams(self) -> List[Dict[str, str]]:
        """Get discovered teams. Call discover_teams() first or this is empty."""
        return self._teams
    
    @property
    def players(self) -> List[Dict[str, Any]]:
        """Get scraped players. Call scrape_team_roster() first or this is empty."""
        return self._players
    
    def scrape(self) -> List[Dict[str, Any]]:
        """
        Run full scrape: teams -> rosters -> player profiles.
        
        This is the "do everything" method. It discovers teams, scrapes each
        team's roster, and fetches player profiles. Takes forever but gets
        all the data in one shot.
        
        Returns list of raw player dicts (not transformed yet).
        """
        self.discover_teams()
        for team in self._teams:
            self.scrape_team_roster(team)
        logger.info(f"Total players scraped: {len(self._players)}")
        return self._players
    
    def discover_teams(self) -> List[Dict[str, str]]:
        """
        Discover all teams from the players page.
        
        The /players/ page has links to every team's roster. We parse those
        links to build our list of teams to scrape.
        
        Returns list of team dicts with: name, slug, roster_url, stats_url
        """
        logger.info(f"Discovering teams from {self.PLAYERS_URL}")
        html = self.navigate(self.PLAYERS_URL)
        soup = BeautifulSoup(html, "html.parser")
        
        # Find all roster links like /clubs/inter-miami-cf/roster/
        # These are scattered throughout the page in various nav elements
        all_links = soup.find_all("a", href=lambda x: x and "/roster" in x)
        
        # Use a set to track seen slugs (avoid duplicates)
        seen_slugs = set()
        for link in all_links:
            href = link.get("href", "")
            # Extract team slug from URL using regex
            # e.g., /clubs/inter-miami-cf/roster/ -> inter-miami-cf
            match = re.search(r"/clubs/([^/]+)/roster", href)
            if match:
                team_slug = match.group(1)
                if team_slug not in seen_slugs:
                    seen_slugs.add(team_slug)
                    # Build team dict with all the URLs we'll need
                    self._teams.append({
                        "name": team_slug.replace("-", " ").title(),  # inter-miami-cf -> Inter Miami Cf
                        "slug": team_slug,
                        "roster_url": f"{self.BASE_URL}/clubs/{team_slug}/roster/",
                        "stats_url": f"{self.BASE_URL}/clubs/{team_slug}/stats/",
                    })
        
        logger.info(f"Discovered {len(self._teams)} teams")
        return self._teams

    def scrape_team_roster(self, team: Dict[str, str]) -> List[Dict[str, Any]]:
        """
        Scrape roster for a single team.
        
        The roster page has multiple tables (different roster categories like
        Senior Roster, Supplemental Roster, etc.). We parse all of them.
        
        For each player, we also visit their profile page to get extra details
        like height, weight, DOB, etc. This is slow but worth it.
        
        Args:
            team: Dict with team info (name, slug, roster_url)
            
        Returns:
            List of player dicts for this team
        """
        logger.info(f"Scraping roster for {team['name']}: {team['roster_url']}")
        html = self.navigate(team["roster_url"])
        soup = BeautifulSoup(html, "html.parser")
        
        # Find all tables - roster page has multiple tables for different categories
        tables = soup.find_all("table")
        if not tables:
            logger.warning(f"No roster tables found for {team['name']}")
            return []
        
        team_players = []
        seen_urls = set()  # Track seen player URLs to avoid duplicates
        
        for table in tables:
            # Get headers to understand column order
            # Headers are usually: Player, Jersey #, Position, Roster Category, etc.
            headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
            if not headers or "player" not in headers:
                continue  # Skip tables without player column (probably not a roster table)
            
            # Parse each row (skip header row with [1:])
            rows = table.find_all("tr")[1:]
            for row in rows:
                cells = row.find_all("td")
                if not cells:
                    continue
                
                # Parse the row into a player dict
                player_data = self._parse_roster_row(row, cells, headers, team)
                if player_data:
                    # Skip duplicates - same player can appear in multiple tables
                    player_url = player_data.get("player_url", "")
                    if player_url in seen_urls:
                        continue
                    seen_urls.add(player_url)
                    
                    # Fetch player profile details (the slow part)
                    self._fetch_player_profile(player_data)
                    team_players.append(player_data)
                    self._players.append(player_data)
        
        logger.info(f"  {team['name']}: {len(team_players)} players")
        return team_players
    
    def _parse_roster_row(self, row, cells, headers: List[str], team: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """
        Parse a roster table row into a player dict.
        
        This is where we extract data from a single table row. The HTML structure
        is a mess of nested elements, so we have to be careful.
        
        Expected headers: ['player', 'jersey #', 'position', 'roster category', 
                          'player category', 'player status']
        
        Args:
            row: The <tr> element
            cells: List of <td> elements in the row
            headers: List of column header names (lowercase)
            team: Team dict for adding team info to player
            
        Returns:
            Player dict or None if parsing failed
        """
        try:
            # Find player link - this contains the player URL and name
            # MLS uses different class names, so we try multiple selectors
            player_link = row.select_one("a.mls-o-table__href, a[href*='/players/']")
            if not player_link:
                return None
            
            # Build full URL (some hrefs are relative, some absolute)
            href = player_link.get("href", "")
            player_url = href if href.startswith("http") else f"{self.BASE_URL}{href}"
            
            # Get player name from the link
            # MLS wraps the name in a span with class "short-name" or "mls-o-table__player-name"
            name_elem = player_link.select_one(".short-name, .mls-o-table__player-name")
            player_name = name_elem.get_text(strip=True) if name_elem else player_link.get_text(strip=True)
            
            # Get player thumbnail image from roster table
            player_thumb = None
            img = player_link.select_one("img")
            if img:
                player_thumb = img.get("src")
            
            # Get text from all cells for mapping to headers
            cell_texts = [c.get_text(strip=True) for c in cells]
            
            # Build base player dict with team info
            player_data = {
                "team_name": team["name"],
                "team_slug": team["slug"],
                "player_name": player_name,
                "player_url": player_url,
                "player_image_thumb": player_thumb,
            }
            
            # Map header names to our field names
            header_map = {
                "jersey #": "jersey_number",
                "position": "position",
                "roster category": "roster_category",
                "player category": "player_category",
                "player status": "player_status",
            }
            
            # Map each header to its value based on column position
            for i, header in enumerate(headers):
                if header in header_map and i < len(cell_texts):
                    player_data[header_map[header]] = cell_texts[i] or None
            
            return player_data
            
        except Exception as e:
            logger.debug(f"Failed to parse roster row: {e}")
            return None

    def _fetch_player_profile(self, player_data: Dict[str, Any]) -> None:
        """
        Fetch and parse player profile page for additional details.
        
        The profile page has way more info than the roster table:
        - Full name, height, weight, DOB, birthplace
        - Large player image, team logo
        - Pronunciation, footedness, etc.
        
        This modifies player_data in place (adds new fields).
        
        Args:
            player_data: Player dict to add profile data to
        """
        player_url = player_data.get("player_url")
        if not player_url:
            return
        
        try:
            logger.debug(f"Fetching profile: {player_url}")
            html = self.navigate(player_url)
            soup = BeautifulSoup(html, "html.parser")
            
            # Parse masthead (header section with jersey #, position, club logo)
            masthead = soup.select_one(".mls-o-masthead")
            if masthead:
                self._parse_masthead(masthead, player_data)
            
            # Parse player details section (the info grid with height, weight, etc.)
            details_section = soup.select_one(".mls-l-module--player-status-details")
            if details_section:
                self._parse_player_details(details_section, player_data)
            
        except Exception as e:
            logger.warning(f"Failed to fetch profile for {player_data.get('player_name')}: {e}")
    
    def _parse_masthead(self, masthead, player_data: Dict[str, Any]) -> None:
        """
        Parse masthead section of player profile.
        
        The masthead is the big header at the top of the profile page.
        Contains: Player image (large), jersey #, position, club name/logo
        
        Args:
            masthead: BeautifulSoup element for the masthead
            player_data: Dict to add parsed data to
        """
        try:
            # Get player image (large version from profile, not the thumbnail)
            player_img = masthead.select_one(".mls-o-masthead__branded-image img")
            if player_img:
                player_data["player_image"] = player_img.get("src")
                # The alt text often has the full name
                if not player_data.get("full_name"):
                    player_data["full_name"] = player_img.get("alt", "").strip()
            
            # Get club logo image
            club_logo = masthead.select_one(".mls-o-masthead__club-logo img")
            if club_logo:
                player_data["team_logo"] = club_logo.get("src")
            
            # Get club link/slug from the logo link
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
                # Extract jersey number (e.g., "#10" -> "10")
                jersey_match = re.search(r"#(\d+)", text)
                if jersey_match:
                    player_data["jersey_number_profile"] = jersey_match.group(1)
                
        except Exception as e:
            logger.debug(f"Failed to parse masthead: {e}")
    
    def _parse_player_details(self, section, player_data: Dict[str, Any]) -> None:
        """
        Parse player details section from profile page.
        
        This section has a grid of info items like:
        - Name, Height, Weight, DOB, Birthplace
        - Footedness, Pronunciation, etc.
        
        HTML structure:
        <div class="mls-l-module--player-status-details__info">
            <h3>Name</h3>
            <span>Lionel Messi</span>
        </div>
        
        We extract each label/value pair and add to player_data with "profile_" prefix.
        
        Args:
            section: BeautifulSoup element for the details section
            player_data: Dict to add parsed data to
        """
        try:
            # Find all info items (each has a label h3 and value span)
            info_items = section.select(".mls-l-module--player-status-details__info")
            
            for item in info_items:
                label_elem = item.select_one("h3")
                value_elem = item.select_one("span")
                
                if label_elem and value_elem:
                    label = label_elem.get_text(strip=True)
                    value = value_elem.get_text(" ", strip=True)
                    
                    # Normalize key (e.g., "Date of Birth" -> "date_of_birth")
                    key = self._normalize_key(label)
                    if key and value:
                        # Prefix with "profile_" so we know where it came from
                        player_data[f"profile_{key}"] = value
                        
        except Exception as e:
            logger.debug(f"Failed to parse player details: {e}")
    
    def _normalize_key(self, text: str) -> str:
        """
        Normalize text to a valid dict key.
        
        "Date of Birth" -> "date_of_birth"
        "Height (ft)" -> "height_ft"
        
        Args:
            text: Raw label text
            
        Returns:
            Normalized key string
        """
        if not text:
            return ""
        # Remove special characters, convert to lowercase
        key = re.sub(r"[^\w\s]", "", text.lower())
        # Replace spaces with underscores
        key = re.sub(r"\s+", "_", key.strip())
        return key
