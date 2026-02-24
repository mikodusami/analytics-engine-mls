"""Script to inspect MLS page HTML structure."""
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

def inspect_pages():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        # 1. Inspect players page for team links
        print("=" * 80)
        print("INSPECTING: https://www.mlssoccer.com/players/")
        print("=" * 80)
        page.goto("https://www.mlssoccer.com/players/", wait_until="domcontentloaded")
        page.wait_for_load_state("load")
        page.wait_for_timeout(3000)
        
        html = page.content()
        soup = BeautifulSoup(html, "html.parser")
        
        # Find roster links
        roster_links = soup.find_all("a", href=lambda x: x and "/roster" in x)
        print(f"\nFound {len(roster_links)} roster links")
        for link in roster_links[:3]:
            print(f"  {link}")
        
        # 2. Inspect a roster page
        print("\n" + "=" * 80)
        print("INSPECTING: https://www.mlssoccer.com/clubs/inter-miami-cf/roster/")
        print("=" * 80)
        page.goto("https://www.mlssoccer.com/clubs/inter-miami-cf/roster/", wait_until="domcontentloaded")
        page.wait_for_load_state("load")
        page.wait_for_timeout(3000)
        
        html = page.content()
        soup = BeautifulSoup(html, "html.parser")
        
        # Find table structure
        tables = soup.find_all("table")
        print(f"\nFound {len(tables)} tables")
        
        for i, table in enumerate(tables):
            print(f"\n--- Table {i} ---")
            # Print header
            headers = table.find_all("th")
            print(f"Headers: {[h.get_text(strip=True) for h in headers]}")
            
            # Print first few rows
            rows = table.find_all("tr")
            print(f"Total rows: {len(rows)}")
            for row in rows[1:3]:  # First 2 data rows
                cells = row.find_all(["td", "th"])
                print(f"Row: {[c.get_text(strip=True)[:30] for c in cells]}")
                # Print player link if exists
                player_link = row.find("a", href=lambda x: x and "/players/" in x)
                if player_link:
                    print(f"  Player link: {player_link.get('href')}")
                    print(f"  Player link HTML: {player_link}")
        
        # 3. Inspect a player profile page
        print("\n" + "=" * 80)
        print("INSPECTING: https://www.mlssoccer.com/players/lionel-messi/")
        print("=" * 80)
        page.goto("https://www.mlssoccer.com/players/lionel-messi/", wait_until="domcontentloaded")
        page.wait_for_load_state("load")
        page.wait_for_timeout(3000)
        
        html = page.content()
        soup = BeautifulSoup(html, "html.parser")
        
        # Print main content structure
        print("\n--- Player Header/Bio Section ---")
        # Look for player info sections
        for selector in [".player", "[class*='player']", "[class*='bio']", "[class*='header']", "[class*='detail']"]:
            elements = soup.select(selector)
            if elements:
                print(f"\nSelector '{selector}' found {len(elements)} elements")
                for elem in elements[:2]:
                    classes = elem.get("class", [])
                    text = elem.get_text(strip=True)[:200]
                    print(f"  Class: {classes}")
                    print(f"  Text: {text}")
        
        # Look for definition lists (common for player details)
        dls = soup.find_all("dl")
        print(f"\n--- Definition Lists ({len(dls)}) ---")
        for dl in dls:
            dts = dl.find_all("dt")
            dds = dl.find_all("dd")
            for dt, dd in zip(dts, dds):
                print(f"  {dt.get_text(strip=True)}: {dd.get_text(strip=True)}")
        
        # Look for labeled spans/divs
        print("\n--- Looking for label:value patterns ---")
        for elem in soup.find_all(["div", "span", "p"]):
            text = elem.get_text(strip=True)
            if ":" in text and len(text) < 50 and len(text) > 3:
                print(f"  {text}")
        
        # Print masthead section (player header with jersey, position, club)
        print("\n--- Masthead Section ---")
        masthead = soup.select_one(".mls-o-masthead")
        if masthead:
            print(str(masthead)[:2000])
        
        # Print player details section
        print("\n--- Player Details Section ---")
        details_section = soup.select_one(".mls-l-module--player-status-details")
        if details_section:
            print(str(details_section)[:2000])
        
        browser.close()

if __name__ == "__main__":
    inspect_pages()
