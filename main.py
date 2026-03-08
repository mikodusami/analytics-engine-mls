"""
MLS Analytics Engine - Main Entry Point

This is the main file that runs everything.

Author: me
Date: Whenever I wrote this listed on github
"""
import logging
import argparse
import sys

from ingestion.salary_scraper import SalaryScraper
from transform.salary_transformer import SalaryTransformer
from load.csv_writer import CSVWriter
from storage.database import SalaryDatabase
from storage.parquet import ParquetStorage


def setup_logging(level: int = logging.INFO) -> None:
    """
    set up logging because print() statements are for amateurs.
    
    If you're adding print() statements to debug, you're doing it wrong.
    Use the d*mn logger like a civilized human being.
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args():
    """
    Parse command line arguments.
    
    This function is a beautiful mess of argparse bs that somehow works.
    Don't touch it unless you absolutely have to dude. I mean it.
    
    If you add a new command and forget to add it to main(), you're finished.
    """
    parser = argparse.ArgumentParser(description="MLS Analytics Engine")
    parser.add_argument("--debug", action="store_true", help="enable debug logging")
    subparsers = parser.add_subparsers(dest="command", help="available commands")
    
    # Discover command - find out what salary data is available
    # This is actually useful
    subparsers.add_parser("discover", help="discover available salary sources")

    # =========================================================================
    # ETL command - the bread and butter of this whole operation
    # If this breaks, everything breaks. No pressure.
    # =========================================================================
    run_parser = subparsers.add_parser("salary", help="run full ETL pipeline")
    run_parser.add_argument("--year", type=int, help="specific year to process")
    run_parser.add_argument("--split", action="store_true", help="split CSV by year")
    run_parser.add_argument("--output", "-o", default="output", help="output directory")
    run_parser.add_argument("--format", choices=["csv", "parquet", "sqlite", "all"], 
                           default="all", help="output format")
    
    # =========================================================================
    # Analytics commands - for when you want to pretend you're a data scientist
    # =========================================================================
    subparsers.add_parser("quality", help="run data quality report")
    
    analyze_parser = subparsers.add_parser("analyze", help="run analytics")
    analyze_parser.add_argument("--type", choices=["trends", "teams", "top", "distribution"],
                               default="trends", help="analysis type")
    analyze_parser.add_argument("--year", type=int, help="filter by year")
    analyze_parser.add_argument("--club", type=str, help="filter by club")
    analyze_parser.add_argument("--top", type=int, default=10, help="number of top results")
    
    # =========================================================================
    # MLS Roster scraping command
    # Scrapes player rosters from mlssoccer.com like a sneaky little b*stard
    # Uses Playwright because requests + BeautifulSoup wasn't painful enough
    # =========================================================================
    roster_parser = subparsers.add_parser("roster", help="scrape MLS team rosters and player profiles")
    roster_parser.add_argument("--output", "-o", default="output", help="output directory")
    roster_parser.add_argument("--format", choices=["csv", "parquet", "all"], 
                               default="all", help="output format")
    roster_parser.add_argument("--team", type=str, help="specific team slug to scrape (e.g., inter-miami-cf)")
    roster_parser.add_argument("--headless", action="store_true", default=True, 
                               help="run browser in headless mode")
    roster_parser.add_argument("--no-headless", dest="headless", action="store_false",
                               help="show browser window")
    
    # =========================================================================
    # MLS Stats scraping command
    # This one's a real piece of work. Scrapes stats for every player,
    # every team, every season, every stat type. It takes forever.
    # Go make some coffee while it runs. Or take a nap. Or both.
    # =========================================================================
    stats_parser = subparsers.add_parser("stats", help="scrape MLS team player stats by season")
    stats_parser.add_argument("--output", "-o", default="output", help="output directory")
    stats_parser.add_argument("--format", choices=["csv", "parquet", "all"], 
                               default="all", help="output format")
    stats_parser.add_argument("--team", type=str, help="specific team slug to scrape (e.g., atlanta-united)")
    stats_parser.add_argument("--season", type=int, action="append", dest="seasons",
                               help="specific season(s) to scrape (can be repeated)")
    stats_parser.add_argument("--no-profiles", dest="fetch_profiles", action="store_false", default=True,
                               help="skip fetching player profile details (faster)")
    stats_parser.add_argument("--headless", action="store_true", default=True, 
                               help="run browser in headless mode")
    stats_parser.add_argument("--no-headless", dest="headless", action="store_false",
                               help="show browser window")
    
    return parser.parse_args()


def cmd_salary(args, logger) -> int:
    """
    Run full ETL pipeline: Extract -> Transform -> Load.
    
    This is where the magic happens. Or the disaster. Depends on the day.
    
    The flow is:
    1. Discover what salary data sources exist (PDFs, CSVs, etc.)
    2. Download and parse each one (pray they haven't changed the format)
    3. Transform the data into something usable (good f**king luck)
    4. Write it out to CSV/Parquet/SQLite (the easy part, finally)
    """
    scraper = SalaryScraper()
    
    # Discover sources - find out what years of salary data are available
    # This scrapes the MLSPA website
    logger.info("Discovering salary sources...")
    sources = scraper.discover_sources()
    logger.info(f"Found {len(sources)} years")
    
    # Determine which years to process
    # If they specified a year, use that. Otherwise, do them all.
    if args.year:
        years_to_process = [args.year] if args.year in sources else []
        if not years_to_process:
            logger.error(f"Year {args.year} not found in sources")
            return 1
    else:
        years_to_process = sorted(sources.keys(), reverse=True)
    
    all_records = []

    # Process each year: Extract -> Transform
    # This is where we pray to the data gods that nothing breaks
    for year in years_to_process:
        source = sources[year]
        logger.info(f"Processing {year} ({source.format})...")
        
        # Scrape the data - this might fail spectacularly
        rows = scraper.scrape_year(year)
        if not rows:
            logger.warning(f"No data for {year}, skipping")
            continue
        
        # Transform the data - turn raw garbage into structured garbage
        transformer = SalaryTransformer(year=year, source_format=source.format)
        records = transformer.transform(rows)
        
        if records:
            all_records.extend(records)
            logger.info(f"  {year}: {len(records)} records")
        else:
            logger.warning(f"  {year}: no records after transform")
    
    # If we got nothing, something went horribly wrong
    if not all_records:
        logger.error("No records to write")
        return 1
    
    # =========================================================================
    # LOAD PHASE - Write this sh*t out to files
    # The only part of ETL that actually works reliably
    # =========================================================================
    logger.info(f"Writing {len(all_records)} total records...")
    
    if args.format in ("csv", "all"):
        csv_writer = CSVWriter(output_dir=args.output)
        if args.split:
            csv_writer.write_by_year(all_records)
        else:
            csv_writer.write_all(all_records, "salaries.csv")
    
    if args.format in ("parquet", "all"):
        parquet = ParquetStorage(output_dir=args.output)
        parquet.save(all_records)
    
    if args.format in ("sqlite", "all"):
        db = SalaryDatabase(db_path=f"{args.output}/salaries.db")
        db.insert_records(all_records, clear_existing=True)
    
    logger.info("Done!")
    return 0


def cmd_discover(args, logger) -> int:
    """
    Discover available salary sources.
    
    Just lists what's available. That's it. Nothing fancy.
    If you expected more, lower your expectations.
    """
    scraper = SalaryScraper()
    sources = scraper.discover_sources()
    
    logger.info(f"Found {len(sources)} salary sources:")
    for year in sorted(sources.keys(), reverse=True):
        src = sources[year]
        logger.info(f"  {year}: {src.format.upper()} - {src.url[:60]}...")
    
    return 0


def cmd_quality(args, logger) -> int:
    """
    Run data quality report.
    
    Tells you how f**ked your data is. Spoiler: it's probably pretty f**ked.
    """
    import pandas as pd
    from analytics.data_quality import DataQualityChecker
    from pathlib import Path
    
    # Try to load existing data - check parquet first because it's faster
    parquet_path = Path("output/salaries.parquet")
    csv_path = Path("output/salaries.csv")
    
    if parquet_path.exists():
        df = pd.read_parquet(parquet_path)
    elif csv_path.exists():
        df = pd.read_csv(csv_path)
    else:
        logger.error("No data found. Run 'python main.py run' first.")
        return 1
    
    checker = DataQualityChecker(df)
    checker.print_report()
    return 0


def cmd_analyze(args, logger) -> int:
    """
    Run analytics.
    
    Makes pretty charts and numbers so you can pretend you know what you're doing.
    """
    from analytics.salary_analytics import SalaryAnalytics
    from pathlib import Path
    
    parquet_path = Path("output/salaries.parquet")
    csv_path = Path("output/salaries.csv")
    
    if parquet_path.exists():
        analytics = SalaryAnalytics(str(parquet_path))
    elif csv_path.exists():
        analytics = SalaryAnalytics(str(csv_path))
    else:
        logger.error("No data found. Run 'python main.py run' first.")
        return 1
    
    # Different analysis types - pick your poison
    if args.type == "trends":
        print("\n📈 SALARY TRENDS BY YEAR")
        print("="*60)
        trends = analytics.salary_trends_by_year()
        print(trends.to_string())
        
        print("\n📊 YEAR-OVER-YEAR GROWTH")
        growth = analytics.salary_growth_rate()
        print(growth.to_string())
    
    elif args.type == "teams":
        year = args.year
        print(f"\n🏟️  TEAM SPENDING {f'({year})' if year else '(ALL TIME)'}")
        print("="*60)
        spending = analytics.team_spending(year)
        print(spending.to_string())
    
    elif args.type == "top":
        year = args.year
        n = args.top
        print(f"\n💰 TOP {n} EARNERS {f'({year})' if year else '(ALL TIME)'}")
        print("="*60)
        top = analytics.top_earners(year, n)
        print(top.to_string(index=False))
    
    elif args.type == "distribution":
        year = args.year
        print(f"\n📊 SALARY DISTRIBUTION {f'({year})' if year else '(ALL TIME)'}")
        print("="*60)
        dist = analytics.salary_distribution(year)
        for k, v in dist.items():
            print(f"  {k}: ${v:,.2f}" if isinstance(v, float) else f"  {k}: {v}")
        
        print("\n📍 PERCENTILES")
        pct = analytics.salary_percentiles(year)
        for p, v in pct.items():
            print(f"  {int(p*100)}th: ${v:,.2f}")
    
    return 0


def cmd_roster(args, logger) -> int:
    """
    Scrape MLS team rosters and player profiles (ETL pipeline).
    
    This bad boy uses Playwright to scrape mlssoccer.com like a boss.
    It grabs every player from every team, then visits each player's
    profile page to get all their juicy details.
    
    Warning: This takes a while. Like, a long while. Go touch grass.
    """
    from ingestion.mls_roster_scraper import MLSRosterScraper
    from transform.mls_roster_transformer import MLSRosterTransformer
    from load.mls_writer import MLSWriter
    
    logger.info("Starting MLS roster ETL pipeline...")
    
    # =========================================================================
    # EXTRACT PHASE - Scrape the living sh*t out of mlssoccer.com
    # =========================================================================
    with MLSRosterScraper(headless=args.headless) as scraper:
        # Discover teams - find all the teams on the site
        teams = scraper.discover_teams()
        logger.info(f"Found {len(teams)} teams")
        
        # Filter to specific team if requested
        # Because sometimes you don't want to wait 3 hours
        if args.team:
            teams = [t for t in teams if t["slug"] == args.team]
            if not teams:
                logger.error(f"Team '{args.team}' not found")
                return 1
        
        # Scrape each team's roster - this is where the fun begins
        for team in teams:
            scraper.scrape_team_roster(team)
        
        raw_players = scraper.players
        raw_teams = scraper.teams
    
    if not raw_players:
        logger.error("No players scraped")
        return 1
    
    logger.info(f"Extracted {len(raw_players)} raw player records")
    
    # =========================================================================
    # TRANSFORM PHASE - Clean up the mess we just made
    # =========================================================================
    transformer = MLSRosterTransformer()
    players = transformer.transform(raw_players)
    
    if not players:
        logger.error("No players after transformation")
        return 1
    
    logger.info(f"Transformed {len(players)} player records")
    
    # =========================================================================
    # LOAD PHASE - Write it all out and pray it worked
    # =========================================================================
    writer = MLSWriter(output_dir=args.output)
    
    if args.format in ("csv", "all"):
        writer.write_players(players, "mls_rosters.csv")
        writer.write_teams_raw(raw_teams, "mls_teams.csv")
    
    if args.format in ("parquet", "all"):
        writer.write_players_parquet(players, "mls_rosters.parquet")
    
    logger.info(f"Done! ETL complete: {len(players)} players from {len(raw_teams)} teams")
    return 0


def cmd_stats(args, logger) -> int:
    """
    Scrape MLS team player stats by season (ETL pipeline).
    
    Oh boy, this one's a doozy. It scrapes stats for every player,
    every team, every season, and all 5 stat types (general, passing,
    attacking, defending, goalkeeping).
    
    The math: 30 teams × 30 seasons × 5 stat types × ~30 players = a lot
    
    Seriously, go do something else while this runs. Learn a new language.
    Write a novel. Question your life choices. Whatever.
    """
    from ingestion.mls_stats_scraper import MLSStatsScraper
    from transform.mls_stats_transformer import MLSStatsTransformer
    from load.mls_stats_writer import MLSStatsWriter
    
    logger.info("Starting MLS stats ETL pipeline...")
    
    # =========================================================================
    # EXTRACT PHASE - The long and painful part
    # =========================================================================
    with MLSStatsScraper(headless=args.headless, fetch_profiles=args.fetch_profiles) as scraper:
        # Discover teams
        teams = scraper.discover_teams()
        logger.info(f"Found {len(teams)} teams")
        
        # Filter to specific team if requested
        # Highly recommended unless you have all day
        if args.team:
            teams = [t for t in teams if t["slug"] == args.team]
            if not teams:
                logger.error(f"Team '{args.team}' not found")
                return 1
        
        # Scrape each team's stats
        # This is where we spend 99% of our time
        for team in teams:
            scraper.scrape_team_stats(team, seasons=args.seasons)
        
        raw_stats = scraper.stats
        raw_teams = scraper.teams
    
    if not raw_stats:
        logger.error("No stats scraped")
        return 1
    
    logger.info(f"Extracted {len(raw_stats)} raw stats records")
    
    # =========================================================================
    # TRANSFORM PHASE - Make sense of the chaos
    # =========================================================================
    transformer = MLSStatsTransformer()
    stats = transformer.transform(raw_stats)
    
    if not stats:
        logger.error("No stats after transformation")
        return 1
    
    logger.info(f"Transformed {len(stats)} stats records")
    
    # =========================================================================
    # LOAD PHASE - Finally, the easy part
    # =========================================================================
    writer = MLSStatsWriter(output_dir=args.output)
    
    if args.format in ("csv", "all"):
        writer.write_stats(stats, "mls_player_stats.csv")
    
    if args.format in ("parquet", "all"):
        writer.write_stats_parquet(stats, "mls_player_stats.parquet")
    
    logger.info(f"Done! ETL complete: {len(stats)} stats records from {len(raw_teams)} teams")
    return 0


def main() -> int:
    """
    Main entry point. The alpha and omega. The beginning and the end.
    
    This function is called when you run the script. It parses args,
    sets up logging, and dispatches to the appropriate command handler.
    
    If you're debugging and ended up here, you've gone too far.
    The bug is probably in one of the cmd_* functions above.
    """
    args = parse_args()
    setup_logging(level=logging.DEBUG if args.debug else logging.INFO)
    logger = logging.getLogger(__name__)

    # Let 'em know we're alive
    logger.info("MLS Analytics Engine")

    # Dispatch to the appropriate command handler
    # If you add a new command, FOR THE LOVE OF GOD add it here too
    if args.command == "salary":
        return cmd_salary(args, logger)
    elif args.command == "discover":
        return cmd_discover(args, logger)
    elif args.command == "quality":
        return cmd_quality(args, logger)
    elif args.command == "analyze":
        return cmd_analyze(args, logger)
    elif args.command == "roster":
        return cmd_roster(args, logger)
    elif args.command == "stats":
        return cmd_stats(args, logger)
    else:
        # No command specified - be helpful for once
        logger.info("Use --help for available commands")
    
    return 0


# ============================================================================
# This is where the magic happens when you run: python main.py
# If this confuses you, maybe Python isn't your thing. No judgment.
# ============================================================================
if __name__ == "__main__":
    sys.exit(main())
