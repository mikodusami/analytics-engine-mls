import logging
import argparse
import sys
from ingestion.salary_scraper import SalaryScraper
from transform.salary_transformer import SalaryTransformer
from load.csv_writer import CSVWriter
from storage.database import SalaryDatabase
from storage.parquet import ParquetStorage


def setup_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args():
    parser = argparse.ArgumentParser(description="MLS Analytics Engine")
    parser.add_argument("--debug", action="store_true", help="enable debug logging")
    subparsers = parser.add_subparsers(dest="command", help="available commands")
    
    subparsers.add_parser("discover", help="discover available salary sources")

    # ETL command
    run_parser = subparsers.add_parser("run", help="run full ETL pipeline")
    run_parser.add_argument("--year", type=int, help="specific year to process")
    run_parser.add_argument("--split", action="store_true", help="split CSV by year")
    run_parser.add_argument("--output", "-o", default="output", help="output directory")
    run_parser.add_argument("--format", choices=["csv", "parquet", "sqlite", "all"], 
                           default="all", help="output format")
    
    # Analytics commands
    subparsers.add_parser("quality", help="run data quality report")
    
    analyze_parser = subparsers.add_parser("analyze", help="run analytics")
    analyze_parser.add_argument("--type", choices=["trends", "teams", "top", "distribution"],
                               default="trends", help="analysis type")
    analyze_parser.add_argument("--year", type=int, help="filter by year")
    analyze_parser.add_argument("--club", type=str, help="filter by club")
    analyze_parser.add_argument("--top", type=int, default=10, help="number of top results")
    
    # MLS Roster scraping command
    roster_parser = subparsers.add_parser("roster", help="scrape MLS team rosters and player profiles")
    roster_parser.add_argument("--output", "-o", default="output", help="output directory")
    roster_parser.add_argument("--format", choices=["csv", "parquet", "all"], 
                               default="all", help="output format")
    roster_parser.add_argument("--team", type=str, help="specific team slug to scrape (e.g., inter-miami-cf)")
    roster_parser.add_argument("--headless", action="store_true", default=True, 
                               help="run browser in headless mode")
    roster_parser.add_argument("--no-headless", dest="headless", action="store_false",
                               help="show browser window")
    
    return parser.parse_args()


def cmd_run(args, logger) -> int:
    """Run full ETL pipeline: Extract -> Transform -> Load."""
    scraper = SalaryScraper()
    
    # Discover sources
    logger.info("Discovering salary sources...")
    sources = scraper.discover_sources()
    logger.info(f"Found {len(sources)} years")
    
    # Determine which years to process
    if args.year:
        years_to_process = [args.year] if args.year in sources else []
        if not years_to_process:
            logger.error(f"Year {args.year} not found in sources")
            return 1
    else:
        years_to_process = sorted(sources.keys(), reverse=True)
    
    all_records = []

    # Process each year: Extract -> Transform
    for year in years_to_process:
        source = sources[year]
        logger.info(f"Processing {year} ({source.format})...")
        
        rows = scraper.scrape_year(year)
        if not rows:
            logger.warning(f"No data for {year}, skipping")
            continue
        
        transformer = SalaryTransformer(year=year, source_format=source.format)
        records = transformer.transform(rows)
        
        if records:
            all_records.extend(records)
            logger.info(f"  {year}: {len(records)} records")
        else:
            logger.warning(f"  {year}: no records after transform")
    
    if not all_records:
        logger.error("No records to write")
        return 1
    
    # Load to various formats
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
    """Discover available salary sources."""
    scraper = SalaryScraper()
    sources = scraper.discover_sources()
    
    logger.info(f"Found {len(sources)} salary sources:")
    for year in sorted(sources.keys(), reverse=True):
        src = sources[year]
        logger.info(f"  {year}: {src.format.upper()} - {src.url[:60]}...")
    
    return 0


def cmd_quality(args, logger) -> int:
    """Run data quality report."""
    import pandas as pd
    from analytics.data_quality import DataQualityChecker
    from pathlib import Path
    
    # Try to load existing data
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
    """Run analytics."""
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
    """Scrape MLS team rosters and player profiles."""
    from ingestion.mls_roster_scraper import MLSRosterScraper
    from load.mls_writer import MLSWriter
    
    logger.info("Starting MLS roster scrape...")
    
    with MLSRosterScraper(headless=args.headless) as scraper:
        # Discover teams
        teams = scraper.discover_teams()
        logger.info(f"Found {len(teams)} teams")
        
        # Filter to specific team if requested
        if args.team:
            teams = [t for t in teams if t.slug == args.team]
            if not teams:
                logger.error(f"Team '{args.team}' not found")
                return 1
        
        # Scrape each team's roster
        for team in teams:
            scraper.scrape_team_roster(team)
        
        players = scraper.players
    
    if not players:
        logger.error("No players scraped")
        return 1
    
    # Write output
    writer = MLSWriter(output_dir=args.output)
    
    if args.format in ("csv", "all"):
        writer.write_players(players, "mls_rosters.csv")
        writer.write_teams(teams, "mls_teams.csv")
    
    if args.format in ("parquet", "all"):
        writer.write_players_parquet(players, "mls_rosters.parquet")
    
    logger.info(f"Done! Scraped {len(players)} players from {len(teams)} teams")
    return 0


def main() -> int:
    args = parse_args()
    setup_logging(level=logging.DEBUG if args.debug else logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info("MLS Analytics Engine")

    if args.command == "run":
        return cmd_run(args, logger)
    elif args.command == "discover":
        return cmd_discover(args, logger)
    elif args.command == "quality":
        return cmd_quality(args, logger)
    elif args.command == "analyze":
        return cmd_analyze(args, logger)
    elif args.command == "roster":
        return cmd_roster(args, logger)
    else:
        logger.info("Use --help for available commands")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
