import logging
import argparse
import sys
from ingestion.salary_scraper import SalaryScraper

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

    scrape_parser = subparsers.add_parser("scrape", help="scrape salary data")

    scrape_parser.add_argument(
        "--year", type=int, help="specific year to scrape (default: all)"
    ) 
    scrape_parser.add_argument(
        "--split", action="store_true", help="split output by year"
    )
    scrape_parser.add_argument(
        "--output", "-o", default="output", help="output directory"
    )
    return parser.parse_args()

def cmd_scrape(args, logger) -> int:
    scraper = SalaryScraper()
    if args.year:
        logger.info(f"Scraping year {args.year}")
        scraper.discover_sources()
        rows = scraper.scrape_year(args.year)
        if not rows:
            logger.error("No data scraped")
            return 1
        logger.info(f"Scraped {len(rows)} rows for {args.year}")
    else:
        logger.info("Scraping all available years")
        records_by_year = scraper.scrape()
        if not records_by_year:
            logger.error("No data scraped")
            return 1
        total_rows = sum(len(rows) for rows in records_by_year.values())
        logger.info(f"Scraped {total_rows} rows across {len(records_by_year)} years")

    ## ingestion phase over
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

def main() -> int:
    args = parse_args()
    setup_logging(level=logging.DEBUG if args.debug else logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info("MLS Analytics Engine")
    logger.info("Use --help for available commands")

    if args.command == "scrape":
        return cmd_scrape(args, logger)
    elif args.command == "discover":
        return cmd_discover(args, logger)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())