import logging
import argparse
import sys
from ingestion.salary_scraper import SalaryScraper
from transform.salary_transformer import SalaryTransformer
from load.csv_writer import CSVWriter


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

    run_parser = subparsers.add_parser("run", help="run full ETL pipeline")
    run_parser.add_argument(
        "--year", type=int, help="specific year to process (default: all)"
    )
    run_parser.add_argument(
        "--split", action="store_true", help="split output by year"
    )
    run_parser.add_argument(
        "--output", "-o", default="output", help="output directory"
    )
    
    return parser.parse_args()


def cmd_run(args, logger) -> int:
    """Run full ETL pipeline: Extract -> Transform -> Load."""
    scraper = SalaryScraper()
    csv_writer = CSVWriter(output_dir=args.output)
    
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
        
        # Extract
        rows = scraper.scrape_year(year)
        if not rows:
            logger.warning(f"No data for {year}, skipping")
            continue
        
        # Transform (pass source format so transformer knows how to parse)
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
    
    # Load
    logger.info(f"Writing {len(all_records)} total records...")
    
    if args.split:
        csv_writer.write_by_year(all_records)
    else:
        csv_writer.write_all(all_records, "salaries.csv")
    
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


def main() -> int:
    args = parse_args()
    setup_logging(level=logging.DEBUG if args.debug else logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info("MLS Analytics Engine")

    if args.command == "run":
        return cmd_run(args, logger)
    elif args.command == "discover":
        return cmd_discover(args, logger)
    else:
        logger.info("Use --help for available commands")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
