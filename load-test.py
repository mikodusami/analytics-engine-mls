# Full ETL pipeline: Extract -> Transform -> Load (all years)
import logging
from ingestion.salary_scraper import SalaryScraper
from ingestion.pdf_salary_parser import PDFSalaryParser
from ingestion.csv_salary_parser import CSVSalaryParser
from transform.salary_transformer import SalaryTransformer
from load.csv_writer import CSVWriter

logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")

# Initialize
scraper = SalaryScraper()
csv_writer = CSVWriter(output_dir="output")

# Discover all sources
print("--- Discovering sources ---")
sources = scraper.discover_sources()
print(f"Found {len(sources)} years: {sorted(sources.keys())}")

all_records = []

for year in sorted(sources.keys(), reverse=True):
    source = sources[year]
    print(f"\n--- Processing {year} ({source.format}) ---")
    
    # Extract
    rows = scraper.scrape_year(year)
    if not rows:
        print(f"  Skipped - no data")
        continue
    
    # Transform
    transformer = SalaryTransformer(year=year)
    records = transformer.transform(rows)
    all_records.extend(records)
    print(f"  Transformed {len(records)} records")

# Load
print(f"\n--- Loading {len(all_records)} total records ---")
csv_writer.write_all(all_records, "salaries_all.csv")
csv_writer.write_by_year(all_records)

print("\nDone! Check the output/ directory.")
