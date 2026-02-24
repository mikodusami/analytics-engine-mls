# Test the full ETL pipeline: Extract -> Transform -> Load
import logging
from ingestion.scrapers import Scraper
from ingestion.pdf_salary_parser import PDFSalaryParser
from transform.salary_transformer import SalaryTransformer
from load.csv_writer import CSVWriter

logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")

pdf_parser = PDFSalaryParser()
csv_writer = CSVWriter(output_dir="output")

test_sources = [
    (2023, "https://s3.amazonaws.com/mlspa/2023-Salary-Report-as-of-Sept-15-2023.pdf"),
    (2022, "https://s3.amazonaws.com/mlspa/2022-Fall-Salary-Guide.pdf"),
    (2011, "https://s3.amazonaws.com/mlspa/2011-09-01-Salary-Information-Alphabetical.pdf"),
]

all_records = []

for year, url in test_sources:
    print(f"\n--- Processing {year} ---")
    
    # Extract
    response = Scraper.fetch_content(url=url)
    rows = pdf_parser.parse(response.content)
    
    # Transform
    transformer = SalaryTransformer(year=year)
    records = transformer.transform(rows)
    all_records.extend(records)

# Load - write combined and by-year
print(f"\n--- Loading {len(all_records)} total records ---")
csv_writer.write_all(all_records, "salaries_combined.csv")
csv_writer.write_by_year(all_records)

print("\nDone! Check the output/ directory.")
