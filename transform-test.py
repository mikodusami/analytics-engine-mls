# Test the transformation layer
import logging
from ingestion.scrapers import Scraper
from ingestion.pdf_salary_parser import PDFSalaryParser
from transform.salary_transformer import SalaryTransformer

logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")

pdf_parser = PDFSalaryParser()

test_sources = [
    (2023, "https://s3.amazonaws.com/mlspa/2023-Salary-Report-as-of-Sept-15-2023.pdf?mtime=20231018173909"),
    (2022, "https://s3.amazonaws.com/mlspa/2022-Fall-Salary-Guide.pdf?mtime=20221017132843"),
    (2020, "https://s3.amazonaws.com/mlspa/2020-Fall-Winter-Salary-List-alphabetical.pdf?mtime=20210513131818"),
    (2018, "http://s3.amazonaws.com/mlspa/2018-09-15-Salary-Information-Alphabetical.pdf?mtime=20190611125547"),
    (2011, "https://s3.amazonaws.com/mlspa/2011-09-01-Salary-Information-Alphabetical.pdf?mtime=20190611125323"),
    (2007, "https://s3.amazonaws.com/mlspa/2007-08-31-Salary-Information-Alphabetical.pdf?mtime=20190611125445"),
]

for year, url in test_sources:
    print(f"\n{'='*60}")
    print(f"YEAR: {year}")
    print(f"{'='*60}")
    
    response = Scraper.fetch_content(url=url)
    rows = pdf_parser.parse(response.content)
    
    transformer = SalaryTransformer(year=year)
    records = transformer.transform(rows)
    
    print(f"Total records: {len(records)}")
    print(f"\nFirst 5 records:")
    for r in records[:5]:
        print(f"  {r.club} | {r.last_name}, {r.first_name} | {r.position} | ${r.base_salary:,.2f}")
