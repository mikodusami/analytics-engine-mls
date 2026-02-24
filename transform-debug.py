# Debug CSV format for 2024/2025
from ingestion.scrapers import Scraper
from ingestion.csv_salary_parser import CSVSalaryParser

csv_parser = CSVSalaryParser()

url = "http://s3.amazonaws.com/mlspa/MLS-Salary-List-10-2025-REVISED.csv?mtime=20251029164256"
response = Scraper.fetch_content(url=url)
rows = csv_parser.parse(response.content)

print(f"Total rows: {len(rows)}")
print("\nFirst 5 rows:")
for i, row in enumerate(rows[:5]):
    print(f"  {i}: {row}")
