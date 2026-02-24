# Analyze all the different PDF/CSV formats to understand edge cases
from ingestion.scrapers import Scraper
from ingestion.pdf_salary_parser import PDFSalaryParser
from ingestion.csv_salary_parser import CSVSalaryParser

pdf_parser = PDFSalaryParser()
csv_parser = CSVSalaryParser()

# Sample URLs from different years to see format variations
test_sources = [
    ("2023", "pdf", "https://s3.amazonaws.com/mlspa/2023-Salary-Report-as-of-Sept-15-2023.pdf?mtime=20231018173909"),
    ("2022", "pdf", "https://s3.amazonaws.com/mlspa/2022-Fall-Salary-Guide.pdf?mtime=20221017132843"),
    ("2020", "pdf", "https://s3.amazonaws.com/mlspa/2020-Fall-Winter-Salary-List-alphabetical.pdf?mtime=20210513131818"),
    ("2018", "pdf", "http://s3.amazonaws.com/mlspa/2018-09-15-Salary-Information-Alphabetical.pdf?mtime=20190611125547"),
    ("2011", "pdf", "https://s3.amazonaws.com/mlspa/2011-09-01-Salary-Information-Alphabetical.pdf?mtime=20190611125323"),
    ("2007", "pdf", "https://s3.amazonaws.com/mlspa/2007-08-31-Salary-Information-Alphabetical.pdf?mtime=20190611125445"),
]

print("=" * 80)
print("ANALYZING FORMAT VARIATIONS ACROSS YEARS")
print("=" * 80)

for year, fmt, url in test_sources:
    print(f"\n{'='*40}")
    print(f"YEAR: {year} ({fmt})")
    print(f"{'='*40}")
    
    try:
        response = Scraper.fetch_content(url=url)
        if fmt == "pdf":
            rows = pdf_parser.parse(response.content)
        else:
            rows = csv_parser.parse(response.content)
        
        # Show first 15 non-empty rows
        non_empty = [r for r in rows if r][:15]
        for i, row in enumerate(non_empty):
            print(f"  {i}: {row}")
            
    except Exception as e:
        print(f"  ERROR: {e}")
