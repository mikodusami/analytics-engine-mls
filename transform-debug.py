# Debug 2023 position issue
from ingestion.scrapers import Scraper
from ingestion.pdf_salary_parser import PDFSalaryParser

pdf_parser = PDFSalaryParser()

url = "https://s3.amazonaws.com/mlspa/2023-Salary-Report-as-of-Sept-15-2023.pdf?mtime=20231018173909"
response = Scraper.fetch_content(url=url)
rows = pdf_parser.parse(response.content)

print("Header row:")
print(rows[0])
print("\nFirst 3 data rows:")
for row in rows[1:4]:
    print(row)
