# from pypdf import PdfReader

# reader = PdfReader("example4.pdf")
# page = reader.pages[0]
# extracted_text = page.extract_text(extraction_mode="layout")
# print("Length Of Text: ", len(extracted_text))
# print("First 50 chars: ", extracted_text[:50])
# splitted = extracted_text.split("\n")
# for i in range(5):
#     print(f"List Of Text Split By Newline[{splitted[i]}]")

# for i in range(5):
#     splitted_line = splitted[i].split()
#     print(type(splitted_line))
#     print(splitted_line)

# from ingestion.scrapers import *
# from ingestion.parsers import *
# from ingestion.pdf_salary_parser import *

# urls = ["https://s3.amazonaws.com/mlspa/2023-Salary-Report-as-of-Sept-15-2023.pdf?mtime=20231018173909",
# "https://s3.amazonaws.com/mlspa/2022-Fall-Salary-Guide.pdf?mtime=20221017132843"]
# for url in urls:
#     contentResponse = Scraper.fetch_content(url=url)
#     p = PDFSalaryParser()
#     print(p.parse(contentResponse.content)[0])

# Test the auto-detection on both PDFs
from ingestion.scrapers import Scraper
from ingestion.pdf_salary_parser import PDFSalaryParser

# 2018 PDF (was fragmented in layout mode)
print("=== 2018 PDF ===")
url_2018 = "http://s3.amazonaws.com/mlspa/2018-09-15-Salary-Information-Alphabetical.pdf?mtime=20190611125547"
response = Scraper.fetch_content(url=url_2018)
parser = PDFSalaryParser()
rows = parser.parse(response.content)
print(f"Rows: {len(rows)}")
for row in rows[2:7]:
    print(f"  {row}")

# 2023 PDF (worked fine in layout mode)
print("\n=== 2023 PDF ===")
url_2023 = "https://s3.amazonaws.com/mlspa/2023-Salary-Report-as-of-Sept-15-2023.pdf?mtime=20231018173909"
response = Scraper.fetch_content(url=url_2023)
rows = parser.parse(response.content)
print(f"Rows: {len(rows)}")
for row in rows[:5]:
    print(f"  {row}")
