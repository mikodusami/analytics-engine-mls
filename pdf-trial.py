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

from ingestion.scrapers import *
from ingestion.parsers import *
from ingestion.pdf_salary_parser import *

urls = ["https://s3.amazonaws.com/mlspa/2023-Salary-Report-as-of-Sept-15-2023.pdf?mtime=20231018173909",
"https://s3.amazonaws.com/mlspa/2022-Fall-Salary-Guide.pdf?mtime=20221017132843"]
for url in urls:
    contentResponse = Scraper.fetch_content(url=url)
    p = PDFSalaryParser()
    print(p.parse(contentResponse.content)[0])