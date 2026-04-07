import fitz  # PyMuPDF
import glob
import pathlib
import pandas as pd
import re 
from attendance_2024 import IRISH_NAME_REGEX
pdf_payment = pathlib.Path(r"C:\Users\pglyn\PycharmProjects\dail_extractor\pdf_payments")
#https://www.oireachtas.ie/en/publications/?q=standard%20allowance&date=&term=%2Fie%2Foireachtas%2Fhouse%2Fdail%2F34&fromDate=03%2F04%2F2026&toDate=03%2F04%2F2026

# totally_exclude = ['Parliamentary', 'Standard', 'Allowance']
# header_names = ['Name', 'TAA Band', 'Narrative', 'Date Paid', 'Amount']
EXCLUDE_PLACEHOLDER = re.compile(r"^(PSA)")
all_rows = []
print('Starting to process payment PDFs...')
for pdf in pdf_payment.glob("*.pdf"):
    # print(f"Processing payment file: {pdf}...")
    doc = fitz.open(pdf)  # Open the PDF document using PyMuPDF
    # print(f"Number of pages in {pdf}: {doc.page_count}")  # Debug: print the number of pages
    for page in doc:
        # print(f"Processing page {page.number} of {pdf}...")  # Debug: print the current page number
        # table = page.find_tables()
        for table in page.find_tables().tables:
            # print(f"Processing table on page {page.number} of {pdf}...") 
            if not EXCLUDE_PLACEHOLDER.search(table):
                all_rows.extend(table.extract())
            # print('DataFrame created from the table:')
        
# year = [2020, 2021, 2022, 2023, 2024, 2025, 2026]
# month = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"]

# construct_string = 