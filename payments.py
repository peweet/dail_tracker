import fitz  # PyMuPDF
import glob
import pathlib
# import pandas as pd
import polars as pl
import re 
pdf_payment = pathlib.Path(r"C:\Users\pglyn\PycharmProjects\dail_extractor\pdf_payments")
#https://www.oireachtas.ie/en/publications/?q=standard%20allowance&date=&term=%2Fie%2Foireachtas%2Fhouse%2Fdail%2F34&fromDate=03%2F04%2F2026&toDate=03%2F04%2F2026

# totally_exclude = ['Parliamentary', 'Standard', 'Allowance']
# header_names = ['Name', 'TAA Band', 'Narrative', 'Date Paid', 'Amount']
EXCLUDE_PLACEHOLDER = re.compile(r"^(Parliamentary Standard)")
all_rows = []
print('Starting to process payment PDFs...')
for pdf in pdf_payment.glob("*.pdf"):
    # print(f"Processing payment file: {pdf}...")
    doc = fitz.open(pdf)  # Open the PDF document using PyMuPDF
    # print(f"Number of pages in {pdf}: {doc.page_count}")  # Debug: print the number of pages
    print(f"Processing payment file: {pdf} with {doc.page_count} pages...")
    for page in doc:
        # print(f"Processing page {page.number} of {pdf}...")  # Debug: print the current page number
        # table = page.find_tables()
        for table in page.find_tables().tables:
            all_rows.extend(table.extract()) 

cleaned_rows = []
for row in all_rows:
    if not EXCLUDE_PLACEHOLDER.search(str(row[0] or "")):
        cleaned_rows.append(row)
all_rows = cleaned_rows

print(f"Total rows extracted: {len(all_rows)}")
df = pl.DataFrame(all_rows, schema=['Name', 'TAA_Band', 'Narrative', 'Date_Paid', 'Amount'], orient='row')
df = df.with_columns(pl.col('Name').str.splitn(by=' ', n=2
                    ).alias('Name_Split'))
df = df.with_columns(pl.col('Name_Split'
                    ).struct.rename_fields(["Position", "Full_Name"])
                    ).unnest("Name_Split").drop('Name')
#TODO filter logic for Ceann Comhairle 
#TODO filter logic for TDs who were elected after the payment date (e.g. payments made in 2024 should only be matched to TDs elected in 2024 or earlier)
df = df.with_columns(
        # NFD converts accented chars into base letter + combining accent mark to make it easier to join
        pl.col('Full_Name')
        .str.to_lowercase()
        .str.replace_all(r"[\x27\u2019]", "")
        .str.replace_all(r"[\u0300-\u036f]", "") # remove accents and fadas as it becomes too difficult to join names with special characters otherwise (e.g. O'Sullivan, Ó Súilleabháin)
        .str.replace_all(r"[^a-z\s]", "")
        .str.normalize("NFD").alias('join_key')
        .str.replace_all(r"\s+", "")# Remove all whitespace
        .str.extract_all(r".")    # Extract individual characters into a list       
        .list.sort() # Sort the list of characters alphabetically (e.g. "OSuilleabhain" → "Oabhiillnsuu")
        .list.join("") # Join the sorted characters back into a string
        )
df.write_csv('C:\\Users\\pglyn\\PycharmProjects\\dail_extractor\\members\\aggregated_payment_tables.csv')
      