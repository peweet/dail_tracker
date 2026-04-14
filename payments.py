from glob import glob
import fitz  # PyMuPDF
from normalise_join_key import normalise_df_td_name
import polars as pl
import re 
from config import MEMBERS_DIR

docstring="""
This module processes the scanned PDFs of TD payments data, extracts the relevant information, and creates structured
CSV files that can be used for analysis. The code uses the PyMuPDF library to read the PDF files and extract the tables containing the payment data. It then cleans and transforms the extracted data, normalizes the TD names for consistent joining with other datasets, and saves the cleaned data to CSV files for further analysis. The module also includes logging to track the progress of the PDF processing and any issues that may arise during the extraction and transformation process.
The resulting CSV files will contain structured information about TD payments, which can be used to analyze payment patterns, identify top recipients of payments, and explore potential correlations with other factors such as attendance records, lobbying activities, and member metadata.
"""

def process_payment_pdfs():
    """Process scanned PDFs of TD payments data, extract relevant information, and create structured CSV files for analysis."""

pdf_dir = MEMBERS_DIR / "pdf_payments"
pdf_files = glob(str(pdf_dir / "*.pdf"))  # if using glob
#https://www.oireachtas.ie/en/publications/?q=standard%20allowance&date=&term=%2Fie%2Foireachtas%2Fhouse%2Fdail%2F34&fromDate=03%2F04%2F2026&toDate=03%2F04%2F2026
print("PDF files found:", pdf_files)
# TODO to file checks to see if end .csv are created successfully and contain expected number of rows, and if not, log errors and reasons why (e.g. API call failure, PDF parsing failure, etc.)
EXCLUDE_PLACEHOLDER = re.compile(r"^(Parliamentary Standard)")
all_rows = []
print('Starting to process payment PDFs...')
for pdf in pdf_files:
    print(f"Processing payment file: {pdf}...")
    # print(f"Processing payment file: {pdf}...")
    doc = fitz.open(pdf)  # Open the PDF document using PyMuPDF
    print(f"Processing payment file: {pdf} with {doc.page_count} pages...")
    for page in doc:
        for table in page.find_tables().tables:
            all_rows.extend(table.extract()) 
cleaned_rows = []
for row in all_rows:
    if not EXCLUDE_PLACEHOLDER.search(str(row[0])): # exclude rows that contain placeholder text like "Parliamentary Standard" in the first column, which are not actual payment records but just headers or footers in the PDF
        cleaned_rows.append(row)
all_rows = cleaned_rows

print(f"Total rows extracted: {len(all_rows)}")
df = pl.DataFrame(all_rows, schema=['Name', 'TAA_Band', 'Narrative', 'Date_Paid', 'Amount'], orient='row')
df = df.with_columns(pl.col('Name').str.splitn(by=' ', n=2
                    ).alias('Name_Split'))
df = df.with_columns(pl.col('Name_Split'
                    ).struct.rename_fields(["Position", "Full_Name"])
                    ).unnest("Name_Split").drop('Name')

df = normalise_df_td_name(df, 'Full_Name').with_columns(
    pl.col('Date_Paid').str.to_date(format="%d/%m/%Y"),
)
df.write_csv(MEMBERS_DIR / "aggregated_payment_tables.csv")
top_tds_by_payment = df.with_columns(
    pl.col('Amount').str.replace_all(
        r"[^.0-9\-]", ""
        ).cast(pl.Float64) #remove euro sign
    )
top_tds_by_payment = top_tds_by_payment.with_columns(
    pl.sum('Amount').over('join_key').alias('total_amount_paid_since_31_01_2025')
).sort(
    'total_amount_paid_since_31_01_2025', 
    descending=True)
top_tds_by_payment= top_tds_by_payment.unique(subset=['join_key'])
#TODO filter logic for Ceann Comhairle 
#TODO filter logic for TDs who were elected after the payment date (e.g. payments made in 2024 should only be matched to TDs elected in 2024 or earlier)
top_tds_by_payment.write_csv(MEMBERS_DIR / "top_tds_by_payment_since_2024.csv")

if __name__ == "__main__":
    print("Payment PDF processing complete. Output saved to aggregated_payment_tables.csv and top_tds_by_payment_2024.csv.")
