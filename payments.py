import re
from glob import glob

import fitz  # PyMuPDF
import polars as pl

from config import PAYMENTS_PDF_DIR, SILVER_DIR
from normalise_join_key import normalise_df_td_name


def process_payment_pdfs():
    """
    This module processes the scanned PDFs of TD payments data, extracts the relevant information, and creates structured
    CSV files that can be used for analysis. The code uses the PyMuPDF library to read the PDF files and extract the tables containing the payment data. It then cleans and transforms the extracted data, normalizes the TD names for consistent joining with other datasets, and saves the cleaned data to CSV files for further analysis. The module also includes logging to track the progress of the PDF processing and any issues that may arise during the extraction and transformation process.
    The resulting CSV files will contain structured information about TD payments, which can be used to analyze payment patterns, identify top recipients of payments, and explore potential correlations with other factors such as attendance records, lobbying activities, and member metadata.
    """
    pdf_dir = PAYMENTS_PDF_DIR
    pdf_files = glob(str(pdf_dir / "*.pdf"))
    #https://www.oireachtas.ie/en/publications/?q=standard%20allowance&date=&term=%2Fie%2Foireachtas%2Fhouse%2Fdail%2F34&fromDate=03%2F04%2F2026&toDate=03%2F04%2F2026
    print("PDF files found:", pdf_files)
    # TODO to file checks to see if end .csv are created successfully and contain expected number of rows, and if not, log errors and reasons why (e.g. API call failure, PDF parsing failure, etc.)
    EXCLUDE_PLACEHOLDER = re.compile(r"^(Parliamentary Standard)")
    all_rows = []
    print('Starting to process payment PDFs...')
    for pdf in pdf_files:
        print(f"Processing payment file: {pdf}...")
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
    df = pl.DataFrame(all_rows, schema=[
        'Name',
        'TAA_Band',
        'Narrative',
        'Date_Paid',
        'Amount'],
        orient='row')
    df = df.with_columns(pl.col('Name').str
                        .splitn(
                        by=' ',
                        n=2
                        ).alias('Name_Split'))
    df = df.with_columns(pl.col('Name_Split'
                        ).struct.rename_fields(["Position", "Full_Name"])
                        ).unnest("Name_Split").drop('Name')
    df = df.with_columns(
        pl.when(
        pl.col('Date_Paid').str.contains('/')
        ).then(pl.col('Date_Paid')
               ).otherwise(None).alias('Date_Paid'))
    df = normalise_df_td_name(
        df,
        'Full_Name').with_columns(
        #iso date format conversion
        pl.col('Date_Paid'
            ).str.to_date(format="%d/%m/%Y"),
    )

    df.write_csv(SILVER_DIR / "aggregated_payment_tables.csv")
    top_tds_by_payment = df.with_columns(
        pl.col('Amount').str
        # filter out the euro symbol and any commas, and then convert to float
        .replace_all(r"[^.0-9\-]", "")
        .cast(pl.Float64, strict=False)
    ).unique(subset=['join_key', 'Date_Paid', 'Amount'])  # dedup before summing

    # Filter out rows where Amount looks like a misread date (>10000) or is null
    top_tds_by_payment = top_tds_by_payment.filter(
        pl.col('Amount').is_not_null() & 
        (pl.col('Amount') < 10_000)
    )
    top_tds_by_payment = top_tds_by_payment.with_columns(
        pl.sum('Amount').over('join_key'
        ).alias('total_amount_paid_since_2020').round(2)
    ).sort(
        ['Date_Paid', 'total_amount_paid_since_2020'],
        descending=True)
    top_tds_by_payment = top_tds_by_payment.unique()
    top_tds_by_payment.write_csv(SILVER_DIR / "top_tds_by_payment_since_2020.csv")

if __name__ == "__main__":
    process_payment_pdfs()
    print("Payment PDF processing complete. Output saved to aggregated_payment_tables.csv and top_tds_by_payment_2020.csv.")
