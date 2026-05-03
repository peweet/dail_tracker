import re
from glob import glob

import fitz  # PyMuPDF
import polars as pl

from config import PAYMENTS_PDF_DIR, SILVER_DIR, GOLD_DIR
from normalise_join_key import normalise_df_td_name

TAA_LABELS = {
    "Dublin": "Dublin / under 25 km",
    "1":      "Band 1 — 25–60 km",
    "2":      "Band 2 — 60–80 km",
    "3":      "Band 3 — 80–100 km",
    "4":      "Band 4 — 100–130 km",
    "5":      "Band 5 — 130–160 km",
    "6":      "Band 6 — 160–190 km",
    "7":      "Band 7 — 190–210 km",
    "8":      "Band 8 — over 210 km",
    # Extended numeric bands — present in data, meaning unclear, retained without label
    "9":      "Band 9 (unmapped)",
    "10":     "Band 10 (unmapped)",
    "11":     "Band 11 (unmapped)",
    "12":     "Band 12 (unmapped)",
}

def _is_clean_band(band: str) -> bool:
    """
    A band is clean if it is 'Dublin' or a pure integer string (any number).
    Everything else — 'Vouched', 'MIN', 'NoTAA', combined codes like '2/MIN',
    garbled values like 'Kenny', encoding artifacts — is quarantined.
    """
    if band == "Dublin":
        return True
    try:
        int(band)
        return True
    except (ValueError, TypeError):
        return False

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

    df = df.with_columns(
        pl.col("TAA_Band")
          .str.strip_chars()
          .replace("nan", None)
          .alias("TAA_Band")
    )
    df = df.unique(subset=['join_key', 'Date_Paid', 'Amount'], keep='first')
    is_clean = (
        pl.col("TAA_Band").eq("Dublin")
        | pl.col("TAA_Band").str.contains(r"^\d+$")
    ).fill_null(False)

    clean       = df.filter(is_clean)
    quarantined = df.filter(~is_clean)

    print(f"Clean rows:      {len(clean)}")
    print(f"Quarantined rows:{len(quarantined)}")
    print(f"Quarantined TAA_Band values: {sorted(quarantined['TAA_Band'].drop_nulls().unique().to_list())}") 
    clean = clean.with_columns(
        pl.col("Full_Name")
          .str.strip_chars()
          .alias("Full_Name")
    ).with_columns(
        pl.when(pl.col("Full_Name").str.contains(","))
          .then(
              pl.col("Full_Name").str.split(",").list.get(1).str.strip_chars()
              + pl.lit(" ")
              + pl.col("Full_Name").str.split(",").list.get(0).str.strip_chars()
          )
          .otherwise(pl.col("Full_Name"))
          .alias("member_name")
    )
    clean = clean.with_columns(
        pl.col("Amount")
          .str.replace_all(r"[^0-9.]", "")
          .cast(pl.Float64, strict=False)
          .alias("Amount")
    )
    clean = clean.with_columns(
        pl.col("TAA_Band")
          .replace(TAA_LABELS)
          .alias("taa_band_label")
    )
    clean = clean.with_columns(
        pl.col("Position").fill_null("Deputy").str.strip_chars().alias("position")
    )
    before_gate = len(clean)
    clean = clean.filter(
        pl.col("Date_Paid").is_not_null()
        & pl.col("Amount").is_not_null()
        & (pl.col("Amount") > 0)
        & pl.col("member_name").is_not_null()
    )
    print(f"Dropped by date/amount gate: {before_gate - len(clean)}")
    print(f"Final clean rows: {len(clean)}")
    print(f"Final clean rows: {len(clean)}")

    # ── Select and order output columns ───────────────────────────────────────
    fact = clean.select([
        "join_key",
        "Full_Name",
        "Position",
        pl.col("TAA_Band").alias("taa_band_raw"),
        "taa_band_label",
        "Date_Paid",
        pl.col("Narrative").str.strip_chars().alias("narrative"),
        "Amount",
        # "payment_year",
    ])
    fact.write_csv(SILVER_DIR / "aggregated_payment_tables.csv")
    fact.write_parquet(SILVER_DIR / "parquet" / "aggregated_payment_tables.parquet")

    quarantined.write_csv(SILVER_DIR / "quarantined_payment_tables.csv")
    quarantined.write_parquet(SILVER_DIR / "parquet" / "quarantined_payment_tables.parquet")

    top_tds_by_payment = fact.with_columns(
        pl.col('Amount').cast(pl.Utf8).str
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

    top_tds_by_payment.write_csv(GOLD_DIR / "top_tds_by_payment_since_2020.csv")
    top_tds_by_payment.write_parquet(GOLD_DIR / "parquet" / "top_tds_by_payment_since_2020.parquet")

if __name__ == "__main__":
    process_payment_pdfs()
    print("Payment PDF processing complete. Output saved to aggregated_payment_tables.csv and top_tds_by_payment_2020.csv.")
