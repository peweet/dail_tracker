# import glob
import logging
import os

import fitz  # PyMuPDF
import ocrmypdf
import pandas as pd
from ocrmypdf import OcrOptions

from config import SCAN_PDF_DIR

# This entire module is under test and experimental, and will be used to convert scanned PDFs of attendance and payments data into readable text that can then be parsed and structured into a DataFrame. The code uses the ocrmypdf library to perform OCR on the scanned PDF, and then uses PyMuPDF to extract the text from the resulting OCR'd PDF. The extracted text can then be processed further to extract relevant information such as TD names, attendance records, payment amounts, etc. This will allow us to include data from scanned PDFs in our analysis alongside data obtained from APIs and other sources.

# conversion of scanned PDF files to readable output UNDER TEST - not yet working, but will be used to convert scanned PDFs of attendance and payments data into readable text that can then be parsed and structured into a DataFrame. The code uses the ocrmypdf library to perform OCR on the scanned PDF, and then uses PyMuPDF to extract the text from the resulting OCR'd PDF. The extracted text can then be processed further to extract relevant information such as TD names, attendance records, payment amounts, etc. This will allow us to include data from scanned PDFs in our analysis alongside data obtained from APIs and other sources.
scanned_pdf = SCAN_PDF_DIR / "target" / "scan_pdf.pdf"
ge_ff_2024 = SCAN_PDF_DIR / "target" / "ff_sipo_ge_2024_expenses.pdf"
# To ensure correct behavior on Windows and macOS
options = OcrOptions(
    input_file=f"{ge_ff_2024}",
    output_file=f"{SCAN_PDF_DIR / 'output' / f'{ge_ff_2024.stem}-ocr.pdf'}",
    deskew=True,
    languages=["eng"],
    progress_bar=True,
    force_ocr=True,  # Force OCR even if the PDF already has text (useful for scanned PDFs)
)
if not os.path.exists(options.output_file):
    try:
        logging.info(f"Starting OCR process for {ge_ff_2024}...")
        ocrmypdf.ocr(options)
        logging.info(f"OCR completed successfully for {ge_ff_2024}. Output saved to {options.output_file}.")
    except Exception as e:
        logging.error(f"Skipping {ge_ff_2024}: {e} already has text (OCR previously run).")

# #TODO replace path with config variable and add error handling and logging to this process, and to the rest of the codebase (e.g. log when OCR process starts and finishes, log any errors that occur during OCR, log when text extraction starts and finishes, log any errors that occur during text extraction, etc.)
pdf_payment = SCAN_PDF_DIR / "output" / f"{ge_ff_2024.stem}-ocr.pdf"
logging.info("Starting to process scanned PDFs...")
doc = fitz.open(pdf_payment)  # Open the PDF document using PyMuPDF
# noOfPages = doc.page_count
# logging.info(f"Processing scanned file: {pdf_payment} with {noOfPages} pages...")
df_to_write = []
for page in doc:
    tabs = page.find_tables(strategy="text")
    for tab in tabs.tables:
        df = tab.to_pandas()
        df_to_write.append(df)
# np_array_of_objects = np.asarray(df_to_write, dtype="object")
counter = 0
for df in df_to_write:
    print(df.head())
    counter += 1
    pd.DataFrame(df).to_csv(SCAN_PDF_DIR / "output" / f"{ge_ff_2024.stem}-tables{counter}.csv", index=False)
    # Extract the first table found on the page and add it to the list of rows to write to CSV
# pd.DataFrame(df_to_write).to_csv(SCAN_PDF_DIR / "output" / f"{ge_ff_2024.stem}-tables.csv", index=False)
# for table in page.get_text():
#     all_rows.extend(table)

# json_output_path = SCAN_PDF_DIR / "output" / f"{ge_ff_2024.stem}-tables.json"
# with open(json_output_path, 'w', encoding='utf-8') as f:
#     json.dump(all_rows, f, ensure_ascii=False, indent=4)
if __name__ == "__main__":
    logging.info("Scanned PDF processing complete. Extracted text logged.")
