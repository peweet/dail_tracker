import fitz  # PyMuPDF
import os   
import glob
import re
import pathlib
import ocrmypdf
from ocrmypdf import OcrOptions

#conversion of scanned PDF files to readable output UNDER TEST - not yet working, but will be used to convert scanned PDFs of attendance and payments data into readable text that can then be parsed and structured into a DataFrame. The code uses the ocrmypdf library to perform OCR on the scanned PDF, and then uses PyMuPDF to extract the text from the resulting OCR'd PDF. The extracted text can then be processed further to extract relevant information such as TD names, attendance records, payment amounts, etc. This will allow us to include data from scanned PDFs in our analysis alongside data obtained from APIs and other sources.
scanned_pdf = pathlib.Path(r"C:\Users\pglyn\PycharmProjects\dail_extractor\scan_pdf\target\scan_pdf.pdf")      
if __name__ == '__main__':  # To ensure correct behavior on Windows and macOS
    options = OcrOptions(
        input_file=f"{scanned_pdf}",
        output_file=f"scan_pdf\\{scanned_pdf.stem}-ocr.pdf",
        deskew=True,
        languages=['eng'],
    )
    ocrmypdf.ocr(options)


pdf_payment = pathlib.Path(r"C:\Users\pglyn\PycharmProjects\dail_extractor\scan_pdf\output\scan_pdf-ocr.pdf")
print('Starting to process scanned PDFs...')
doc = fitz.open(pdf_payment)  # Open the PDF document using PyMuPDF
print(f"Processing scanned file: {pdf_payment} with {doc.page_count} pages...")
for page in doc:
    text = page.get_text("text")
    print(text)
