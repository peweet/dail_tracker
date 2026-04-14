import re
#TODO: this code is currently in the payments.py file, but it should be refactored into a separate module (e.g. pdf_processing.py) that can be imported and used in the main pipeline script (e.g. main.py) to process the scanned PDFs of TD payments data, extract the relevant information, and create structured CSV files for analysis. This will help to keep the code organized and modular, and make it easier to maintain and extend in the future as we add more functionality to the pipeline. The pdf_processing module can contain functions for processing different types of PDFs (e.g. attendance, payments, etc.) and can be called from the main pipeline script to perform the necessary processing steps for each type of PDF data. 
EXCLUDE_PLACEHOLDER = re.compile(r"^(Parliamentary Standard)")
CATEGORIES = re.compile(r"^\d+\.\s")      
MEMBER_NAME = re.compile(r"^[A-Z]{2,},\s")

IRISH_NAME_REGEX = re.compile(r"^[A-ZÁÉÍÓÚ][a-zA-ZáéíóúÁÉÍÓÚ'\s\-]+$")

REPLACE_APOSTROPHES = re.compile(r"[\x27\u2019]", "") 
REMOVE_DIACRITICS = re.compile(r"[\u0300-\u036f]", "") 
REMOVE_NON_ALPHABETIC = re.compile(r"[^a-z\s]", "")
NORMALIZE_UNICODE = re.compile(r"\u00D")
REMOVE_WHITESPACE = re.compile(r"\s+", "")
EXTRACT_INDIVIDUAL = re.compile(r".")   

YEAR = re.compile(r"\b\d{4}\b")