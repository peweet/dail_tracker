

# config.py
from pathlib import Path
PROJECT_ROOT = Path(__file__)
print(f"Project root directory: {PROJECT_ROOT}")
DATA_DIR = PROJECT_ROOT / "dail_extractor"
print(f"Data directory: {DATA_DIR}")
API_BASE = "https://api.oireachtas.ie/v1"
# PARTY_CODES = ["Social_Democrats", "Sinn_Féin", ...]
DATE_RANGE = ("2024-01-01", "2099-01-01")
CHAMBER_DAIL = "chamber=dail"
CHAMBER_SEANAD = "chamber=seanad"
Y_M_D_format = "%Y-%m-%d"

# Project root - anchored relative to this file
ROOT = Path(__file__).resolve().parent

# Directory tree
DATA_DIR = ROOT / "data"
print(DATA_DIR)
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
OUTPUT_DIR = ROOT / "output"
REPORTS_DIR = OUTPUT_DIR / "reports"
CHARTS_DIR = OUTPUT_DIR / "charts"
LOGS_DIR = ROOT / "logs"
PDF_PAYMENTS = DATA_DIR / "pdf_payments"
PDF_MEMBERS = DATA_DIR / "members"
PDF_LOBBYING = DATA_DIR / "lobbyist"
BILLS = DATA_DIR / "bills"
SCAN_PDF = DATA_DIR / "scan_pdf"
UTILITY = ROOT / "utility"
PDF_STORAGE = DATA_DIR / "pdf_storage"

# Collect all directories that need to exist
_DIRS = [
    # RAW_DIR,
    # PROCESSED_DIR,
    # REPORTS_DIR,
    # CHARTS_DIR,
    # LOGS_DIR,
    PDF_PAYMENTS,
    PDF_MEMBERS,
    PDF_LOBBYING,
    BILLS,
    SCAN_PDF,
    UTILITY,
]
def init_dirs() -> None:
    """Create all project directories if they don't exist."""
    for d in _DIRS:
        d.mkdir(parents=True, exist_ok=True)


# Auto-create on import
init_dirs()

# example

# pipeline.py
# from config import RAW_DIR, PROCESSED_DIR

# input_path = RAW_DIR / "lobbying_2024.csv"
# output_path = PROCESSED_DIR / "lobbying_cleaned.parquet"

# df = pl.read_csv(input_path)
# df.write_parquet(output_path)