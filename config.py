import logging
import os
from pathlib import Path

docstring = """
This module contains configuration settings for the project, including paths to various directories, API endpoints, date ranges, and other constants used throughout the codebase.
"""
LOGGING_CONFIG = logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Base project directory
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
# Medallion architecture layers
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
SILVER_PARQUET_DIR = SILVER_DIR / "parquet"
GOLD_DIR = DATA_DIR / "gold"
GOLD_PARQUET_DIR = GOLD_DIR / "parquet"

LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Bronze: PDF and CSV source file locations (aligned with existing directory structure)
BRONZE_PDF_DIR = BRONZE_DIR / "pdfs"
ATTENDANCE_PDF_DIR = BRONZE_PDF_DIR / "attendance"

PAYMENTS_PDF_DIR = BRONZE_PDF_DIR / "payments"
INTERESTS_PDF_DIR = BRONZE_DIR / "interests"
LOBBYING_RAW_DIR = BRONZE_DIR / "lobbying_csv_data"
VOTES_RAW_DIR = BRONZE_DIR / "votes"
# Bronze: API JSON and member data storage
MEMBERS_DIR = BRONZE_DIR / "members"
LEGISLATION_DIR = BRONZE_DIR / "legislation"
VOTES_DIR = BRONZE_DIR / "votes"
# Silver: lobbying processed output
LOBBY_OUTPUT_DIR = SILVER_DIR / "lobbying"
LOBBY_PARQUET_DIR = LOBBY_OUTPUT_DIR / "parquet"


# LOGGING SETUP
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE_PATH = os.path.join(LOG_DIR, "pipeline.log")
FILE_HANDLER = file_handler = logging.FileHandler(LOG_FILE_PATH, encoding="utf-8")
file_handler.setLevel(logging.INFO)
# Set a formatter for the file handler to include timestamps and log levels
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logger = logging.getLogger(__name__)
# Add the file handler to the logger
logger.addHandler(file_handler)

PROJECT_ROOT = Path(__file__).parent.resolve()
API_BASE = "https://api.oireachtas.ie/v1"
# PARTY_CODES = ["Social_Democrats", "Sinn_Féin", ...]
DATE_RANGE = ("2024-01-01", "2099-01-01")
CHAMBER_DAIL = "chamber=dail"
CHAMBER_SEANAD = "chamber=seanad"
Y_M_D_format = "%Y-%m-%d"


DIRS = [
    DATA_DIR,
    SILVER_DIR,
    SILVER_PARQUET_DIR,
    GOLD_DIR,
    GOLD_PARQUET_DIR,
    BRONZE_DIR,
    BRONZE_PDF_DIR,
    ATTENDANCE_PDF_DIR,
    PAYMENTS_PDF_DIR,
    INTERESTS_PDF_DIR,
    LOBBYING_RAW_DIR,
    VOTES_RAW_DIR,
    MEMBERS_DIR,
    LEGISLATION_DIR,
    VOTES_DIR,
    LOBBY_OUTPUT_DIR,
    LOBBY_PARQUET_DIR,
]


def init_dirs() -> None:
    """Create all project directories if they don't exist."""
    for d in DIRS:
        d.mkdir(parents=True, exist_ok=True)


# Auto-create on import
init_dirs()
