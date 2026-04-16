from pathlib import Path
import logging
docstring="""
This module contains configuration settings for the project, including paths to various directories, API endpoints, date ranges, and other constants used throughout the codebase.
"""
#TODO: adapt config to reflect bronze, silver, gold medallion architecture, and to include more specific paths for different types of data (e.g. attendance, payments, lobbying, etc.) and for different stages of the pipeline (e.g. raw, processed, etc.). This will help to keep the project organized and make it easier to manage the data and the codebase as the project grows and evolves over time. For example, we could have a directory structure like this:
# data/


#TODO add logging configuration to this file and import it into the other modules that need logging, to avoid duplication and ensure consistent logging configuration across the codebase. We can define a function in config.py that sets up the logging configuration and returns a logger instance that can be imported and used in other modules. This will help to keep the code organized and make it easier to maintain the logging setup as the project evolves over time.
LOGGING_CONFIG = logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
#LOGGING SETUP
FILE_HANDLER= file_handler = logging.FileHandler("pipeline.log")
file_handler.setLevel(logging.INFO)
# Set a formatter for the file handler to include timestamps and log levels
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logger = logging.getLogger(__name__)
# Add the file handler to the logger
logger.addHandler(file_handler)

PROJECT_ROOT = Path(__file__).parent.resolve()

print(f"Project root directory: {PROJECT_ROOT}")
DATA_DIR = PROJECT_ROOT / "data"
print(f"Data directory: {DATA_DIR}")
API_BASE = "https://api.oireachtas.ie/v1"
# PARTY_CODES = ["Social_Democrats", "Sinn_Féin", ...]
DATE_RANGE = ("2024-01-01", "2099-01-01")
CHAMBER_DAIL = "chamber=dail"
CHAMBER_SEANAD = "chamber=seanad"
Y_M_D_format = "%Y-%m-%d"


# BASE patjhs for different types of data
BASE_DIR = Path(__file__).resolve().parent
LOBBY_DIR = BASE_DIR / "lobbyist"
BILLS_DIR = BASE_DIR / "bills"
MEMBERS_DIR = BASE_DIR / "members"
BILLS_DIR = BASE_DIR / "bills"
SCAN_PDF_DIR = BASE_DIR / "scan_pdf"


# RAW_DIR = str(BASE_DIR / "raw")
# OUTPUT_DIR = str(BASE_DIR / "output")


#TODO adapt config to reflect bronze, silver, gold medallion architecture, and to include more specific paths for different types of data (e.g. attendance, payments, lobbying, etc.) and for different stages of the pipeline (e.g. raw, processed, etc.). This will help to keep the project organized and make it easier to manage the data and the codebase as the project grows and evolves over time. For example, we could have a directory structure like this:
# data/
# Collect all directories that need to exist
# _DIRS = [
#     # RAW_DIR,
#     # PROCESSED_DIR,
#     # REPORTS_DIR,
#     # CHARTS_DIR,
#     # LOGS_DIR,
#     PDF_PAYMENTS,
#     PDF_MEMBERS,
#     PDF_LOBBYING,
#     BILLS,
#     SCAN_PDF,
#     UTILITY,
# ]
# def init_dirs() -> None:
#     """Create all project directories if they don't exist."""
#     for d in _DIRS:
#         d.mkdir(parents=True, exist_ok=True)


# # Auto-create on import
# init_dirs()


# config.py
# from pathlib import Path
# from dataclasses import dataclass

# @dataclass(frozen=True)
# class Paths:
#     base:      Path
#     data:      Path
#     raw:       Path
#     processed: Path
#     output:    Path
#     logs:      Path

#     @classmethod
#     def from_base(cls, base: Path) -> "Paths":
#         return cls(
#             base      = base,
#             data      = base / "data",
#             raw       = base / "data" / "raw",
#             processed = base / "data" / "processed",
#             output    = base / "output",
#             logs      = base / "logs",
#         )

#     def ensure_dirs(self) -> None:
#         """Create all directories if they don't exist."""
#         for path in [self.data, self.raw, self.processed, self.output, self.logs]:
#             path.mkdir(parents=True, exist_ok=True)


# # Instantiate once, import everywhere
# PATHS = Paths.from_base(Path(__file__).resolve().parent)