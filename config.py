"""
Configuration constants: paths, API endpoints, date ranges.

Pure config — no logging setup, no HTTP, no side effects beyond `init_dirs()`
which runs at import to keep existing scripts working. Use `services.logging_setup`
for logger configuration.
"""

from paths import PROJECT_ROOT

# Single source of truth for the project root lives in paths.py (side-effect-free).
# BASE_DIR is kept as an alias for the ~32 modules that import it from config.
BASE_DIR = PROJECT_ROOT
DATA_DIR = BASE_DIR / "data"
LOG_DIR = BASE_DIR / "logs"

# Medallion architecture layers
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
SILVER_PARQUET_DIR = SILVER_DIR / "parquet"
GOLD_DIR = DATA_DIR / "gold"
GOLD_PARQUET_DIR = GOLD_DIR / "parquet"
GOLD_CSV_DIR = GOLD_DIR / "csv"

# Bronze: PDF and CSV source file locations
BRONZE_PDF_DIR = BRONZE_DIR / "pdfs"
ATTENDANCE_PDF_DIR = BRONZE_PDF_DIR / "attendance"
PAYMENTS_PDF_DIR = BRONZE_PDF_DIR / "payments"
# Seanad sources live in sibling dirs so the deputies-format ETL globs never see
# Senator PDFs; the Senator chain (seanad_refresh.py) globs these instead.
ATTENDANCE_PDF_DIR_SEANAD = BRONZE_PDF_DIR / "attendance_seanad"
PAYMENTS_PDF_DIR_SEANAD = BRONZE_PDF_DIR / "payments_seanad"
INTERESTS_PDF_DIR = BRONZE_DIR / "interests"
LOBBYING_RAW_DIR = BRONZE_DIR / "lobbying_csv_data"
# Bronze: API JSON and member data storage
MEMBERS_DIR = BRONZE_DIR / "members"
LEGISLATION_DIR = BRONZE_DIR / "legislation"
VOTES_DIR = BRONZE_DIR / "votes"
VOTES_RAW_DIR = VOTES_DIR  # back-compat alias — both names point to the same dir
# Bronze: parliamentary questions + debate-section listings JSON
# (consolidated from the former services/dail_config.py duplicate).
QUESTIONS_DIR = BRONZE_DIR / "questions"
DEBATES_DIR = BRONZE_DIR / "debates"
DEBATES_LISTINGS_DIR = DEBATES_DIR / "listings"
AKN_DIR = DEBATES_DIR / "akn"  # reserved for Stage 2 (AKN XML pool)
# Silver: lobbying processed output
LOBBY_OUTPUT_DIR = SILVER_DIR / "lobbying"
LOBBY_PARQUET_DIR = LOBBY_OUTPUT_DIR / "parquet"

# Gold parquet consumed by votes_data and member_overview_data
GOLD_VOTE_HISTORY_PARQUET = GOLD_PARQUET_DIR / "current_dail_vote_history.parquet"
# Seanad gold — produced by enrich.main_seanad(); consumed by the house-aware views.
GOLD_SEANAD_VOTE_HISTORY_PARQUET = GOLD_PARQUET_DIR / "current_seanad_vote_history.parquet"
SEANAD_PAYMENTS_PARQUET = GOLD_PARQUET_DIR / "seanad_payments_full_psa.parquet"
SEANAD_ATTENDANCE_BY_YEAR_PARQUET = GOLD_PARQUET_DIR / "seanad_attendance_by_year.parquet"
# Debates: member-attributed floor speeches (both chambers). Silver is the raw
# parsed transcript; gold joins member identity + language/topic enrichment.
SILVER_SPEECHES_PARQUET = SILVER_PARQUET_DIR / "speeches.parquet"
# Dual artifact: the FULL fact (all years, full speech_text) is gitignored and
# used locally + by the API; the committed `speeches_fact.parquet` is a lite
# slice (recent years, truncated excerpt) small enough for GitHub/Streamlit Cloud
# (which boot from a git clone with no ETL). Views prefer the full file when present.
GOLD_SPEECHES_FACT_FULL_PARQUET = GOLD_PARQUET_DIR / "speeches_fact_full.parquet"
GOLD_SPEECHES_FACT_PARQUET = GOLD_PARQUET_DIR / "speeches_fact.parquet"

API_BASE = "https://api.oireachtas.ie/v1"
VOTES_DATE_START = "2016-01-01"  # cutoff for paginated vote fetch
DATE_RANGE = ("2024-01-01", "2099-01-01")
CHAMBER_DAIL = "chamber=dail"
CHAMBER_SEANAD = "chamber=seanad"
Y_M_D_format = "%Y-%m-%d"

DIRS = [
    DATA_DIR,
    LOG_DIR,
    SILVER_DIR,
    SILVER_PARQUET_DIR,
    GOLD_DIR,
    GOLD_PARQUET_DIR,
    GOLD_CSV_DIR,
    BRONZE_DIR,
    BRONZE_PDF_DIR,
    ATTENDANCE_PDF_DIR,
    PAYMENTS_PDF_DIR,
    ATTENDANCE_PDF_DIR_SEANAD,
    PAYMENTS_PDF_DIR_SEANAD,
    INTERESTS_PDF_DIR,
    LOBBYING_RAW_DIR,
    MEMBERS_DIR,
    LEGISLATION_DIR,
    VOTES_DIR,
    QUESTIONS_DIR,
    DEBATES_DIR,
    DEBATES_LISTINGS_DIR,
    AKN_DIR,
    LOBBY_OUTPUT_DIR,
    LOBBY_PARQUET_DIR,
]


def init_dirs() -> None:
    """Create all project directories if they don't exist."""
    for d in DIRS:
        d.mkdir(parents=True, exist_ok=True)


# Auto-create on import so scripts that write before any explicit setup don't crash.
init_dirs()
