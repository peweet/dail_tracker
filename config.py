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

# Pipeline-end data-age signal (tools/check_freshness.py writes it; CI canaries
# and the Streamlit provenance lines read it). Mirrored in utility/config.py;
# keep both in sync (dual-config convention).
FRESHNESS_JSON = DATA_DIR / "_meta" / "freshness.json"

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

# ── UI constants mirrored from utility/config.py ─────────────────────────────────
# The repo has TWO config.py (root = paths/pipeline; utility/ = Streamlit UI). A
# Streamlit page does `from config import NOTABLE_TDS`, but page-import order can
# bind sys.modules['config'] to THIS root module (it's what dail_tracker_core
# imports for GOLD_PARQUET_DIR etc.). Mirroring these two display constants here
# makes the page import resolve regardless of which config wins — the documented
# dual-config rule: a name a page imports must exist in BOTH or it breaks under
# one resolution order while passing under the other. Keep in sync with
# utility/config.py (these change at most once per year).
NOTABLE_TDS: list[str] = [
    "Mary Lou McDonald",
    "Micheál Martin",
    "Simon Harris",
    "Leo Varadkar",
    "Pearse Doherty",
    "Eamon Ryan",
    "Michael Healy-Rae",
    "Danny Healy-Rae",
    "Michael Collins",
    "Michael Lowry",
    "Marian Harkin",
    "Holly Cairns",
    "Robert Troy",
    "Pauline Tully",
]

# Official plenary sitting-day counts (Houses of the Oireachtas Commission
# reports). Used ONLY as a loose cross-check against the data-derived count — the
# UI denominator now comes from v_attendance_chamber_sitting_days (distinct sitting
# dates actually in the TAA record) for BOTH chambers, so a member can never show
# more sitting days than the denominator. test_attendance_data_consistency.py
# reconciles this dict against the data and would have caught the old 2025 entry.
#
# NOTE: the AUTHORITATIVE per-year sitting-day denominator now lives in the
# committed, source-derived reference data/_meta/official_sitting_days.csv (built
# by tools/curate_official_sitting_days.py from the TAA PDFs; the union of distinct
# sitting dates). test/pipeline/test_attendance_official_sitting_days.py pins the
# live pipeline to it. This Commission dict may legitimately run a few days higher
# (a scheduled sitting day where no member's attendance was captured won't appear
# in the TAA record), which is why it stays a tolerance cross-check, not the value.
#
# 2025 is intentionally ABSENT: the prior value (82, copied from 2020) was a
# stale placeholder, while the record holds 94 distinct sitting dates — the exact
# "82 scheduled days vs 94 recorded" contradiction this rework fixes. Add 2025
# here only once the official Commission figure for 2025 is published, and only
# if it is >= the data-derived count. Keep in sync with utility/config.py.
SITTING_DAYS_BY_YEAR: dict[int, int] = {
    2020: 82,
    2021: 94,
    2022: 106,
    2023: 100,
    2024: 83,
}

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


# ── UI-constant bridge (two-config trap mitigation, 2026-06-11) ─────────────────
# The repo has TWO modules named `config` (this file and utility/config.py);
# which one an import resolves to depends on sys.path order at import time.
# Code under utility/ and dail_tracker_core/ runs in BOTH resolution contexts
# (live Streamlit app vs pytest/pipeline), so every symbol it pulls from
# `config` must exist in both modules. UI constants live in utility/config.py
# as their single source; this block re-exports any public UPPERCASE symbol
# not already defined above so both resolutions agree without hand-copying.
# Guarded by test/test_config_parity.py. Kill this bridge when the reorg moves
# config into a package (one import name, no ambiguity).
def _bridge_ui_constants() -> None:
    import importlib.util as _ilu

    _ui_path = BASE_DIR / "utility" / "config.py"
    _spec = _ilu.spec_from_file_location("_dail_tracker_ui_config", _ui_path)
    if _spec is None or _spec.loader is None:  # pragma: no cover — repo layout broken
        return
    _ui = _ilu.module_from_spec(_spec)
    _spec.loader.exec_module(_ui)
    for _name, _value in vars(_ui).items():
        if _name.isupper() and not _name.startswith("_") and _name not in globals():
            globals()[_name] = _value


_bridge_ui_constants()
