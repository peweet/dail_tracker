"""
Post-pipeline cleanup.

Removes intermediate files produced during the pipeline run.
Safe to call after pipeline.py completes.

Assumes the future medallion layout where raw/intermediate PDFs and
API responses land in data/raw/ and data/intermediate/ — those directories
are wiped in full when they exist.

Files in data/silver/ and data/gold/ are the final outputs and are never touched.
PDFs in members/pdf_*/ are source data and are never touched.
"""

import logging
import shutil
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

_ROOT = Path(__file__).parent


def _delete_file(path: Path) -> None:
    if path.exists():
        path.unlink()
        log.info("deleted  %s", path.relative_to(_ROOT))
    else:
        log.debug("absent   %s", path.relative_to(_ROOT))


def _delete_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
        log.info("rmtree   %s", path.relative_to(_ROOT))
    else:
        log.debug("absent   %s", path.relative_to(_ROOT))


def _delete_glob(directory: Path, pattern: str) -> None:
    for f in directory.glob(pattern):
        f.unlink()
        log.info("deleted  %s", f.relative_to(_ROOT))


def clean_members_json() -> None:
    """Raw API member responses and intermediate filtered copies."""
    members_dir = _ROOT / "members"
    for name in [
        "members.json",
        "members_dail.json",
        "members_seanad.json",
        "filtered_members.json",
        "filtered_members_dail.json",
        "filtered_members_seanad.json",
    ]:
        _delete_file(members_dir / name)

    # Year-stamped register-of-interests JSON responses
    _delete_glob(members_dir, "*_dail.json")
    _delete_glob(members_dir, "*_seanad.json")


def clean_member_interest_year_csvs() -> None:
    """Per-year interest CSVs that are rolled up into *_combined.csv."""
    silver = _ROOT / "data" / "silver"
    _delete_glob(silver, "dail_member_interests_grouped_*.csv")
    _delete_glob(silver, "seanad_member_interests_grouped_*.csv")


def clean_bills() -> None:
    """Flattened bills CSVs and intermediate JSON — not used by the Streamlit app."""
    bills_dir = _ROOT / "bills"
    for name in [
        "all_bills_by_td.json",
        "legislation_results.json",
        "flattened_bills.csv",
        "new_flattened_bills.csv",
        "drop_cols_flattened_bills.csv",
    ]:
        _delete_file(bills_dir / name)


def clean_bronze() -> None:
    """Raw API JSON responses saved to data/bronze/."""
    bronze = _ROOT / "data" / "bronze"
    for name in [
        "legislation_results.json",
        "questions_results.json",
    ]:
        _delete_file(bronze / name)

def clean_vote_jsons() -> None:
    """Raw vote API responses under data/ — converted to pretty_votes.csv."""
    _delete_glob(_ROOT / "data", "vote_*.json")


def clean_lobbying_raw() -> None:
    """Intermediate stacking files in lobbyist/raw/ — rolled up into lobbyist/output/."""
    raw = _ROOT / "lobbyist" / "raw"
    for name in [
        "cleaned.csv",
        "cleaned_output.csv",
        "filtered_lobby.json",
    ]:
        _delete_file(raw / name)

    # The original Lobbying_ie_returns_results_*.csv source files stay —
    # they are the canonical raw export from lobbying.ie and are re-used on
    # each pipeline run.  Delete only the derived intermediates above.


def clean_members_duplicates() -> None:
    """CSV copies in members/ that duplicate gold/ outputs."""
    members_dir = _ROOT / "members"
    for name in [
        "enriched_td_attendance.csv",  # canonical copy lives in data/gold/
        "member_interests_combined.csv",  # canonical copy lives in data/silver/
    ]:
        _delete_file(members_dir / name)


# ── Future medallion directories ─────────────────────────────────────────────


def clean_raw_intermediate() -> None:
    """
    Wipe data/raw/ and data/intermediate/ in full.

    These directories don't exist yet — they are part of the planned medallion
    layout where all API responses and PDF extracts land before promotion to
    silver/gold.  Once the pipeline writes there, this function handles the wipe.
    """
    _delete_dir(_ROOT / "data" / "raw")
    _delete_dir(_ROOT / "data" / "intermediate")


# ── Pipeline log ──────────────────────────────────────────────────────────────


def clean_pipeline_log() -> None:
    _delete_file(_ROOT / "pipeline.log")


# ── Entry point ───────────────────────────────────────────────────────────────


def run() -> None:
    log.info("=== tear_down: starting cleanup ===")
    clean_members_json()
    clean_member_interest_year_csvs()
    clean_bills()
    clean_bronze()
    clean_vote_jsons()
    clean_lobbying_raw()
    clean_members_duplicates()
    clean_raw_intermediate()
    clean_pipeline_log()
    log.info("=== tear_down: done ===")


if __name__ == "__main__":
    run()
