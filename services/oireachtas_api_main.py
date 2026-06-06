import logging
import os
from pathlib import Path

from services.dbsect_harvest import harvest_dbsect_index
from services.http_engine import fetch_all
from services.legislation_unscoped import fetch_all_bills
from services.logging_setup import setup_logging
from services.member_paginated import fetch_all_member_paginated
from services.members import get_or_create_member_df
from services.storage import output_exists, result_file_path, save_json
from services.urls import (
    build_debates_day_urls,
    build_legislation_url,
    build_questions_url,
)
from services.votes import fetch_votes

logger = logging.getLogger(__name__)

# DAIL-160 guard: how old a cached bronze JSON may be before a run refetches it.
# Without this, every scenario short-circuits on a bare path.exists() and a daily
# cron silently freezes members/questions/votes/legislation/debates at first-run
# values. 18h means a once-a-day cron always refetches, while two interactive runs
# in the same session reuse the cache. Override with DAIL_DATA_MAX_AGE_HOURS
# (e.g. 0 to force a full refetch; a large value to restore the old skip-if-exists
# behaviour for offline dev).
DATA_MAX_AGE_HOURS: float = float(os.environ.get("DAIL_DATA_MAX_AGE_HOURS", "18"))


def run_member_scenario(
    scenario_name: str,
    urls: list[str],
    overwrite: bool = False,
    max_workers: int = 5,
    max_age_hours: float | None = None,
) -> None:
    output_path = result_file_path(scenario_name)

    if output_exists(output_path, overwrite=overwrite, max_age_hours=max_age_hours):
        return

    logger.info("=" * 70)
    logger.info(f"Starting scenario: {scenario_name}")
    logger.info("=" * 70)

    results, total_bytes, failures = fetch_all(urls, max_workers=max_workers)
    save_json(results, output_path)

    logger.info(
        f"Finished scenario='{scenario_name}' | results={len(results)} | failures={failures} | bytes={total_bytes:,}"
    )


def run_member_scenario_paginated(
    scenario_name: str,
    member_df,
    url_builder,
    overwrite: bool = False,
    max_workers: int = 5,
    max_age_hours: float | None = None,
) -> None:
    """Paginated alternative to run_member_scenario.

    The single-fetch run_member_scenario silently truncates members past the
    1000-row API server cap. This runner loops skip+=1000 per member until
    head.counts.resultCount is satisfied; output shape matches what
    questions.py / legislation.py already iterate.
    """
    output_path = result_file_path(scenario_name)

    if output_exists(output_path, overwrite=overwrite, max_age_hours=max_age_hours):
        return

    logger.info("=" * 70)
    logger.info(f"Starting paginated scenario: {scenario_name}")
    logger.info("=" * 70)

    results, total_bytes = fetch_all_member_paginated(
        member_df,
        url_builder=url_builder,
        scenario_label=scenario_name,
        max_workers=max_workers,
    )
    save_json(results, output_path)

    logger.info(f"Finished paginated scenario='{scenario_name}' | members={len(results)} | bytes={total_bytes:,}")


def _load_debates_worklist() -> list[tuple[str, str]]:
    """Read deduplicated (date, chamber) pairs from dbsect_index.parquet.

    The harvester (services/dbsect_harvest.py, run in STEP 4.5) writes
    silver/parquet/dbsect_index.parquet which already carries date + chamber
    per dbsect. Returns an empty list when the index is absent so the
    pipeline can run end-to-end even if STEP 4.5 was skipped.
    """
    try:
        import polars as pl  # local import — keeps services/__init__.py light
    except ImportError:
        logger.warning("polars not available; debates_listings worklist empty")
        return []

    from config import SILVER_PARQUET_DIR

    index_path = Path(SILVER_PARQUET_DIR) / "dbsect_index.parquet"
    if not index_path.exists():
        logger.warning(
            "dbsect_index.parquet not found at %s — STEP 4.5 "
            "(services/dbsect_harvest.py) must run before this scenario "
            "to populate the worklist.",
            index_path,
        )
        return []

    df = pl.read_parquet(index_path)
    if df.is_empty():
        return []

    pairs_df = (
        df.filter(pl.col("date").is_not_null() & (pl.col("chamber") != ""))
        .select(["date", "chamber"])
        .unique()
        .sort(["date", "chamber"])
    )
    return [(row["date"], row["chamber"]) for row in pairs_df.iter_rows(named=True)]


def run_votes(overwrite: bool = False, max_age_hours: float | None = None) -> None:
    output_path = result_file_path("votes")

    if output_exists(output_path, overwrite=overwrite, max_age_hours=max_age_hours):
        return

    logger.info("=" * 70)
    logger.info("Starting votes scenario")
    logger.info("=" * 70)

    votes, vote_bytes = fetch_votes()
    save_json(votes, output_path)

    logger.info(f"Finished votes | batches={len(votes)} | bytes={vote_bytes:,}")


def run_legislation_unscoped(overwrite: bool = False, max_age_hours: float | None = None) -> None:
    """Fetch every Bill (Government + Private Member) — unscoped pagination.

    Consumed by legislation.py, which reads legislation_results_unscoped.json
    instead of the per-TD legislation_results.json so Government bills are
    not silently dropped.
    """
    output_path = result_file_path("legislation_unscoped")

    if output_exists(output_path, overwrite=overwrite, max_age_hours=max_age_hours):
        return

    logger.info("=" * 70)
    logger.info("Starting legislation_unscoped scenario")
    logger.info("=" * 70)

    payload, total_bytes = fetch_all_bills()
    save_json(payload, output_path)

    bills = payload[0]["results"] if payload else []
    logger.info(f"Finished legislation_unscoped | bills={len(bills)} | bytes={total_bytes:,}")


def main() -> None:
    setup_logging()

    logger.info("Starting Oireachtas API pipeline...")

    # Change these when you want to force a full re-fetch. Routine freshness is
    # handled by DATA_MAX_AGE_HOURS below (DAIL-160): a cached JSON older than the
    # threshold is refetched even with overwrite=False, so a daily cron no longer
    # freezes on the first run's data.
    overwrite_members = False
    overwrite_legislation = False
    overwrite_questions = False
    overwrite_votes = False
    overwrite_debates_listings = False
    max_age = DATA_MAX_AGE_HOURS
    logger.info("Cache freshness: refetch any cached scenario older than %.0fh", max_age)

    logger.info("=" * 70)
    logger.info("STEP 1: Preparing member dataframe")
    logger.info("=" * 70)

    member_df = get_or_create_member_df(overwrite_members=overwrite_members, max_age_hours=max_age)
    logger.info(f"Member dataframe contains {member_df.height} unique members")

    logger.info("=" * 70)
    logger.info("STEP 2: Fetching member-based scenarios (paginated)")
    logger.info("=" * 70)

    # Both /v1/legislation?member_id=… and /v1/questions?member_id=… cap each
    # response at 1000 rows server-side. The paginated runner loops skip+=1000
    # per member and asserts the running total reaches head.counts.resultCount.
    # Before this change, 79 of 174 members were silently truncated on
    # questions (~150k rows lost). Legislation was less affected (most members
    # have <1000 sponsored bills) but the same fix applies for hygiene.
    run_member_scenario_paginated(
        scenario_name="legislation",
        member_df=member_df,
        url_builder=build_legislation_url,
        overwrite=overwrite_legislation,
        max_workers=5,
        max_age_hours=max_age,
    )

    run_member_scenario_paginated(
        scenario_name="questions",
        member_df=member_df,
        url_builder=build_questions_url,
        overwrite=overwrite_questions,
        max_workers=5,
        max_age_hours=max_age,
    )

    run_legislation_unscoped(overwrite=overwrite_legislation, max_age_hours=max_age)

    logger.info("=" * 70)
    logger.info("STEP 4: Fetching votes")
    logger.info("=" * 70)

    run_votes(overwrite=overwrite_votes, max_age_hours=max_age)

    logger.info("=" * 70)
    logger.info("STEP 4.5: Harvesting dbsect index from bronze")
    logger.info("=" * 70)
    # Contain a harvester failure: a malformed bronze JSON must not abort
    # the whole Members API step (and with it STEP 5). Degrade to "no
    # debate listings this run" - matches the DAIL-163 continue-past-
    # failure philosophy in pipeline.py.
    try:
        n_dbsect = harvest_dbsect_index()
        logger.info(f"dbsect index harvested: {n_dbsect} rows")
    except Exception as e:
        logger.error(f"dbsect harvest failed (debate listings will be skipped): {e}")

    logger.info("=" * 70)
    logger.info("STEP 5: Fetching debates day-window listings")
    logger.info("=" * 70)

    debates_pairs = _load_debates_worklist()
    debates_urls = build_debates_day_urls(debates_pairs)
    run_member_scenario(
        scenario_name="debates_listings",
        urls=debates_urls,
        overwrite=overwrite_debates_listings,
        max_workers=5,
        max_age_hours=max_age,
    )

    logger.info("=" * 70)
    logger.info("✓ Oireachtas API pipeline complete.")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
