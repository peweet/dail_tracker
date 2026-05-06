import logging
from pathlib import Path

from services.http_engine import fetch_all
from services.logging_setup import setup_logging
from services.members import get_or_create_member_df
from services.storage import output_exists, result_file_path, save_json
from services.urls import (
    build_debates_day_urls,
    build_legislation_urls,
    build_questions_urls,
)
from services.votes import fetch_votes

logger = logging.getLogger(__name__)


def run_member_scenario(
    scenario_name: str,
    urls: list[str],
    overwrite: bool = False,
    max_workers: int = 5,
) -> None:
    output_path = result_file_path(scenario_name)

    if output_exists(output_path, overwrite=overwrite):
        return

    logger.info("=" * 70)
    logger.info(f"Starting scenario: {scenario_name}")
    logger.info("=" * 70)

    results, total_bytes, failures = fetch_all(urls, max_workers=max_workers)
    save_json(results, output_path)

    logger.info(
        f"Finished scenario='{scenario_name}' | "
        f"results={len(results)} | failures={failures} | bytes={total_bytes:,}"
    )


def _load_debates_worklist() -> list[tuple[str, str]]:
    """Read deduplicated (date, chamber) pairs from dbsect_index.parquet.

    The harvester (pipeline_sandbox/dbsect_harvest.py for now, graduates
    later) writes silver/parquet/dbsect_index.parquet which already carries
    date + chamber per dbsect. Returns an empty list when the index is
    absent so the pipeline can run end-to-end before the harvester graduates.
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
            "dbsect_index.parquet not found at %s — run "
            "pipeline_sandbox/dbsect_harvest.py before this scenario "
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


def run_votes(overwrite: bool = False) -> None:
    output_path = result_file_path("votes")

    if output_exists(output_path, overwrite=overwrite):
        return

    logger.info("=" * 70)
    logger.info("Starting votes scenario")
    logger.info("=" * 70)

    votes, vote_bytes = fetch_votes()
    save_json(votes, output_path)

    logger.info(f"Finished votes | batches={len(votes)} | bytes={vote_bytes:,}")


def main() -> None:
    setup_logging()

    logger.info("Starting Oireachtas API pipeline...")

    # Change these when you want to force re-runs
    overwrite_members = False
    overwrite_legislation = False
    overwrite_questions = False
    overwrite_votes = False
    overwrite_debates_listings = False

    logger.info("=" * 70)
    logger.info("STEP 1: Preparing member dataframe")
    logger.info("=" * 70)

    member_df = get_or_create_member_df(overwrite_members=overwrite_members)
    logger.info(f"Member dataframe contains {member_df.height} unique members")

    logger.info("=" * 70)
    logger.info("STEP 2: Building URLs")
    logger.info("=" * 70)

    legislation_urls = build_legislation_urls(member_df)
    questions_urls = build_questions_urls(member_df)

    logger.info("=" * 70)
    logger.info("STEP 3: Fetching member-based scenarios")
    logger.info("=" * 70)

    run_member_scenario(
        scenario_name="legislation",
        urls=legislation_urls,
        overwrite=overwrite_legislation,
        max_workers=5,
    )

    run_member_scenario(
        scenario_name="questions",
        urls=questions_urls,
        overwrite=overwrite_questions,
        max_workers=5,
    )

    run_member_scenario(
        scenario_name="legislation_unscoped",
        urls=legislation_urls,
        overwrite=overwrite_legislation,
        max_workers=5,
    )

    logger.info("=" * 70)
    logger.info("STEP 4: Fetching votes")
    logger.info("=" * 70)

    run_votes(overwrite=overwrite_votes)

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
    )

    logger.info("=" * 70)
    logger.info("✓ Oireachtas API pipeline complete.")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()