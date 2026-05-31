"""
Sandbox validation tests for attendance_sandbox.py.

Strategy:
  - Golden test: run sandbox.main() against the REAL bronze PDFs, writing
    output to a tmp dir. Compare tmp output to the canonical files in
    data/silver/ produced by the unmodified attendance.py. Validates the
    refactor is byte-or-content-equivalent.
  - Empty test: run with an empty bronze dir → assert exit 0, no files written.
  - Skip-when-silver-exists test: copy canonical silver CSV into tmp,
    run main() → assert PDF processing is skipped and fact table still
    produces a byte-identical output to canonical.

All writes go to tmp_path. Canonical data/silver/ is read-only.

Run from repo root:
    pytest pipeline_sandbox/_phase1_refactor/test_attendance_sandbox.py -v -s
"""
from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SANDBOX_DIR = Path(__file__).resolve().parent

# Ensure sandbox dir is importable and repo root is on path for `config` import.
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(SANDBOX_DIR))

CANONICAL_SILVER = REPO_ROOT / "data" / "silver"
CANONICAL_AGG_CSV = CANONICAL_SILVER / "aggregated_td_tables.csv"
CANONICAL_FACT_CSV = CANONICAL_SILVER / "td_attendance_fact_table.csv"
CANONICAL_FACT_PARQUET = CANONICAL_SILVER / "parquet" / "td_attendance_fact_table.parquet"
REAL_ATTENDANCE_PDFS = REPO_ROOT / "data" / "bronze" / "pdfs" / "attendance"


def _fresh_sandbox_import(monkeypatch, silver_dir: Path, pdf_dir: Path):
    """Import attendance_sandbox cleanly and patch its captured paths.

    Removes any cached module so the import is fresh and our monkeypatches
    aren't applied on top of a stale namespace.
    """
    sys.modules.pop("attendance_sandbox", None)
    import attendance_sandbox  # noqa: PLC0415 — intentional fresh import

    monkeypatch.setattr(attendance_sandbox, "SILVER_DIR", silver_dir)
    monkeypatch.setattr(attendance_sandbox, "ATTENDANCE_PDF_DIR", pdf_dir)
    return attendance_sandbox


def _assert_frames_equivalent(produced_path: Path, canonical_path: Path, label: str) -> None:
    """Content equivalence even if bytes differ (float repr / row order)."""
    produced = pd.read_csv(produced_path)
    canonical = pd.read_csv(canonical_path)

    assert list(produced.columns) == list(canonical.columns), (
        f"{label}: column order differs.\n"
        f"  produced:  {list(produced.columns)}\n"
        f"  canonical: {list(canonical.columns)}"
    )
    assert len(produced) == len(canonical), (
        f"{label}: row count differs. produced={len(produced):,} canonical={len(canonical):,}"
    )

    # Sort both by all columns for deterministic comparison (dedup order may shift).
    sort_cols = [c for c in produced.columns if produced[c].dtype != "float64" or not produced[c].isna().any()]
    if sort_cols:
        produced_sorted = produced.sort_values(sort_cols, ignore_index=True, na_position="last")
        canonical_sorted = canonical.sort_values(sort_cols, ignore_index=True, na_position="last")
    else:
        produced_sorted = produced
        canonical_sorted = canonical

    pd.testing.assert_frame_equal(
        produced_sorted,
        canonical_sorted,
        check_dtype=False,  # CSV roundtrip can shift int↔float for NA cols
        check_like=False,
        obj=label,
    )


@pytest.mark.skipif(
    not REAL_ATTENDANCE_PDFS.exists() or not any(REAL_ATTENDANCE_PDFS.glob("*.pdf")),
    reason="No real attendance PDFs in data/bronze/pdfs/attendance/ — skipping golden test",
)
@pytest.mark.skipif(
    not CANONICAL_AGG_CSV.exists() or not CANONICAL_FACT_CSV.exists(),
    reason="Canonical silver CSVs missing — cannot golden-compare",
)
def test_golden_full_pipeline_matches_canonical(monkeypatch, tmp_path):
    """Run sandbox against REAL PDFs, compare output to canonical silver files.

    SLOW — processes all attendance PDFs (~minutes). Worth it: this is the
    only test that proves the refactor is logic-preserving end-to-end.
    """
    silver = tmp_path / "silver"
    (silver / "parquet").mkdir(parents=True)

    sandbox = _fresh_sandbox_import(monkeypatch, silver, REAL_ATTENDANCE_PDFS)
    rc = sandbox.main()
    assert rc == 0, f"sandbox.main() returned {rc}, expected 0"

    produced_agg = silver / "aggregated_td_tables.csv"
    produced_fact = silver / "td_attendance_fact_table.csv"
    produced_parquet = silver / "parquet" / "td_attendance_fact_table.parquet"

    assert produced_agg.exists(), "aggregated_td_tables.csv not written"
    assert produced_fact.exists(), "td_attendance_fact_table.csv not written"
    assert produced_parquet.exists(), "td_attendance_fact_table.parquet not written"

    _assert_frames_equivalent(produced_agg, CANONICAL_AGG_CSV, "aggregated_td_tables.csv")
    _assert_frames_equivalent(produced_fact, CANONICAL_FACT_CSV, "td_attendance_fact_table.csv")

    # Parquet comparison via dataframe equivalence (binary parquet encoding
    # is non-deterministic across writes — timestamps, row group layout).
    produced_parquet_df = pd.read_parquet(produced_parquet)
    canonical_parquet_df = pd.read_parquet(CANONICAL_FACT_PARQUET)
    sort_cols = ["identifier", "year", "iso_sitting_days_attendance", "iso_other_days_attendance"]
    sort_cols = [c for c in sort_cols if c in produced_parquet_df.columns]
    produced_sorted = produced_parquet_df.sort_values(sort_cols, ignore_index=True, na_position="last")
    canonical_sorted = canonical_parquet_df.sort_values(sort_cols, ignore_index=True, na_position="last")
    pd.testing.assert_frame_equal(produced_sorted, canonical_sorted, check_dtype=False, obj="parquet")


def test_empty_bronze_dir_skips_cleanly(monkeypatch, tmp_path):
    """No PDFs → exit 0, no silver written, no crash."""
    empty_bronze = tmp_path / "bronze"
    empty_bronze.mkdir()
    silver = tmp_path / "silver"
    (silver / "parquet").mkdir(parents=True)

    sandbox = _fresh_sandbox_import(monkeypatch, silver, empty_bronze)
    rc = sandbox.main()
    assert rc == 0, f"empty-bronze run returned {rc}, expected 0 (clean skip)"

    assert not (silver / "aggregated_td_tables.csv").exists(), (
        "Silver CSV should not be written when no PDFs are present"
    )
    assert not (silver / "td_attendance_fact_table.csv").exists(), (
        "Fact CSV should not be written when no PDFs are present"
    )


@pytest.mark.skipif(
    not CANONICAL_AGG_CSV.exists() or not CANONICAL_FACT_CSV.exists(),
    reason="Canonical silver CSVs missing — cannot test skip-when-exists branch",
)
def test_skip_when_silver_already_exists(monkeypatch, tmp_path):
    """If aggregated_td_tables.csv already exists, PDFs are not re-processed;
    fact table is still rebuilt from the existing silver CSV and matches canonical.
    """
    silver = tmp_path / "silver"
    (silver / "parquet").mkdir(parents=True)
    shutil.copy(CANONICAL_AGG_CSV, silver / "aggregated_td_tables.csv")

    # Point bronze at an empty dir to PROVE the PDF branch isn't entered
    # (would otherwise skip-and-return early on empty bronze).
    empty_bronze = tmp_path / "bronze_empty"
    empty_bronze.mkdir()

    sandbox = _fresh_sandbox_import(monkeypatch, silver, empty_bronze)
    rc = sandbox.main()
    assert rc == 0

    produced_fact = silver / "td_attendance_fact_table.csv"
    assert produced_fact.exists()
    _assert_frames_equivalent(produced_fact, CANONICAL_FACT_CSV, "td_attendance_fact_table.csv (skip branch)")
