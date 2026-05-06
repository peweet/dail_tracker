"""
pipeline_sandbox/quarantine.py

Prototype of the shared per-source quarantine writer described in
[quarantine_plan.md](quarantine_plan.md). Implements the writer signature,
the row shape, and a smoke test that round-trips a fake quarantine through
the proposed file layout.

Sandbox-only. Does NOT modify pipeline.py, enrich.py, normalise_join_key.py,
or any silver writer. Existing parsers (attendance.py, lobby_processing.py,
member_interests.py, legislation.py, payments.py) keep working unchanged.

What this file IS:
  - The proposed `quarantine(...)` signature.
  - A worked example showing the four-column metadata shape.
  - A standalone smoke test (`python pipeline_sandbox/quarantine.py`) that
    writes a fake quarantine to a tmp dir and reads it back.

What this file IS NOT:
  - Wired into the pipeline. Nothing here is called from pipeline.py yet.
  - A general data-quality framework. It does not run validation rules; it
    only persists rows that callers have already classified as failed.
  - A replacement for the existing `quarantined_payment_tables.parquet` —
    payments.py keeps its bespoke layout until slice 3 of the rollout.

Reading order:
  1. quarantine_plan.md  — the why, the file-layout decision, validating links
  2. this file           — the how, narrowly
  3. (future) services/quarantine.py — once promoted out of sandbox

Run as a smoke test:
  python pipeline_sandbox/quarantine.py
"""

from __future__ import annotations

import tempfile
from datetime import UTC, datetime
from pathlib import Path

import polars as pl


QUARANTINE_DIR = Path("data/silver/_quarantine")

_META_COLS = ("_quarantine_rule", "_quarantine_reason", "_run_id", "_quarantined_at")


def quarantine(
    df_failed: pl.DataFrame,
    *,
    source: str,
    rule: str,
    reason: str,
    run_id: str,
    out_dir: Path = QUARANTINE_DIR,
) -> Path:
    """
    Persist rejected rows to the per-source quarantine table.

    Parameters
    ----------
    df_failed : pl.DataFrame
        The rows that failed validation, with the source's original schema.
        Empty frames are accepted and write nothing — callers do not need
        to guard.
    source : str
        Stable per-source name. One of: "payments", "attendance",
        "member_interests", "lobbying", "legislation". Used in the output
        filename and as the partition key for the SQL view layer.
    rule : str
        Machine-readable rule id, e.g. "taa_band_unrecognised",
        "name_regex_failed", "collective_dpo_filter". Stable identifier so
        the SQL view layer can group by it.
    reason : str
        Human-readable one-line explanation. Free-form. Goes in the UI
        sample alongside the row.
    run_id : str
        The pipeline run id from manifest.create_run_manifest(). Ties the
        quarantine row back to the run that rejected it.
    out_dir : Path
        Override for tests; defaults to data/silver/_quarantine/.

    Returns
    -------
    Path
        Path to the file written. Caller can ignore unless asserting in a
        test.

    File layout — Option A from the plan doc:
        data/silver/_quarantine/<source>_<run_id>.parquet

    The original row's columns are preserved verbatim. Four metadata
    columns are appended. If the metadata column names already exist in
    the source schema, callers should rename upstream — this writer does
    not silently overwrite.
    """
    if df_failed.is_empty():
        return out_dir / f"{source}_{_safe(run_id)}.parquet"  # not written

    overlap = set(df_failed.columns) & set(_META_COLS)
    if overlap:
        raise ValueError(
            f"Source schema for {source!r} collides with reserved quarantine "
            f"metadata columns: {sorted(overlap)}. Rename upstream."
        )

    now = datetime.now(UTC).isoformat(timespec="seconds")
    annotated = df_failed.with_columns(
        pl.lit(rule).alias("_quarantine_rule"),
        pl.lit(reason).alias("_quarantine_reason"),
        pl.lit(run_id).alias("_run_id"),
        pl.lit(now).alias("_quarantined_at"),
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{source}_{_safe(run_id)}.parquet"
    annotated.write_parquet(out_path)
    return out_path


def _safe(run_id: str) -> str:
    # run_id contains ':' from the ISO timestamp, which is illegal in
    # Windows filenames. Mirror the substitution the manifest layer would
    # use if it ever wrote run-named files itself.
    return run_id.replace(":", "-")


# --- worked example: how attendance.py would call this ---------------------
#
# At [attendance.py:39](../attendance.py#L39) the regex filter currently looks
# roughly like:
#
#     if IRISH_NAME_REGEX.search(line) and not EXCLUDE_CASES.search(line):
#         ...keep...
#
# After wiring, the same filter would split into kept + failed and call
# quarantine() on the failed set:
#
#     mask = lines.str.contains(IRISH_NAME_REGEX) & ~lines.str.contains(EXCLUDE_CASES)
#     kept   = df.filter(mask)
#     failed = df.filter(~mask)
#     if not failed.is_empty():
#         quarantine(
#             failed,
#             source="attendance",
#             rule="name_regex_failed",
#             reason="Line did not match IRISH_NAME_REGEX or matched EXCLUDE_CASES",
#             run_id=run_id,
#         )
#
# No other change to the parser. The kept rows continue down the existing
# silver-write path unchanged.


# --- smoke test ------------------------------------------------------------

def _smoke() -> None:
    """Round-trip a fake quarantine through a tmp dir."""
    fake_failed = pl.DataFrame(
        {
            "Full_Name": ["Kenny", "2/MIN", "MIN"],
            "TAA_Band": ["Kenny", "2/MIN", "MIN"],
            "Amount": [None, "1234.56", "789.00"],
        }
    )

    with tempfile.TemporaryDirectory() as tmp:
        out = quarantine(
            fake_failed,
            source="payments",
            rule="taa_band_unrecognised",
            reason="Value not in clean band set",
            run_id="2026-04-30T08:14:11+00:00-a3f9c812",
            out_dir=Path(tmp),
        )
        assert out.exists(), f"quarantine file not written: {out}"

        round_trip = pl.read_parquet(out)
        assert round_trip.height == fake_failed.height
        for col in _META_COLS:
            assert col in round_trip.columns, f"missing metadata column: {col}"
        assert (round_trip["_quarantine_rule"] == "taa_band_unrecognised").all()

        print(f"[ok] wrote {out.name} with {round_trip.height} rows")
        print(f"[ok] metadata columns present: {list(_META_COLS)}")
        print("[ok] schema:")
        print(round_trip.schema)

    # empty-frame behaviour: no file written, no exception
    with tempfile.TemporaryDirectory() as tmp:
        out = quarantine(
            fake_failed.head(0),
            source="payments",
            rule="taa_band_unrecognised",
            reason="empty",
            run_id="2026-04-30T08:14:11+00:00-empty",
            out_dir=Path(tmp),
        )
        assert not out.exists(), "empty frame should write no file"
        print("[ok] empty-frame call writes no file")

    # collision guard: pre-existing _run_id column should refuse, not overwrite
    bad = fake_failed.with_columns(pl.lit("oops").alias("_run_id"))
    try:
        quarantine(
            bad,
            source="payments",
            rule="x",
            reason="x",
            run_id="r",
            out_dir=Path(tempfile.gettempdir()),
        )
    except ValueError as e:
        print(f"[ok] collision guard fired: {e}")
    else:
        raise AssertionError("collision guard did not fire")


if __name__ == "__main__":
    _smoke()
