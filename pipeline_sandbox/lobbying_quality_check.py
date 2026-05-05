"""
lobbying_quality_check.py -- bronze-level quality gate for newly-fetched
lobbying.ie CSVs.

STATUS: SANDBOX. Companion to lobbying_fetch.py. Reads raw bronze CSVs,
splits each into kept + quarantined frames against a small set of
shape-level rules, and writes the quarantined rows via the shared
pipeline_sandbox/quarantine.py writer.

This is NOT a replacement for downstream parsing in lobby_processing.py.
It only catches rows that are structurally broken at the bronze layer:
missing Id, unparseable Date Published, malformed Period string, blank
Lobbyist Name. Everything else flows through unchanged.

USAGE:
    python pipeline_sandbox/lobbying_quality_check.py
    python pipeline_sandbox/lobbying_quality_check.py --files Lobbying_ie_returns_results_01_02_2016_to_01_02_2017.csv
    python pipeline_sandbox/lobbying_quality_check.py --pattern "*_2016_to_*"
"""

from __future__ import annotations

import argparse
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
BRONZE_DIR = REPO_ROOT / "data" / "bronze" / "lobbying_csv_data"

sys.path.insert(0, str(Path(__file__).parent))
from quarantine import quarantine  # noqa: E402

SOURCE = "lobbying"

# Rule ids (machine-readable, stable so the SQL view can group by them).
RULE_NULL_ID = "lobbying_null_id"
RULE_BAD_DATE_PUBLISHED = "lobbying_bad_date_published"
RULE_BAD_PERIOD = "lobbying_bad_period"
RULE_NULL_LOBBYIST_NAME = "lobbying_null_lobbyist_name"

# Date Published in raw CSV: "DD/MM/YYYY HH:MM"
DATE_PUBLISHED_FMT = "%d/%m/%Y %H:%M"
# Period in raw CSV: "D MMM, YYYY to D MMM, YYYY"  (eg "1 Sep, 2016 to 31 Dec, 2016")
PERIOD_REGEX = r"^\s*\d{1,2}\s+[A-Za-z]{3,9},\s+\d{4}\s+to\s+\d{1,2}\s+[A-Za-z]{3,9},\s+\d{4}\s*$"


def make_run_id() -> str:
    """ISO timestamp + short uuid; mirrors the manifest layer's run id shape."""
    ts = datetime.now(UTC).isoformat(timespec="seconds")
    return f"{ts}-{uuid.uuid4().hex[:8]}"


def check_one_file(path: Path, run_id: str) -> dict:
    """Apply rules to one file. Quarantine failed rows. Return per-file summary."""
    df = pl.read_csv(path, infer_schema_length=0)  # all-string read; we validate ourselves

    total = df.height
    summary = {
        "file": path.name,
        "rows_in": total,
        "rows_kept": total,
        "quarantined": {},
        "quarantine_files": [],
    }
    if total == 0:
        return summary

    # Rule 1: null/empty Id
    if "Id" in df.columns:
        bad_id = df.filter(pl.col("Id").is_null() | (pl.col("Id").str.strip_chars() == ""))
        if not bad_id.is_empty():
            out = quarantine(
                bad_id, source=SOURCE, rule=RULE_NULL_ID,
                reason="Id is null or empty",
                run_id=f"{run_id}_{path.stem}_{RULE_NULL_ID}",
            )
            summary["quarantined"][RULE_NULL_ID] = bad_id.height
            summary["quarantine_files"].append(str(out.relative_to(REPO_ROOT)))
            df = df.filter(pl.col("Id").is_not_null() & (pl.col("Id").str.strip_chars() != ""))

    # Rule 2: unparseable Date Published
    if "Date Published" in df.columns:
        parsed = df.with_columns(
            pl.col("Date Published")
            .str.strptime(pl.Datetime, format=DATE_PUBLISHED_FMT, strict=False)
            .alias("__dp_parsed")
        )
        bad_dp = parsed.filter(
            pl.col("Date Published").is_not_null()
            & (pl.col("Date Published").str.strip_chars() != "")
            & pl.col("__dp_parsed").is_null()
        ).drop("__dp_parsed")
        if not bad_dp.is_empty():
            out = quarantine(
                bad_dp, source=SOURCE, rule=RULE_BAD_DATE_PUBLISHED,
                reason=f"Date Published does not match format {DATE_PUBLISHED_FMT}",
                run_id=f"{run_id}_{path.stem}_{RULE_BAD_DATE_PUBLISHED}",
            )
            summary["quarantined"][RULE_BAD_DATE_PUBLISHED] = bad_dp.height
            summary["quarantine_files"].append(str(out.relative_to(REPO_ROOT)))
            keep_mask = parsed["__dp_parsed"].is_not_null() | parsed["Date Published"].is_null() | (
                parsed["Date Published"].str.strip_chars() == ""
            )
            df = df.filter(keep_mask)

    # Rule 3: malformed Period string
    if "Period" in df.columns:
        bad_period = df.filter(
            pl.col("Period").is_not_null()
            & (pl.col("Period").str.strip_chars() != "")
            & ~pl.col("Period").str.contains(PERIOD_REGEX)
        )
        if not bad_period.is_empty():
            out = quarantine(
                bad_period, source=SOURCE, rule=RULE_BAD_PERIOD,
                reason="Period does not match 'D MMM, YYYY to D MMM, YYYY' shape",
                run_id=f"{run_id}_{path.stem}_{RULE_BAD_PERIOD}",
            )
            summary["quarantined"][RULE_BAD_PERIOD] = bad_period.height
            summary["quarantine_files"].append(str(out.relative_to(REPO_ROOT)))
            df = df.filter(
                pl.col("Period").is_null()
                | (pl.col("Period").str.strip_chars() == "")
                | pl.col("Period").str.contains(PERIOD_REGEX)
            )

    # Rule 4: null/empty Lobbyist Name (orphan row -- no filing entity)
    if "Lobbyist Name" in df.columns:
        bad_lob = df.filter(
            pl.col("Lobbyist Name").is_null() | (pl.col("Lobbyist Name").str.strip_chars() == "")
        )
        if not bad_lob.is_empty():
            out = quarantine(
                bad_lob, source=SOURCE, rule=RULE_NULL_LOBBYIST_NAME,
                reason="Lobbyist Name is null or empty",
                run_id=f"{run_id}_{path.stem}_{RULE_NULL_LOBBYIST_NAME}",
            )
            summary["quarantined"][RULE_NULL_LOBBYIST_NAME] = bad_lob.height
            summary["quarantine_files"].append(str(out.relative_to(REPO_ROOT)))
            df = df.filter(
                pl.col("Lobbyist Name").is_not_null()
                & (pl.col("Lobbyist Name").str.strip_chars() != "")
            )

    summary["rows_kept"] = df.height
    return summary


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    p.add_argument("--files", nargs="*", help="explicit file names (relative to bronze dir)")
    p.add_argument("--pattern", default="Lobbying_ie_returns_results_*_to_*.csv",
                   help="glob pattern within bronze dir")
    args = p.parse_args(argv)

    if args.files:
        targets = [BRONZE_DIR / f for f in args.files]
    else:
        targets = sorted(BRONZE_DIR.glob(args.pattern))

    targets = [t for t in targets if t.is_file() and t.parent == BRONZE_DIR]
    if not targets:
        print(f"No files matched in {BRONZE_DIR.relative_to(REPO_ROOT)}", file=sys.stderr)
        return 1

    run_id = make_run_id()
    print(f"Lobbying quality check -- run_id={run_id}")
    print(f"Files: {len(targets)}")
    print("=" * 60)

    grand = {"rows_in": 0, "rows_kept": 0, "quarantined": 0}
    for path in targets:
        s = check_one_file(path, run_id)
        print(f"\n{s['file']}")
        print(f"  rows in    : {s['rows_in']:>7,}")
        print(f"  rows kept  : {s['rows_kept']:>7,}")
        if s["quarantined"]:
            for rule, n in s["quarantined"].items():
                print(f"  quarantined: {n:>7,}   rule={rule}")
            for qf in s["quarantine_files"]:
                print(f"  -> {qf}")
        else:
            print("  quarantined: 0  (clean)")
        grand["rows_in"] += s["rows_in"]
        grand["rows_kept"] += s["rows_kept"]
        grand["quarantined"] += sum(s["quarantined"].values())

    print("\n" + "=" * 60)
    print(f"TOTAL  in: {grand['rows_in']:,}  kept: {grand['rows_kept']:,}  quarantined: {grand['quarantined']:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
