"""tools/freshness_report.py — read-only staleness canary over freshness.json.

READ-ONLY companion to ``tools/check_freshness.py``. That script GENERATES
``data/_meta/freshness.json`` at pipeline-end (it needs silver + gold present);
this one only READS the committed JSON, so it runs anywhere with zero
third-party deps (stdlib only) — including a scheduled GitHub Action that has no
pipeline data and no project sync.

What it alerts on — and why
---------------------------
The alert fires on the age of ``generated_at`` (when the pipeline last ran),
NOT on per-dataset record dates. ``generated_at`` advances on every pipeline
run regardless of whether new upstream data existed, so it is the clean "did
the pipeline silently stop running?" canary. Per-dataset record dates are
noisier: votes/questions legitimately go quiet during Dáil recess, lobbying is
quarterly — alerting on those would false-alarm constantly. So per-dataset ages
are PRINTED as context but never trigger the alert.

This is a staleness canary, not a missed-update detector: a quiet week with no
new divisions looks identical to "we failed to fetch new divisions". Only the
source pollers can tell those apart (see check_freshness.py).

It does NOT refresh anything. Making data fresh means running the pipeline (the
deferred cloud-automation work); this only reports how old the committed data is.

Exit code:
    0  fresh — generated_at within the threshold and JSON well-formed
    1  stale — generated_at older than --max-age-days, OR json missing/malformed
A scheduled workflow turns exit 1 into a GitHub issue (.github/workflows/freshness.yml).

Usage:
    python tools/freshness_report.py                  # default 14-day threshold
    python tools/freshness_report.py --max-age-days 10
    FRESHNESS_MAX_AGE_DAYS=21 python tools/freshness_report.py
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, date, datetime
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_FRESHNESS_JSON = _PROJECT_ROOT / "data" / "_meta" / "freshness.json"

# Default "the pipeline should have run by now" budget: the manual refresh
# cadence (~weekly) plus buffer. Tune via --max-age-days or FRESHNESS_MAX_AGE_DAYS.
_DEFAULT_MAX_AGE_DAYS = 14


def _parse_iso_date(value: str | None) -> date | None:
    """Parse an ISO date or 'Z'-suffixed datetime to a date; None on failure."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None


def _dataset_value(entry: dict) -> str:
    """The single most relevant date/label a dataset entry carries."""
    value = (
        entry.get("latest_record_date") or entry.get("latest_period_end_date") or entry.get("latest_fetch_at") or "--"
    )
    label = entry.get("period_label")
    return f"{value} ({label})" if label else value


def _report(max_age_days: int, json_path: Path = _FRESHNESS_JSON) -> int:
    """Print a freshness report; return 0 if fresh, 1 if stale/missing/malformed."""
    if not json_path.exists():
        print(f"FRESHNESS: ERROR — {json_path} not found.")
        print("  The pipeline writes it at the end of a run (tools/check_freshness.py).")
        return 1

    try:
        payload = json.loads(json_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        print(f"FRESHNESS: ERROR — could not read freshness.json: {exc}")
        return 1

    generated_at = payload.get("generated_at")
    generated_date = _parse_iso_date(generated_at)
    if generated_date is None:
        print(f"FRESHNESS: ERROR — missing/invalid 'generated_at' ({generated_at!r}).")
        return 1

    today = datetime.now(UTC).date()
    age_days = (today - generated_date).days

    print(f"FRESHNESS REPORT  (as of {today.isoformat()} UTC)")
    print(f"  pipeline last ran: {generated_at}  ->  {age_days}d ago  (threshold {max_age_days}d)")
    print("  per-dataset data age (context only — recess/quarterly gaps are normal):")
    for key, entry in (payload.get("datasets") or {}).items():
        status = entry.get("status", "?")
        print(f"    {key:<22} {status:<14} {_dataset_value(entry)}")

    # Surface non-ok dataset statuses as a warning, but they do not fail the run —
    # only the generated_at age (the pipeline-ran canary) gates the exit code.
    not_ok = [k for k, e in (payload.get("datasets") or {}).items() if e.get("status") != "ok"]
    if not_ok:
        print(f"  note: datasets not 'ok' in last run: {', '.join(not_ok)}")

    if age_days > max_age_days:
        print(
            f"\nSTALE: the pipeline has not produced fresh data in {age_days}d "
            f"(> {max_age_days}d). Likely the scheduled/manual pipeline run stopped. "
            "Run pipeline.py and commit the refreshed gold + freshness.json."
        )
        return 1

    print(f"\nOK: data is {age_days}d old, within the {max_age_days}d threshold.")
    return 0


def main(argv: list[str] | None = None) -> int:
    env_default = os.environ.get("FRESHNESS_MAX_AGE_DAYS")
    parser = argparse.ArgumentParser(description="Read-only data-freshness canary over freshness.json.")
    parser.add_argument(
        "--max-age-days",
        type=int,
        default=int(env_default) if env_default and env_default.isdigit() else _DEFAULT_MAX_AGE_DAYS,
        help="Alert if the pipeline last ran more than this many days ago (default 14; or set FRESHNESS_MAX_AGE_DAYS).",
    )
    args = parser.parse_args(argv)
    return _report(args.max_age_days)


if __name__ == "__main__":
    sys.exit(main())
