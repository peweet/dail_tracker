"""tools/check_freshness.py — derive a data-age signal for each domain.

Writes ``data/_meta/freshness.json``: one entry per dataset recording the
latest record date (or fetch timestamp) we currently hold, the source it came
from, and a status.

What this measures — and what it does NOT
-----------------------------------------
This is the *age of the data we already have*. It is a canary for "did the
pipeline silently stop running?". It is **not** proof that no newer upstream
data exists — a quiet week with no new divisions looks identical to "we failed
to fetch new divisions". Only the source pollers (``*_poller.py``) can tell
those apart. Treat this file as a staleness canary, not a missed-update
detector.

Why generate at pipeline-end (not in CI)
-----------------------------------------
Committed gold alone cannot see ``questions`` (silver) or ``members`` (a fetch
timestamp, no record-date column). This script runs as the final pipeline chain
where silver + gold are both present, computes everything, and the result is
committed alongside the refreshed data. CI and the Streamlit badge only *read*
the committed JSON — the UI never touches parquet (the logic firewall forbids
raw reads in the UI layer), and CI never regenerates it.

Future-dated placeholder guard
-------------------------------
Iris can emit future-dated placeholder PDFs (see repair_future_iris_placeholders
.py), so a naive ``max(signed_date)`` can return a future date that makes stale
data look fresh. Every record-date measure filters ``<= today`` first.

Usage:
    python tools/check_freshness.py            # write data/_meta/freshness.json
    python tools/check_freshness.py --print    # also echo the result to stdout
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import orjson
import polars as pl

# tools/ is not on sys.path when this runs as a script; put the repo root first
# so `config` (and the modules it pulls in) import the same way the pipeline's
# root-level chains do.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import GOLD_PARQUET_DIR, PROJECT_ROOT, SILVER_PARQUET_DIR  # noqa: E402

OUTPUT_PATH = PROJECT_ROOT / "data" / "_meta" / "freshness.json"

_NOTE = (
    "Age of the data already held — a canary for 'did the pipeline stop running'. "
    "NOT proof that no newer upstream data exists; only the source pollers can "
    "confirm that. Generated at pipeline-end (sees silver + gold) and read-only "
    "thereafter (Streamlit badge + scheduled report)."
)


# Each dataset declares how its freshness is measured. Three measures:
#   record_date  — max of a date column, filtered to <= today (one source)
#   record_date  — max across several (source, column) pairs (iris spans 3 golds)
#   period       — like record_date but rendered as a quarter (lobbying is quarterly)
#   fetch_mtime  — file mtime; for state-snapshot tables with no record date (members)
DATASETS: dict[str, dict] = {
    "members": {
        "measure": "fetch_mtime",
        "source": SILVER_PARQUET_DIR / "flattened_members.parquet",
    },
    "votes": {
        "measure": "record_date",
        "source": GOLD_PARQUET_DIR / "current_dail_vote_history.parquet",
        "column": "date",
    },
    "questions": {
        "measure": "record_date",
        "source": SILVER_PARQUET_DIR / "questions.parquet",
        "column": "question_date",
    },
    "lobbying": {
        "measure": "period",
        "source": GOLD_PARQUET_DIR / "lobbyist_persistence.parquet",
        "column": "last_return_date",
    },
    "iris": {
        "measure": "record_date",
        # Iris is a *source*, not a table — it feeds three gold outputs. Freshness
        # is the latest issue/signed date across all of them.
        "sources": [
            (GOLD_PARQUET_DIR / "corporate_notices.parquet", "issue_date"),
            (GOLD_PARQUET_DIR / "public_appointments.parquet", "issue_date"),
            (GOLD_PARQUET_DIR / "statutory_instruments.parquet", "si_signed_date"),
        ],
    },
    "corporate": {
        "measure": "record_date",
        "source": GOLD_PARQUET_DIR / "corporate_notices.parquet",
        "column": "issue_date",
    },
    "statutory_instruments": {
        "measure": "record_date",
        "source": GOLD_PARQUET_DIR / "statutory_instruments.parquet",
        "column": "si_signed_date",
    },
}


def _rel(path: Path) -> str:
    """Repo-relative, forward-slash path for stable cross-platform output."""
    try:
        return path.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def _as_date_expr(col: str, dtype: pl.DataType) -> pl.Expr:
    """Coerce a date/datetime/string column to ``pl.Date`` for comparison."""
    if dtype == pl.Utf8:
        return pl.col(col).str.strptime(pl.Date, strict=False)
    return pl.col(col).cast(pl.Date, strict=False)


def _latest_record_date(sources: list[tuple[Path, str]], today: date) -> tuple[str | None, str]:
    """Max date <= today across one or more (path, column) pairs.

    Returns (iso_date_or_none, status). status is ``ok`` when at least one
    source yielded a valid past date, else the most informative failure reason.
    """
    best: date | None = None
    statuses: list[str] = []
    for path, column in sources:
        if not path.exists():
            statuses.append("unavailable")
            continue
        lf = pl.scan_parquet(path)
        if column not in lf.collect_schema().names():
            statuses.append("missing_column")
            continue
        dtype = lf.collect_schema()[column]
        out = (
            lf.select(_as_date_expr(column, dtype).alias("_d"))
            .filter(pl.col("_d") <= pl.lit(today))
            .select(pl.col("_d").max())
            .collect()
        )
        val = out["_d"][0]
        if val is None:
            statuses.append("no_valid_dates")
            continue
        statuses.append("ok")
        if best is None or val > best:
            best = val
    if best is not None:
        return best.isoformat(), "ok"
    # No source produced a date — surface the single distinct reason if there is
    # one, else a generic note.
    distinct = set(statuses)
    if distinct == {"unavailable"}:
        return None, "unavailable"
    return None, "no_valid_dates" if "no_valid_dates" in distinct else "unavailable"


def _quarter_label(d: date) -> str:
    return f"Q{(d.month - 1) // 3 + 1} {d.year}"


def _build() -> dict:
    today = datetime.now(UTC).date()
    datasets: dict[str, dict] = {}

    for key, spec in DATASETS.items():
        measure = spec["measure"]
        entry: dict = {"measure": measure}

        if measure == "fetch_mtime":
            source: Path = spec["source"]
            entry["source"] = _rel(source)
            if source.exists():
                mtime = datetime.fromtimestamp(source.stat().st_mtime, UTC)
                entry["latest_fetch_at"] = mtime.replace(microsecond=0).isoformat().replace("+00:00", "Z")
                entry["status"] = "ok"
            else:
                entry["latest_fetch_at"] = None
                entry["status"] = "unavailable"

        else:  # record_date / period
            if "sources" in spec:
                pairs = [(Path(p), c) for p, c in spec["sources"]]
                entry["sources"] = [_rel(p) for p, _ in pairs]
            else:
                pairs = [(spec["source"], spec["column"])]
                entry["source"] = _rel(spec["source"])

            iso, status = _latest_record_date(pairs, today)
            entry["status"] = status
            if measure == "period":
                entry["latest_period_end_date"] = iso
                if iso:
                    entry["period_label"] = _quarter_label(date.fromisoformat(iso))
            else:
                entry["latest_record_date"] = iso

        datasets[key] = entry

    return {
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "as_of_utc_date": today.isoformat(),
        "note": _NOTE,
        "datasets": datasets,
    }


def _summary_line(key: str, entry: dict) -> str:
    """One ascii-safe line per dataset for the pipeline log."""
    value = (
        entry.get("latest_record_date") or entry.get("latest_period_end_date") or entry.get("latest_fetch_at") or "--"
    )
    extra = f" ({entry['period_label']})" if entry.get("period_label") else ""
    return f"  {key:<22} {entry['status']:<14} {value}{extra}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--print", action="store_true", help="echo the result to stdout")
    args = parser.parse_args(argv)

    payload = _build()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_bytes(orjson.dumps(payload, option=orjson.OPT_INDENT_2))

    print(f"freshness: wrote {_rel(OUTPUT_PATH)} ({payload['generated_at']})")
    for key, entry in payload["datasets"].items():
        print(_summary_line(key, entry))

    if args.print:
        sys.stdout.write(orjson.dumps(payload, option=orjson.OPT_INDENT_2).decode() + "\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
