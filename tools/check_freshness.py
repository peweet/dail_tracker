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

from config import (  # noqa: E402
    GOLD_PARQUET_DIR,
    MEMBERS_DIR,
    PROJECT_ROOT,
    QUESTIONS_DIR,
    SILVER_PARQUET_DIR,
    VOTES_DIR,
)

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
#
# STALENESS (status == "stale") — two threshold kinds, chosen to avoid recess noise:
#   fetch_after_days  : age of the FETCH (bronze file mtime, `fetch_file`). Recess-
#       IMMUNE — a Dáil recess stops new *records*, not our *fetching*; this only
#       grows when the pipeline silently stops pulling a source (DAIL-160). Use for
#       the parliamentary API sources whose record dates legitimately go quiet.
#   record_after_days : age of the newest RECORD. Only used for year-round sources
#       (Iris gazette twice-weekly; procurement CSV) that have no recess, so an old
#       newest-record genuinely means the feed stalled.
# A dataset with neither threshold is reported but never marked stale (e.g. lobbying:
# quarterly + manual — record-date age would false-alarm constantly).
DATASETS: dict[str, dict] = {
    "members": {
        # The roster has no record-date column, so freshness = when we last fetched it.
        # FIX (2026-06-06): point at the BRONZE members.json (the actual API pull), not
        # the silver parquet — the silver mtime advances on every flatten even when the
        # underlying bronze is a month stale, which reported a frozen roster as "1d fresh".
        "measure": "fetch_mtime",
        "source": MEMBERS_DIR / "members.json",
        "fetch_after_days": 14,
    },
    "votes": {
        "measure": "record_date",
        "source": GOLD_PARQUET_DIR / "current_dail_vote_history.parquet",
        "column": "date",
        # Divisions go quiet in recess, so gate on FETCH age, not record age.
        "fetch_file": VOTES_DIR / "votes_results.json",
        "fetch_after_days": 14,
    },
    "questions": {
        "measure": "record_date",
        "source": SILVER_PARQUET_DIR / "questions.parquet",
        "column": "question_date",
        "fetch_file": QUESTIONS_DIR / "questions_results.json",
        "fetch_after_days": 14,
    },
    "lobbying": {
        "measure": "period",
        "source": GOLD_PARQUET_DIR / "lobbyist_persistence.parquet",
        "column": "last_return_date",
        # quarterly + manual CSV — no staleness threshold (would false-alarm).
    },
    "iris": {
        "measure": "record_date",
        # Iris is a *source*, not a table — it feeds three gold outputs. Freshness
        # is the latest issue/signed date across all of them. The gazette publishes
        # Tue/Fri year-round (no recess), so record-date age is a safe canary.
        "sources": [
            (GOLD_PARQUET_DIR / "corporate_notices.parquet", "issue_date"),
            (GOLD_PARQUET_DIR / "public_appointments.parquet", "issue_date"),
            (GOLD_PARQUET_DIR / "statutory_instruments.parquet", "si_signed_date"),
        ],
        "record_after_days": 14,
    },
    "corporate": {
        "measure": "record_date",
        "source": GOLD_PARQUET_DIR / "corporate_notices.parquet",
        "column": "issue_date",
        "record_after_days": 14,
    },
    "statutory_instruments": {
        "measure": "record_date",
        "source": GOLD_PARQUET_DIR / "statutory_instruments.parquet",
        "column": "si_signed_date",
        "record_after_days": 21,  # SI signing is lumpier than the twice-weekly gazette
    },
    "procurement": {
        "measure": "record_date",
        # eTenders/OGP awards (full-file CSV, refreshed ~quarterly). The notice date
        # is the raw source string in DD/MM/YYYY, so declare its format.
        "source": GOLD_PARQUET_DIR / "procurement_awards.parquet",
        "column": "Notice Published Date/Contract Created Date",
        "date_format": "%d/%m/%Y",
        "record_after_days": 120,  # ~quarterly source; >4 months means the pull stalled
    },
}


def _rel(path: Path) -> str:
    """Repo-relative, forward-slash path for stable cross-platform output."""
    try:
        return path.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def _as_date_expr(col: str, dtype: pl.DataType, fmt: str | None = None) -> pl.Expr:
    """Coerce a date/datetime/string column to ``pl.Date`` for comparison.

    ``fmt`` is the strptime pattern for string columns whose dates are not ISO
    (e.g. the eTenders DD/MM/YYYY notice date); ISO/typed columns ignore it."""
    if dtype == pl.Utf8:
        return pl.col(col).str.strptime(pl.Date, format=fmt, strict=False)
    return pl.col(col).cast(pl.Date, strict=False)


def _latest_record_date(sources: list[tuple[Path, str]], today: date, fmt: str | None = None) -> tuple[str | None, str]:
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
            lf.select(_as_date_expr(column, dtype, fmt).alias("_d"))
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


def _is_stale(age_days: int | None, threshold: int | None) -> bool:
    """Pure staleness rule: stale iff we have both an age and a threshold and the
    age strictly exceeds it. Missing age or no configured threshold is NOT stale
    (an absent dataset is 'unavailable'; a threshold-less one is informational)."""
    return age_days is not None and threshold is not None and age_days > threshold


def _mtime_age(path: Path, today: date) -> tuple[str | None, int | None]:
    """(iso_utc_timestamp, age_in_days) for a file's mtime, or (None, None)."""
    if not path.exists():
        return None, None
    mtime = datetime.fromtimestamp(path.stat().st_mtime, UTC)
    iso = mtime.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return iso, (today - mtime.date()).days


def _build() -> dict:
    today = datetime.now(UTC).date()
    datasets: dict[str, dict] = {}

    for key, spec in DATASETS.items():
        measure = spec["measure"]
        entry: dict = {"measure": measure}

        if measure == "fetch_mtime":
            source: Path = spec["source"]
            entry["source"] = _rel(source)
            iso, age = _mtime_age(source, today)
            entry["latest_fetch_at"] = iso
            if iso is None:
                entry["status"] = "unavailable"
            else:
                threshold = spec.get("fetch_after_days")
                entry["age_days"] = age
                entry["stale_after_days"] = threshold
                entry["status"] = "stale" if _is_stale(age, threshold) else "ok"

        else:  # record_date / period
            if "sources" in spec:
                pairs = [(Path(p), c) for p, c in spec["sources"]]
                entry["sources"] = [_rel(p) for p, _ in pairs]
            else:
                pairs = [(spec["source"], spec["column"])]
                entry["source"] = _rel(spec["source"])

            iso, status = _latest_record_date(pairs, today, spec.get("date_format"))
            entry["status"] = status
            if measure == "period":
                entry["latest_period_end_date"] = iso
                if iso:
                    entry["period_label"] = _quarter_label(date.fromisoformat(iso))
            else:
                entry["latest_record_date"] = iso

            # Staleness — only when the record date resolved (status ok). Prefer the
            # recess-immune FETCH age where a bronze fetch file is declared; otherwise
            # fall back to the record-date age (year-round sources only).
            if status == "ok":
                if "fetch_file" in spec:
                    f_iso, f_age = _mtime_age(spec["fetch_file"], today)
                    entry["fetched_at"] = f_iso
                    entry["fetch_age_days"] = f_age
                    threshold = spec.get("fetch_after_days")
                    age = f_age
                else:
                    threshold = spec.get("record_after_days")
                    age = (today - date.fromisoformat(iso)).days if iso else None
                    entry["age_days"] = age
                entry["stale_after_days"] = threshold
                if _is_stale(age, threshold):
                    entry["status"] = "stale"

        datasets[key] = entry

    stale = sorted(k for k, e in datasets.items() if e.get("status") == "stale")
    return {
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "as_of_utc_date": today.isoformat(),
        "note": _NOTE,
        "stale_datasets": stale,
        "datasets": datasets,
    }


def _summary_line(key: str, entry: dict) -> str:
    """One ascii-safe line per dataset for the pipeline log."""
    value = (
        entry.get("latest_record_date") or entry.get("latest_period_end_date") or entry.get("latest_fetch_at") or "--"
    )
    extra = f" ({entry['period_label']})" if entry.get("period_label") else ""
    # Surface the age that drives staleness (fetch age preferred, else record/mtime age).
    age = entry.get("fetch_age_days", entry.get("age_days"))
    age_str = ""
    if age is not None:
        thr = entry.get("stale_after_days")
        age_str = f"  [{age}d{'/' + str(thr) + 'd' if thr is not None else ''}]"
    return f"  {key:<22} {entry['status']:<14} {value}{extra}{age_str}"


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

    stale = payload.get("stale_datasets") or []
    if stale:
        print(
            f"\n[STALE] ({len(stale)}): {', '.join(stale)} - a source stopped updating while the "
            "pipeline kept running (e.g. DAIL-160 fetch freeze). Force-refresh: "
            "DAIL_DATA_MAX_AGE_HOURS=0 python bootstrap_refresh.py"
        )

    if args.print:
        sys.stdout.write(orjson.dumps(payload, option=orjson.OPT_INDENT_2).decode() + "\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
