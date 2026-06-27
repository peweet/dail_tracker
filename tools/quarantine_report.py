"""Consolidated quarantine ledger — the single, traceable record of every value the
fidelity / contract gates held back from the app.

The engine already writes per-resource evidence to ``data/_meta/quarantine/`` (full
offending rows as ``<name>_quarantine.parquet`` + a ``<name>_quarantine.json`` summary).
This reporter GLOBS those and folds them into ONE file —
``data/_meta/quarantine_ledger.json`` — shaped like ``fetch_failures.json`` (a timestamp
plus a map keyed by resource) so it reads as a native ``_meta`` artifact and can later
merge into a single data-health surface.

Why a reporter and not a live append: the per-resource parquets are written race-free by
each gate; aggregating them here means no shared mutable file as the pipeline goes parallel
(same pattern as the ``*_coverage.json`` files). Run it at the end of a pipeline pass, or
ad hoc, to refresh the ledger and print a human-readable trace:

    ./.venv/Scripts/python.exe tools/quarantine_report.py

Each ledger row carries WHAT was held (the offending column + its value), WHY
(``_quarantine_reason``) and — for OCR-derived facts — WHERE it came from (``source_pdf`` +
``source_page``), so a held figure is traceable back to the exact page. ``source_url`` is
captured when the frame carries it; resolving a filename → download URL via the source
registry is a small follow-on.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

QUARANTINE_DIR = ROOT / "data" / "_meta" / "quarantine"
LEDGER_PATH = ROOT / "data" / "_meta" / "quarantine_ledger.json"

# Provenance columns we surface per held row when the frame carries them — enough to trace
# a figure back to its source. Order = display priority.
PROVENANCE_CANDIDATES: tuple[str, ...] = (
    "source_pdf",
    "source_page",
    "source_url",
    "source_file",
    "url",
    "page",
    "publisher_name",
    "publisher_id",
    "supplier_raw",
    "donor_name",
    "candidate_name",
    "candidate_name_raw",
    "party",
    "name",
)
_REASON = "_quarantine_reason"


def _resource_name(parquet: Path) -> str:
    stem = parquet.stem
    return stem[: -len("_quarantine")] if stem.endswith("_quarantine") else stem


def build_ledger(
    quarantine_dir: Path = QUARANTINE_DIR,
    ledger_path: Path | None = LEDGER_PATH,
    *,
    max_rows_per_resource: int = 100,
    now: str | None = None,
) -> dict:
    """Fold every ``*_quarantine.parquet`` under ``quarantine_dir`` into one ledger dict.

    Writes it to ``ledger_path`` (unless None) and returns it. ``now`` lets a caller pin
    the timestamp (tests); otherwise it is stamped at call time.
    """
    resources: dict[str, dict] = {}
    total_held = 0

    for parquet in sorted(quarantine_dir.glob("*_quarantine.parquet")):
        name = _resource_name(parquet)
        df = pl.read_parquet(parquet)
        total_held += df.height

        offending_cols: set[str] = set()
        if _REASON in df.columns:
            for reason in df[_REASON].drop_nulls().to_list():
                offending_cols.update(c for c in str(reason).split(";") if c)

        present_prov = [c for c in PROVENANCE_CANDIDATES if c in df.columns]
        # one row = reason + the offending column value(s) + whatever provenance exists.
        keep = list(
            dict.fromkeys(
                ([_REASON] if _REASON in df.columns else [])
                + [c for c in sorted(offending_cols) if c in df.columns]
                + present_prov
            )
        )
        rows = df.select(keep).head(max_rows_per_resource).to_dicts() if keep else []

        entry: dict = {
            "n_held": df.height,
            "offending_columns": sorted(offending_cols),
            "provenance_columns": present_prov,
            "quarantine_parquet": str(parquet.relative_to(ROOT)),
            "rows": rows,
        }
        summary_json = quarantine_dir / f"{name}_quarantine.json"
        if summary_json.exists():
            try:
                s = json.loads(summary_json.read_text(encoding="utf-8"))
                entry["run_utc"] = s.get("generated_utc")
                entry["n_rows_total"] = s.get("n_rows_total")
                entry["frac_quarantined"] = s.get("frac_quarantined")
                entry["breaches"] = s.get("breaches")
            except (json.JSONDecodeError, OSError):
                pass
        resources[name] = entry

    ledger = {
        "generated_utc": now or datetime.now(UTC).isoformat(),
        "n_resources": len(resources),
        "n_rows_held": total_held,
        "note": "Values held back from the app by the fidelity/contract gates. Empty == healthy. "
        "Full offending rows in each resource's sibling .parquet.",
        "resources": resources,
    }
    if ledger_path is not None:
        ledger_path.parent.mkdir(parents=True, exist_ok=True)
        ledger_path.write_text(json.dumps(ledger, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    return ledger


def _trace_line(name: str, entry: dict) -> str:
    bits = [f"{name}: {entry['n_held']} row(s) held"]
    if entry.get("offending_columns"):
        bits.append("cols=" + ",".join(entry["offending_columns"]))
    sample = entry["rows"][0] if entry.get("rows") else {}
    prov = {k: sample[k] for k in ("source_pdf", "source_page", "source_url") if k in sample}
    if sample:
        first_val = {c: sample.get(c) for c in entry.get("offending_columns", []) if c in sample}
        bits.append(f"e.g. {first_val} from {prov}" if prov else f"e.g. {first_val}")
    return " | ".join(bits)


def main() -> int:
    ledger = build_ledger()
    print(f"=== quarantine ledger -> {LEDGER_PATH.relative_to(ROOT)} ===")
    if not ledger["resources"]:
        print("clean: nothing held back across any resource.")
        return 0
    print(f"{ledger['n_rows_held']} row(s) held across {ledger['n_resources']} resource(s):")
    for name, entry in sorted(ledger["resources"].items()):
        print("  " + _trace_line(name, entry))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
