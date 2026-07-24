"""SOURCE-FIDELITY audit — which fields does a source offer that never reach a fact?

Every existing quality gate in this repo asks "is the value we extracted CORRECT?" (reconcile
gates, row-floor guards, vocab contracts, never-sum invariants). None asks "is there a field in
the source we are IGNORING?" — so a column that is parsed and then discarded produces no error,
no warning and no failing test.

That blind spot hid the AFS capital funding split for months: the extractor read all 10 columns
of the statutory appendix and persisted 4, dropping Grants and LPT / Non-Mortgage Loans / Other.
It surfaced only because someone asked a question the data could not answer and then opened the
raw PDF. See the method note in memory (feedback_source_fidelity_audit_method).

WHAT THIS DOES
    Reads ONLY the field names of each bronze source (CSV/XLSX header row, JSON object keys) —
    never the row data, so it is cheap and safe on multi-GB files — then compares them against
    the column names of every silver/gold parquet. A source field whose normalised name appears
    in NO fact is a CANDIDATE DROP.

WHAT IT IS NOT
    A defect list. A candidate can be legitimate:
      * deliberately excluded (PII, e.g. donor addresses; personal-insolvency names);
      * renamed on the way through (`Sum of Awarded Value (€)` -> `value_eur`);
      * an intermediate the pipeline derives something else from.
    So the output RANKS candidates for a human to judge. Treat it as leads, never as verdicts.

LIMITS (be honest about these when reporting)
    * Finds only fields we PARSED AND DROPPED. It cannot see data a publisher never released
      (e.g. a council that does not publish older registers), nor fields in a document SECTION
      the parser never opens (the AFS notes needed a human to establish they carry nothing).
    * PDF sources are excluded: "what the source offers" only exists after parsing, so they
      need a per-extractor probe rather than a header read.
    * Name-based matching. A field that survives under a very different name reads as dropped.

Run:
    ./.venv/Scripts/python.exe tools/audit_source_fidelity.py
    ./.venv/Scripts/python.exe tools/audit_source_fidelity.py --json data/_meta/source_fidelity.json
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

BRONZE = ROOT / "data" / "bronze"
FACT_DIRS = [ROOT / "data" / "silver", ROOT / "data" / "gold"]

# Reading a header is cheap; parsing a 900 MB JSON is not. JSON above this is peeked only.
JSON_FULL_READ_BYTES = 20_000_000
PEEK_BYTES = 400_000
# Formats whose field names can be read without parsing the payload.
TABULAR = {".csv", ".xlsx", ".xls", ".json"}


# Bookkeeping artefacts, not data sources: download manifests, resume checkpoints, file
# indexes. Their "fields" (sha256, local_path, bytes) are OURS, so of course they never reach a
# fact — including them buried the real signal on the first run.
_ARTEFACT_RE = re.compile(r"(^_|/_|manifest|_ckpt|checkpoint|index|coverage|failures)", re.I)

# Words that carry no distinguishing meaning when matching a source field to a fact column.
_STOP = {
    "the", "a", "an", "of", "or", "and", "any", "who", "was", "were", "is", "are", "on", "in",
    "for", "to", "by", "this", "that", "with", "activities", "activity", "details", "detail",
    "name", "names", "value", "values", "date", "dates", "number", "no", "s",
}


def norm(name: str) -> str:
    """Normalise a field name for cross-format comparison: lowercase alphanumerics only."""
    return re.sub(r"[^a-z0-9]", "", name.lower())


def tokens(name: str) -> set[str]:
    """Significant word-stems of a field name, for tolerant concept matching.

    Exact normalised equality is too brittle: the lobbying source field 'DPOs Lobbied' maps to
    the column 'dpo_lobbied', but `dposlobbied` != `dpolobbied`, so the first version of this
    audit reported a field that plainly survives. Singular/plural is stripped and stop-words
    dropped so the comparison is about CONCEPT overlap, not spelling.
    """
    raw = re.split(r"[^a-z0-9]+", name.lower())
    out = set()
    for w in raw:
        if not w or w in _STOP:
            continue
        out.add(w[:-1] if len(w) > 3 and w.endswith("s") else w)
    return out


def is_covered(field: str, persisted_norm: set[str], persisted_tokens: list[set[str]]) -> bool:
    """True if this source field plausibly survives into some fact.

    Two tiers, both deliberately generous — a FALSE 'dropped' wastes a human's time, which is
    the failure mode that made the first run useless:
      1. exact normalised-name hit;
      2. all of the field's significant tokens appear in one fact column's tokens.
    """
    n = norm(field)
    if not n:
        return True
    if n in persisted_norm:
        return True
    ft = tokens(field)
    if not ft:
        return True
    return any(ft <= pt for pt in persisted_tokens)


def _csv_fields(path: Path) -> list[str]:
    for enc in ("utf-8-sig", "latin-1"):
        try:
            with path.open("r", encoding=enc, newline="") as fh:
                head = fh.readline()
                if not head:
                    return []
                dialect_delim = max(",;\t|", key=head.count)
                return [c.strip() for c in next(csv.reader([head], delimiter=dialect_delim))]
        except (UnicodeDecodeError, csv.Error, StopIteration):
            continue
    return []


def _xlsx_fields(path: Path) -> list[str]:
    try:
        import openpyxl
    except ImportError:
        return []
    try:
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    except Exception:
        return []
    out: list[str] = []
    try:
        for ws in wb.worksheets[:3]:  # a few sheets is plenty to characterise the source
            for row in ws.iter_rows(min_row=1, max_row=1, values_only=True):
                out += [str(c).strip() for c in row if c is not None]
                break
    except Exception:
        pass
    finally:
        wb.close()
    return out


def _json_fields(path: Path) -> list[str]:
    """Top-level object keys, or the keys of the first record in a list-of-records."""
    size = path.stat().st_size
    try:
        if size <= JSON_FULL_READ_BYTES:
            obj = json.loads(path.read_text(encoding="utf-8", errors="replace"))
        else:
            # Peek: pull the first complete-looking object out of the head of the file.
            head = path.open("r", encoding="utf-8", errors="replace").read(PEEK_BYTES)
            m = re.search(r"\{.*?\}", head, re.S)
            if not m:
                return []
            obj = json.loads(m.group(0))
    except (json.JSONDecodeError, OSError, ValueError):
        return []
    if isinstance(obj, list):
        obj = next((o for o in obj if isinstance(o, dict)), None)
    if isinstance(obj, dict):
        # One level of nesting: a wrapper like {"results": [...]} hides the real record.
        for v in obj.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return sorted({*obj.keys(), *v[0].keys()})
        return sorted(obj.keys())
    return []


def source_fields(path: Path) -> list[str]:
    ext = path.suffix.lower()
    if ext == ".csv":
        return _csv_fields(path)
    if ext in (".xlsx", ".xls"):
        return _xlsx_fields(path)
    if ext == ".json":
        return _json_fields(path)
    return []


def persisted_columns() -> tuple[set[str], list[set[str]], dict[str, list[str]]]:
    """Every column across every silver/gold parquet -> (normalised set, token sets, by file)."""
    import polars as pl

    allcols: set[str] = set()
    toks: list[set[str]] = []
    by_file: dict[str, list[str]] = {}
    seen_tok: set[frozenset] = set()
    for d in FACT_DIRS:
        if not d.exists():
            continue
        for p in sorted(d.rglob("*.parquet")):
            try:
                names = list(pl.read_parquet_schema(p).keys())
            except Exception:
                continue
            by_file[p.name] = names
            for c in names:
                allcols.add(norm(c))
                t = tokens(c)
                if t and frozenset(t) not in seen_tok:
                    seen_tok.add(frozenset(t))
                    toks.append(t)
    return allcols, toks, by_file


BASELINE = ROOT / "tools" / "baselines" / "source_fidelity_baseline.json"


def _dropped_map(findings: list[dict]) -> dict[str, list[str]]:
    """{'family.ext': sorted dropped-field names} — the shape the ratchet compares."""
    return {f"{f['family']}{f['ext']}": sorted(f["dropped_fields"]) for f in findings if f["dropped_fields"]}


def new_drops(current: dict[str, list[str]], baseline: dict[str, list[str]]) -> dict[str, list[str]]:
    """Fields a source drops NOW that the baseline did not — the one-way ratchet's trip set.

    A field dropped in the baseline and still dropped = accepted, no trip. A field the source
    stops dropping (we captured it, or it went away) = never trips. Only a genuinely NEW dropped
    field fires — that is a publisher adding a column we ignore (the 'Payment Currency' failure).
    """
    out: dict[str, list[str]] = {}
    for src, fields in current.items():
        unseen = sorted(set(fields) - set(baseline.get(src, [])))
        if unseen:
            out[src] = unseen
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--json", type=Path, help="write the full report here")
    ap.add_argument("--min-fields", type=int, default=3, help="ignore sources with fewer fields")
    ap.add_argument("--top", type=int, default=30, help="rows to print")
    ap.add_argument("--write-baseline", action="store_true", help="snapshot the current dropped set as the accepted baseline")
    ap.add_argument("--check", action="store_true", help="fail (exit 3) if a source now drops a field NOT in the baseline")
    args = ap.parse_args()

    print("reading persisted fact columns ...")
    persisted, persisted_tokens, by_file = persisted_columns()
    print(f"  {len(by_file)} parquet facts, {len(persisted)} distinct normalised column names")

    print("reading bronze source headers (names only, no row data) ...")
    # Group by (top-level bronze dir, extension): one representative per group is enough to
    # characterise a source family, and it keeps 25k files tractable.
    groups: dict[tuple[str, str], list[Path]] = defaultdict(list)
    n_artefact = 0
    for p in BRONZE.rglob("*"):
        if not (p.is_file() and p.suffix.lower() in TABULAR):
            continue
        rel = p.relative_to(BRONZE)
        if _ARTEFACT_RE.search(rel.as_posix()):
            n_artefact += 1  # our own bookkeeping, not a source
            continue
        groups[(rel.parts[0], p.suffix.lower())].append(p)
    print(f"  skipped {n_artefact} manifest/checkpoint/index files (our bookkeeping, not sources)")

    findings = []
    for (family, ext), paths in sorted(groups.items()):
        rep = max(paths, key=lambda x: x.stat().st_size)  # richest file in the family
        fields = source_fields(rep)
        if len(fields) < args.min_fields:
            continue
        dropped = [f for f in fields if not is_covered(f, persisted, persisted_tokens)]
        findings.append(
            {
                "family": family,
                "ext": ext,
                "n_files": len(paths),
                "representative": str(rep.relative_to(ROOT)).replace("\\", "/"),
                "n_source_fields": len(fields),
                "n_dropped": len(dropped),
                "pct_dropped": round(100 * len(dropped) / max(len(fields), 1)),
                "dropped_fields": dropped[:40],
            }
        )

    # Rank by how much of the source is unaccounted for, then by how many files it affects.
    findings.sort(key=lambda f: (-f["pct_dropped"], -f["n_files"]))

    print()
    print(f"{'family':28} {'ext':6} {'files':>6} {'src':>5} {'drop':>5} {'%':>4}")
    print("-" * 62)
    for f in findings[: args.top]:
        print(
            f"{f['family'][:28]:28} {f['ext']:6} {f['n_files']:>6} "
            f"{f['n_source_fields']:>5} {f['n_dropped']:>5} {f['pct_dropped']:>3}%"
        )

    total_dropped = sum(f["n_dropped"] for f in findings)
    print()
    print(f"{len(findings)} source families examined; {total_dropped} candidate dropped fields.")
    print("CANDIDATES, NOT DEFECTS — a field may be deliberately excluded (PII), renamed, or")
    print("an intermediate. Judge each before acting.")

    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(findings, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"wrote {args.json}")

    current = _dropped_map(findings)

    if args.write_baseline:
        BASELINE.parent.mkdir(parents=True, exist_ok=True)
        BASELINE.write_text(json.dumps(current, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
        print(f"\nwrote baseline {BASELINE} ({sum(len(v) for v in current.values())} accepted drops "
              f"across {len(current)} sources)")
        return 0

    if args.check:
        # THE GATE. Fires only on a field a source NOW drops that the baseline did not — i.e. a
        # publisher added a column we are not capturing (exactly how 'Payment Currency' slipped
        # into amount_eur as EUR). A field we STOP dropping (captured it, or the source removed it)
        # never fails — this is a one-way ratchet, like tools/check_conventions.py.
        if not BASELINE.exists():
            print("no baseline — run --write-baseline first (nothing to check against)")
            return 0
        base = json.loads(BASELINE.read_text(encoding="utf-8"))
        new = new_drops(current, base)
        if new:
            print("\nSOURCE-FIDELITY GATE FAILED — a source now drops a field the baseline did not:")
            for src, fields in sorted(new.items()):
                print(f"  {src}: {fields}")
            print("\nEach is a NEW source field reaching no fact. Decide per field: capture it, or")
            print("(if it is PII / a rename / an intermediate) accept it via --write-baseline.")
            return 3
        print(f"\nsource-fidelity gate OK — no new dropped fields vs baseline ({len(base)} sources tracked)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
