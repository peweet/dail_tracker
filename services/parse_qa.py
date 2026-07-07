"""Parse-quality sanity check — find string cells that were parsed wrong.

The failure class this catches is *field collapse*: a parser that should have
split a page into rows/columns instead drops a whole table, a whole OCR'd
calendar page, or a multi-value list into a single cell. The tell is not "the
cell is long" — plenty of columns (``speech_text``, ``raw_text``,
``charitable_objects``, a bill's ``long_title``) are legitimately long for every
row. The tell is that ONE cell is wildly longer than its own column's typical
value: ``payments.description`` has a p99 of ~58 chars and a max of 14,197 (an
entire payments table collapsed into one description); ``diary.subject`` has a
p99 of ~220 and a max of 9,220 (an OCR'd month-view calendar dumped into the
subject of one engagement). A flat length threshold flags the free-text columns
too and drowns the real breakages; the **outlier ratio** ``max / p99`` separates
them — 245x and 42x for the two breakages above, 2.7x for the legitimately-long
``corporate_notices.raw_text``.

Use it three ways:

1. **Audit** existing parquet from the CLI::

       python -m services.parse_qa data/silver          # scan a tree
       python -m services.parse_qa data/gold/parquet/x.parquet --show

2. **Gate an extractor** before it writes (like a contract / row-floor)::

       from services.parse_qa import assert_clean
       assert_clean(df, allow={"raw_text", "speech_text"})  # raises ParseQAError

3. **Iteratively break a bad cell down** into the records it should have been::

       from services.parse_qa import suggest_split
       pieces = suggest_split(bad_cell)   # splits on the detected record boundary

The scan is column-relative and self-calibrating, so it works on any frame
without per-source tuning. ``allow`` exempts columns that are long *by design*.
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

# --- detection knobs -------------------------------------------------------
DEFAULT_RATIO = 10.0  # cell flagged when len > RATIO * column-p99
DEFAULT_FLOOR = 120  # ...and len exceeds this absolute floor (skip tiny cols)
MAX_EXAMPLES = 5  # worst cells captured per flagged column

# A money amount like 1,234.56 / 1234.00 — the signature of a collapsed ledger.
_MONEY = re.compile(r"\d[\d,]*\.\d{2}\b")
# A date like 01/02/2023 or 2023-02-01 or "11 January 2023".
_DATE = re.compile(
    r"\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}-\d{2}-\d{2}|"
    r"\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4})\b"
)
_PIPE = " | "


@dataclass
class CellFlag:
    row_index: int
    length: int
    diagnosis: str
    excerpt: str


@dataclass
class ColumnReport:
    file: str
    column: str
    n_rows: int
    median: int
    p99: int
    max_len: int
    ratio: float
    n_outliers: int
    diagnosis: str
    examples: list[CellFlag] = field(default_factory=list)

    def __str__(self) -> str:  # one-line summary
        return (
            f"{self.ratio:>6.1f}x  max={self.max_len:<6} p99={self.p99:<5} "
            f"med={self.median:<4} bad={self.n_outliers:<4} "
            f"[{self.diagnosis}]  {self.file} :: {self.column}"
        )


class ParseQAError(AssertionError):
    """Raised by :func:`assert_clean` when a frame has collapsed cells."""


def diagnose_cell(text: str) -> str:
    """Best-guess at *why* a cell is an outlier — drives the split strategy."""
    if not text:
        return "EMPTY"
    money = len(_MONEY.findall(text))
    dates = len(_DATE.findall(text))
    pipes = text.count(_PIPE)
    newlines = text.count("\n")
    # A run of money amounts (and usually dates) = a table dropped into one cell.
    if money >= 3 and (dates >= 2 or money >= 6):
        return "MERGED_RECORDS"
    # Many explicit delimiters = a multi-value list that should be exploded.
    if pipes >= 5:
        return "MULTI_VALUE_DELIMITED"
    if newlines >= 8:
        return "MULTILINE_DUMP"
    # Long, but no internal structure — most likely an OCR page or free text.
    if money >= 1 or dates >= 1:
        return "MIXED_CONTENT"
    return "LONG_FREETEXT"


def suggest_split(text: str) -> list[str]:
    """Break a collapsed cell into the records/values it most likely should be.

    Tries delimiters in order of how strong a record boundary they signal and
    returns the first split that yields >1 non-trivial piece. Returns ``[text]``
    unchanged when no boundary is found (caller decides it's genuine free text).
    """
    if not text:
        return [text]
    # 1. Explicit pipe-delimited list (framework-agreement supplier lists etc.).
    if text.count(_PIPE) >= 2:
        parts = [p.strip(" |") for p in text.split(_PIPE)]
        parts = [p for p in parts if p]
        if len(parts) > 1:
            return parts
    # 2. A ledger: split *before* each date so each record keeps its own date.
    if len(_DATE.findall(text)) >= 2:
        parts = [p.strip() for p in re.split(r"(?=" + _DATE.pattern + r")", text) if p and p.strip()]
        if len(parts) > 1:
            return parts
    # 3. Multi-line block.
    if text.count("\n") >= 2:
        parts = [p.strip() for p in text.splitlines() if p.strip()]
        if len(parts) > 1:
            return parts
    # 4. Money-run with no dates: split before each amount-led record.
    if len(_MONEY.findall(text)) >= 3:
        parts = [p.strip() for p in re.split(r"(?<=\.\d{2})\s+", text) if p and p.strip()]
        if len(parts) > 1:
            return parts
    return [text]


def scan_frame(
    df: pl.DataFrame,
    *,
    ratio: float = DEFAULT_RATIO,
    floor: int = DEFAULT_FLOOR,
    allow: set[str] | None = None,
    source: str = "<frame>",
) -> list[ColumnReport]:
    """Return a report for every string column with collapsed-cell outliers.

    A column flags when its longest cell exceeds both ``floor`` and
    ``ratio * p99`` (so it is far longer than the column's own 99th percentile).
    Columns named in ``allow`` are skipped (long by design).
    """
    allow = allow or set()
    reports: list[ColumnReport] = []
    str_cols = [c for c, t in df.schema.items() if t == pl.String and c not in allow]
    for c in str_cols:
        ln = df[c].str.len_chars()
        mx = ln.max()
        if mx is None or mx < floor:
            continue
        p99 = ln.quantile(0.99) or 0.0
        r = (mx / p99) if p99 and p99 > 0 else float("inf")
        if r < ratio:
            continue
        cutoff = max(floor, ratio * (p99 or 1))
        idx = (
            df.with_row_index("__i")
            .filter(ln > cutoff)
            .select("__i", c)
            .sort(pl.col(c).str.len_chars(), descending=True)
        )
        examples = [
            CellFlag(int(i), len(v or ""), diagnose_cell(v or ""), (v or "")[:200])
            for i, v in idx.head(MAX_EXAMPLES).iter_rows()
        ]
        diag = examples[0].diagnosis if examples else "UNKNOWN"
        reports.append(
            ColumnReport(
                file=source,
                column=c,
                n_rows=df.height,
                median=int(ln.median() or 0),
                p99=int(p99),
                max_len=int(mx),
                ratio=round(r, 1),
                n_outliers=int(idx.height),
                diagnosis=diag,
                examples=examples,
            )
        )
    reports.sort(key=lambda x: -x.ratio)
    return reports


def scan_parquet(path: str | Path, **kw) -> list[ColumnReport]:
    p = Path(path)
    df = pl.read_parquet(p)
    kw.setdefault("source", str(p))
    return scan_frame(df, **kw)


def scan_tree(root: str | Path, **kw) -> list[ColumnReport]:
    root = Path(root)
    paths = [p for p in root.rglob("*.parquet") if "_quarantine" not in str(p)]
    out: list[ColumnReport] = []
    for p in paths:
        try:
            out.extend(scan_parquet(p, **kw))
        except Exception as e:  # noqa: BLE001 — keep scanning the rest
            print(f"  ! read error {p}: {e}", file=sys.stderr)
    out.sort(key=lambda x: -x.ratio)
    return out


def count_huge(df: pl.DataFrame, hard_len: int, allow: set[str] | None = None) -> int:
    """Cells longer than an absolute ``hard_len``, across non-``allow`` string cols.

    This is the p99-blind backstop: if a regression collapses *most* rows, the
    column's own p99 inflates and the relative :func:`scan_frame` ratio drops
    below threshold — the mass failure hides itself. An absolute length never
    inflates, so a flood of multi-record cells is still caught.
    """
    allow = allow or set()
    n = 0
    for c, t in df.schema.items():
        if t == pl.String and c not in allow:
            n += int((df[c].str.len_chars() > hard_len).sum())
    return n


def assert_clean(
    df: pl.DataFrame,
    *,
    ratio: float = DEFAULT_RATIO,
    floor: int = DEFAULT_FLOOR,
    allow: set[str] | None = None,
    tolerate: int = 0,
    hard_len: int = 2000,
) -> None:
    """Gate an extractor: raise :class:`ParseQAError` when cells collapse.

    Drop this in before ``save_parquet`` so a parser regression that merges rows
    fails the run loudly instead of writing silently-broken silver/gold.

    Two independent signals, so it catches both a handful of newly-collapsed
    cells *and* a mass collapse that would hide from a relative metric:

    * **relative** — total outlier cells (``scan_frame``, max/p99 ratio); and
    * **absolute** — cells longer than ``hard_len`` (:func:`count_huge`).

    ``tolerate`` is the budget for a *known, un-repairable* residual (e.g. the
    DCEDIY reading-order PDFs that dump a page into one cell and need a bespoke
    parser, not a regex). Set it to the current residual plus headroom: the gate
    then stays green on today's data but trips when the count grows. Use ``allow``
    only for columns that are long *by design* for every row. ``tolerate`` is the
    honest choice for a column that is *mostly* clean with a known bad tail.
    """
    reports = scan_frame(df, ratio=ratio, floor=floor, allow=allow)
    n_outliers = sum(r.n_outliers for r in reports)
    n_huge = count_huge(df, hard_len, allow)
    if max(n_outliers, n_huge) > tolerate:
        lines = "\n".join("  " + str(r) for r in reports) or "  (no ratio outliers)"
        raise ParseQAError(
            f"collapsed/over-long cells exceed tolerate={tolerate}: "
            f"{n_outliers} ratio-outlier cell(s), {n_huge} cell(s) over {hard_len} chars.\n"
            f"{lines}\n"
            f"Raise tolerate for a known un-repairable residual, or allow={{...}} "
            f"for a column that is long by design."
        )


def _main(argv: list[str]) -> int:
    import argparse

    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("path", help="parquet file or directory to scan")
    ap.add_argument("--ratio", type=float, default=DEFAULT_RATIO)
    ap.add_argument("--floor", type=int, default=DEFAULT_FLOOR)
    ap.add_argument("--allow", default="", help="comma-separated columns to skip")
    ap.add_argument("--show", action="store_true", help="print worst-cell excerpts")
    ap.add_argument("--split", action="store_true", help="print suggest_split() of the worst cell")
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    a = ap.parse_args(argv)

    allow = {c.strip() for c in a.allow.split(",") if c.strip()}
    p = Path(a.path)
    reports = (
        scan_tree(p, ratio=a.ratio, floor=a.floor, allow=allow)
        if p.is_dir()
        else scan_parquet(p, ratio=a.ratio, floor=a.floor, allow=allow)
    )

    if a.json:
        print(json.dumps([r.__dict__ for r in reports], default=lambda o: o.__dict__, indent=2))
        return 1 if reports else 0

    print(f"parse-qa: {len(reports)} flagged column(s)  (ratio>={a.ratio}, floor={a.floor})")
    for r in reports:
        print(str(r))
        if a.show:
            for ex in r.examples:
                print(f"      row {ex.row_index} len={ex.length} [{ex.diagnosis}]: {ex.excerpt!r}")
        if a.split and r.examples:
            pieces = suggest_split(r.examples[0].excerpt)
            print(f"      -> suggest_split -> {len(pieces)} pieces: {pieces[:4]}")
    return 1 if reports else 0


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv[1:]))
