"""Coverage QA — did the parser pull *everything* the source document contained?

``parse_qa`` answers "are the cells we wrote *clean*?". This answers the other half:
"are there rows we *never wrote*?" — silent under-extraction, where a parser reads a
PDF, mis-detects its layout, and emits a fraction of the records (or one collapsed
blob) while every downstream check (row-floor, schema, even reconciliation) stays
green because it only ever sees what was written.

The instrument is a **control total** (classic ETL): derive an independent record
count from the SOURCE and compare it to the parsed output — ``yield = parsed /
expected``. For the public-body payments PDFs the source-side signal is the count of
**money tokens** in the raw text layer: every disclosed line carries exactly one
amount, so amount-tokens ≈ true rows.

CAUTION baked in (learned the hard way, 2026-06-26): a naive money regex (``\\d{4,}``)
also matches the 5-digit **PO reference numbers** (52355, 54329 …) that sit ≥ €20k,
which doubled the count and faked a uniform ~50% shortfall on *every* file. Real PO
amounts always carry a **decimal (.dd) or a € sign**; reference numbers are bare
integers. ``AMOUNT_TOKEN`` requires the decimal, so refs can't pollute the denominator.
After that one constraint, healthy files read 98–100% and genuine failures stand out
(dept_children 36d03592: 0.4%).

Use it three ways:

1. **Audit** a publisher's source PDFs vs the fact::

       python -m services.coverage_qa --publisher dept_children
       python -m services.coverage_qa --all --min-yield 0.9      # whole payment corpus

2. **Feedback loop** — fix a reader, re-run, watch the yield climb to ~100%.

3. **Gate / drift** — store ``expected/extracted/yield`` per source in a coverage JSON
   and alert when a refresh drops below the last run (the Spidermon "vs previous" idea).
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# A money token: optional €, thousands-grouped or bare, ALWAYS two decimals (so a bare
# integer reference/PO number can never match). This single rule is what makes the
# control total trustworthy — see the module docstring.
AMOUNT_TOKEN = re.compile(r"(?:€\s*)?(\d{1,3}(?:,\d{3})+\.\d{2}|\d+\.\d{2})(?![\d])")

DEFAULT_THRESHOLD = 20_000.0  # disclosure floor: each disclosed PO/payment line is ≥ this
DEFAULT_MIN_YIELD = 0.90


class CoverageError(AssertionError):
    """Raised by :func:`assert_yield` when extraction yield falls below the floor."""


@dataclass
class YieldReport:
    source: str
    expected: int
    extracted: int
    expected_eur: float = 0.0
    extracted_eur: float = 0.0
    note: str = ""

    @property
    def yield_frac(self) -> float:
        return (self.extracted / self.expected) if self.expected else float("nan")

    @property
    def eur_yield_frac(self) -> float:
        return (self.extracted_eur / self.expected_eur) if self.expected_eur else float("nan")

    @property
    def missing(self) -> int:
        return max(0, self.expected - self.extracted)

    def __str__(self) -> str:
        return (
            f"{self.yield_frac:>6.1%} row  {self.eur_yield_frac:>6.1%} €  "
            f"exp={self.expected:<6} got={self.extracted:<6} miss={self.missing:<5} "
            f"{self.source}{('  | ' + self.note) if self.note else ''}"
        )


def pdf_text(path: str | Path) -> str:
    """Raw text layer of a born-digital PDF (no OCR). Empty string for image-only PDFs."""
    import fitz  # local import: heavy, and not every caller needs it

    doc = fitz.open(path)
    try:
        return "\n".join(p.get_text() for p in doc)
    finally:
        doc.close()


def amount_tokens(text: str, threshold: float = 0.0) -> list[float]:
    """Every decimal/€-qualified money value in ``text`` at or above ``threshold``."""
    out: list[float] = []
    for m in AMOUNT_TOKEN.finditer(text):
        v = float(m.group(1).replace(",", ""))
        if v >= threshold:
            out.append(v)
    return out


def reconcile(
    expected: int,
    extracted: int,
    source: str,
    *,
    expected_eur: float = 0.0,
    extracted_eur: float = 0.0,
    note: str = "",
) -> YieldReport:
    """Build a :class:`YieldReport` from a source-side and an output-side count.

    Source-agnostic — ``expected`` can come from amount-tokens (PDF), a listing's
    "N results" header (HTML), record-node count (XML) or source line count (CSV).
    """
    return YieldReport(source, expected, extracted, expected_eur, extracted_eur, note)


def pdf_amount_yield(
    pdf_path: str | Path,
    extracted_count: int,
    *,
    extracted_eur: float = 0.0,
    threshold: float = DEFAULT_THRESHOLD,
    source: str | None = None,
) -> YieldReport:
    """Control total for one payment PDF: amount-tokens (source) vs parsed rows (output)."""
    amts = amount_tokens(pdf_text(pdf_path), threshold)
    return YieldReport(
        source=source or Path(pdf_path).name,
        expected=len(amts),
        extracted=extracted_count,
        expected_eur=sum(amts),
        extracted_eur=extracted_eur,
    )


def tabular_amount_rows(path: str | Path, *, threshold: float = DEFAULT_THRESHOLD) -> int | None:
    """Source-side disclosable-row count for a CSV / XLSX / XLS file.

    The tabular analogue of :func:`amount_tokens`. Reuses the pipeline's own reader +
    column-role detection so the count keys off the DETECTED amount column — a ref / PO /
    voucher-number column (often itself ≥ the €20k threshold) can't inflate it, the tabular
    edition of the bare-integer trap in the module docstring. Returns ``None`` when the file
    can't be read or has no amount column (e.g. an image-only or malformed sheet).

    Compare against parsed rows in the fact (via :func:`reconcile`) to catch the tabular
    failure modes: a mis-detected header row, the wrong amount column, dropped rows, or a
    total row pulled in as data.
    """
    path = Path(path)
    ext = path.suffix.lower().lstrip(".")
    try:
        from extractors import procurement_public_body_extract as pbe  # lazy: heavy import
    except Exception:  # pragma: no cover - extractor not importable in some contexts
        return None
    reader = {"csv": pbe.read_csv, "xlsx": pbe.read_xlsx, "xls": pbe.read_xls}.get(ext)
    if reader is None:
        return None
    try:
        header, rows, _full = reader(path.read_bytes())
    except Exception:
        return None
    roles = pbe.detect_roles_tab(header, rows)
    ai = roles.get("amount")
    if ai is None:
        return None
    return sum(1 for r in rows if ai < len(r) and (pbe.to_eur(r[ai]) or 0) >= threshold)


def assert_yield(report: YieldReport, *, min_yield: float = DEFAULT_MIN_YIELD) -> None:
    """Gate a parser: raise :class:`CoverageError` if row-yield is below ``min_yield``.

    Drop this in after a parser writes a source's rows. Unlike a row-floor (which only
    sees the output), this compares against the source, so a layout the parser silently
    half-reads fails loudly instead of shipping a partial fact.
    """
    if report.expected and report.yield_frac < min_yield:
        raise CoverageError(
            f"extraction yield {report.yield_frac:.1%} below floor {min_yield:.0%}: {report}\n"
            f"The source document contains ~{report.expected} records but only "
            f"{report.extracted} were parsed (~{report.missing} missing). Fix the reader."
        )


# --------------------------------------------------------------------------- payments scanner
_GUID = re.compile(r"/([0-9a-f]{8})[-/]")
_PUBLIC_BODY_BRONZE = ROOT / "data/bronze/pdfs/public_body_procurement"
_LA_BRONZE = ROOT / "data/bronze/pdfs/la_procurement"
_PAYMENT_FACTS = {
    "public": ROOT / "data/silver/parquet/public_payments_fact.parquet",
    "la": ROOT / "data/silver/parquet/la_payments_fact.parquet",
}


def _bronze_for(bronze_dir: Path, publisher: str, url: str) -> Path | None:
    d = bronze_dir / publisher
    if not d.exists():
        return None
    g = _GUID.search(url)
    if g:
        hits = list(d.glob(f"{g.group(1)}*"))
        if hits:
            return hits[0]
    base = url.rstrip("/").split("/")[-1]
    hits = list(d.glob(base))
    return hits[0] if hits else None


def scan_payment_publisher(
    publisher: str,
    *,
    threshold: float = DEFAULT_THRESHOLD,
    fact_path: Path | None = None,
    bronze_dir: Path | None = None,
) -> list[YieldReport]:
    """Per-source-PDF yield for one publisher in a payments fact. XLSX/CSV sources skipped
    (no text layer to control against — their structure is the control)."""
    import polars as pl

    fact_path = fact_path or _PAYMENT_FACTS["public"]
    bronze_dir = bronze_dir or _PUBLIC_BODY_BRONZE
    df = pl.read_parquet(fact_path).filter(pl.col("publisher_id") == publisher)
    reports: list[YieldReport] = []
    for url in sorted(df["source_file_url"].unique().to_list()):
        f = _bronze_for(bronze_dir, publisher, url)
        if f is None or f.suffix.lower() != ".pdf":
            continue
        sub = df.filter(pl.col("source_file_url") == url)
        try:
            rep = pdf_amount_yield(
                f,
                sub.height,
                extracted_eur=float(sub["amount_eur"].sum() or 0.0),
                threshold=threshold,
                source=f"{publisher}/{f.name[:40]}",
            )
        except Exception as e:  # noqa: BLE001
            rep = YieldReport(f"{publisher}/{f.name[:40]}", 0, sub.height, note=f"read error: {e}")
        reports.append(rep)
    return reports


def scan_all_payment_publishers(**kw) -> list[YieldReport]:
    import polars as pl

    out: list[YieldReport] = []
    for key, fact in _PAYMENT_FACTS.items():
        if not fact.exists():
            continue
        bronze = _LA_BRONZE if key == "la" else _PUBLIC_BODY_BRONZE
        pubs = pl.read_parquet(fact, columns=["publisher_id"])["publisher_id"].unique().to_list()
        for p in sorted(pubs):
            out.extend(scan_payment_publisher(p, fact_path=fact, bronze_dir=bronze, **kw))
    return out


def _main(argv: list[str]) -> int:
    import argparse

    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--publisher", help="scan one publisher_id in the payments fact")
    ap.add_argument("--all", action="store_true", help="scan the whole payment-PDF corpus")
    ap.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD)
    ap.add_argument("--min-yield", type=float, default=DEFAULT_MIN_YIELD)
    ap.add_argument("--only-failing", action="store_true", help="print only sub-floor files")
    a = ap.parse_args(argv)

    if a.publisher:
        reports = scan_payment_publisher(a.publisher, threshold=a.threshold)
    elif a.all:
        reports = scan_all_payment_publishers(threshold=a.threshold)
    else:
        ap.error("pass --publisher <id> or --all")
        return 2

    reports.sort(key=lambda r: (r.yield_frac if r.expected else 9))
    failing = [r for r in reports if r.expected >= 20 and r.yield_frac < a.min_yield]
    shown = failing if a.only_failing else reports
    print(f"coverage-qa: {len(reports)} PDF source(s), {len(failing)} below {a.min_yield:.0%} yield")
    for r in shown:
        print(("  ! " if r in failing else "    ") + str(r))
    if failing:
        print(f"\nrecoverable rows (sum of shortfalls): {sum(r.missing for r in failing):,}")
    return 1 if failing else 0


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv[1:]))
