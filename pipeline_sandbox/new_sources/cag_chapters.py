"""Derive a chapter/Vote-level C&AG fact from the already-ingested reports index.

SANDBOX ONLY (see STATUS_LEDGER.md isolation contract). Pure local transform:
no fetching. Reads silver/cag_reports.parquet (built by cag_reports.py), explodes
the per-report ``pdf_urls`` into one row per constituent PDF, and classifies each
by its filename slug:

- RoAPS volumes ("report_on_accounts") publish ~20-28 per-CHAPTER PDFs per year,
  slugged ``<n>-<chapter-title>.pdf`` (e.g. ``10-management-of-international-
  protection-accommodation-contracts-copy.pdf``). The chapter list per year is
  the "what did the independent auditor examine" layer.
- Appropriation Accounts volumes publish one PDF per VOTE, slugged
  ``<seq>-vote-<vote_no>-<vote-name>.pdf`` — the Vote->department map falls out
  of the filename without touching the PDF.

Titles are de-slugged from filenames (apostrophes/case are lossy — marked
``extraction_method='filename_slug'``, confidence medium). Harden from the PDF
tables of contents before any promotion. The trailing ``-copy`` CMS artifact is
stripped but flagged.
"""
from __future__ import annotations

import re

import polars as pl

from _common import SILVER, now_iso

SRC = SILVER / "cag_reports.parquet"
OUT_NAME = "cag_chapters"

CHAPTER_RX = re.compile(r"^(\d{1,2})-(.+?)(-copy)?\.pdf$")
VOTE_RX = re.compile(r"^(\d{1,2})-vote-(\d{1,2})-(.+?)(-copy)?\.pdf$")
FRONT_MATTER = ("preface", "statement-of-accounting")


def deslug(s: str) -> str:
    return re.sub(r"\s+", " ", s.replace("-", " ")).strip()


def classify(basename: str, report_type: str) -> dict | None:
    """Return kind/seq/vote/title for one constituent PDF, or None for the
    combined-volume PDF (already represented by the parent index row)."""
    b = basename.lower()
    if any(b.startswith(fm) for fm in FRONT_MATTER):
        return {"kind": "front_matter", "seq": None, "vote_number": None,
                "title_slug": b[:-4], "copy_artifact": False}
    if report_type == "appropriation_accounts":
        m = VOTE_RX.match(b)
        if m:
            return {"kind": "vote_account", "seq": int(m.group(1)),
                    "vote_number": int(m.group(2)), "title_slug": m.group(3),
                    "copy_artifact": bool(m.group(4))}
    m = CHAPTER_RX.match(b)
    if m:
        return {"kind": "chapter", "seq": int(m.group(1)), "vote_number": None,
                "title_slug": m.group(2), "copy_artifact": bool(m.group(3))}
    return None  # combined volume / unrecognised => not a constituent row


def main() -> None:
    cg = pl.read_parquet(SRC).filter(
        pl.col("report_type").is_in(["report_on_accounts", "appropriation_accounts"])
    )
    rows: list[dict] = []
    unrecognised: list[str] = []
    for r in cg.iter_rows(named=True):
        urls = [u.strip() for u in (r["pdf_urls"] or "").split(";") if u.strip()]
        for url in urls:
            base = url.rsplit("/", 1)[-1]
            c = classify(base, r["report_type"])
            if c is None:
                if "report-on-the-accounts" not in base and "appropriation-accounts" not in base:
                    unrecognised.append(base)
                continue
            rows.append({
                "report_type": r["report_type"],
                "report_year": r["report_year"],
                "volume_title": r["title"],
                "kind": c["kind"],
                "seq": c["seq"],
                "vote_number": c["vote_number"],
                "title_slug": c["title_slug"],
                "title_display": deslug(c["title_slug"]),
                "slug_copy_artifact": c["copy_artifact"],
                "pdf_url": url,
                # provenance carried from the parent index row
                "source_url": r["source_url"],
                "source_document_hash": r["source_document_hash"],
                "source_published_date": r["source_published_date"],
                "fetched_at": r["fetched_at"],
                "derived_at": now_iso(),
                "extraction_method": "filename_slug",
                "confidence": "medium",
                "privacy_tier": r["privacy_tier"],
                "value_safe_to_sum": False,
            })
    df = pl.DataFrame(rows, schema_overrides={"seq": pl.Int64, "vote_number": pl.Int64},
                      infer_schema_length=None)
    out = SILVER / f"{OUT_NAME}.parquet"
    if df.is_empty():
        raise SystemExit("no constituent rows derived — inspect pdf_urls format")
    df.write_parquet(out, compression="zstd", statistics=True)

    print(f"wrote {out} — {df.height} rows")
    print(df.group_by("report_type", "kind").len().sort("report_type", "kind"))
    print("\nper-year chapter counts (RoAPS):")
    print(df.filter(pl.col("kind") == "chapter")
            .group_by("report_year").len().sort("report_year", descending=True).head(12))
    print("\nper-year vote counts (Appropriation):")
    print(df.filter(pl.col("kind") == "vote_account")
            .group_by("report_year").len().sort("report_year", descending=True).head(8))
    if unrecognised:
        print(f"\nUNRECOGNISED slugs ({len(unrecognised)}, first 10):")
        for b in unrecognised[:10]:
            print("  ", b)


if __name__ == "__main__":
    main()
