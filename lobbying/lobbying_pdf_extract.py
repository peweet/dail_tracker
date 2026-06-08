"""Extract third-party PDF URLs embedded in lobbying return free-text fields.

Lobbyists often paste links to their position papers, pre-budget submissions
and policy briefs into the unstructured fields of a lobbying.ie return.
This script regex-mines those URLs out of `data/silver/lobbying/returns.csv`
and writes a tabular silver parquet with one row per (return, PDF) pair.

Output: data/silver/parquet/lobbying_return_documents.parquet
    primary_key                str   matches returns.csv primary_key
    lobbyist_name              str
    lobby_url                  str   canonical lobbying.ie return page
    source_field               str   which CSV column carried the link
    pdf_url                    str   the extracted PDF URL
    host                       str   e.g. amcham.ie  (for grouping/display)
    date_published_timestamp   str
    public_policy_area         str

Usage:
    python lobbying_pdf_extract.py
"""

from __future__ import annotations

import re
import sys

import polars as pl

from config import SILVER_DIR, SILVER_PARQUET_DIR
from services.parquet_io import save_parquet

RETURNS_CSV = SILVER_DIR / "lobbying" / "returns.csv"

TEXT_FIELDS: tuple[str, ...] = (
    "intended_results",
    "specific_details",
    "lobbying_activities",
    "grassroots_directive",
)

# Match http(s) PDFs. Stop at whitespace, commas, semicolons, or trailing
# punctuation that's commonly attached to URLs in free text but not part
# of the URL itself.
PDF_RE = re.compile(r"https?://[^\s,;<>\"')\]]+?\.pdf", re.IGNORECASE)

# Trim trailing punctuation that the regex may have included (e.g. a
# sentence-ending period or closing paren that wasn't excluded).
TRAILING_NOISE = (".", ",", ";", ")", "]", "'", '"')


def _clean_url(url: str) -> str:
    while url and url[-1] in TRAILING_NOISE:
        url = url[:-1]
    return url


def _host(url: str) -> str | None:
    m = re.match(r"https?://([^/]+)", url)
    if not m:
        return None
    h = m.group(1).lower()
    return h[4:] if h.startswith("www.") else h


def main() -> int:
    if not RETURNS_CSV.exists():
        print(f"ERROR: {RETURNS_CSV} not found", file=sys.stderr)
        return 1

    keep_cols = [
        "primary_key",
        "lobbyist_name",
        "lobby_url",
        "date_published_timestamp",
        "public_policy_area",
        *TEXT_FIELDS,
    ]
    df = pl.read_csv(RETURNS_CSV, ignore_errors=True, infer_schema_length=0)
    missing = [c for c in keep_cols if c not in df.columns]
    if missing:
        print(f"ERROR: returns.csv missing expected columns: {missing}", file=sys.stderr)
        return 1
    df = df.select(keep_cols)

    rows: list[dict[str, str | None]] = []
    for r in df.iter_rows(named=True):
        for field in TEXT_FIELDS:
            txt = r.get(field)
            if not txt:
                continue
            for raw_url in PDF_RE.findall(txt):
                url = _clean_url(raw_url)
                if not url.lower().endswith(".pdf"):
                    continue
                rows.append(
                    {
                        "primary_key": r["primary_key"],
                        "lobbyist_name": r.get("lobbyist_name"),
                        "lobby_url": r.get("lobby_url"),
                        "source_field": field,
                        "pdf_url": url,
                        "host": _host(url),
                        "date_published_timestamp": r.get("date_published_timestamp"),
                        "public_policy_area": r.get("public_policy_area"),
                    }
                )

    if not rows:
        print("No embedded PDF URLs found in returns.csv — nothing to write.")
        return 0

    out = pl.DataFrame(rows).unique(
        subset=["primary_key", "pdf_url"],
        keep="first",
        maintain_order=True,
    )

    SILVER_PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    target = SILVER_PARQUET_DIR / "lobbying_return_documents.parquet"
    save_parquet(out, target)

    by_field = out.group_by("source_field").len().sort("len", descending=True)
    top_hosts = out.group_by("host").len().sort("len", descending=True).head(10)

    print(f"Wrote {target}  rows={out.height}  returns={out['primary_key'].n_unique()}")
    print(f"  unique PDF URLs: {out['pdf_url'].n_unique()}")
    print("\nBy source field:")
    print(by_field)
    print("\nTop hosts:")
    print(top_hosts)
    return 0


if __name__ == "__main__":
    sys.exit(main())
