"""pq_pdf_garble_score.py -- quality-gate PDF-parsed table cells.

Recomputes data/_sandbox/pq_pdf_garble_scores.parquet from
pq_attachment_cells.parquet, scoring only genuine table cells (excludes
'page_text' fallback rows, which are prose blocks and not table data).

is_clean drops the pct_newline criterion. It was miscalibrated: it flagged
genuine multi-line table cells (long project descriptions wrapping) as
"garbled" even at 0% short-cell corruption, excluding 435 of 658 PDFs that
were otherwise clean. pct_short alone -- the corruption signal -- is kept.
See reference_pq_attachment_harvest_2026_08_01.md for the calibration case.
"""
from __future__ import annotations

import services.runtime_env  # noqa: F401 -- must be first import

import polars as pl

from services.parquet_io import save_parquet

_CELLS = "data/_sandbox/pq_attachment_cells.parquet"
_OUT = "data/_sandbox/pq_pdf_garble_scores.parquet"

SHORT_THRESHOLD = 0.10


def main() -> int:
    cells = pl.read_parquet(_CELLS)
    pdf_cells = cells.filter(
        pl.col("attachment_url").str.to_lowercase().str.ends_with(".pdf")
        & (pl.col("col_name") != "page_text")
    )
    scored = pdf_cells.group_by("attachment_url").agg(
        pl.len().alias("n_cells"),
        (pl.col("value").fill_null("").str.len_chars() <= 2).sum().alias("n_short"),
        pl.col("value").fill_null("").str.contains("\n").sum().alias("n_newline"),
    ).with_columns(
        (pl.col("n_short") / pl.col("n_cells")).alias("pct_short"),
        (pl.col("n_newline") / pl.col("n_cells")).alias("pct_newline"),
    ).with_columns(
        (pl.col("pct_short") <= SHORT_THRESHOLD).alias("is_clean"),
    )
    save_parquet(scored, _OUT, min_rows=1)
    clean = scored.filter(pl.col("is_clean"))
    print(f"scored {scored.height} PDFs, {clean.height} clean "
          f"({int(clean['n_cells'].sum())} cells)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
