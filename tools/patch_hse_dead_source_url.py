"""One-shot metadata patch: repoint the dead HSE source_file_url to HSE's live
procurement publications page.

WHY: every HSE row's source_file_url pointed at
  healthservice.hse.ie/filemanager/HSE_FOI_Model_Publication_..._above_20k.pdf
HSE removed that file in their 2026 site rebuild; the deep-link now 302-redirects
to a generic hse.ie/website-update/ placeholder, so the "View published source"
link looks broken. The €20k-cumulative file (Q4-2021..Q3-2025) we parsed is gone
and cannot be re-fetched (HSE now publishes a €100k-cumulative file + €20k from
Q4-2025 only). The underlying data is unaffected — only the provenance click.

This rewrites two string columns (source_file_url, source_caveat) for HSE rows
only, in both the gold and silver facts. Row counts are unchanged, so the
save_parquet row-floor guard holds. Reversible via `git checkout` on the parquets.

Run: ./.venv/Scripts/python.exe tools/patch_hse_dead_source_url.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.parquet_io import save_parquet  # noqa: E402

# Live HSE procurement publications page (200; lists the replacement FOI files).
LANDING = "https://healthservice.hse.ie/staff/information-healthcare-workers/procurement/"
NOTE = (
    " Original €20k-cumulative PDF (Q4-2021–Q3-2025) removed in HSE's 2026 site "
    "rebuild; HSE now publishes a €100k-cumulative file plus €20k from Q4-2025. "
    "Link points to HSE's procurement publications page (the original deep-link is dead)."
)
IS_HSE = pl.col("publisher_name").str.contains("Health Service")

TARGETS = [
    ROOT / "data/gold/parquet/procurement_payments_fact.parquet",
    ROOT / "data/silver/parquet/hse_tusla_payments_fact.parquet",
]


def patch(path: Path) -> None:
    df = pl.read_parquet(path)
    n = df.height
    hse_n = df.filter(IS_HSE).height
    df = df.with_columns(
        pl.when(IS_HSE).then(pl.lit(LANDING)).otherwise(pl.col("source_file_url")).alias("source_file_url"),
        pl.when(IS_HSE & ~pl.col("source_caveat").fill_null("").str.contains("removed in HSE"))
        .then(pl.col("source_caveat").fill_null("") + pl.lit(NOTE))
        .otherwise(pl.col("source_caveat"))
        .alias("source_caveat"),
    )
    save_parquet(df, path, min_rows=n)
    urls = df.filter(IS_HSE).select("source_file_url").unique().to_series().to_list()
    print(f"{path.name}: rows={df.height} (was {n}); HSE rows patched={hse_n}; new url={urls}")


if __name__ == "__main__":
    for p in TARGETS:
        patch(p)
