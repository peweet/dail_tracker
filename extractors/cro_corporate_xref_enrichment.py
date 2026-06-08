"""CRO ↔ corporate-notice cross-reference (PROMOTED enrichment, writes gold).

Replaces the throwaway probe_cro_corporate_join.py once its D3 feasibility
assumption was confirmed (usable-name 75.1%, clean 1:1 match 76.4%, ambiguous
2.1%). This is the real, committed-gold enrichment — mirrors the CBI xref
(extractors/cbi_registers_extract.py :: xref_corporate_notices) one-for-one.

Output:
    data/gold/parquet/cro_xref_corporate_notices.parquet
    PROMOTED — read by sql_views/corporate/corporate_cro_match.sql (v_corporate_cro_notice_match),
    a display-only registration badge on the Corporate page (status / reg date /
    dissolved date for the wound-up entity named on the notice).

Civic frame (honest): we surface the CRO registration provenance of the entity
that appears on the notice. We do NOT claim the liquidator/receiver action is
itself a CRO matter. Match is EXACT on the normalised company name (no fuzz).

Key invariant — DO NOT re-implement the key:
    The notice entity_name is normalised with cro_normalise.name_norm_expr, the
    SAME rule that produced the CRO silver `name_norm` column. Re-implementing it
    here is how drift hides and a join silently rots. Always import it.

Ambiguity policy:
    ~0.98% of CRO `name_norm` keys map to >1 company_num (e.g. "ULSTER BANK" →
    112 companies). A notice fanning out to many companies is not a clean badge,
    so the CRO index is restricted to names that resolve to EXACTLY ONE company.
    Ambiguous and no-match notices simply get no badge (left join on the page).

Run:
    .venv/Scripts/python.exe extractors/cro_corporate_xref_enrichment.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    sys.stdout.reconfigure(encoding="utf-8")  # accented company names on cp1252 consoles
except Exception:
    pass

from services.parquet_io import save_parquet  # noqa: E402

# Import CRO's production normalisation so the join key is byte-identical to the
# CRO silver `name_norm` column (see module docstring — do NOT re-implement).
from shared.name_norm import name_norm_expr  # noqa: E402

NOTICES_PARQUET = ROOT / "data" / "gold" / "parquet" / "corporate_notices.parquet"
CRO_PARQUET = ROOT / "data" / "silver" / "cro" / "companies.parquet"
OUT_PARQUET = ROOT / "data" / "gold" / "parquet" / "cro_xref_corporate_notices.parquet"

# Mirror corporate_notices_enrichment / the probe: entity_name values that are
# notice boilerplate, not a company name. Excluded before the join.
JUNK_RE = re.compile(
    r"NOTICE IS HEREBY|ABOVE NAMED|IN THE MATTER|COMPANIES ACT|ICAV ACT|COLLECTIVE ASSET",
    re.I,
)

# Minimum normalised-name length. Shorter keys are extraction noise that
# over-matches (the probe gated at < 4 — a 3-char company name is almost always
# junk left after suffix stripping).
_MIN_NORM_CHARS = 4

# Columns carried from the CRO side onto the badge.
_CRO_COLS = ["company_num", "company_status", "company_reg_date", "comp_dissolved_date", "status_pill_value"]

# Final xref column order (notice grain + CRO badge fields).
_OUTPUT_COLS = [
    "notice_ref",
    "entity_name",
    "entity_norm",
    "issue_date",
    "notice_category",
    "notice_subtype",
    *_CRO_COLS,
]


def _is_junk(name: str | None) -> bool:
    return bool(JUNK_RE.search(name or ""))


def build_cro_xref(notices: pl.DataFrame, cro: pl.DataFrame) -> pl.DataFrame:
    """Inner-join usable corporate-notice entity names to unambiguous CRO companies.

    Pure: no IO. One row per notice that has a clean 1:1 CRO name match.

    notices: must carry entity_name, notice_ref, issue_date, notice_category,
             notice_subtype (the v_corporate_notices grain).
    cro:     must carry name_norm + the _CRO_COLS (cro_normalise silver output).
    """
    usable = (
        notices.with_columns(name_norm_expr("entity_name").alias("entity_norm"))
        .filter(pl.col("entity_name").is_not_null() & (pl.col("entity_name").str.len_chars() > 0))
        .filter(~pl.col("entity_name").map_elements(_is_junk, return_dtype=pl.Boolean))
        .filter(pl.col("entity_norm").str.len_chars() >= _MIN_NORM_CHARS)
    )

    # Restrict CRO to names that resolve to EXACTLY ONE company (ambiguity policy).
    unambiguous_names = (
        cro.group_by("name_norm")
        .agg(pl.col("company_num").n_unique().alias("_n_companies"))
        .filter(pl.col("_n_companies") == 1)
        .select("name_norm")
    )
    cro_one = cro.join(unambiguous_names, on="name_norm", how="inner").select(["name_norm", *_CRO_COLS])

    xref = usable.join(cro_one, left_on="entity_norm", right_on="name_norm", how="inner")
    return xref.select(_OUTPUT_COLS)


def main() -> int:
    if not NOTICES_PARQUET.exists():
        raise SystemExit(f"corporate notices gold not found: {NOTICES_PARQUET} (run the iris chain first)")
    if not CRO_PARQUET.exists():
        raise SystemExit(f"CRO silver not found: {CRO_PARQUET} (run cro_normalise.py / the lobbying chain first)")

    notices = pl.read_parquet(NOTICES_PARQUET)
    cro = pl.read_parquet(CRO_PARQUET)
    xref = build_cro_xref(notices, cro)

    save_parquet(xref, OUT_PARQUET)

    matched_names = xref.select(pl.col("entity_norm").n_unique()).item() if xref.height else 0
    print(f"[cro_xref] wrote {OUT_PARQUET}")
    print(f"  notices in        : {notices.height:,}")
    print(f"  xref rows out     : {xref.height:,}")
    print(f"  distinct names    : {matched_names:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
