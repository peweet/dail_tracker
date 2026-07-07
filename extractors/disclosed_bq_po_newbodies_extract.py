"""disclosed_bq_po_newbodies: SILVER lane for GENUINELY-NEW bodies from the DISCLOSED national
PO/payments-over-€20k BigQuery extract -> data/silver/parquet/disclosed_bq_po_newbodies_fact.parquet,
folded into gold as its OWN disjoint SOURCE_FACTS entry by procurement_consolidate.

SCOPE (Tranche 1): the new bodies whose amount_semantics is AUTHORITATIVE (no blank-PO guess) —
8 county councils (6 already in the LA SCHEMA_MAP as po_committed but whose live web harvest is
broken so they hold 0 gold rows; Tipperary + Louth verified po_committed from their own published
PO PDFs) + An Garda Síochána + EPA + Louth & Meath ETB (publishers_seed grain=purchase_order).
ALL are po_committed. The registry IS the allow-list: `data/_meta/procurement_disclosed_bodies.csv`.
A body in the raw extract but NOT in the registry is SILENTLY EXCLUDED (that is the point — we only
ship bodies with a confirmed identity + regime). A registry row absent from the raw extract WARNS.

DISJOINTNESS (the cross-lane double-count trap, plan §7/§10/§12a): every publisher_id emitted here
is asserted DISJOINT from every other silver lane + the existing gold fact. If a future LA-harvest
recovery starts emitting one of the 6 council ids, THIS extractor HALTS (fail-closed) rather than
let gold double-count (the consolidator's per-source reconcile cannot catch a cross-lane duplicate).

SOURCE: the same manual BigQuery drop at data/raw_bq/bq-results-*.csv (gitignored, ~62MB). NO-OP-SAFE:
absent drop -> leaves existing silver untouched, exit 0; the fold carry-forwards any prior gold rows.

Run:  ./.venv/Scripts/python.exe extractors/disclosed_bq_po_newbodies_extract.py
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from services.parquet_io import save_parquet  # noqa: E402

sys.path.insert(0, str(ROOT / "extractors"))
import procurement_public_body_extract as pbe  # noqa: E402  (classify_and_flag)

RAW_DIR = ROOT / "data/raw_bq"
REGISTRY = ROOT / "data/_meta/procurement_disclosed_bodies.csv"
SILVER = ROOT / "data/silver/parquet"
GOLD = ROOT / "data/gold/parquet/procurement_payments_fact.parquet"
OUT = SILVER / "disclosed_bq_po_newbodies_fact.parquet"
COVERAGE = ROOT / "data/_meta/disclosed_bq_po_newbodies_coverage.json"
MIN_ROWS = 70_000  # ~84% of the first healthy run (83,504); a truncated CSV must not silently shrink the lane

# Other silver lanes whose publisher_id sets must stay disjoint from this lane (the double-count guard).
_OTHER_LANES = [
    "public_payments_fact.parquet",
    "hse_tusla_payments_fact.parquet",
    "nta_payments_fact.parquet",
    "nphdb_payments_fact.parquet",
    "seai_payments_fact.parquet",
    "dept_readingorder_payments_fact.parquet",
    "la_payments_fact.parquet",
    "disclosed_bq_po_payments_fact.parquet",
]

# EXACT base column ORDER of the SOURCE_FACTS lanes (public_payments_fact …) — _load_facts concats
# directly with no reorder, so order must match byte-for-byte (public_display BEFORE source_caveat).
SCHEMA_COLS = [
    "publisher_id",
    "publisher_name",
    "publisher_type",
    "sector",
    "source_landing_url",
    "source_file_url",
    "source_file_hash",
    "period",
    "year",
    "quarter",
    "supplier_raw",
    "supplier_normalised",
    "amount_eur",
    "amount_semantics",
    "value_safe_to_sum",
    "description",
    "po_number",
    "paid_flag",
    "source_row_number",
    "source_page_number",
    "parser_name",
    "parser_version",
    "extraction_status",
    "extraction_confidence",
    "caveat_text_detected",
    "supplier_class",
    "privacy_status",
    "public_display",
    "source_caveat",
]
_NULL_TOKENS = {"null", "na", "n/a", "none", ""}


def _find_raw() -> Path | None:
    if not RAW_DIR.exists():
        return None
    cands = sorted(RAW_DIR.glob("bq-results-*.csv"))
    return cands[-1] if cands else None


def _clean(col: str) -> pl.Expr:
    e = pl.col(col).cast(pl.Utf8).str.replace_all(r"[\r\n\t]", " ").str.replace_all(r"\s+", " ").str.strip_chars()
    return pl.when(e.str.to_lowercase().is_in(list(_NULL_TOKENS))).then(None).otherwise(e)


PARSER_NAME = "disclosed_bq_newbodies"


def _assert_disjoint(my_ids: set[str]) -> None:
    """Fail-closed: HALT if any emitted publisher_id is already present in ANOTHER lane or gold.

    Gold is rebuilt from silver each run and already holds THIS lane's own rows from the prior build
    (parser_name=disclosed_bq_newbodies) — those are not "another lane", so exclude them, else every
    re-run after the first would false-trip on its own output (mirrors the LA carry-forward fix)."""
    seen: dict[str, str] = {}
    for f in _OTHER_LANES:
        path = SILVER / f
        if not path.exists():
            continue
        ids = set(pl.read_parquet(path, columns=["publisher_id"])["publisher_id"].unique().to_list())
        for pid in my_ids & ids:
            seen[pid] = f
    if GOLD.exists():
        gold = pl.read_parquet(GOLD, columns=["publisher_id", "parser_name"]).filter(
            pl.col("parser_name") != PARSER_NAME
        )
        gids = set(gold["publisher_id"].unique().to_list())
        for pid in my_ids & gids:
            seen[pid] = "gold"
    if seen:
        raise SystemExit(
            f"disclosed_bq_po_newbodies: DOUBLE-COUNT GUARD TRIPPED — publisher_id(s) already in another "
            f"lane/gold: {seen}. A lane (likely a recovered LA harvest) now owns these; remove them from "
            f"the disclosed registry or route ownership before rebuilding."
        )


def build() -> int:
    raw = _find_raw()
    if raw is None:
        print(
            f"disclosed_bq_po_newbodies: no raw drop under {RAW_DIR}/bq-results-*.csv — leaving existing silver untouched, exit 0"
        )
        return 0
    if not REGISTRY.exists():
        print(f"disclosed_bq_po_newbodies: registry missing ({REGISTRY}) — cannot ship un-typed bodies. exit 0")
        return 0

    reg = pl.read_csv(REGISTRY)
    reg_bodies = reg["entity_clean"].to_list()

    df = pl.read_csv(raw, schema_overrides={"Total": pl.Float64, "Year": pl.Int64}).with_columns(
        pl.col("entity").str.replace(r"^(Agency|Section 38|Section 39)\s*:\s*", "").str.strip_chars().alias("body"),
        (pl.col("Year").cast(pl.Utf8) + "-" + pl.col("QTR").str.to_uppercase()).alias("period"),
    )
    raw_bodies = set(df["body"].unique().to_list())
    for b in reg_bodies:  # registry rows absent from the raw extract are surfaced (not fatal)
        if b not in raw_bodies:
            print(f"  WARN registry body absent from raw extract: {b!r}")

    lane = df.filter(pl.col("body").is_in(reg_bodies)).join(
        reg.select(
            "entity_clean",
            "publisher_id",
            "publisher_name",
            "publisher_type",
            "sector",
            "amount_semantics",
            "source_landing_url",
        ),
        left_on="body",
        right_on="entity_clean",
        how="left",
    )
    if lane.is_empty():
        print("disclosed_bq_po_newbodies: no registered bodies present in raw. exit 0")
        return 0

    csv_hash = hashlib.sha1(raw.read_bytes()[: 1 << 20]).hexdigest()[:16]
    lane = lane.with_columns(
        _clean("Supplier").alias("supplier_raw"),
        _clean("Description").alias("description"),
        _clean("PO").alias("po_number"),
        pl.col("Total").alias("amount_eur"),
        pl.col("Year").alias("year"),
        pl.col("QTR").str.replace("q", "").cast(pl.Int64).alias("quarter"),
        ("disclosed-bq:" + pl.lit(raw.name) + "#" + pl.col("body") + "/" + pl.col("period")).alias("source_file_url"),
        pl.lit(csv_hash).alias("source_file_hash"),
        pl.lit(None, dtype=pl.Utf8).alias("paid_flag"),
        pl.int_range(pl.len()).alias("source_row_number"),
        pl.lit(None, dtype=pl.Int64).alias("source_page_number"),
        pl.lit("disclosed_bq_newbodies").alias("parser_name"),
        pl.lit("0.1.0").alias("parser_version"),
        pl.lit("extracted").alias("extraction_status"),
        pl.lit("medium").alias(
            "extraction_confidence"
        ),  # faithful BQ copy; regime authoritative, lines not independently re-parsed
        pl.lit(False).alias("caveat_text_detected"),
        pl.lit(
            "Disclosed BigQuery PO extract; new-body coverage (Tranche 1, regime=po_committed, source-authoritative)."
        ).alias("source_caveat"),
    )
    # supplier_normalised + supplier_class + privacy_status + public_display + value_safe_to_sum
    lane = pbe.classify_and_flag(lane)
    missing = [c for c in SCHEMA_COLS if c not in lane.columns]
    if missing:
        raise SystemExit(f"disclosed_bq_po_newbodies: missing schema columns {missing}")
    lane = lane.select(SCHEMA_COLS)

    # privacy invariant (refuse to write a leak)
    leak = lane.filter(pl.col("public_display") & (pl.col("supplier_class") == "sole_trader_or_individual")).height
    if leak:
        raise SystemExit(
            f"disclosed_bq_po_newbodies: privacy quarantine breached — {leak} individual rows public_display=True"
        )

    # cross-lane double-count guard (fail-closed)
    _assert_disjoint(set(lane["publisher_id"].unique().to_list()))

    save_parquet(lane, OUT, min_rows=MIN_ROWS)
    cov = {
        "built_at": datetime.now(UTC).isoformat(),
        "source_csv": raw.name,
        "source_file_hash": csv_hash,
        "scope": "new-body coverage Tranche 1 (po_committed, source-authoritative)",
        "rows": lane.height,
        "publishers": int(lane["publisher_id"].n_unique()),
        "publisher_ids": sorted(lane["publisher_id"].unique().to_list()),
        "gross_eur": float(lane["amount_eur"].sum()),
        "summable_eur": float(lane.filter(pl.col("value_safe_to_sum"))["amount_eur"].sum()),
        "rows_review_personal_data": int((lane["privacy_status"] == "review_personal_data").sum()),
    }
    COVERAGE.write_text(json.dumps(cov, indent=2), encoding="utf-8")
    print(
        f"disclosed_bq_po_newbodies: wrote {lane.height:,} rows / {cov['publishers']} new publishers "
        f"-> {OUT.relative_to(ROOT)}  (€{cov['gross_eur'] / 1e9:,.2f}bn gross, "
        f"€{cov['summable_eur'] / 1e9:,.2f}bn summable-within-body, {cov['rows_review_personal_data']:,} privacy-gated)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(build())
