"""disclosed_bq_po: build the SILVER lane for the DISCLOSED national PO/payments-over-€20k
BigQuery extract -> data/silver/parquet/disclosed_bq_po_payments_fact.parquet, folded into gold
by the procurement_consolidate chain (see extractors/procurement_payments_consolidate.py
`_load_facts`, which concatenates this sidecar INTO the hse_tusla source so ie_hse stays
single-source for the disjoint-publisher reconcile).

SCOPE (Phase 1): HSE HISTORY RECOVERY only. The disclosed extract reconciles to our independent
HSE PDF parse to the cent on the overlap (shared lineage off the same HSE FOI €20k file family),
so it is a trustworthy faithful copy. We emit ONLY the HSE periods our PDF parse lacks
(2017-Q3..2020-Q2 + 2025-Q4 + 2026-Q1 — i.e. periods absent from the hse_tusla lane), inherit
ie_hse's identity + amount_semantics=payment_actual from gold (NOT the blank-PO heuristic, which
mislabels HSE), and reuse procurement_public_body_extract.classify_and_flag for the privacy gate.

NOT YET IN SCOPE (Phases 2/3): the 141 genuinely-new bodies + 53 renames need a fail-closed
entity->publisher_id registry and authoritative per-body amount_semantics first (the blank-PO
heuristic is proven wrong). See doc/DISCLOSED_PO_INTEGRATION_PLAN.md + pipeline_sandbox/
disclosed_po_spend/.

SOURCE: a manual BigQuery drop at data/raw_bq/bq-results-*.csv (gitignored, ~62MB). This step is
NO-OP-SAFE: if the drop is absent (e.g. a cloud run), it leaves any existing silver untouched and
exits 0; the consolidation then carry-forwards the disclosed HSE rows already in gold. Re-run
locally to regenerate the silver from the raw CSV.

Run:  ./.venv/Scripts/python.exe extractors/disclosed_bq_po_extract.py
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
import procurement_public_body_extract as pbe  # noqa: E402  (classify_and_flag: supplier_class/privacy/public_display/value_safe_to_sum)

RAW_DIR = ROOT / "data/raw_bq"
GOLD = ROOT / "data/gold/parquet/procurement_payments_fact.parquet"
OUT = ROOT / "data/silver/parquet/disclosed_bq_po_payments_fact.parquet"
COVERAGE = ROOT / "data/_meta/disclosed_bq_po_coverage.json"

# Bodies that publish quarterly CATEGORY ROLL-UPS (blank supplier/PO, whole-category aggregates),
# NOT €20k line items — excluded from the line-item fact (DQ scan: pipeline_sandbox/
# disclosed_po_spend/DATA_QUALITY_BQ.md). Eirgrid even carries explicit "TOTAL" rows.
ROLLUP_BODIES = {"Irish Water", "Eirgrid", "Gas Networks Ireland"}

SCHEMA_COLS = [
    "publisher_id", "publisher_name", "publisher_type", "sector", "source_landing_url",
    "source_file_url", "source_file_hash", "period", "year", "quarter", "supplier_raw",
    "supplier_normalised", "amount_eur", "amount_semantics", "value_safe_to_sum", "description",
    "po_number", "paid_flag", "source_row_number", "source_page_number", "parser_name",
    "parser_version", "extraction_status", "extraction_confidence", "caveat_text_detected",
    "supplier_class", "privacy_status", "source_caveat", "public_display",
]
_NULL_TOKENS = {"null", "na", "n/a", "none", ""}


def _find_raw() -> Path | None:
    if not RAW_DIR.exists():
        return None
    cands = sorted(RAW_DIR.glob("bq-results-*.csv"))
    return cands[-1] if cands else None


def _clean(col: str) -> pl.Expr:
    # DQ rules (DATA_QUALITY_BQ.md): strip embedded \n\t\r, collapse whitespace, trim; null-tokens -> NULL.
    e = pl.col(col).cast(pl.Utf8).str.replace_all(r"[\r\n\t]", " ").str.replace_all(r"\s+", " ").str.strip_chars()
    return pl.when(e.str.to_lowercase().is_in(list(_NULL_TOKENS))).then(None).otherwise(e)


def build() -> int:
    raw = _find_raw()
    if raw is None:
        print(f"disclosed_bq_po: no raw drop under {RAW_DIR}/bq-results-*.csv — leaving existing silver untouched, exit 0")
        return 0
    if not GOLD.exists():
        print("disclosed_bq_po: gold fact missing — cannot inherit ie_hse identity/semantics; run procurement_consolidate first. exit 0")
        return 0

    # ie_hse identity + AUTHORITATIVE semantics + already-held periods, from gold (not guessed).
    g = pl.read_parquet(GOLD, columns=["publisher_id", "publisher_name", "publisher_type", "sector", "amount_semantics", "period", "parser_name"])
    hse_g = g.filter(pl.col("publisher_id") == "ie_hse")
    if hse_g.is_empty():
        print("disclosed_bq_po: ie_hse absent from gold — nothing to anchor to. exit 0")
        return 0
    # periods already held by the hse_tusla PDF parse (exclude these to avoid cross-lane double-count)
    held = {p for p in hse_g.filter(pl.col("parser_name") != "disclosed_bq_po")["period"].unique().to_list() if p}
    meta = hse_g.select("publisher_name", "publisher_type", "sector", "amount_semantics").head(1).to_dicts()[0]

    df = pl.read_csv(raw, schema_overrides={"Total": pl.Float64, "Year": pl.Int64}).with_columns(
        pl.col("entity").str.replace(r"^Agency\s*:\s*", "").str.strip_chars().alias("body")
    )
    df = df.filter(~pl.col("body").is_in(list(ROLLUP_BODIES)))  # general guard (no HSE rollup, but future-proof)
    hse = df.filter(pl.col("body") == "Health Service Executive").with_columns(
        (pl.col("Year").cast(pl.Utf8) + "-" + pl.col("QTR").str.to_uppercase()).alias("period")
    )
    netnew = sorted(set(hse["period"].unique().to_list()) - held)
    lane = hse.filter(pl.col("period").is_in(netnew))
    if lane.is_empty():
        print(f"disclosed_bq_po: no net-new HSE periods (gold already holds {sorted(held)}). exit 0")
        return 0

    csv_hash = hashlib.sha1(raw.read_bytes()[: 1 << 20]).hexdigest()[:16]
    lane = lane.with_columns(
        _clean("Supplier").alias("supplier_raw"),
        _clean("Description").alias("description"),
        _clean("PO").alias("po_number"),
        pl.col("Total").alias("amount_eur"),
        pl.col("Year").alias("year"),
        pl.col("QTR").str.replace("q", "").cast(pl.Int64).alias("quarter"),
        pl.lit("ie_hse").alias("publisher_id"),
        pl.lit(meta["publisher_name"]).alias("publisher_name"),
        pl.lit(meta["publisher_type"]).alias("publisher_type"),
        pl.lit(meta["sector"]).alias("sector"),
        pl.lit("https://www.hse.ie/eng/about/who/finance/").alias("source_landing_url"),
        ("disclosed-bq:" + pl.lit(raw.name) + "#" + pl.col("body") + "/" + pl.col("period")).alias("source_file_url"),
        pl.lit(csv_hash).alias("source_file_hash"),
        pl.lit(meta["amount_semantics"]).alias("amount_semantics"),  # AUTHORITATIVE from gold ie_hse
        pl.lit(None, dtype=pl.Utf8).alias("paid_flag"),
        pl.int_range(pl.len()).alias("source_row_number"),
        pl.lit(None, dtype=pl.Int64).alias("source_page_number"),
        pl.lit("disclosed_bq_po").alias("parser_name"),
        pl.lit("0.1.0").alias("parser_version"),
        pl.lit("extracted").alias("extraction_status"),
        pl.lit("high").alias("extraction_confidence"),  # cent-reconciled to our independent parse
        pl.lit(False).alias("caveat_text_detected"),
        pl.lit("Disclosed BigQuery PO/payments extract; HSE history recovery (periods absent from hse_tusla lane).").alias("source_caveat"),
    )
    # privacy + supplier_class + supplier_normalised + value_safe_to_sum (battle-tested classifier)
    lane = pbe.classify_and_flag(lane)
    missing = [c for c in SCHEMA_COLS if c not in lane.columns]
    if missing:
        raise SystemExit(f"disclosed_bq_po: missing schema columns {missing}")
    lane = lane.select(SCHEMA_COLS)

    # privacy invariant (refuse to write a leak — mirror the base extractor)
    leak = lane.filter(pl.col("public_display") & (pl.col("supplier_class") == "sole_trader_or_individual")).height
    if leak:
        raise SystemExit(f"disclosed_bq_po: privacy quarantine breached — {leak} individual rows public_display=True")

    save_parquet(lane, OUT, min_rows=40_000)  # floor: a truncated CSV must not silently shrink the lane
    cov = {
        "built_at": datetime.now(UTC).isoformat(),
        "source_csv": raw.name,
        "source_file_hash": csv_hash,
        "scope": "HSE history recovery (Phase 1)",
        "rows": lane.height,
        "periods": netnew,
        "gross_eur": float(lane["amount_eur"].sum()),
        "summable_eur": float(lane.filter(pl.col("value_safe_to_sum"))["amount_eur"].sum()),
        "rows_review_personal_data": int((lane["privacy_status"] == "review_personal_data").sum()),
    }
    COVERAGE.write_text(json.dumps(cov, indent=2), encoding="utf-8")
    print(
        f"disclosed_bq_po: wrote {lane.height:,} HSE rows ({len(netnew)} periods {netnew[0]}..{netnew[-1]}) "
        f"-> {OUT.relative_to(ROOT)}  (€{cov['gross_eur']/1e6:,.0f}m gross, {cov['rows_review_personal_data']:,} privacy-gated)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(build())
