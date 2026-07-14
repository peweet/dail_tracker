"""Promote the IPAS / international-protection accommodation corpus to gold.

TRANSFORM-ONLY (same contract as ``enrichment_promote_to_gold.py``): reads the vetted
sandbox parquets, applies the privacy + grain gates, and writes committed gold. It does
no fetching and no parsing — the extractors in ``pipeline_sandbox/new_sources/`` do that.

GOLD IS THE PUBLIC REPO. Everything written here is published, so the gates are hard:

PRIVACY
  * No resident is ever named, aged, located or quoted. The source extractors already
    excluded resident detail; ``assert_no_pii_columns`` re-checks at the gate.
  * PROVIDER names are companies, not natural persons — but they inherit the
    accommodation-providers ``public_display`` gate at the view layer, and only
    identity-resolved operators (``match_confidence='exact'``) are promoted at all.

GRAIN — the never-sum rule
  * Every row is ``value_safe_to_sum=False``. This corpus is AUDIT/REPORT NARRATIVE
    grain: figures quoted by the C&AG, HIQA and IGEES. They must NEVER be summed or
    unioned with ``procurement_payments_fact``, ``procurement_awards`` or grants.
  * Money columns carried from ``dceidy_ipas_legacy_spend`` are already filtered to
    ``stream='International Protection'`` upstream (Ukraine EXCLUDED — unfiltered, one
    provider reads EUR 46m against a true EUR 10.9m IP spend).

Run:  ./.venv/Scripts/python.exe extractors/ipas_promote_to_gold.py
"""

from __future__ import annotations

import contextlib
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.parquet_io import save_parquet  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

SANDBOX = Path("c:/tmp/dail_new_sources/silver")
GOLD = ROOT / "data/gold/parquet"

_PII_TOKENS = ("address", "national_id", "home", "dob", "birth", "pps", "phone", "email",
               "resident_name", "eircode")


def pii_columns(df: pl.DataFrame) -> list[str]:
    return [c for c in df.columns if any(t in c.lower() for t in _PII_TOKENS)]


def assert_no_pii_columns(df: pl.DataFrame, name: str) -> None:
    leaked = pii_columns(df)
    if leaked:
        raise ValueError(f"{name}: PII-suspect columns would reach public gold: {leaked}")


def _stamp(df: pl.DataFrame) -> pl.DataFrame:
    """Force the never-sum invariant at the gate, whatever the source said."""
    return df.with_columns(pl.lit(False).alias("value_safe_to_sum"))


def _write(df: pl.DataFrame, name: str, min_rows: int, note: str) -> None:
    assert_no_pii_columns(df, name)
    df = _stamp(df)
    dest = GOLD / f"{name}.parquet"
    save_parquet(df, dest, min_rows=min_rows)
    print(f"  {name:34s} -> {dest.relative_to(ROOT)}  ({df.height:,} rows)  {note}")


def _src(name: str) -> pl.DataFrame | None:
    p = SANDBOX / f"{name}.parquet"
    if not p.exists():
        print(f"  !! missing sandbox input: {p}")
        return None
    return pl.read_parquet(p)


def promote_facts() -> None:
    """The citation backing store — every published figure traces to a row here."""
    df = _src("ipas_facts")
    if df is None:
        return
    _write(df, "ipas_facts", min_rows=4_000,
           note="citation store; 23 categories; explicit unknowns preserved")


def promote_operators() -> None:
    """Only identity-RESOLVED operators (exact match on the house name_norm key)."""
    df = _src("ipas_entity_resolution")
    if df is None:
        return
    pub = df.filter(pl.col("match_confidence") == "exact")
    out = pub.select(
        "entity_key", "display_name", "centres", "judgments", "not_compliant",
        "pct_not_compliant", "dcediy_ip_eur", "doj_eur", "match_confidence",
        "caveat", "join_caveat", "value_safe_to_sum",
    )
    _write(out, "ipas_operators", min_rows=15,
           note="exact-match only; NEVER causal (compliance vs payment windows differ)")


def promote_compliance() -> None:
    """Per-centre x per-standard HIQA judgments (the drill-down)."""
    df = _src("hiqa_centre_compliance")
    if df is None:
        return
    keep = [c for c in ("centre_id", "centre_name", "county", "provider_name",
                        "provider_name_canonical", "provider_key", "inspection_date",
                        "standard_ref", "standard_title", "judgment", "judgment_conflict",
                        "risk_rating", "source_url", "page", "confidence")
            if c in df.columns]
    _write(df.select(keep), "ipas_centre_compliance", min_rows=2_000,
           note="2,668 judgments; judgment_conflict flags HIQA self-contradictions")


def promote_standards() -> None:
    df = _src("national_standards_lookup")
    if df is None:
        return
    _write(df, "ipas_national_standards", min_rows=30,
           note="joins 100% of the compliance judgments")


def promote_county() -> None:
    """Per-LA applicants (+ per-capita if the CSO population landed)."""
    pc = _src("ipas_la_percapita")
    if pc is not None:
        _write(pc, "ipas_la_profile", min_rows=25,
               note="31 LAs, IP applicants + per-1,000 population")
        return
    la = _src("ipas_by_local_authority")
    if la is None:
        return
    _write(la, "ipas_la_profile", min_rows=25,
           note="31 LAs, IP applicants (per-capita PENDING CSO population)")


def promote_rates() -> None:
    df = _src("ipas_sample_property_rates")
    if df is None:
        return
    _write(df, "ipas_property_rates", min_rows=15,
           note="C&AG Annex 10A: EUR 40-170 per person per night")


def promote_entitlements() -> None:
    df = _src("ipas_entitlements")
    if df is None:
        return
    _write(df, "ipas_entitlements", min_rows=8,
           note="entitlement vs reality; law quoted from SI 230/2018 as amended")


def main() -> None:
    print("IPAS -> gold (transform-only; gold is the PUBLIC repo)")
    GOLD.mkdir(parents=True, exist_ok=True)
    promote_facts()
    promote_operators()
    promote_compliance()
    promote_standards()
    promote_county()
    promote_rates()
    promote_entitlements()
    print("done. Every row value_safe_to_sum=False — audit-narrative grain, never union "
          "with payments/awards/grants.")


if __name__ == "__main__":
    main()
