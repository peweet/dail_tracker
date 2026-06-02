"""PROBE (throwaway): feasibility of the CROSS-enrichments from the review plan
that use data ALREADY on disk. Measures real overlap + collision risk so we know
which links are worth building and which are too sparse / too risky.

Links probed:
  E-D  corporate notice entity  -> lobbying organisation   (name_norm exact)
  ---  corporate notice entity  -> charity register        (name_norm exact)
  E-E  corporate notice entity  -> member declared interest (company-token match)
  E-G  statutory instrument     -> signing minister / member (key link)

PRIVACY: member interests are PUBLIC declarations; we only ever match a COMPANY
name, never a person. Personal insolvency is already out of corporate scope.

Run:  .venv/Scripts/python.exe pipeline_sandbox/probe_cross_enrichment.py
Reads only; writes nothing.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from cro_normalise import name_norm_expr  # noqa: E402

NOTICES = ROOT / "data/gold/parquet/corporate_notices.parquet"
LOBBY_ORGS = ROOT / "data/gold/parquet/top_lobbyist_organisations.parquet"
CHARITY = ROOT / "data/silver/charities/charity_resolved.parquet"
INTERESTS = ROOT / "data/silver/parquet/dail_member_interests_combined.parquet"
SIS = ROOT / "data/gold/parquet/statutory_instruments.parquet"

BOILERPLATE_RE = re.compile(r"NOTICE IS HEREBY|ABOVE NAMED|IN THE MATTER|COMPANIES ACT|ICAV ACT|COLLECTIVE ASSET", re.I)
COMPANY_TOKEN_RE = re.compile(r"([A-Z][A-Za-z0-9&.,'\- ]{2,60}?\b(?:Limited|Ltd|DAC|PLC|CLG|Unlimited Company|UC))\b")


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def clean_corporate_names(n: pl.DataFrame) -> pl.DataFrame:
    """Distinct, clean, join-ready normalised corporate entity names."""
    n = n.with_columns(name_norm_expr("entity_name").alias("nn"))
    n = n.filter(
        pl.col("entity_name").is_not_null()
        & ~pl.col("entity_name").map_elements(lambda s: bool(BOILERPLATE_RE.search(s or "")), return_dtype=pl.Boolean)
        & (pl.col("nn").str.len_chars() >= 6)
    )
    return n.select(["nn", "entity_name", "notice_subtype"]).unique(subset=["nn"])


def main() -> None:
    notices = pl.read_parquet(NOTICES)
    corp = clean_corporate_names(notices)
    hr("BASIS")
    print(f"distinct clean corporate entity names (>=6 chars): {corp.height:,}")

    # ---- corporate -> charity (name_norm exact) --------------------------
    ch = pl.read_parquet(CHARITY).select(["name_norm", "registered_charity_name", "status", "cro_number"]).unique(subset=["name_norm"])
    m_ch = corp.join(ch, left_on="nn", right_on="name_norm", how="inner")
    hr("corporate notice -> CHARITY register (exact name_norm)")
    print(f"charities (distinct name_norm): {ch.height:,}")
    print(f"corporate names matching a charity: {m_ch.height:,}  ({m_ch.height / corp.height:.2%} of corporate)")
    print(m_ch.select(["entity_name", "registered_charity_name", "notice_subtype", "status"]).head(8))

    # ---- corporate -> lobbying organisation (name_norm exact) ------------
    lob = pl.read_parquet(LOBBY_ORGS).with_columns(name_norm_expr("lobbyist_name").alias("nn")).unique(subset=["nn"])
    m_lob = corp.join(lob.select(["nn", "lobbyist_name", "returns_filed"]), on="nn", how="inner")
    hr("corporate notice -> LOBBYING organisation (exact name_norm)")
    print(f"lobbying orgs (distinct name_norm): {lob.height:,}")
    print(f"corporate names matching a lobby org: {m_lob.height:,}  ({m_lob.height / corp.height:.2%} of corporate)")
    print(m_lob.select(["entity_name", "lobbyist_name", "notice_subtype", "returns_filed"]).sort("returns_filed", descending=True).head(10))

    # ---- corporate -> MEMBER INTERESTS (company token in free text) ------
    intr = pl.read_parquet(INTERESTS)
    descs = intr.select(["full_name", "interest_category", "interest_description_cleaned"]).drop_nulls("interest_description_cleaned")
    # extract company-like tokens from the free-text declarations
    rows = descs.to_dicts()
    tok_rows = []
    for r in rows:
        for m in COMPANY_TOKEN_RE.findall(r["interest_description_cleaned"]):
            tok_rows.append({"member": r["full_name"], "category": r["interest_category"], "company_token": m.strip()})
    toks = pl.DataFrame(tok_rows) if tok_rows else pl.DataFrame({"member": [], "category": [], "company_token": []})
    hr("member interests -> COMPANY tokens (feasibility of E-E)")
    print(f"interest rows with text          : {descs.height:,}")
    print(f"company-like tokens extracted    : {toks.height:,}")
    if toks.height:
        toks = toks.with_columns(name_norm_expr("company_token").alias("nn")).filter(pl.col("nn").str.len_chars() >= 6)
        m_int = toks.join(corp.select(["nn", "entity_name", "notice_subtype"]), on="nn", how="inner")
        hr("corporate notice -> MEMBER INTEREST company (exact, via extracted token)")
        print(f"declared-interest companies matching a corporate notice: {m_int.height:,}")
        print("(this is the HIGHEST-RISK link — show samples, judge precision by eye)")
        print(m_int.select(["member", "category", "entity_name", "notice_subtype"]).unique().head(12))

    # ---- SI -> signing minister / member (E-G, key link) -----------------
    sis = pl.read_parquet(SIS)
    hr("statutory instrument -> SIGNING MINISTER / member profile (E-G)")
    has_code = sis.filter(pl.col("si_minister_member_code").is_not_null() & (pl.col("si_minister_member_code") != "")).height
    has_name = sis.filter(pl.col("si_minister_name").is_not_null() & (pl.col("si_minister_name") != "")).height
    print(f"SIs total                  : {sis.height:,}")
    print(f"  with minister NAME        : {has_name:,}  ({has_name / sis.height:.1%})")
    print(f"  with member CODE (linkable): {has_code:,}  ({has_code / sis.height:.1%})  <- clean profile link")
    print(f"  distinct linked members   : {sis.filter(pl.col('si_minister_member_code') != '')['si_minister_member_code'].n_unique():,}")
    print("top signatory ministers by SI count:")
    print(sis.filter(pl.col("si_minister_member_code").is_not_null() & (pl.col("si_minister_member_code") != ""))
          .group_by("si_minister_name").len().sort("len", descending=True).head(8))

    hr("VERDICT SUMMARY")
    print(f"corp->charity        : {m_ch.height:,} matches  ({m_ch.height / corp.height:.2%})")
    print(f"corp->lobbying org   : {m_lob.height:,} matches  ({m_lob.height / corp.height:.2%})")
    print(f"corp->member interest: {(m_int.height if toks.height else 0):,} matches (high risk, eyeball precision)")
    print(f"SI->minister (key)   : {has_code:,} linkable  ({has_code / sis.height:.1%})")


if __name__ == "__main__":
    main()
