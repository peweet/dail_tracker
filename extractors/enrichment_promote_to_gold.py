"""Promote the three round-2 enrichment facts (ISIF / CBI / EU-TAM) sandbox → gold.

The medallion pattern (like extractors/epa_promote_to_gold.py and
extractors/diary_promote_gold.py): the live scrapers stay in pipeline_sandbox/
(ISIF portfolio page, CBI enforcement PDFs, EU-TAM 15k-row crawl — network-heavy,
fragile, run by hand, NOT wired into pipeline.py / the GH-Action). This script is
TRANSFORM-ONLY: it reads their already-vetted sandbox parquet and writes the
committed gold projection, so it is cheap, deterministic and reproducible.

Three gold facts, one row per source record:
  * isif_portfolio.parquet         — ISIF sovereign-fund investment commitments
  * cbi_enforcement_actions.parquet — Central Bank settlement / enforcement actions
  * eu_tam_state_aid.parquet        — EU State-Aid Transparency awards by Irish authorities

⚠️ PRIVACY (non-negotiable): data/gold/parquet/ is COMMITTED to the public repo.
Each extractor flags suspected natural persons (CBI ex-officers sanctioned in a
professional capacity; EU-TAM agri sole-traders). Those rows are DROPPED here so a
private individual's regulatory / grant record never reaches git or the UI
([[feedback_personal_insolvency_privacy]], same stance as epa_promote_to_gold.py).
Company names + amounts are the public regulatory record and are kept. The EU-TAM
raw ``national_id`` column is also dropped — it can carry a non-CRO natural-person
identifier; the parsed ``cro_company_num`` (companies only) is the safe join key.

VALUE SEMANTICS: every row carries ``value_safe_to_sum=False`` — these are
COMMITTED investments / sanction fines / AWARDED grant aid in mixed currencies and
"up to" / range phrasings. They must NEVER be summed or unioned with payment/award
facts. The views and tests lock this.

Run:  ./.venv/Scripts/python.exe extractors/enrichment_promote_to_gold.py
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

SANDBOX = ROOT / "data/sandbox/enrichment"
GOLD = ROOT / "data/gold/parquet"

# any column whose name contains one of these tokens is a potential natural-person
# identifier and must never reach committed gold.
_PII_TOKENS = ("address", "national_id", "home", "dob", "birth", "pps", "phone", "email")


def drop_individuals(df: pl.DataFrame, flag_col: str) -> pl.DataFrame:
    """Remove rows the extractor flagged as a SUSPECTED natural person.

    Conservative: a null flag is treated as 'not an individual' (kept), because the
    flag is only ever set True by a positive heuristic match — a null means the
    extractor never evaluated it, not that the row is a person.
    """
    if flag_col not in df.columns:
        return df
    return df.filter(~pl.col(flag_col).fill_null(False))


def pii_columns(df: pl.DataFrame) -> list[str]:
    """Columns whose name looks like a natural-person identifier (must not reach gold)."""
    return [c for c in df.columns if any(t in c.lower() for t in _PII_TOKENS)]


def assert_no_individuals(df: pl.DataFrame, flag_col: str) -> None:
    """Raise (so -O does not strip it) if any row is still flagged as a suspected person.

    Run on the FILTERED frame (flag column still present) right after drop_individuals.
    """
    if flag_col in df.columns:
        n = df.filter(pl.col(flag_col).fill_null(False)).height
        if n:
            raise RuntimeError(f"{n} suspected-individual row(s) would reach committed gold")


def assert_no_pii_columns(df: pl.DataFrame) -> None:
    """Raise if the gold-bound frame carries a natural-person identifier column."""
    leaked = pii_columns(df)
    if leaked:
        raise RuntimeError(f"PII column(s) {leaked} must not reach committed gold")


def promote_isif() -> None:
    src = SANDBOX / "isif_portfolio.parquet"
    if not src.exists():
        print(f"  !! no ISIF sandbox at {src} — run pipeline_sandbox/isif_portfolio_extract.py first")
        return
    df = pl.read_parquet(src)
    # investees are companies / funds (no natural-person flag); drop only the decorative image_url.
    out = df.select(
        "investee_name",
        "commitment_date",
        "commitment_year_label",
        "description",
        "amount_stated",
        "amount_currency",
        "amount_is_up_to",
        "value_kind",
        "realisation_tier",
        "value_safe_to_sum",
        "source_url",
        "ingested_date",
    )
    assert_no_pii_columns(out)
    dest = GOLD / "isif_portfolio.parquet"
    save_parquet(out, dest)
    print(f"  isif_portfolio          -> {dest.relative_to(ROOT)}  ({out.height} commitments)")


def promote_cbi() -> None:
    src = SANDBOX / "cbi_enforcement_actions.parquet"
    if not src.exists():
        print(f"  !! no CBI sandbox at {src} — run pipeline_sandbox/cbi_enforcement_extract.py first")
        return
    df = pl.read_parquet(src)
    flag = "party_is_individual_suspected"
    n_in = df.height
    df = drop_individuals(df, flag)
    assert_no_individuals(df, flag)  # flag still present here — must be all-False now
    out = df.select(
        "notice_date",
        "title",
        "party_name",
        "pdf_url",
        "doc_type",
        "fine_amount_eur",
        "has_text_layer",
        "n_euro_mentions",
        "value_kind",
        "value_safe_to_sum",
        "source_url",
        "ingested_date",
    )
    assert_no_pii_columns(out)
    dest = GOLD / "cbi_enforcement_actions.parquet"
    save_parquet(out, dest)
    print(f"  cbi_enforcement_actions -> {dest.relative_to(ROOT)}  ({out.height} firms, {n_in - out.height} individuals dropped)")


def promote_eu_tam() -> None:
    src = SANDBOX / "eu_tam_ireland_awards.parquet"
    if not src.exists():
        print(f"  !! no EU-TAM sandbox at {src} — run pipeline_sandbox/eu_tam_ireland_extract.py first")
        return
    df = pl.read_parquet(src)
    flag = "beneficiary_is_individual_suspected"
    n_in = df.height
    df = drop_individuals(df, flag)
    assert_no_individuals(df, flag)
    # national_id deliberately excluded (can be a non-CRO natural-person id); cro_company_num
    # is the parsed companies-only join key. country (always IRL) + all-null EU columns dropped.
    out = df.select(
        "aid_measure_title",
        "sa_number",
        "ref_no",
        "beneficiary_name",
        "beneficiary_type",
        "region",
        "sector_nace",
        "aid_instrument",
        "objective",
        "nominal_amount_raw",
        "nominal_amount_value",
        "nominal_amount_currency",
        "aid_element_raw",
        "aid_element_value",
        "aid_element_currency",
        "date_granted",
        "granting_authority",
        "entrusted_entity",
        "financial_intermediary",
        "published_date",
        "award_detail_url",
        "cro_company_num",
        "value_kind",
        "realisation_tier",
        "value_safe_to_sum",
        "ingested_date",
    )
    assert_no_pii_columns(out)
    dest = GOLD / "eu_tam_state_aid.parquet"
    save_parquet(out, dest)
    n_cro = out["cro_company_num"].is_not_null().sum()
    print(
        f"  eu_tam_state_aid        -> {dest.relative_to(ROOT)}  "
        f"({out.height} awards, {n_in - out.height} individuals dropped, {n_cro} CRO-joinable)"
    )


def main() -> None:
    print("=== PROMOTE enrichment sandbox -> gold (ISIF / CBI / EU-TAM) ===")
    GOLD.mkdir(parents=True, exist_ok=True)
    promote_isif()
    promote_cbi()
    promote_eu_tam()
    print("done.")


if __name__ == "__main__":
    main()
