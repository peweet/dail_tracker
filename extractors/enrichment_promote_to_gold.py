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
Company names + amounts are the public regulatory record and are kept.

EU-TAM uses a PRIVACY-FIRST ALLOWLIST, not the extractor's name flag (which has false negatives —
it leaked named persons and family partnerships into gold: >4-word names, ``&``-joined couples,
``T/A`` sole traders). The National-ID field is the real classifier — it labels beneficiaries
literally ('Sole-Trader', 'Personal ID number', 'Herd Number', numeric CRO, …). ``tam_organisation_mask``
keeps a row ONLY if it is provably an organisation (parsed CRO number, a 'CRO …'-prefixed company
number, charity CHY registration, a real incorporation / institution token in the name, or an exact
match in the hand-curated allowlist data/_meta/tam_org_allowlist.csv) AND its National-ID is not an
unambiguous natural-person label. ~7,976 of 15,593 survive (5,576 CRO-joinable); every sole-trader /
herd-number farmer / personal-ID beneficiary is dropped. The raw ``national_id`` column itself never
ships — the parsed ``cro_company_num`` (companies only) is the safe join key.

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


# --- EU-TAM beneficiary classification: the National-ID field is a structured classifier ---
# NAME tokens that POSITIVELY prove an organisation: incorporation suffixes (incl. unlimited
# companies and foreign forms inc/llc/gmbh) and public/institutional bodies. Deliberately EXCLUDES
# 'partnership' / 'bros' / '& sons' / 'T/A' / 'farm(s)' / 'productions' — those denote NAMED NATURAL
# PERSONS or are too weak to tell a person's trading name from a company.
_TAM_ORG_NAME = (
    r"(?i)(\b(limited|ltd|plc|dac|clg|ulc|unlimited|unltd|ultd|inc|llc|gmbh|teoranta|teo|company|"
    r"holdings|group|council|comhairle|university|college|institute|board|authority|commission|"
    r"hospital|society|associations?|federation|foundation|cooperative|co-?op|creamery|mart|marts|"
    r"credit union|centre|center|club|gaa)\b|unlimited company)"
)
# National-ID values that UNAMBIGUOUSLY denote a natural person — a HARD drop that overrides any
# coincidental org token in the name (a 'Sole-Trader' is never a company, even if named "X & Co").
_TAM_PERSON_NATIONAL_ID = r"(?i)(sole.?trader|personal id)"
# National-ID values that prove a registered COMPANY: a parsed numeric CRO (cro_company_num) OR a
# 'CRO 119570'-style prefixed number the bare ^\d{5,7}$ parse missed (e.g. CIÉ / Bus Éireann).
_TAM_COMPANY_NATIONAL_ID = r"(?i)^cro\s*\d{4,7}$"

# Hand-curated allowlist of verified organisations whose TAM name carries NO machine-detectable
# token or number (state bodies, multinationals trading under a business name, community orgs).
# Source of truth: data/_meta/tam_org_allowlist.csv. Matching is case-insensitive on the trimmed name.
ALLOWLIST_CSV = ROOT / "data/_meta/tam_org_allowlist.csv"


def load_org_allowlist() -> set[str]:
    """Lower-cased, trimmed set of verified org names from data/_meta/tam_org_allowlist.csv."""
    if not ALLOWLIST_CSV.exists():
        return set()
    names = pl.read_csv(ALLOWLIST_CSV)["beneficiary_name"].drop_nulls().to_list()
    return {n.strip().lower() for n in names if n.strip()}


def drop_private_individuals(df: pl.DataFrame, flag_col: str) -> pl.DataFrame:
    """Remove rows the extractor flagged as a suspected natural person (CBI: ex-officers).

    A null flag is treated as 'not an individual' (kept) — the flag is only ever set True by a
    positive heuristic match. EU-TAM does NOT use this path (its name flag has too many false
    negatives); it uses the National-ID-driven ``tam_organisation_mask`` allowlist instead.
    """
    if flag_col not in df.columns:
        return df
    return df.filter(~pl.col(flag_col).fill_null(False))


def assert_no_private_individuals(df: pl.DataFrame, flag_col: str) -> None:
    """Raise (so -O does not strip it) if any flagged suspected person survives. CBI guard."""
    if flag_col in df.columns:
        n = df.filter(pl.col(flag_col).fill_null(False)).height
        if n:
            raise RuntimeError(f"{n} suspected private-individual row(s) would reach committed gold")


def tam_organisation_mask(df: pl.DataFrame, allowlist: set[str] | None = None) -> pl.Expr:
    """KEEP mask for EU-TAM: a row reaches committed gold ONLY if it is PROVABLY an organisation.

    Privacy-first (gold is public): the extractor's name-shape flag has false negatives — it let
    through named persons / family partnerships (>4-word names, ``&``-joined couples, ``T/A`` sole
    traders) and the National-ID field even labels beneficiaries literally ('Sole-Trader',
    'Personal ID number'). So we ALLOWLIST instead of denylist. A row is an organisation iff:
      * its National-ID is NOT an unambiguous natural-person label, AND
      * it has a parsed CRO number, a 'CRO …'-prefixed company number, a charity (CHY)
        registration, an incorporation / institution token in the name, OR an exact match in the
        hand-curated org allowlist (data/_meta/tam_org_allowlist.csv).
    Everything else (sole traders, herd-number farmers without an incorporated name, TIN /
    Personal-ID beneficiaries) is dropped.
    """
    allow = sorted(allowlist or set())
    person_label = pl.col("national_id").fill_null("").str.contains(_TAM_PERSON_NATIONAL_ID)
    has_cro = pl.col("cro_company_num").is_not_null()
    cro_prefixed = pl.col("national_id").fill_null("").str.contains(_TAM_COMPANY_NATIONAL_ID)
    is_charity = pl.col("national_id").fill_null("").str.contains(r"(?i)^chy\s?\d+")
    org_name = pl.col("beneficiary_name").fill_null("").str.contains(_TAM_ORG_NAME)
    in_allowlist = (
        pl.col("beneficiary_name").fill_null("").str.strip_chars().str.to_lowercase().is_in(allow)
        if allow
        else pl.lit(False)
    )
    return ~person_label & (has_cro | cro_prefixed | is_charity | org_name | in_allowlist)


def assert_tam_no_named_person(df: pl.DataFrame) -> None:
    """Raise (so -O does not strip it) if any unambiguous natural-person National-ID survives.

    Run on the KEPT frame BEFORE national_id is projected away. -O-proof backstop on the allowlist.
    """
    if "national_id" in df.columns:
        n = df.filter(pl.col("national_id").fill_null("").str.contains(_TAM_PERSON_NATIONAL_ID)).height
        if n:
            raise RuntimeError(f"{n} unambiguous natural-person row(s) (Sole-Trader/Personal ID) would reach gold")


def pii_columns(df: pl.DataFrame) -> list[str]:
    """Columns whose name looks like a natural-person identifier (must not reach gold)."""
    return [c for c in df.columns if any(t in c.lower() for t in _PII_TOKENS)]


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
    df = drop_private_individuals(df, flag)  # CBI has no company-number column: drop every flagged person
    assert_no_private_individuals(df, flag)  # flag still present here — must be all-False now
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
    print(
        f"  cbi_enforcement_actions -> {dest.relative_to(ROOT)}  ({out.height} firms, {n_in - out.height} individuals dropped)"
    )


def promote_eu_tam() -> None:
    src = SANDBOX / "eu_tam_ireland_awards.parquet"
    if not src.exists():
        print(f"  !! no EU-TAM sandbox at {src} — run pipeline_sandbox/eu_tam_ireland_extract.py first")
        return
    df = pl.read_parquet(src)
    n_in = df.height
    # Privacy-first ALLOWLIST: keep only provable organisations (the extractor's name flag has too
    # many false negatives — it leaked named persons / family partnerships into gold). The National-ID
    # field is the real classifier; tam_organisation_mask keeps CRO / 'CRO …' / charity / org-token /
    # curated-allowlist rows and drops every sole-trader, herd-number farmer and personal-ID
    # beneficiary. ~5,576 carry a CRO.
    df = df.filter(tam_organisation_mask(df, load_org_allowlist()))
    assert_tam_no_named_person(
        df
    )  # -O-proof: no Sole-Trader / Personal-ID row survived (before national_id is dropped)
    # national_id deliberately excluded (can be a natural-person id); cro_company_num is the parsed
    # companies-only join key. country (always IRL) + all-null EU columns dropped.
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
        f"({out.height} organisations kept, {n_in - out.height} natural-persons / unprovable dropped, "
        f"{n_cro} CRO-joinable)"
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
