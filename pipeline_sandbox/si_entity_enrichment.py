"""
pipeline_sandbox/si_entity_enrichment.py

Builds the Statutory Instrument as a first-class entity: one row per SI,
sourced from the Iris Oifigiuil SI taxonomy directly — NOT gated on a bill
match. Writes data/gold/parquet/statutory_instruments.parquet.

Companion to iris_si_bill_enrichment.py. That script answers "which SIs hang
off this bill" (36% match rate, skewed to pre-2014 framework Acts); this one
treats the SI itself as the entity, so the browsable SI surface is not gated
on a thin join. The bill link is carried as one optional attribute.

DERIVES:
  - si_id                 <year>-<number zero-padded to 3>
  - si_is_eu              si_eu_relationship is not in the "no EU" set
  - si_department         canonical department key, mapped from the free-text
                          si_responsible_actor role via the curated
                          data/_meta/si_department_aliases.csv
  - si_department_label
  - bill_id / bill_short_title
                          left-joined from bill_statutory_instruments.parquet
                          where the SI was matched to an enabling bill; null
                          otherwise (the matcher only ran on si_year >= 2018)

  - si_minister_member_code / si_minister_name
                          the actual minister, resolved by joining the SI's
                          department + signing date against the ministerial
                          tenure table (data/silver/ministerial_tenure.parquet,
                          built by ministerial_tenure_build.py from Wikidata).
                          member_code is null for ministers no longer in the
                          Oireachtas.

COVERAGE FLOOR: si_year >= 2016 (the taxonomy thins sharply below this).

USAGE:
    python pipeline_sandbox/si_entity_enrichment.py
"""
from __future__ import annotations

import logging
import re
import sys
from pathlib import Path

import pandas as pd

# Make root config importable when run via subprocess from project root.
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from config import GOLD_PARQUET_DIR, SILVER_DIR

logger = logging.getLogger(__name__)

_SI_CSV   = SILVER_DIR / "iris_oifigiuil" / "iris_si_taxonomy.csv"
_BILL_SI  = GOLD_PARQUET_DIR / "bill_statutory_instruments.parquet"
_ALIASES  = _ROOT / "data" / "_meta" / "si_department_aliases.csv"
_TENURE   = SILVER_DIR / "ministerial_tenure.parquet"
_OUT      = GOLD_PARQUET_DIR / "statutory_instruments.parquet"

SI_YEAR_FLOOR       = 2016
MIN_TAXO_CONFIDENCE = 0.5

# si_eu_relationship values that carry no EU dimension.
_EU_NEGATIVE = {"none_detected", "", "nan"}


def load_si() -> pd.DataFrame:
    """Clean SI universe from the taxonomy — same filters as the bill matcher
    except the year floor is 2016 (vs 2018), recovering ~1,500 SIs."""
    df = pd.read_csv(_SI_CSV, low_memory=False)
    df = df[df["notice_category"] == "statutory_instrument"]
    df = df[~df["is_quarantined"].fillna(False).astype(bool)]
    df = df[df["si_number"].notna() & df["si_year"].notna() & df["title"].notna()]
    df["si_year"]   = df["si_year"].astype(int)
    df["si_number"] = df["si_number"].astype(int)
    df = df[df["si_year"] >= SI_YEAR_FLOOR]
    df = df[df["si_taxonomy_confidence"].fillna(0) >= MIN_TAXO_CONFIDENCE]
    df["si_id"] = (df["si_year"].astype(str) + "-"
                   + df["si_number"].astype(str).str.zfill(3))
    df["issue_date"] = pd.to_datetime(df["issue_date"], errors="coerce")
    return df.drop_duplicates(subset=["si_id"]).reset_index(drop=True)


def load_department_aliases() -> list[tuple[str, str, str]]:
    """Curated (alias, department_key, department_label), sorted longest-alias
    first so specific phrases ('higher education') beat generic ones
    ('education') in the substring scan."""
    df = pd.read_csv(_ALIASES)
    rows = [
        (str(r["alias"]).strip().lower(),
         str(r["department_key"]).strip(),
         str(r["department_label"]).strip())
        for r in df.to_dict("records")
        if str(r.get("alias", "")).strip()
    ]
    rows.sort(key=lambda t: len(t[0]), reverse=True)
    return rows


_DEPT_LEAD_RE = re.compile(r"\bminister\s+(?:for|of)\s+(.+)", re.IGNORECASE)


def _lead_department_phrase(text: str) -> str:
    """The department phrase from a role / office title: the text after
    'Minister for', truncated at the first comma — office titles list
    several departments and the first is the primary one. Strings without
    'Minister for' (e.g. 'The Taoiseach', 'The Government') pass through."""
    if not isinstance(text, str) or not text.strip():
        return ""
    m = _DEPT_LEAD_RE.search(text)
    phrase = m.group(1) if m else text
    return phrase.split(",")[0].strip()


def canonicalise_department(text, aliases: list[tuple[str, str, str]]):
    """Map a free-text role / office title to a canonical department, via the
    leading department phrase. Returns (key, label); (None, None) when blank
    or unmatched (e.g. regulators, which are not ministerial departments)."""
    phrase = _lead_department_phrase(text).lower()
    if not phrase:
        return None, None
    for alias, key, label in aliases:
        if alias in phrase:
            return key, label
    return None, None


def load_bill_links() -> pd.DataFrame:
    """One row per si_id with its matched enabling bill, from the companion
    bill-matching enrichment. Missing file → empty frame (bill link stays
    null everywhere)."""
    if not _BILL_SI.exists():
        logger.warning("bill_statutory_instruments.parquet not found — bill "
                        "link will be null for every SI: %s", _BILL_SI)
        return pd.DataFrame(columns=["si_id", "bill_id", "bill_short_title"])
    df = pd.read_parquet(_BILL_SI)
    return (df[["si_id", "bill_id", "bill_short_title"]]
            .drop_duplicates(subset=["si_id"])
            .reset_index(drop=True))


def load_ministerial_offices() -> list[tuple[str, str | None, str, pd.Timestamp, pd.Timestamp]]:
    """Senior-minister tenure spans, as tuples of
    (department_key, member_code, minister_name, start, end).

    Sourced from data/silver/ministerial_tenure.parquet — built by
    pipeline_sandbox/ministerial_tenure_build.py from Wikidata, covering every
    Irish government since 2016. An open (null) end date — a serving minister
    — is treated as far-future so the date-window join works. member_code is
    null for ministers no longer in the Oireachtas (their SIs resolve to a
    name, not a clickable profile)."""
    if not _TENURE.exists():
        logger.warning("ministerial_tenure.parquet not found — minister person "
                        "will be null for every SI; run "
                        "ministerial_tenure_build.py first: %s", _TENURE)
        return []
    df = pd.read_parquet(_TENURE)
    far_future = pd.Timestamp("2099-12-31")
    out: list[tuple[str, str | None, str, pd.Timestamp, pd.Timestamp]] = []
    for r in df.to_dict("records"):
        start = pd.to_datetime(r.get("start_date"), errors="coerce")
        if pd.isna(start):
            continue
        end  = pd.to_datetime(r.get("end_date"), errors="coerce")
        code = r.get("member_code")
        code = None if (code is None or (isinstance(code, float) and pd.isna(code))) else str(code)
        out.append((str(r["department_key"]), code, str(r["minister_name"]),
                    start, far_future if pd.isna(end) else end))
    return out


def resolve_minister(dept_key, signed_date, offices):
    """The minister who held `dept_key` on `signed_date`. Returns
    (member_code, member_name); (None, None) when the SI has no department,
    no date, or falls outside every known office span (e.g. pre-2024 SIs —
    flattened_members has no historical tenures)."""
    if not dept_key or signed_date is None or pd.isna(signed_date):
        return None, None
    sd = pd.Timestamp(signed_date)
    hits = [o for o in offices if o[0] == dept_key and o[3] <= sd <= o[4]]
    if not hits:
        return None, None
    # A reshuffle on the signing date could yield two holders — take the one
    # whose tenure started most recently.
    hits.sort(key=lambda o: o[3], reverse=True)
    return hits[0][1], hits[0][2]


def run() -> dict:
    if not _SI_CSV.exists():
        raise SystemExit(f"SI taxonomy not found: {_SI_CSV}")
    if not _ALIASES.exists():
        raise SystemExit(f"department alias table not found: {_ALIASES}")

    si      = load_si()
    aliases = load_department_aliases()
    links   = load_bill_links()
    offices = load_ministerial_offices()

    dept = si["si_responsible_actor"].apply(
        lambda a: pd.Series(canonicalise_department(a, aliases),
                            index=["si_department", "si_department_label"])
    )
    si = pd.concat([si, dept], axis=1)

    # Resolve the SI's department + signing date to the minister who held
    # that office then. Current-government coverage only — see
    # load_ministerial_offices.
    minister = si.apply(
        lambda r: pd.Series(
            resolve_minister(r["si_department"], r["issue_date"], offices),
            index=["si_minister_member_code", "si_minister_name"]),
        axis=1,
    )
    si = pd.concat([si, minister], axis=1)

    si = si.merge(links, on="si_id", how="left")

    out = pd.DataFrame({
        "si_id":                  si["si_id"],
        "si_year":                si["si_year"],
        "si_number":              si["si_number"],
        "si_title":               si["title"],
        "si_signed_date":         si["issue_date"].dt.date,
        "si_operation":           si["si_operation_primary"],
        "si_operation_flags":     si["si_operation_flags"],
        "si_form":                si["si_form"],
        "si_eu_relationship":     si["si_eu_relationship"],
        "si_is_eu":               ~si["si_eu_relationship"].astype(str)
                                    .str.lower().isin(_EU_NEGATIVE),
        "si_policy_domain":       si["si_policy_domain_primary"],
        "si_policy_domains_all":  si["si_policy_domains"],
        "si_responsible_actor":   si["si_responsible_actor"],
        "si_department":          si["si_department"],
        "si_department_label":    si["si_department_label"],
        "si_minister_member_code": si["si_minister_member_code"],
        "si_minister_name":       si["si_minister_name"],
        "si_parent_legislation":  si["si_parent_legislation"],
        "bill_id":                si["bill_id"],
        "bill_short_title":       si["bill_short_title"],
        "eisb_url":               si["eisb_url"],
        "iris_source_pdf":        si["source_file"],
        "si_taxonomy_confidence": si["si_taxonomy_confidence"],
    })

    _OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(_OUT, index=False)

    total          = len(out)
    actor_known    = int(si["si_responsible_actor"].notna().sum())
    dept_known     = int(out["si_department"].notna().sum())
    minister_named = int(out["si_minister_name"].notna().sum())
    minister_coded = int(out["si_minister_member_code"].notna().sum())
    bill_linked    = int(out["bill_id"].notna().sum())
    eu_count       = int(out["si_is_eu"].sum())
    domain_known   = int(out["si_policy_domain"].notna().sum())

    summary = {
        "total_sis":        total,
        "year_range":       f"{int(out['si_year'].min())}-{int(out['si_year'].max())}",
        "actor_present":    actor_known,
        "department_known": dept_known,
        "minister_named":   minister_named,
        "minister_coded":   minister_coded,
        "bill_linked":      bill_linked,
        "domain_known":     domain_known,
        "eu_driven":        eu_count,
    }
    logger.info("statutory_instruments: %d SIs (%s)", total, summary["year_range"])
    logger.info("  responsible_actor present : %d (%.0f%%)",
                actor_known, 100 * actor_known / total)
    logger.info("  department canonicalised  : %d (%.0f%%)  — of actor-present: %.0f%%",
                dept_known, 100 * dept_known / total,
                100 * dept_known / actor_known if actor_known else 0)
    logger.info("  minister resolved (name)  : %d (%.0f%%)  — of department-known: %.0f%%",
                minister_named, 100 * minister_named / total,
                100 * minister_named / dept_known if dept_known else 0)
    logger.info("  minister linked (profile) : %d (%.0f%%)  — sitting members only",
                minister_coded, 100 * minister_coded / total)
    logger.info("  enabling bill linked      : %d (%.0f%%)",
                bill_linked, 100 * bill_linked / total)
    logger.info("  policy domain known       : %d (%.0f%%)",
                domain_known, 100 * domain_known / total)
    logger.info("  EU-driven                 : %d (%.0f%%)",
                eu_count, 100 * eu_count / total)
    return summary


if __name__ == "__main__":
    try:
        from services.logging_setup import setup_logging
        setup_logging()
    except Exception:
        logging.basicConfig(level=logging.INFO,
                            format="%(asctime)s %(levelname)s %(message)s")
    run()
