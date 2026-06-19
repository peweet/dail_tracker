"""EXPERIMENTAL (tracked code, gitignored sandbox data) — the EPA-LICENCE capability register: the
Tier-A register pilot end-to-end (register → CRO identity/health → public award + spend track record).

Joins the EPA licensed-facility pull ([[epa_licence_register_scrape]]) to the CRO company master and
to the procurement award/spend spine, producing one row per CRO firm: the EPA licences it holds + its
company identity/health + what it has WON (eTenders/TED awards) and been PAID (payments fact) on public
work. The environmental-credential answer to "who is licensed to do this, and how do they perform on
public contracts" — the supply-side loop the Standards & Credentials page is built on.

It reuses the shared join discipline in [[_capability_join]] (the same one behind
[[nsai_capability_register]]): canonical ``name_norm`` exact match, ambiguity-aware live-preference,
and a location-corroborated fuzzy gate. Public money is reported in THREE tiers that are NEVER summed —
SPENT (``public_paid_eur``) / COMMITTED (``public_committed_eur``, purchase orders) / AWARDED
(``total_award_eur`` = eTenders + TED).

Findings are LEADS TO INVESTIGATE, not conclusions (no-inference rule).

Outputs (gitignored):
  data/sandbox/parquet/epa_capability_register.parquet
  data/sandbox/epa_capability_register_summary.json
Run (scrape first): ./.venv/Scripts/python.exe pipeline_sandbox/epa_capability_register.py
"""

from __future__ import annotations

import json
import logging
import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline_sandbox._capability_join import (  # noqa: E402
    PUBLIC_BODY,
    attach_award_and_spend,
    collapse_by_cro,
    match_to_cro,
)
from services.logging_setup import setup_standalone_logging  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402

log = logging.getLogger(__name__)

LICENCES = ROOT / "data/sandbox/parquet/epa_licensed_facilities.parquet"
OPERATORS = ROOT / "data/sandbox/parquet/epa_licence_operators.parquet"  # corporate operator per licence
OUT = ROOT / "data/sandbox/parquet/epa_capability_register.parquet"
OUT_SUMMARY = ROOT / "data/sandbox/epa_capability_register_summary.json"

# a licence-holder that is a named individual (sole trader) rather than a company — personal data, so
# we keep it for the match but flag it (mirrors the NSAI register's privacy posture).
_INDIVIDUAL = re.compile(r"^(mr|mrs|ms|miss|dr)\b\.?\s+\w", re.I)
_COMPANY_WORD = re.compile(r"\b(ltd|limited|plc|dac|clg|company|teo|teoranta|unlimited|holdings|group)\b", re.I)


def _looks_individual(name: str) -> bool:
    n = str(name)
    return bool(_INDIVIDUAL.match(n)) or (not _COMPANY_WORD.search(n) and 1 <= n.count(" ") <= 2 and n.istitle())


def _licensees() -> pd.DataFrame:
    """One row per EPA OPERATOR (corporate entity): licence portfolio + a single (modal) location.

    Keyed on the LEAP ``operator_name`` ([[epa_operator_enrich]]) rather than the WFS facility Name —
    it is the operating company (cleaner CRO key, +13.6pp match rate) and correctly collapses a firm's
    multiple sites into one row. Falls back to the facility name where no operator is known.
    """
    lic = pd.read_parquet(LICENCES)
    if OPERATORS.exists():
        op = pd.read_parquet(OPERATORS)[["profile_number", "operator_name", "operator_id"]]
        lic = lic.merge(op, left_on="reg_code", right_on="profile_number", how="left")
    else:
        lic["operator_name"], lic["operator_id"] = None, None
    lic["entity_name"] = lic["operator_name"].fillna(lic["licensee_name"]).astype(str).str.strip()
    lic["is_active_licence"] = lic["licence_status"].astype(str).str.fullmatch("Licensed", case=False)
    firms = (
        lic.groupby("entity_name")
        .agg(
            location=("location", lambda x: x.mode().iloc[0] if len(x.mode()) else ""),
            facility_names=("licensee_name", lambda x: sorted(set(x.dropna()))),
            licences=("licence_number", lambda x: sorted(set(x.dropna()))),
            licence_classes=("licence_class", lambda x: sorted(set(x.dropna()))),
            licence_statuses=("licence_status", lambda x: sorted(set(x.dropna()))),
            any_active_licence=("is_active_licence", "any"),
            operator_id=("operator_id", lambda x: x.dropna().iloc[0] if x.notna().any() else None),
        )
        .reset_index()
        .rename(columns={"entity_name": "licensee_name"})  # display/match key = the corporate operator
    )
    firms["n_licences"] = firms["licences"].map(len)
    firms["is_public_body"] = firms["licensee_name"].map(lambda n: bool(PUBLIC_BODY.search(str(n))))
    firms["looks_individual"] = firms["licensee_name"].map(_looks_individual)
    return firms


def build() -> pd.DataFrame:
    firms = _licensees()
    df = match_to_cro(firms, name_col="licensee_name", location_col="location")
    df = attach_award_and_spend(df)
    df = collapse_by_cro(
        df,
        list_cols=("licences", "licence_classes", "licence_statuses", "facility_names"),
        name_col="licensee_name",
    )
    # recompute portfolio size after name-variant merge so it stays consistent with the unioned list
    df["n_licences"] = df["licences"].map(lambda v: len(v) if isinstance(v, (list, tuple)) else 0)
    return df


def _summary(df: pd.DataFrame) -> dict:
    matched = df[df["cro_company_num"].notna()]
    hi = matched[~matched["match_review_needed"] & (matched["match_confidence"] >= 0.85)]
    paid = hi[hi["received_public_money"]]
    won = hi[hi["won_public_award"]]
    track = hi[hi["has_public_track_record"]]
    leads = paid[paid["not_good_standing"]]
    return {
        "epa_licensees": int(len(df)),
        "named_individuals_flagged": int(df["looks_individual"].sum()),
        "matched_to_cro": int(len(matched)),
        "high_confidence_live": int(len(hi)),
        "review_needed_dissolved": int(matched["match_review_needed"].sum()),
        "match_methods": {k: int(v) for k, v in matched["match_method"].value_counts().items()},
        # PUBLIC MONEY — three never-summed tiers: SPENT (paid) / COMMITTED (PO) / AWARDED (eTenders+TED)
        "high_conf_received_public_money": int(len(paid)),
        "high_conf_public_paid_eur_safe_sum": float(round(paid["public_paid_eur"].fillna(0).sum(), 2)),
        "high_conf_public_committed_eur_safe_sum": float(round(paid["public_committed_eur"].fillna(0).sum(), 2)),
        "high_conf_won_public_award": int(len(won)),
        "high_conf_won_etenders": int(hi["won_etenders"].sum()),
        "high_conf_won_ted": int(hi["won_ted"].sum()),
        "high_conf_total_awarded_eur_safe_sum": float(round(won["total_award_eur"].sum(), 2)),
        # certified/licensed AND any public track record — the keystone capability signal
        "high_conf_with_public_track_record": int(len(track)),
        "live_but_overdue_with_public_money": int(len(leads)),
        "live_but_overdue_public_paid_eur": float(round(leads["public_paid_eur"].fillna(0).sum(), 2)),
        "caveats": [
            "Findings are leads to investigate, not conclusions.",
            "Public money is THREE tiers — SPENT (public_paid_eur) / COMMITTED (public_committed_eur) / "
            "AWARDED (total_award_eur) — NEVER added together; po_committed is an order, not money out.",
            "EPA 'Name' is the facility/licensee name; for waste/industrial it is usually the operating "
            "company, but some site-named waste facilities will legitimately not match CRO.",
            "looks_individual flags sole-trader licence holders (personal data) — handle per privacy rules.",
            "fuzzy_name_loc can admit rare false positives where name+county coincide (review bucket).",
        ],
    }


def main() -> None:
    setup_standalone_logging("epa_capability_register")
    df = build()
    save_parquet(df, OUT)
    summary = _summary(df)
    OUT_SUMMARY.parent.mkdir(parents=True, exist_ok=True)
    OUT_SUMMARY.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    log.info(
        "WROTE %s — %d licensees | matched %d | PAID €%.0f / COMMITTED €%.0f (%d) | AWARDED €%.0f (%d) | track %d",
        OUT,
        len(df),
        summary["matched_to_cro"],
        summary["high_conf_public_paid_eur_safe_sum"],
        summary["high_conf_public_committed_eur_safe_sum"],
        summary["high_conf_received_public_money"],
        summary["high_conf_total_awarded_eur_safe_sum"],
        summary["high_conf_won_public_award"],
        summary["high_conf_with_public_track_record"],
    )
    log.info("summary: %s", json.dumps(summary["match_methods"]))


if __name__ == "__main__":
    main()
