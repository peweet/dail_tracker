"""EXPERIMENTAL (tracked code, gitignored sandbox data) — the EPA ACCOUNTABILITY view: the join that
makes the EPA leg worth building — public money × environmental-licence × EPA enforcement record.

Three axes nobody publishes joined-up, per firm:
  * IDENTITY + MONEY — from [[epa_capability_register]]: CRO firm, what it WON (eTenders/TED) and was
    PAID (payments fact), each kept in its own never-summed tier.
  * LICENCE          — the EPA licence(s) it holds (industrial / waste) and their status.
  * COMPLIANCE       — its EPA enforcement record from [[epa_enforcement_pull]]: incidents, complaints
    and non-compliances logged against those licences.

The headline LEAD: firms that took public money while carrying a poor EPA enforcement record. Councils
operating their own landfills are separated out (``is_public_body``) — that is a different story
(public-body self-operation) from a private contractor on the public payroll with a bad compliance log.

Findings are LEADS TO INVESTIGATE, not conclusions (no-inference rule): an open non-compliance is not
proven wrongdoing, an enforcement count is workload not guilt, and money tiers are never summed.

Outputs (gitignored):
  data/sandbox/parquet/epa_accountability_view.parquet
  data/sandbox/epa_accountability_summary.json
Run (after register + enforcement pull):
  ./.venv/Scripts/python.exe pipeline_sandbox/epa_accountability_view.py
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.logging_setup import setup_standalone_logging  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402

log = logging.getLogger(__name__)

REGISTER = ROOT / "data/sandbox/parquet/epa_capability_register.parquet"
ENFORCEMENT = ROOT / "data/sandbox/parquet/epa_enforcement.parquet"
OUT = ROOT / "data/sandbox/parquet/epa_accountability_view.parquet"
OUT_SUMMARY = ROOT / "data/sandbox/epa_accountability_summary.json"

_EVENT_COLS = ["n_incident", "n_complaint", "n_non_compliance", "n_open", "n_site_visit", "n_enforcement_events"]


def _enforcement_per_firm(reg: pd.DataFrame, enf: pd.DataFrame) -> pd.DataFrame:
    """Aggregate per-licence enforcement up to the CRO firm, via the register's own ``licences`` list."""
    pairs = reg[["cro_company_num", "licences"]].explode("licences").rename(columns={"licences": "licence_number"})
    pairs = pairs[pairs["licence_number"].notna() & pairs["cro_company_num"].notna()]
    j = pairs.merge(enf, on="licence_number", how="inner")
    if j.empty:
        return pd.DataFrame(columns=["cro_company_num", *_EVENT_COLS, "n_licences_crawled", "last_record_date", "operator_name"])
    agg = (
        j.groupby("cro_company_num")
        .agg(
            **{c: (c, "sum") for c in _EVENT_COLS},
            n_licences_crawled=("licence_number", "nunique"),
            last_record_date=("last_record_date", "max"),
            uww_priority_site=("uww_priority_site", "any"),
            operator_name=("organisation_name", lambda x: x.dropna().iloc[0] if x.notna().any() else None),
        )
        .reset_index()
    )
    return agg


def build() -> pd.DataFrame:
    reg = pd.read_parquet(REGISTER)
    enf = pd.read_parquet(ENFORCEMENT)
    enf = enf[enf["status"] == "ok"].copy()

    agg = _enforcement_per_firm(reg, enf)
    df = reg.merge(agg, on="cro_company_num", how="left")
    for c in _EVENT_COLS + ["n_licences_crawled"]:
        df[c] = df[c].fillna(0).astype("int64")
    df["uww_priority_site"] = df["uww_priority_site"].fillna(False)

    df["has_enforcement"] = df["n_enforcement_events"] > 0
    df["enforcement_crawled"] = df["n_licences_crawled"] > 0
    # the headline lead: public money AND a non-trivial enforcement record (and a real CRO match)
    df["public_money_and_enforcement"] = (
        df["has_public_track_record"] & df["has_enforcement"] & df["cro_company_num"].notna()
    )
    return df


def _leads(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "licensee_name", "cro_name", "is_public_body", "licence_classes",
        "public_eur", "total_award_eur", "n_enforcement_events",
        "n_incident", "n_complaint", "n_non_compliance", "n_open", "last_record_date", "match_caveat",
    ]
    leads = df[df["public_money_and_enforcement"]].copy()
    return leads.sort_values("n_enforcement_events", ascending=False)[[c for c in cols if c in leads.columns]]


def _summary(df: pd.DataFrame, leads: pd.DataFrame) -> dict:
    priv = leads[~leads["is_public_body"]]
    pub = leads[leads["is_public_body"]]
    return {
        "firms_in_view": int(len(df)),
        "firms_enforcement_crawled": int(df["enforcement_crawled"].sum()),
        "firms_with_public_money": int(df["has_public_track_record"].sum()),
        "LEADS_public_money_and_enforcement": int(len(leads)),
        "  of_which_private_firms": int(len(priv)),
        "  of_which_public_bodies": int(len(pub)),
        "private_leads_public_eur_safe_sum": float(round(priv["public_eur"].fillna(0).sum(), 2)),
        "private_leads_total_award_eur_safe_sum": float(round(priv["total_award_eur"].fillna(0).sum(), 2)),
        "private_leads_enforcement_events": int(priv["n_enforcement_events"].sum()),
        "caveats": [
            "Findings are LEADS TO INVESTIGATE, not conclusions or proven wrongdoing.",
            "An enforcement event (incident/complaint/non-compliance) is EPA workload, not guilt; an "
            "open record is unresolved, not adverse-confirmed.",
            "Public money is two never-summed tiers: SPENT (public_eur) vs AWARDED (total_award_eur).",
            "Enforcement crawl is scoped to the waste sector + public-money firms, not all licensees.",
            "Public bodies (councils running their own landfills) are a separate story from private "
            "contractors on the public payroll — split by is_public_body.",
        ],
    }


def main() -> None:
    setup_standalone_logging("epa_accountability_view")
    df = build()
    leads = _leads(df)
    save_parquet(df, OUT)
    summary = _summary(df, leads)
    OUT_SUMMARY.parent.mkdir(parents=True, exist_ok=True)
    OUT_SUMMARY.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    log.info("WROTE %s — %d firms | %d leads (public money + enforcement)", OUT, len(df), len(leads))
    log.info("LEADS (private firms on public money with an EPA enforcement record):")
    priv = leads[~leads["is_public_body"]]
    for r in priv.itertuples():
        log.info(
            "  %-42s paid €%-12s awarded €%-12s | events %d (inc %d / comp %d / nc %d, open %d) last %s",
            str(r.cro_name)[:42],
            f"{(r.public_eur or 0):,.0f}",
            f"{(r.total_award_eur or 0):,.0f}",
            r.n_enforcement_events, r.n_incident, r.n_complaint, r.n_non_compliance, r.n_open, r.last_record_date,
        )


if __name__ == "__main__":
    main()
