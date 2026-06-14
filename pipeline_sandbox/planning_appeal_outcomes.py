"""Sandbox: authoritative council-overturn metric — applications x ACP appeal decisions.

Fixes the data-quality trap in the national profile (planning_decision_profiles.py): the applications
feed's self-reported `AppealDecision` is unreliable (empty-string default; per-council vendor quirks —
Wexford stamps AppealRefNumber on every row + uses "n/a", Westmeath logs appeals as "MODIFIED"). The
TRUSTWORTHY source is An Coimisiún Pleanála's OWN decision (registry PC02, CC-BY), joined to the
council decision via the §Angle-4 recipe (6-digit core of AppealRefNumber -> ABPCASEID).

Output: per matched appeal — council decision vs ABP decision -> overturned/upheld; + a per-council
ranking of how often ABP overturns the council. OCR-free.

Inputs:  ACP Cases_2016_Onwards FeatureServer layer 3 (PC02); planning_applications_silver.parquet (PC01)
Output:  pipeline_sandbox/_planning_output/planning_appeal_outcomes.parquet
         data/_meta/planning_appeal_outcomes_coverage.json
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import re
from pathlib import Path

import polars as pl
import requests

from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

LOG = logging.getLogger("planning_appeal_outcomes")
ROOT = Path(__file__).resolve().parents[1]
SILVER = ROOT / "pipeline_sandbox/_planning_output/planning_applications_silver.parquet"
OUT = ROOT / "pipeline_sandbox/_planning_output/planning_appeal_outcomes.parquet"
OUT_COV = ROOT / "data/_meta/planning_appeal_outcomes_coverage.json"
ACP = "https://services-eu1.arcgis.com/o56BSnENmD5mYs3j/arcgis/rest/services/Cases_2016_Onwards/FeatureServer/3/query"

_SIX = re.compile(r"\d{6}")


def _norm_abp(d: str | None) -> str:
    """ABP planning-appeal decision -> GRANT / REFUSE / OTHER (no-inference; OTHER = non-substantive:
    s.5 declarations, contribution appeals, withdrawn/invalid/leave, confirm/set-aside determinations)."""
    s = (d or "").lower()
    if not s.strip() or any(w in s for w in (
        "withdraw", "invalid", "leave to appeal", "is development", "contribution appeal",
        "confirm the determination", "set aside", "determination of the local")):
        return "OTHER"
    if "refuse" in s:
        return "REFUSE"
    if "grant" in s or "approve" in s or "permission" in s:
        return "GRANT"
    return "OTHER"


def _fetch_acp() -> pl.DataFrame:
    rows, off = [], 0
    while True:
        r = requests.get(ACP, params={"where": "1=1", "outFields": "ABPCASEID,DECISION,PLANINGATY,CATEGORY,DECIDED_ON",
                                      "returnGeometry": "false", "resultOffset": off, "resultRecordCount": 2000, "f": "json"},
                         timeout=120).json()
        f = r.get("features", [])
        if not f:
            break
        rows += [x["attributes"] for x in f]
        off += len(f)
        if len(f) < 2000:
            break
    df = pl.DataFrame(rows)
    return df.with_columns(
        pl.col("ABPCASEID").cast(pl.Utf8).str.strip_chars().alias("abp_case"),
        pl.col("DECISION").map_elements(_norm_abp, return_dtype=pl.Utf8).alias("abp_decision"),
    )


def main() -> None:
    setup_standalone_logging("planning_appeal_outcomes")
    if not SILVER.exists():
        raise SystemExit(f"silver missing: {SILVER}")
    acp = _fetch_acp()
    LOG.info("ACP cases: %d (GRANT=%d REFUSE=%d OTHER=%d)", acp.height,
             *(acp.filter(pl.col("abp_decision") == v).height for v in ("GRANT", "REFUSE", "OTHER")))

    apps = pl.read_parquet(SILVER).select("ApplicationNumber", "PlanningAuthority", "decision_normalised", "AppealRefNumber")
    apps = apps.filter(pl.col("AppealRefNumber").is_not_null() & (pl.col("AppealRefNumber").str.strip_chars() != ""))
    apps = apps.with_columns(
        pl.col("AppealRefNumber").map_elements(lambda s: (_SIX.search(s or "") or [None])[0]
                                               if _SIX.search(s or "") else None, return_dtype=pl.Utf8).alias("abp_case"),
        pl.when(pl.col("decision_normalised").is_in(["Granted", "Granted-Conditional"])).then(pl.lit("GRANT"))
          .when(pl.col("decision_normalised") == "Refused").then(pl.lit("REFUSE"))
          .otherwise(pl.lit("OTHER")).alias("council_decision"),
    ).filter(pl.col("abp_case").is_not_null())

    j = apps.join(acp.select("abp_case", "abp_decision", "PLANINGATY", "CATEGORY", "DECIDED_ON"), on="abp_case", how="inner")
    LOG.info("appeals joined: %d (of %d apps with an appeal ref)", j.height, apps.height)

    clear = j.filter((pl.col("council_decision") != "OTHER") & (pl.col("abp_decision") != "OTHER"))
    clear = clear.with_columns((pl.col("council_decision") != pl.col("abp_decision")).alias("overturned"))
    save_parquet(clear, OUT)

    n = clear.height
    rev = clear.filter(pl.col("overturned")).height
    g2r = clear.filter((pl.col("council_decision") == "GRANT") & (pl.col("abp_decision") == "REFUSE")).height
    r2g = clear.filter((pl.col("council_decision") == "REFUSE") & (pl.col("abp_decision") == "GRANT")).height
    LOG.info("clear-vs-clear appeals: %d | ABP OVERTURNED council: %d (%.1f%%) [grant->refuse %d, refuse->grant %d] | upheld %d",
             n, rev, 100 * rev / n, g2r, r2g, n - rev)

    # per-council overturn ranking (authoritative; min 25 clear appeals)
    rank = (clear.group_by("PlanningAuthority")
            .agg(pl.len().alias("appeals"), pl.col("overturned").sum().alias("overturned"))
            .with_columns((100 * pl.col("overturned") / pl.col("appeals")).round(1).alias("overturn_pct"))
            .filter(pl.col("appeals") >= 25).sort("overturn_pct", descending=True))
    LOG.info("per-council overturn (top, min 25 appeals):\n%s", rank.head(12))

    cov = {
        "generated_utc": dt.datetime.now(dt.UTC).isoformat(),
        "layer": "sandbox",
        "source": "PC02 ACP Cases_2016_Onwards joined to PC01 applications via AppealRefNumber 6-digit -> ABPCASEID",
        "acp_cases": acp.height, "appeals_joined": j.height, "clear_vs_clear": n,
        "abp_overturned_council": rev, "overturn_pct": round(100 * rev / n, 1),
        "council_grant_to_abp_refuse": g2r, "council_refuse_to_abp_grant": r2g,
        "caveat": "ABP appeals are de novo; overturn = outcome flipped. ACP feed 2016+. Correlation/record, not a quality judgement.",
    }
    OUT_COV.write_text(json.dumps(cov, indent=2))
    LOG.info("wrote %d outcomes -> %s ; coverage -> %s", n, OUT, OUT_COV)


if __name__ == "__main__":
    main()
