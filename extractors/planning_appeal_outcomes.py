"""Authoritative council-overturn metric — applications x ACP appeal decisions.

Fixes the data-quality trap in the national profile (planning_decision_profiles.py): the applications
feed's self-reported `AppealDecision` is unreliable (empty-string default; per-council vendor quirks —
Wexford stamps AppealRefNumber on every row + uses "n/a", Westmeath logs appeals as "MODIFIED"). The
TRUSTWORTHY source is An Coimisiún Pleanála's OWN decision (registry PC02, CC-BY), joined to the
council decision via the §Angle-4 recipe (6-digit core of AppealRefNumber -> ABPCASEID).

Output: per matched appeal — council decision vs ABP decision -> overturned/upheld; + a per-council
ranking of how often ABP overturns the council. OCR-free. Powers v_la_planning_overturn ("Who runs
your county") and the per-council overturn signal.

Promoted out of pipeline_sandbox/ 2026-06-20 — runs in pipeline.py as the planning_appeal_outcomes
chain. FETCHES the ACP ArcGIS FeatureServer; reads the COMMITTED planning_applications_silver (the
national planning ingest is NOT yet a pipeline chain, so that silver is a static input here). The
save_parquet min_rows floor refuses to overwrite the silver with a degraded/partial ArcGIS pull.

Two link methods (each row tagged ``match_method``):
  • appeal_ref — the EXACT council→ACP link via the 6-digit core of AppealRefNumber → ABPCASEID.
    Primary and authoritative wherever the council populates that field.
  • spatial_temporal — fallback for appeals the ref join missed (e.g. Cork County publishes NO
    AppealRefNumber on any of its 126k applications). Matches each ACP case to the application at the
    SAME authority that is nearest (~≤55 m, via ACP case-polygon centroid ↔ application lon/lat) AND
    whose decision PRE-DATES the appeal lodgement (most recent such). Validated against Kerry (which
    DOES populate the ref): this rule reproduces the known council decision 98.4% of the time — the
    date constraint is what disambiguates rural sites with a refusal followed by a later grant
    (spatial-only is only 86%). Recovers all 31 councils; Cork County lands at 26.4% (≈ the national rate).

Inputs:  ACP Cases_2016_Onwards FeatureServer layer 3 (PC02); planning_applications_silver.parquet (PC01)
Output:  data/silver/parquet/planning_appeal_outcomes.parquet
         data/_meta/planning_appeal_outcomes_coverage.json
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import re
import unicodedata
from pathlib import Path

import polars as pl
import requests

from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

LOG = logging.getLogger("planning_appeal_outcomes")
ROOT = Path(__file__).resolve().parents[1]
SILVER = ROOT / "data/silver/parquet/planning_applications_silver.parquet"
OUT = ROOT / "data/silver/parquet/planning_appeal_outcomes.parquet"
OUT_COV = ROOT / "data/_meta/planning_appeal_outcomes_coverage.json"
ACP = "https://services-eu1.arcgis.com/o56BSnENmD5mYs3j/arcgis/rest/services/Cases_2016_Onwards/FeatureServer/3/query"

_SIX = re.compile(r"\d{6}")
_SPATIAL_DEG = 0.0006  # ~55 m at 53°N — nearest-application search radius for the fallback
_GRID = 0.001  # grid-cell size for the spatial index (must be >= _SPATIAL_DEG)


def _auth_key(name: str | None) -> str:
    """Normalise a planning-authority name to a join key: drop any ' - … Section' suffix
    (only 'Cork County Council - West Cork Section' exists), strip accents/punctuation.
    Maps ACP's PLANINGATY onto the application feed's PlanningAuthority (e.g. West Cork
    folds into Cork County) so the spatial fallback only matches within the same council."""
    s = re.sub(r"\s*-\s*.*$", "", name or "")
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z]", "", s.lower())


def _ms_to_date(ms) -> dt.date | None:
    return dt.datetime.fromtimestamp(ms / 1000, dt.UTC).date() if ms is not None else None


def _centroid(geom: dict | None) -> tuple[float | None, float | None]:
    if not geom:
        return None, None
    pts = [p for ring in geom.get("rings", []) for p in ring]
    if not pts:
        return None, None
    return sum(p[0] for p in pts) / len(pts), sum(p[1] for p in pts) / len(pts)


def _council_decision(d: str | None) -> str:
    if d in ("Granted", "Granted-Conditional"):
        return "GRANT"
    return "REFUSE" if d == "Refused" else "OTHER"


def _norm_abp(d: str | None) -> str:
    """ABP planning-appeal decision -> GRANT / REFUSE / OTHER (no-inference; OTHER = non-substantive:
    s.5 declarations, contribution appeals, withdrawn/invalid/leave, confirm/set-aside determinations)."""
    s = (d or "").lower()
    if not s.strip() or any(
        w in s
        for w in (
            "withdraw",
            "invalid",
            "leave to appeal",
            "is development",
            "contribution appeal",
            "confirm the determination",
            "set aside",
            "determination of the local",
        )
    ):
        return "OTHER"
    if "refuse" in s:
        return "REFUSE"
    if "grant" in s or "approve" in s or "permission" in s:
        return "GRANT"
    return "OTHER"


def _fetch_acp() -> pl.DataFrame:
    rows, off = [], 0
    while True:
        r = requests.get(
            ACP,
            params={
                "where": "1=1",
                "outFields": "ABPCASEID,DECISION,PLANINGATY,CATEGORY,DECIDED_ON,LODGEDON",
                "returnGeometry": "true",  # case-site polygon — centroid feeds the spatial fallback
                "outSR": 4326,
                "resultOffset": off,
                "resultRecordCount": 2000,
                "f": "json",
            },
            timeout=120,
        ).json()
        f = r.get("features", [])
        if not f:
            break
        for x in f:
            lon, lat = _centroid(x.get("geometry"))
            rows.append({**x["attributes"], "lon": lon, "lat": lat})
        off += len(f)
        if len(f) < 2000:
            break
    df = pl.DataFrame(rows)
    return df.with_columns(
        pl.col("ABPCASEID").cast(pl.Utf8).str.strip_chars().alias("abp_case"),
        pl.col("DECISION").map_elements(_norm_abp, return_dtype=pl.Utf8).alias("abp_decision"),
        pl.col("LODGEDON").map_elements(_ms_to_date, return_dtype=pl.Date).alias("lodged_date"),
        pl.col("PLANINGATY").map_elements(_auth_key, return_dtype=pl.Utf8).alias("auth_key"),
    )


# Columns every outcome row carries, in order — shared by the appeal_ref and spatial_temporal sets.
_OUT_COLS = [
    "ApplicationNumber",
    "PlanningAuthority",
    "decision_normalised",
    "AppealRefNumber",
    "abp_case",
    "council_decision",
    "abp_decision",
    "PLANINGATY",
    "CATEGORY",
    "DECIDED_ON",
    "match_method",
]


def _spatial_temporal_matches(residual: pl.DataFrame, apps: pl.DataFrame) -> pl.DataFrame:
    """For ACP cases the ref join missed, match each to the nearest application of the SAME
    authority whose decision pre-dates the appeal lodgement (most recent such). Returns rows
    in the _OUT_COLS schema, tagged match_method='spatial_temporal'."""
    cand = apps.filter(pl.col("lon").is_not_null() & pl.col("lat").is_not_null()).with_columns(
        pl.col("PlanningAuthority").map_elements(_auth_key, return_dtype=pl.Utf8).alias("auth_key")
    )
    # spatial index: (auth_key, rounded lat, rounded lon) → list of candidate application rows
    grid: dict[tuple, list[dict]] = {}
    cols = {c: cand[c].to_list() for c in ("auth_key", "lat", "lon", "ApplicationNumber", "PlanningAuthority", "decision_normalised", "DecisionDate")}
    for i in range(cand.height):
        key = (cols["auth_key"][i], round(cols["lat"][i] / _GRID), round(cols["lon"][i] / _GRID))
        grid.setdefault(key, []).append({c: cols[c][i] for c in cols})

    def nearest(ak: str, lat: float, lon: float, lodged: dt.date | None) -> dict | None:
        gl, go = round(lat / _GRID), round(lon / _GRID)
        pool = [a for dl in (-1, 0, 1) for dd in (-1, 0, 1) for a in grid.get((ak, gl + dl, go + dd), [])]
        pool = [a for a in pool if abs(a["lat"] - lat) <= _SPATIAL_DEG and abs(a["lon"] - lon) <= _SPATIAL_DEG]
        if not pool:
            return None
        if lodged is not None:
            before = [a for a in pool if a["DecisionDate"] is not None and a["DecisionDate"] <= lodged]
            if before:  # the application this appeal most plausibly concerns
                return max(before, key=lambda a: a["DecisionDate"])
        return min(pool, key=lambda a: (a["lat"] - lat) ** 2 + (a["lon"] - lon) ** 2)

    out = []
    rc = {c: residual[c].to_list() for c in ("auth_key", "lat", "lon", "lodged_date", "abp_case", "abp_decision", "PLANINGATY", "CATEGORY", "DECIDED_ON")}
    for i in range(residual.height):
        m = nearest(rc["auth_key"][i], rc["lat"][i], rc["lon"][i], rc["lodged_date"][i])
        if not m:
            continue
        out.append(
            {
                "ApplicationNumber": m["ApplicationNumber"],
                "PlanningAuthority": m["PlanningAuthority"],
                "decision_normalised": m["decision_normalised"],
                "AppealRefNumber": None,
                "abp_case": rc["abp_case"][i],
                "council_decision": _council_decision(m["decision_normalised"]),
                "abp_decision": rc["abp_decision"][i],
                "PLANINGATY": rc["PLANINGATY"][i],
                "CATEGORY": rc["CATEGORY"][i],
                "DECIDED_ON": rc["DECIDED_ON"][i],
                "match_method": "spatial_temporal",
            }
        )
    schema = {"ApplicationNumber": pl.Utf8, "PlanningAuthority": pl.Utf8, "decision_normalised": pl.Utf8,
              "AppealRefNumber": pl.Utf8, "abp_case": pl.Utf8, "council_decision": pl.Utf8, "abp_decision": pl.Utf8,
              "PLANINGATY": pl.Utf8, "CATEGORY": pl.Utf8, "DECIDED_ON": pl.Int64, "match_method": pl.Utf8}
    return pl.DataFrame(out, schema=schema) if out else pl.DataFrame(schema=schema)


def main() -> None:
    setup_standalone_logging("planning_appeal_outcomes")
    if not SILVER.exists():
        raise SystemExit(f"silver missing: {SILVER}")
    acp = _fetch_acp()
    LOG.info(
        "ACP cases: %d (GRANT=%d REFUSE=%d OTHER=%d)",
        acp.height,
        *(acp.filter(pl.col("abp_decision") == v).height for v in ("GRANT", "REFUSE", "OTHER")),
    )

    apps_all = pl.read_parquet(SILVER).select(
        "ApplicationNumber", "PlanningAuthority", "decision_normalised", "AppealRefNumber", "DecisionDate", "lon", "lat"
    )
    # PRIMARY — exact appeal_ref → ABPCASEID link (authoritative wherever the council fills it).
    apps_ref = apps_all.filter(
        pl.col("AppealRefNumber").is_not_null() & (pl.col("AppealRefNumber").str.strip_chars() != "")
    ).with_columns(
        pl.col("AppealRefNumber")
        .map_elements(
            lambda s: (_SIX.search(s or "") or [None])[0] if _SIX.search(s or "") else None, return_dtype=pl.Utf8
        )
        .alias("abp_case"),
        pl.col("decision_normalised").map_elements(_council_decision, return_dtype=pl.Utf8).alias("council_decision"),
    ).filter(pl.col("abp_case").is_not_null())

    primary = apps_ref.join(
        acp.select("abp_case", "abp_decision", "PLANINGATY", "CATEGORY", "DECIDED_ON"), on="abp_case", how="inner"
    ).with_columns(pl.lit("appeal_ref").alias("match_method")).select(_OUT_COLS)
    LOG.info("appeal_ref matches: %d (of %d apps with an appeal ref)", primary.height, apps_ref.height)

    # FALLBACK — spatial+temporal recovery for ACP cases the ref join didn't reach.
    matched = set(primary["abp_case"].to_list())
    residual = acp.filter(~pl.col("abp_case").is_in(matched) & pl.col("lat").is_not_null() & pl.col("lon").is_not_null())
    fallback = _spatial_temporal_matches(residual, apps_all)
    LOG.info("spatial_temporal matches: %d (of %d unmatched ACP cases with coords)", fallback.height, residual.height)

    allm = pl.concat([primary, fallback], how="vertical")
    clear = allm.filter((pl.col("council_decision") != "OTHER") & (pl.col("abp_decision") != "OTHER"))
    clear = clear.with_columns((pl.col("council_decision") != pl.col("abp_decision")).alias("overturned"))
    # Row floor: the clear-vs-clear set is ~14k+ (ref + spatial) and only grows as ABP adds cases.
    # A partial ArcGIS pull (outage mid-pagination, schema drift) would thin it; refuse to overwrite
    # the silver below this floor rather than ship a truncated overturn metric to the LA page.
    save_parquet(clear, OUT, min_rows=10_000)

    n = clear.height
    rev = clear.filter(pl.col("overturned")).height
    g2r = clear.filter((pl.col("council_decision") == "GRANT") & (pl.col("abp_decision") == "REFUSE")).height
    r2g = clear.filter((pl.col("council_decision") == "REFUSE") & (pl.col("abp_decision") == "GRANT")).height
    by_method = dict(clear.group_by("match_method").len().iter_rows())
    LOG.info(
        "clear-vs-clear appeals: %d (appeal_ref=%d spatial_temporal=%d) | ABP OVERTURNED council: %d (%.1f%%) "
        "[grant->refuse %d, refuse->grant %d] | upheld %d",
        n,
        by_method.get("appeal_ref", 0),
        by_method.get("spatial_temporal", 0),
        rev,
        100 * rev / n,
        g2r,
        r2g,
        n - rev,
    )

    # per-council overturn ranking (authoritative; min 25 clear appeals)
    rank = (
        clear.group_by("PlanningAuthority")
        .agg(pl.len().alias("appeals"), pl.col("overturned").sum().alias("overturned"))
        .with_columns((100 * pl.col("overturned") / pl.col("appeals")).round(1).alias("overturn_pct"))
        .filter(pl.col("appeals") >= 25)
        .sort("overturn_pct", descending=True)
    )
    LOG.info("per-council overturn (top, min 25 appeals):\n%s", rank.head(12))

    cov = {
        "generated_utc": dt.datetime.now(dt.UTC).isoformat(),
        "layer": "silver",
        "source": "PC02 ACP Cases_2016_Onwards x PC01 applications. Primary: AppealRefNumber 6-digit -> "
        "ABPCASEID. Fallback (match_method=spatial_temporal): nearest same-authority application "
        "(ACP centroid <=55m) whose decision pre-dates the appeal; validated 98.4% vs Kerry ground truth.",
        "acp_cases": acp.height,
        "appeals_joined": int(primary.height + fallback.height),
        "matches_appeal_ref": int(by_method.get("appeal_ref", 0)),
        "matches_spatial_temporal": int(by_method.get("spatial_temporal", 0)),
        "clear_vs_clear": n,
        "abp_overturned_council": rev,
        "overturn_pct": round(100 * rev / n, 1),
        "council_grant_to_abp_refuse": g2r,
        "council_refuse_to_abp_grant": r2g,
        "caveat": "ABP appeals are de novo; overturn = outcome flipped. ACP feed 2016+. Correlation/record, not a quality judgement.",
    }
    OUT_COV.write_text(json.dumps(cov, indent=2))
    LOG.info("wrote %d outcomes -> %s ; coverage -> %s", n, OUT, OUT_COV)


if __name__ == "__main__":
    main()
