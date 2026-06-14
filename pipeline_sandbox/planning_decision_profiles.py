"""Sandbox: national per-decision PROFILE over the whole 495k planning corpus.

Generalises the Galway SAC case study (doc/PLANNING_PERMISSION_SCOPING.md §13) to the ENTIRE
country: for every planning application it attaches (a) the structured DECISION-FUNCTION fields
(decided / refused / latency / RFI / appealed), and (b) the spatial OBLIGATION TRIGGERS — which
nature-conservation designations the site sits in (NPWS SAC / SPA / NHA / pNHA). The result is a
per-decision profile parquet + a national dose-response (refusal rate by trigger), the data spine
for the "rulebook as axioms" model (§16) and the mitigation-profile triage.

Inputs:  pipeline_sandbox/_planning_output/planning_applications_silver.parquet (495,632 pts, lon/lat)
         NPWS Designated Areas FeatureServer (registry PC09 SAC / PC10 SPA / PC11 NHA+pNHA)
Output:  pipeline_sandbox/_planning_output/planning_decision_profiles.parquet
         data/_meta/planning_decision_profiles_coverage.json

Spatial method (lessons from §13.6 / project_planning_arcgis_validation):
  - shapely 2.x STRtree (NOT geopandas — GDAL not installed; NOT DuckDB-spatial — OOMs on giants).
  - maxAllowableOffset generalisation on fetch (~55 m) so the 472k-vertex Lough Corrib SAC can't
    truncate the response — a national CORRELATION pass, not per-site determination (app must use
    exact containment + live polygons, per the §13 no-frozen-rate caveat).
  - make_valid() every polygon + Ireland-bbox sanity assert (drops the −9e12 corrupt-polygon case).
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from pathlib import Path

import polars as pl
import requests
from shapely import STRtree
from shapely import points as shp_points
from shapely.geometry import shape
from shapely.validation import make_valid

from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

LOG = logging.getLogger("planning_decision_profiles")
ROOT = Path(__file__).resolve().parents[1]
SILVER = ROOT / "pipeline_sandbox/_planning_output/planning_applications_silver.parquet"
OUT = ROOT / "pipeline_sandbox/_planning_output/planning_decision_profiles.parquet"
OUT_COV = ROOT / "data/_meta/planning_decision_profiles_coverage.json"

NPWS = "https://services-eu1.arcgis.com/Jhij7i46ouO8Cc0N/arcgis/rest/services/NPWSDesignatedAreas/FeatureServer"
NMS = "https://services-eu1.arcgis.com/HyjXgkV6KGMSF3jt/arcgis/rest/services"
# col -> FeatureServer layer URL. NPWS nature designations (PC09/10/11) + NMS archaeology zone (PC28).
# in_smr_zone is a MITIGATABLE trigger (testing/preservation-by-record) vs the HARD SAC/SPA — its
# refusal-lift vs SAC's is the empirical test of the §21 hard-vs-mitigatable taxonomy.
LAYERS = {
    "in_sac": f"{NPWS}/3", "in_spa": f"{NPWS}/0", "in_nha": f"{NPWS}/2", "in_pnha": f"{NPWS}/1",
    "in_smr_zone": f"{NMS}/SMRZoneOpenData/FeatureServer/0",
}
IRL = (-11.0, 51.0, -5.0, 56.0)  # lon/lat envelope; a polygon escaping it is corrupt (§13.6)
OFFSET = 0.0005  # ~55 m generalisation — shrinks giant polygons, keeps containment honest enough


def _fetch_polys(layer_url: str) -> list:
    """Paginated generalised geometry pull → list of valid, in-bounds shapely polygons."""
    polys, offset = [], 0
    while True:
        r = requests.get(
            f"{layer_url}/query",
            # outFields="*" not a named field — layers differ (NPWS has SITECODE, SMR does not; a
            # missing named field errors to empty). Geometry is all we need for containment anyway.
            params={"where": "1=1", "outFields": "*", "returnGeometry": "true", "outSR": "4326",
                    "maxAllowableOffset": OFFSET, "resultOffset": offset, "resultRecordCount": 2000, "f": "geojson"},
            timeout=180,
        )
        j = r.json()
        if "error" in j:
            raise SystemExit(f"ArcGIS error fetching {layer_url}: {j['error']}")
        feats = j.get("features", [])
        if not feats:
            break
        for f in feats:
            if not f.get("geometry"):
                continue
            g = make_valid(shape(f["geometry"]))  # repair self-intersections (§13.6)
            b = g.bounds
            if IRL[0] <= b[0] and IRL[1] <= b[1] and b[2] <= IRL[2] and b[3] <= IRL[3]:
                polys.append(g)
            else:
                LOG.warning("dropped out-of-bounds polygon (corrupt geom) in %s: bounds=%s", layer_url, b)
        offset += len(feats)
        if len(feats) < 2000:
            break
    return polys


def _flag(pts, polys) -> list[bool]:
    """Vectorised point-in-polygon: which points fall inside ANY polygon of the layer."""
    if not polys:
        return [False] * len(pts)
    tree = STRtree(polys)
    hit = [False] * len(pts)
    pt_idx, _ = tree.query(pts, predicate="within")  # (point_idx, poly_idx) pairs
    for i in set(pt_idx.tolist()):
        hit[i] = True
    return hit


def main() -> None:
    setup_standalone_logging("planning_decision_profiles")
    if not SILVER.exists():
        raise SystemExit(f"silver missing: {SILVER} (run planning_applications_ingest.py first)")
    df = pl.read_parquet(SILVER)
    LOG.info("loaded %d applications", df.height)

    # ── structured DECISION-FUNCTION fields (no network) ──
    decided = pl.col("decision_normalised").is_in(["Granted", "Granted-Conditional", "Refused"])
    df = df.with_columns(
        decided.alias("decided"),
        (pl.col("decision_normalised") == "Refused").alias("refused"),
        pl.col("decision_normalised").is_in(["Granted", "Granted-Conditional"]).alias("granted"),
        ((pl.col("DecisionDate") - pl.col("ReceivedDate")).dt.total_days()).alias("decision_latency_days"),
        pl.col("FIRequestDate").is_not_null().alias("had_rfi"),
        # NB: AppealDecision is an EMPTY STRING (not null) on most rows, so guard on trimmed length too.
        (pl.col("AppealDecision").fill_null("").str.strip_chars().str.to_lowercase()
         .pipe(lambda s: (s.str.len_chars() > 0) & (s != "n/a") & ~s.str.contains("withdraw"))).alias("appealed"),
    )

    # ── spatial OBLIGATION TRIGGERS: NPWS designations (national) ──
    lons = df["lon"].to_list()
    lats = df["lat"].to_list()
    pts = shp_points(lons, lats)
    for col, url in LAYERS.items():
        polys = _fetch_polys(url)
        LOG.info("%s: %d valid polygons", col, len(polys))
        df = df.with_columns(pl.Series(col, _flag(pts, polys)))
    df = df.with_columns((pl.col("in_sac") | pl.col("in_spa")).alias("in_natura2000"))

    # keep a focused profile column set
    profile = df.select(
        "ApplicationNumber", "PlanningAuthority", "ApplicationType", "application_type_normalised",
        "decision_normalised", "decided", "granted", "refused", "decision_latency_days", "had_rfi",
        "appealed", "is_one_off_house", "NumResidentialUnits", "lon", "lat",
        "in_sac", "in_spa", "in_nha", "in_pnha", "in_natura2000", "in_smr_zone", "DecisionDate",
    )
    save_parquet(profile, OUT)
    LOG.info("wrote %d decision profiles -> %s", profile.height, OUT)

    # ── national DOSE-RESPONSE (refusal rate by trigger, decided apps only) ──
    dec = profile.filter(pl.col("decided"))
    base = 100 * dec["refused"].sum() / dec.height
    LOG.info("NATIONAL baseline refusal rate: %.1f%% (n=%d decided)", base, dec.height)
    dose = {}
    for flag in ("in_sac", "in_spa", "in_nha", "in_pnha", "in_natura2000", "in_smr_zone", "is_one_off_house"):
        sub = dec.filter(pl.col(flag))
        if sub.height:
            r = 100 * sub["refused"].sum() / sub.height
            dose[flag] = {"n_decided": sub.height, "refusal_pct": round(r, 1), "lift_vs_baseline": round(r / base, 2)}
            LOG.info("  %-15s refusal %.1f%% (n=%d)  lift x%.2f", flag, r, sub.height, r / base)

    cov = {
        "generated_utc": dt.datetime.now(dt.UTC).isoformat(),
        "layer": "sandbox",
        "n_applications": profile.height,
        "n_decided": dec.height,
        "national_refusal_pct": round(base, 1),
        "designation_counts": {c: int(profile[c].sum()) for c in ("in_sac", "in_spa", "in_nha", "in_pnha", "in_natura2000", "in_smr_zone")},
        "dose_response": dose,
        "method": "shapely STRtree, generalised (~55m) polygons, make_valid + Ireland-bbox guard; correlation not causation",
        "sources": ["PC01 IrishPlanningApplications", "PC09 SAC", "PC10 SPA", "PC11 NHA/pNHA", "PC28 SMR archaeology zone"],
    }
    OUT_COV.write_text(json.dumps(cov, indent=2))
    LOG.info("coverage -> %s", OUT_COV)


if __name__ == "__main__":
    main()
