"""Live civic extractor: national per-decision profile over the planning corpus.

Generalises the Galway SAC case study (doc/archive/PLANNING_PERMISSION_SCOPING.md §13) to the ENTIRE
country: for every planning application it attaches (a) the structured DECISION-FUNCTION fields
(decided / refused / latency / RFI / appealed), and (b) the spatial OBLIGATION TRIGGERS — which
nature-conservation designations the site sits in (NPWS SAC / SPA / NHA / pNHA). The result is a
per-decision profile parquet + a national dose-response (refusal rate by trigger), the data spine
for the "rulebook as axioms" model (§16) and the mitigation-profile triage.

Inputs:  data/silver/parquet/planning_applications_silver.parquet (points with lon/lat)
         NPWS Designated Areas FeatureServer (registry PC09 SAC / PC10 SPA / PC11 NHA+pNHA)
Output:  data/silver/parquet/planning_decision_profiles.parquet
         data/_meta/planning_decision_profiles_coverage.json

Spatial method (lessons from §13.6 / project_planning_arcgis_validation):
  - shapely 2.x STRtree (NOT geopandas — GDAL not installed; NOT DuckDB-spatial — OOMs on giants).
  - maxAllowableOffset generalisation on fetch (~55 m) so the 472k-vertex Lough Corrib SAC can't
    truncate the response — a national CORRELATION pass, not per-site determination (app must use
    exact containment + live polygons, per the §13 no-frozen-rate caveat).
  - make_valid() every polygon + Ireland-bbox sanity assert (drops the −9e12 corrupt-polygon case).
  - every designation pull is reconciled against the layer's own returnCountOnly; a short pull stops
    the build instead of silently under-flagging the applications inside the missing polygons.

The coverage receipt answers "how complete is this?" without opening the parquet: per-council
depth (received-year counts, first/last dates), decided share, the register's lag behind the live
feed at build time, and each designation layer's fetched-vs-expected reconcile.
"""

from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path

import polars as pl
from shapely import STRtree
from shapely import points as shp_points
from shapely.geometry import shape
from shapely.validation import make_valid

from planning.civic.register_supplement import COUNCIL, NATIONAL, SOURCE_COL
from services.coverage_io import save_coverage
from services.http_engine import fetch_json
from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

LOG = logging.getLogger("planning_decision_profiles")
ROOT = Path(__file__).resolve().parents[3]
SILVER = ROOT / "data/silver/parquet/planning_applications_silver.parquet"
OUT = ROOT / "data/silver/parquet/planning_decision_profiles.parquet"
OUT_COV = ROOT / "data/_meta/planning_decision_profiles_coverage.json"

# The register's live point layer — counted at build time so the receipt records how far the
# local silver copy lags the feed. Same layer planning_applications_ingest.py pulls (its L0).
REGISTER_L0 = (
    "https://services.arcgis.com/NzlPQPKn5QF9v2US/arcgis/rest/services/IrishPlanningApplications/FeatureServer/0"
)
NPWS = "https://services-eu1.arcgis.com/Jhij7i46ouO8Cc0N/arcgis/rest/services/NPWSDesignatedAreas/FeatureServer"
NMS = "https://services-eu1.arcgis.com/HyjXgkV6KGMSF3jt/arcgis/rest/services"
# col -> FeatureServer layer URL. NPWS nature designations (PC09/10/11) + NMS archaeology zone (PC28).
# in_smr_zone is a MITIGATABLE trigger (testing/preservation-by-record) vs the HARD SAC/SPA — its
# refusal-lift vs SAC's is the empirical test of the §21 hard-vs-mitigatable taxonomy.
LAYERS = {
    "in_sac": f"{NPWS}/3",
    "in_spa": f"{NPWS}/0",
    "in_nha": f"{NPWS}/2",
    "in_pnha": f"{NPWS}/1",
    "in_smr_zone": f"{NMS}/SMRZoneOpenData/FeatureServer/0",
}
IRL = (-11.0, 51.0, -5.0, 56.0)  # lon/lat envelope; a polygon escaping it is corrupt (§13.6)
OFFSET = 0.0005  # ~55 m generalisation — shrinks giant polygons, keeps containment honest enough
PAGE = 2000  # every layer above serves maxRecordCount 2000 (checked 2026-09-29)
# Same floor the register ingest applies to its site layer; the profile is one row per register point.
MIN_PROFILE_ROWS = 450_000
# Receipt summaries are expected to be small, but keep row materialisation fail-closed if the
# grouping cardinality ever grows unexpectedly.
RECEIPT_ROW_LIMIT = 100_000

# Only what the profile and the receipt read — the silver register carries 38 columns.
SOURCE_COLS = [
    "ApplicationNumber",
    "PlanningAuthority",
    "ApplicationType",
    "application_type_normalised",
    "decision_normalised",
    "ReceivedDate",
    "DecisionDate",
    "FIRequestDate",
    "AppealDecision",
    "is_one_off_house",
    "NumResidentialUnits",
    "lon",
    "lat",
]


class SourceCountError(RuntimeError):
    """A source layer's count could not be read, or a pull did not reconcile against it."""


def _layer_count(layer_url: str) -> int:
    """The layer's own feature count (returnCountOnly) — the reconcile target for a paged pull."""
    response, _ = fetch_json(
        f"{layer_url}/query",
        params={"where": "1=1", "returnCountOnly": "true", "f": "json"},
        timeout=60,
    )
    if "error" in response or "count" not in response:
        raise SourceCountError(f"ArcGIS count failed for {layer_url}: {response.get('error', response)}")
    return int(response["count"])


def _fetch_polys(layer_url: str) -> tuple[list, dict]:
    """Paginated generalised geometry pull → (valid in-bounds shapely polygons, reconcile stats).

    Pages until the server returns an EMPTY page. A short page is not the end: ArcGIS trims a
    page to stay under its response-size limit, so stopping on `len(page) < PAGE` can keep only
    part of a layer with no error. The fetched total must then equal returnCountOnly.
    """
    expected = _layer_count(layer_url)
    polys, offset = [], 0
    no_geometry = out_of_bounds = 0
    while True:
        response, _ = fetch_json(
            f"{layer_url}/query",
            # outFields="*" not a named field — layers differ (NPWS has SITECODE, SMR does not; a
            # missing named field errors to empty). Geometry is all we need for containment anyway.
            # orderByFields keeps resultOffset paging stable across requests.
            params={
                "where": "1=1",
                "outFields": "*",
                "orderByFields": "OBJECTID",
                "returnGeometry": "true",
                "outSR": "4326",
                "maxAllowableOffset": OFFSET,
                "resultOffset": offset,
                "resultRecordCount": PAGE,
                "f": "geojson",
            },
            timeout=180,
        )
        if "error" in response:
            raise SourceCountError(f"ArcGIS error fetching {layer_url}: {response['error']}")
        feats = response.get("features", [])
        if not feats:
            break
        for f in feats:
            if not f.get("geometry"):
                no_geometry += 1
                continue
            g = make_valid(shape(f["geometry"]))  # repair self-intersections (§13.6)
            b = g.bounds
            if IRL[0] <= b[0] and IRL[1] <= b[1] and b[2] <= IRL[2] and b[3] <= IRL[3]:
                polys.append(g)
            else:
                out_of_bounds += 1
                LOG.warning("dropped out-of-bounds polygon (corrupt geom) in %s: bounds=%s", layer_url, b)
        offset += len(feats)
        if offset > expected:  # a server ignoring resultOffset would otherwise loop forever
            break
    if offset != expected:
        raise SourceCountError(f"{layer_url}: fetched {offset} features, layer reports {expected}")
    stats = {
        "expected": expected,
        "fetched": offset,
        "no_geometry": no_geometry,
        "out_of_bounds": out_of_bounds,
        "kept": len(polys),
    }
    return polys, stats


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


def _decision_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Structured DECISION-FUNCTION fields (no network).

    A decision dated before its receipt is impossible, so its latency is nulled and flagged in
    `_negative_latency` for the receipt — left in, it drags every council median downwards.
    """
    if SOURCE_COL not in df.columns:
        df = df.with_columns(pl.lit(NATIONAL).alias(SOURCE_COL))
    decided = pl.col("decision_normalised").is_in(["Granted", "Granted-Conditional", "Refused"])
    latency = (pl.col("DecisionDate") - pl.col("ReceivedDate")).dt.total_days()
    # Council-published rows (register_supplement.py) carry no FI-request or appeal-decision field;
    # those flags are UNKNOWN (null) for them, never False — False would dilute both rates.
    national = pl.col(SOURCE_COL).fill_null(NATIONAL) != COUNCIL
    return df.with_columns(
        decided.alias("decided"),
        (pl.col("decision_normalised") == "Refused").alias("refused"),
        pl.col("decision_normalised").is_in(["Granted", "Granted-Conditional"]).alias("granted"),
        pl.when(latency >= 0).then(latency).alias("decision_latency_days"),
        (latency < 0).fill_null(False).alias("_negative_latency"),
        pl.when(national).then(pl.col("FIRequestDate").is_not_null()).alias("had_rfi"),
        # NB: AppealDecision is an EMPTY STRING (not null) on most rows, so guard on trimmed length too.
        pl.when(national)
        .then(
            pl.col("AppealDecision")
            .fill_null("")
            .str.strip_chars()
            .str.to_lowercase()
            .pipe(lambda s: (s.str.len_chars() > 0) & (s != "n/a") & ~s.str.contains("withdraw"))
        )
        .alias("appealed"),
    )


def _iso(value) -> str | None:
    return value.isoformat() if value is not None else None


def _bounded_receipt_rows(frame: pl.DataFrame):
    """Materialise only a receipt summary whose complete row count is bounded."""
    if frame.height > RECEIPT_ROW_LIMIT:
        raise ValueError(f"receipt summary has {frame.height} rows; limit is {RECEIPT_ROW_LIMIT}")
    return frame.head(RECEIPT_ROW_LIMIT).iter_rows(named=True)


def _authority_completeness(df: pl.DataFrame) -> list[dict]:
    """Per-council depth and fill, read straight from the register — no threshold inferred.

    Year counts are reported raw, so a reader sees where each council's history starts and ramps
    up; a "first complete year" cut-off would be a judgement the receipt should not make.
    """
    per = (
        df.group_by("PlanningAuthority")
        .agg(
            pl.len().alias("n_applications"),
            pl.col("decided").sum().alias("n_decided"),
            pl.col("ReceivedDate").is_null().sum().alias("n_received_date_null"),
            pl.col("ReceivedDate").cast(pl.Date).min().alias("received_first"),
            pl.col("ReceivedDate").cast(pl.Date).max().alias("received_last"),
            pl.col("DecisionDate").cast(pl.Date).max().alias("decision_last"),
            pl.col("_negative_latency").sum().alias("n_negative_latency_nulled"),
            (pl.col(SOURCE_COL) == COUNCIL).sum().alias("n_council_supplement"),
        )
        .sort("PlanningAuthority")
    )
    years = (
        df.filter(pl.col("ReceivedDate").is_not_null())
        .group_by("PlanningAuthority", pl.col("ReceivedDate").dt.year().alias("year"))
        .len()
        .sort("PlanningAuthority", "year")
    )
    by_authority: dict[str, dict[str, int]] = {}
    for row in _bounded_receipt_rows(years):
        by_authority.setdefault(row["PlanningAuthority"], {})[str(row["year"])] = row["len"]
    out = []
    for row in _bounded_receipt_rows(per):
        out.append(
            {
                "planning_authority": row["PlanningAuthority"],
                "n_applications": row["n_applications"],
                "n_decided": row["n_decided"],
                "n_received_date_null": row["n_received_date_null"],
                "n_negative_latency_nulled": row["n_negative_latency_nulled"],
                "n_council_supplement": row["n_council_supplement"],
                "received_first": _iso(row["received_first"]),
                "received_last": _iso(row["received_last"]),
                "decision_last": _iso(row["decision_last"]),
                "received_year_counts": by_authority.get(row["PlanningAuthority"], {}),
            }
        )
    return out


def _register_identity(n_rows: int) -> dict:
    """How far the local register lags the live feed at build time — informational, never blocking.

    Refreshing the register is planning_applications_ingest.py's job; this build only records the
    lag so a stale profile is visible from its receipt instead of by comparing files by hand.
    """
    mtime = dt.datetime.fromtimestamp(SILVER.stat().st_mtime, dt.UTC).isoformat()
    try:
        live = _layer_count(REGISTER_L0)
    except (SourceCountError, OSError) as exc:  # requests' exceptions subclass OSError
        LOG.warning("live register count unavailable: %s", exc)
        return {"rows": n_rows, "silver_mtime_utc": mtime, "live_rows_at_build": None, "lag_rows": None}
    if live != n_rows:
        LOG.warning("register lags the live feed: %d local vs %d live", n_rows, live)
    return {"rows": n_rows, "silver_mtime_utc": mtime, "live_rows_at_build": live, "lag_rows": live - n_rows}


def main() -> None:
    setup_standalone_logging("planning_decision_profiles")
    if not SILVER.exists():
        raise SystemExit(f"silver missing: {SILVER} (run planning_applications_ingest.py first)")
    present = pl.read_parquet_schema(SILVER)
    df = pl.read_parquet(SILVER, columns=[c for c in [*SOURCE_COLS, SOURCE_COL] if c in present])
    df = _decision_fields(df)
    n_council = int((df[SOURCE_COL] == COUNCIL).sum())
    LOG.info("loaded %d applications (%d council-published)", df.height, n_council)
    # The live-feed lag compares like with like: national rows against the national layer's count.
    register = _register_identity(df.height - n_council)
    register["council_supplement_rows"] = n_council

    # ── spatial OBLIGATION TRIGGERS: NPWS designations (national) ──
    pts = shp_points(df["lon"].to_numpy(), df["lat"].to_numpy())
    layer_stats = {}
    for col, url in LAYERS.items():
        polys, stats = _fetch_polys(url)
        layer_stats[col] = stats
        LOG.info("%s: %d valid polygons (%d/%d fetched)", col, len(polys), stats["fetched"], stats["expected"])
        df = df.with_columns(pl.Series(col, _flag(pts, polys)))
    df = df.with_columns((pl.col("in_sac") | pl.col("in_spa")).alias("in_natura2000"))

    # keep a focused profile column set
    profile = df.select(
        "ApplicationNumber",
        "PlanningAuthority",
        SOURCE_COL,
        "ApplicationType",
        "application_type_normalised",
        "decision_normalised",
        "decided",
        "granted",
        "refused",
        "decision_latency_days",
        "had_rfi",
        "appealed",
        "is_one_off_house",
        "NumResidentialUnits",
        "lon",
        "lat",
        "in_sac",
        "in_spa",
        "in_nha",
        "in_pnha",
        "in_natura2000",
        "in_smr_zone",
        "DecisionDate",
    )
    save_parquet(profile, OUT, min_rows=MIN_PROFILE_ROWS)
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
        "schema": "dail-planning-decision-profiles-coverage/2",
        "generated_utc": dt.datetime.now(dt.UTC).isoformat(),
        "layer": "silver",
        "n_applications": profile.height,
        "n_decided": dec.height,
        "n_negative_latency_nulled": int(df["_negative_latency"].sum()),
        "national_refusal_pct": round(base, 1),
        "register": register,
        "designation_layers": layer_stats,
        "designation_counts": {
            c: int(profile[c].sum()) for c in ("in_sac", "in_spa", "in_nha", "in_pnha", "in_natura2000", "in_smr_zone")
        },
        "dose_response": dose,
        "authorities": _authority_completeness(df),
        "method": "shapely STRtree, generalised (~55m) polygons, make_valid + Ireland-bbox guard, "
        "designation pulls reconciled to returnCountOnly; correlation not causation",
        "sources": [
            "PC01 IrishPlanningApplications",
            "PC09 SAC",
            "PC10 SPA",
            "PC11 NHA/pNHA",
            "PC28 SMR archaeology zone",
        ],
    }
    save_coverage(cov, OUT_COV)
    LOG.info("coverage -> %s", OUT_COV)


if __name__ == "__main__":
    main()
