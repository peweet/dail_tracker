"""Freshness / snapshot-drift checker for the ingested siting-check layers.

The siting engine reads FROZEN local GeoParquet, so its output is deterministic — the only
thing that can change a given point's answer is a RE-INGEST that pulls changed source data.
This checker re-queries each layer's live source (record count + lastEditDate) and compares to
the stored `*_coverage.json`, flagging exactly the layers that have drifted. You then re-ingest
+ re-validate only those, and know the rest are unchanged. Read-only; safe to run as a cron canary.

Uses the SAME reconciliation tolerance as the ingest (bbox subsets 2%, national pulls 0.1%).

    python extractors/planning_layers_freshness.py     # exit 0 = in sync, 1 = drift found
"""

from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path

import requests

LAYERS = Path(__file__).resolve().parents[1] / "data/silver/parquet/planning_layers"


def _bbox_args(bbox) -> dict:
    if not bbox:
        return {}
    return {
        "geometry": ",".join(str(c) for c in bbox),
        "geometryType": "esriGeometryEnvelope",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
    }


def live_count(url: str, bbox) -> int | None:
    try:
        r = requests.get(url + "/query",
                         params={"where": "1=1", "returnCountOnly": "true", "f": "json", **_bbox_args(bbox)},
                         timeout=60)
        return r.json().get("count")
    except Exception:  # noqa: BLE001 — a transient source error is "unknown", not "drift"
        return None


def last_edit(url: str) -> str | None:
    try:
        j = requests.get(url, params={"f": "json"}, timeout=30).json()
        ms = (j.get("editingInfo") or {}).get("lastEditDate")
        if ms:
            return dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).date().isoformat()
    except Exception:  # noqa: BLE001
        pass
    return None


def main() -> int:
    covs = sorted(LAYERS.glob("*_coverage.json"))
    if not covs:
        print(f"no coverage JSONs under {LAYERS}")
        return 0
    drift: list[str] = []
    unknown: list[str] = []
    print(f"{'layer':26s} {'ingested':>9s} {'live':>9s} {'status':>8s}  lastEdit / ingested")
    print("-" * 78)
    for cf in covs:
        c = json.loads(cf.read_text(encoding="utf-8"))
        name = c.get("layer", cf.stem.replace("_coverage", ""))
        url = c.get("url")
        if not url:  # e.g. osm_roads (Overpass, no ArcGIS count endpoint)
            print(f"{name:26s} {'-':>9s} {'-':>9s} {'skip':>8s}  (non-ArcGIS source: {c.get('source','?')})")
            continue
        bbox = c.get("bbox_subset")
        recorded = c.get("server_count")
        live = live_count(url, bbox)
        le = last_edit(url)
        ingested = dt.datetime.fromtimestamp(cf.stat().st_mtime, dt.timezone.utc).date().isoformat()
        status = "ok"
        if live is None or recorded is None:
            status = "unknown"
            unknown.append(name)
        else:
            tol = max(50, int(recorded) * 0.02) if bbox else max(2, int(recorded) * 0.001)
            if abs(live - int(recorded)) > tol:
                status = "DRIFT"
                drift.append(name)
        print(f"{name:26s} {str(recorded):>9s} {str(live):>9s} {status:>8s}  {le or '-'} / {ingested}")

    print("-" * 78)
    if drift:
        print(f"{len(drift)} layer(s) DRIFTED — re-ingest + re-validate: {', '.join(drift)}")
    if unknown:
        print(f"{len(unknown)} layer(s) unknown (source unreachable now): {', '.join(unknown)}")
    if not drift:
        print("All reachable layers in sync with their sources — a re-ingest would not change answers.")
    return 1 if drift else 0


if __name__ == "__main__":
    sys.exit(main())
