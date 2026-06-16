"""EXPERIMENTAL (tracked code, gitignored sandbox data) — pull the EPA licensed-facility register:
every facility holding an EPA licence (Industrial Emissions / IPPC / IPC) or a waste licence, with the
licensee name, the licence number, its status and the facility location.

This is the environmental-credential leg of the supplier-capability register (companion to the NSAI
certified-company pull, [[nsai_certified_companies_scrape]]). EPA licensees are exactly the firms that
win council and public-sector waste, remediation and industrial-services contracts, so the register
joins cleanly to the procurement award/spend track record in [[epa_capability_register]].

Mechanism: the EPA publishes its licence layers on a public GeoServer (no auth) as OGC WFS. We pull
the LEMA facility layers as GeoJSON and keep the attribute table (geometry centroid kept best-effort
for an optional later map). Two operator-named layers are pulled:
  * LEMA_Facilities_P_IPPC_IPC_IEL — combined IPPC/IPC/Industrial-Emissions licences (Name = company)
  * LEMA_Facilities_Waste          — waste licences (Name = facility/operator)
(IEL & IPC are already folded into the combined layer; Extractive/MCP/COA/UWW are site-named or
public-body operated and are left as extensions.)

Output (gitignored): data/sandbox/parquet/epa_licensed_facilities.parquet
Run: ./.venv/Scripts/python.exe pipeline_sandbox/epa_licence_register_scrape.py
"""

from __future__ import annotations

import contextlib
import logging
import sys
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.logging_setup import setup_standalone_logging  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

log = logging.getLogger(__name__)

WFS = "https://gis.epa.ie/geoserver/EPA/ows"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36"
OUT = ROOT / "data/sandbox/parquet/epa_licensed_facilities.parquet"
# layers are ~1k rows each; one large request returns the whole layer. Geometry is served in Irish
# Transverse Mercator (EPSG:29902) — kept best-effort as itm_x/itm_y for an optional later map.
FETCH_COUNT = 10000

# layer -> licence_class label. Name field is the licensee/facility name in both.
LAYERS = {
    "LEMA_Facilities_P_IPPC_IPC_IEL": "industrial",  # IPPC + IPC + Industrial Emissions (combined)
    "LEMA_Facilities_Waste": "waste",
}

# WFS attribute -> our column
KEEP = {
    "Name": "licensee_name",
    "ActiveLicenceNumber": "licence_number",
    "RegCD": "reg_code",
    "LicenceStatusType": "licence_status",
    "LicenceTypeName": "licence_type",
    "Category": "category",
    "SubCategory": "sub_category",
    "Address": "location",
    "DateFrom": "date_from",
}


def _new_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    return s


def _centroid(geom: dict | None) -> tuple[float | None, float | None]:
    """Best-effort representative (x, y) in EPSG:29902 (ITM) from a GeoJSON point/multipoint geometry."""
    if not geom:
        return None, None
    c = geom.get("coordinates")
    with contextlib.suppress(Exception):
        while isinstance(c, list) and c and isinstance(c[0], list):
            c = c[0]
        if isinstance(c, list) and len(c) >= 2:
            return float(c[0]), float(c[1])
    return None, None


def _fetch_layer(session: requests.Session, layer: str, licence_class: str) -> list[dict]:
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": f"EPA:{layer}",
        "outputFormat": "application/json",
        "count": str(FETCH_COUNT),
    }
    resp = session.get(WFS, params=params, timeout=120)
    resp.raise_for_status()
    feats = resp.json().get("features", [])
    rows: list[dict] = []
    for f in feats:
        props = f.get("properties", {}) or {}
        x, y = _centroid(f.get("geometry"))
        rec = {our: props.get(src) for src, our in KEEP.items()}
        rec.update(licence_class=licence_class, source_layer=layer, itm_x=x, itm_y=y)
        rows.append(rec)
    log.info("  %s: %d features", layer, len(rows))
    return rows


def scrape() -> pd.DataFrame:
    session = _new_session()
    rows: list[dict] = []
    for layer, klass in LAYERS.items():
        rows.extend(_fetch_layer(session, layer, klass))
    df = pd.DataFrame(rows)
    # one row per active licence number; the combined industrial layer + waste rarely overlap, but a
    # facility carrying both an IE and a waste licence would appear twice — keep the first (industrial).
    df["licensee_name"] = df["licensee_name"].fillna("").str.strip()
    df = df[df["licensee_name"].str.len() > 1].reset_index(drop=True)
    df = df.drop_duplicates(subset=["licence_number", "licensee_name"], keep="first").reset_index(drop=True)
    return df


def main() -> None:
    setup_standalone_logging("epa_licence_register_scrape")
    df = scrape()
    save_parquet(df, OUT)
    log.info(
        "WROTE %s — %d licences / %d licensees / classes=%s",
        OUT,
        len(df),
        df["licensee_name"].nunique(),
        dict(df["licence_class"].value_counts()),
    )


if __name__ == "__main__":
    main()
