"""Constituency outline extractor — tiny simplified SVG paths for the locator map.

Downloads the official ungeneralised 2023 Dáil constituency boundaries (Tailte
Éireann / OSi open data, CC-BY-4.0), dissolves the 1,000+ polygon parts to one
shape per constituency, simplifies hard, and projects every constituency into ONE
shared Ireland-wide SVG viewbox. The result is a small JSON of 43 SVG path strings
used to draw a discreet "you are here" locator on the constituency dossier — no JS
map library, no runtime geometry, no 54 MB file shipped (only the ~tens-of-KB JSON).

Source : https://data.gov.ie/dataset/constituency-boundaries-ungeneralised-...-2023
         (ArcGIS GeoJSON export; name field ENG_NAME_VALUE = "Mayo (5)").
Writes : data/_meta/constituency_outlines.json  (--write; committed reference)

Integrity self-check (before --write): all 43 canonical constituencies have a path.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import math
import re
import sys
from pathlib import Path

import requests

from paths import PROJECT_ROOT as _ROOT

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

# Reuse the canonical 43 names from the crosswalk extractor (single source of truth).
from reference.ec_constituency_crosswalk_extract import _CONSTITUENCIES  # noqa: E402

_GEOJSON_URL = (
    "https://data-osi.opendata.arcgis.com/api/download/v1/items/a37ad6a3a6ff47e4a5a0ff313b418448/geojson?layers=0"
)
_CACHE = Path("c:/tmp/constituency_boundaries_2023.geojson")  # 54 MB, NOT committed
_OUT = _ROOT / "data" / "_meta" / "constituency_outlines.json"

_VIEW = 1000.0  # SVG viewbox is 0..1000 on the long axis
_SIMPLIFY_DEG = 0.02  # ~2 km — a coarse thumbnail locator, not a precise map
_MIN_PART_DEG2 = 0.0015  # drop polygon parts smaller than this (tiny islands)
_NAME_RE = re.compile(r"\s*\(\d+\)\s*$")  # strip the trailing " (5)" seat count

_NAME_FIXUPS = {  # ENG_NAME spelling -> canonical registry spelling, where they differ
    "Laois-Offaly": "Laois-Offaly",  # placeholder; none needed in practice
}


def fetch_geojson(dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(_GEOJSON_URL, timeout=180, stream=True, headers={"User-Agent": "Mozilla/5.0"}) as r:
        r.raise_for_status()
        dest.write_bytes(r.content)
    return dest


def _canon(eng_name: str) -> str:
    n = _NAME_RE.sub("", eng_name).strip()
    return _NAME_FIXUPS.get(n, n)


def build_outlines(geojson_path: Path) -> dict:
    from shapely.geometry import shape
    from shapely.ops import unary_union

    data = json.loads(geojson_path.read_text(encoding="utf-8"))
    by_name: dict[str, list] = {}
    for feat in data.get("features", []):
        name = _canon(str(feat["properties"].get("ENG_NAME_VALUE", "")))
        if name not in _CONSTITUENCIES:
            continue
        geom = shape(feat["geometry"])
        if not geom.is_valid:
            geom = geom.buffer(0)
        by_name.setdefault(name, []).append(geom.simplify(_SIMPLIFY_DEG, preserve_topology=True))

    shapes = {
        name: unary_union(parts).simplify(_SIMPLIFY_DEG, preserve_topology=True) for name, parts in by_name.items()
    }

    # Shared projection: one bbox over ALL constituencies (Ireland extent), lon scaled
    # by cos(mid-lat) so the thumbnail isn't horizontally stretched at 53°N.
    minx = min(s.bounds[0] for s in shapes.values())
    miny = min(s.bounds[1] for s in shapes.values())
    maxx = max(s.bounds[2] for s in shapes.values())
    maxy = max(s.bounds[3] for s in shapes.values())
    cos = math.cos(math.radians((miny + maxy) / 2))
    w = (maxx - minx) * cos
    h = maxy - miny
    scale = _VIEW / max(w, h)
    vb_w = round(w * scale, 1)
    vb_h = round(h * scale, 1)

    def project(x: float, y: float) -> tuple[int, int]:
        px = (x - minx) * cos * scale
        py = (maxy - y) * scale  # flip Y (SVG origin top-left)
        return round(px), round(py)

    def ring_to_path(coords) -> str:
        # de-dupe consecutive identical integer points after rounding
        pts: list[tuple[int, int]] = []
        for x, y in coords:
            p = project(x, y)
            if not pts or pts[-1] != p:
                pts.append(p)
        if len(pts) < 3:
            return ""
        d = f"M{pts[0][0]},{pts[0][1]}"
        d += "".join(f"L{px},{py}" for px, py in pts[1:])
        return d + "Z"

    def geom_to_path(geom) -> str:
        polys = geom.geoms if geom.geom_type == "MultiPolygon" else [geom]
        out = []
        for poly in polys:
            if poly.is_empty or poly.area < _MIN_PART_DEG2:  # drop tiny islands
                continue
            out.append(ring_to_path(poly.exterior.coords))  # exterior only — thumbnail
        return "".join(out)

    paths = {name: geom_to_path(s) for name, s in shapes.items()}
    return {
        "viewbox": f"0 0 {vb_w} {vb_h}",
        "source": "Tailte Éireann / OSi — Constituency Boundaries 2023 (CC-BY-4.0)",
        "constituencies": paths,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--geojson", type=Path, default=_CACHE)
    args = ap.parse_args()

    if not args.geojson.exists():
        print(f"Downloading constituency boundaries (~54 MB) → {args.geojson} ...")
        fetch_geojson(args.geojson)
    print(f"Building outlines from {args.geojson} ...")

    result = build_outlines(args.geojson)
    got = set(result["constituencies"])
    missing = set(_CONSTITUENCIES) - got
    print(f"  {len(got)}/43 constituencies have an outline; viewbox={result['viewbox']}")
    if missing:
        print(f"  [FAIL] missing: {sorted(missing)}")
    avg_len = sum(len(p) for p in result["constituencies"].values()) // max(len(got), 1)
    print(f"  avg path length: {avg_len} chars")

    if args.write and not missing:
        _OUT.write_text(json.dumps(result, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        kb = _OUT.stat().st_size / 1024
        print(f"\n  Wrote {_OUT.relative_to(_ROOT)} ({kb:.0f} KB)")
    elif args.write:
        print("\n  REFUSING to write — not all 43 constituencies resolved.")
        sys.exit(1)


if __name__ == "__main__":
    main()
