"""Resolve which local authority's rulebook governs a point.

v1 is data-anchored with NO new source: the national applications silver has 495,632
geocoded points, each carrying its `PlanningAuthority`. The nearest application's authority
is the governing council (dense feed → reliable except hard on a council boundary, flagged
via distance). A future refinement joins an LA administrative-boundary polygon for exactness.

The authority string is mapped to a planning_rules slug via the _criteria_map council names
(accent- and punctuation-insensitive), so the rulebook resolver (rulebook.py) can quote the
correct council's standards.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from functools import lru_cache

import numpy as np

from .catalogue import REPO_ROOT
from .rulebook import COUNCIL_SUBDIRS, PLANNING_RULES, _council_names

SILVER = REPO_ROOT / "data" / "silver" / "parquet" / "planning_applications_silver.parquet"
_M_PER_DEG_LAT = 111_320.0


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


@dataclass(frozen=True)
class CouncilResult:
    slug: str | None
    authority: str           # raw PlanningAuthority string
    council_name: str
    distance_m: float
    on_boundary: bool        # nearest app is far -> treat council as uncertain


@lru_cache(maxsize=1)
def _authority_to_slug() -> dict[str, str]:
    """normalised authority/council-name -> slug, from the per-council rulebook dirs."""
    out: dict[str, str] = {}
    # every real council dir is a valid slug; index it by its normalised slug
    for sub in COUNCIL_SUBDIRS:
        d = PLANNING_RULES / sub
        if d.is_dir():
            for child in d.iterdir():
                if child.is_dir():
                    out[_norm(child.name)] = child.name
    # also index by the human council name from _criteria_map (e.g. "Galway County Council")
    for slug, (name, _plan) in _council_names().items():
        if name:
            out[_norm(name)] = slug
    return out


def authority_to_slug(authority: str | None) -> str | None:
    if not authority:
        return None
    return _authority_to_slug().get(_norm(authority))


@lru_cache(maxsize=1)
def _app_points():
    """(STRtree, lons, lats, authorities) of the application spine. Cached."""
    import polars as pl
    from shapely import STRtree
    from shapely.geometry import Point

    df = pl.read_parquet(SILVER, columns=["lon", "lat", "PlanningAuthority"]).filter(
        pl.col("lon").is_not_null() & pl.col("lat").is_not_null()
    )
    lons = df["lon"].to_numpy()
    lats = df["lat"].to_numpy()
    auth = df["PlanningAuthority"].to_list()
    pts = [Point(x, y) for x, y in zip(lons, lats)]
    return STRtree(pts), lons, lats, auth


def resolve_council(lon: float, lat: float) -> CouncilResult:
    from shapely.geometry import Point

    tree, lons, lats, auth = _app_points()
    i = int(tree.nearest(Point(lon, lat)))
    authority = auth[i] or ""
    # approximate distance to the nearest application (boundary uncertainty signal)
    dlat = (lat - float(lats[i])) * _M_PER_DEG_LAT
    import math

    dlon = (lon - float(lons[i])) * _M_PER_DEG_LAT * max(math.cos(math.radians(lat)), 0.2)
    dist = math.hypot(dlat, dlon)
    slug = authority_to_slug(authority)
    name = _council_names().get(slug, (authority, ""))[0] if slug else authority
    return CouncilResult(
        slug=slug, authority=authority, council_name=name,
        distance_m=dist, on_boundary=dist > 2000,
    )
