"""Resolve which local authority's rulebook governs a point.

Two signals, strongest first:

  1. ZONING CONTAINMENT (authoritative where available). The ingested zoning layer carries a
     `PLAN_NAME` per polygon ("Galway City Development Plan…" vs "Galway County Development
     Plan…"). A point COVERED by a zoning polygon is governed by that plan's council — an exact
     containment test, so on_boundary is False and resolved_via="zoning". This is the only
     signal that gets the City↔County line right; nearest-application can snap across it.

  2. NEAREST APPLICATION (fallback). The national applications silver has 495,632 geocoded
     points, each carrying its `PlanningAuthority`; the nearest one's authority governs. Dense
     feed → reliable away from boundaries, but at a council boundary the nearest app can sit on
     the wrong side. `on_boundary` here means only "nearest application is >2 km away" (a
     data-sparsity / low-confidence flag), NOT "near an administrative boundary".

Zoning coverage today is Galway-only; everywhere else falls back to nearest-application. The
durable fix is a national LA administrative-boundary polygon (Tailte Éireann) — a new source,
so it is left as a follow-up rather than ingested blind.

The authority/plan string is mapped to a planning_rules slug via the _criteria_map council
names (accent- and punctuation-insensitive), so rulebook.py can quote the right standards.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from functools import lru_cache

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
    authority: str  # raw PlanningAuthority / plan string
    council_name: str
    distance_m: float
    on_boundary: bool  # nearest app is far -> treat council as uncertain
    resolved_via: str = "nearest_application"  # "zoning" (authoritative) | "nearest_application"


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
    pts = [Point(x, y) for x, y in zip(lons, lats, strict=False)]
    return STRtree(pts), lons, lats, auth


# generic tokens carried by every council/plan name — useless for identifying the council
_GENERIC_TOKENS = frozenset(
    {
        "council",
        "the",
        "of",
        "and",
        "development",
        "plan",
        "county",
        "city",
        "local",
        "area",
        "joint",
        "urban",
        "settlement",
        "plans",
        "environs",
    }
)


def _plan_name_to_slug(plan_name: str) -> str | None:
    """Map a zoning polygon's PLAN_NAME to a council slug (national zoning_gzt is messy).

    PLAN_NAMEs vary wildly ("Galway City Development Plan 2023-2029", "The Fingal Development
    Plan 2023 – 2029", "South Dublin County Development Plan 2022-2028", abbreviations like
    "WCCC", and bare "Development Plan 2022-2028"). Strategy: a council is a CANDIDATE when
    all its distinctive PLACE tokens (its name minus generic words and the city/county type)
    appear in the plan name; disambiguate same-place city-vs-county by the type keyword in the
    plan name (e.g. "South Dublin County …" beats "Dublin City" on both token-count and the
    "county" keyword). Returns None when nothing matches or the top two tie — so an ambiguous
    or unrecognised plan name falls back to nearest-application rather than mis-resolving.
    """
    norm = _norm(plan_name)
    if not norm:
        return None
    plan_toks = set(norm.split("_"))
    has_city, has_county = "city" in plan_toks, "county" in plan_toks

    scored: list[tuple[int, str]] = []
    for slug, (name, _plan) in _council_names().items():
        name_toks = _norm(name).split("_")
        place = [t for t in name_toks if t and t not in _GENERIC_TOKENS]
        if not place or not all(t in plan_toks for t in place):
            continue
        is_city, is_county = "city" in name_toks, "county" in name_toks
        score = 10 * len(place)  # specificity: more place tokens wins
        if has_city and is_city or has_county and is_county:
            score += 5  # type keyword agrees
        if has_city and is_county and not is_city or has_county and is_city and not is_county:
            score -= 5  # type keyword disagrees (city vs county)
        scored.append((score, slug))

    if not scored:
        return None
    scored.sort(reverse=True)
    if len(scored) > 1 and scored[0][0] == scored[1][0]:
        return None  # ambiguous -> let nearest-application decide
    return scored[0][1]


def _zoning_council(lon: float, lat: float):
    """Authoritative council from zoning-polygon containment, or None if not covered.

    Returns (slug, plan_name) for the polygon covering the point. Lazy LayerStore import keeps
    the (heavy) polars/shapely load off module import.
    """
    try:
        from .layers import LayerStore
    except Exception:
        return None
    store = LayerStore()
    if "zoning_gzt" not in store.available():
        return None
    cov = store.covering("zoning_gzt", lon, lat)
    if not cov:
        return None
    plan = str(cov[0].get("PLAN_NAME") or "")
    slug = _plan_name_to_slug(plan)
    return (slug, plan) if slug else None


def resolve_council(lon: float, lat: float) -> CouncilResult:
    from shapely.geometry import Point

    # 1. authoritative: zoning-polygon containment (exact; gets the City/County line right)
    z = _zoning_council(lon, lat)
    if z:
        slug, plan = z
        name = _council_names().get(slug, (plan, ""))[0]
        return CouncilResult(
            slug=slug, authority=plan, council_name=name, distance_m=0.0, on_boundary=False, resolved_via="zoning"
        )

    # 2. fallback: nearest application's PlanningAuthority
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
        slug=slug,
        authority=authority,
        council_name=name,
        distance_m=dist,
        on_boundary=dist > 2000,
    )
