"""The siting-check engine: point + development type -> triggered issues.

evaluate() walks the issue catalogue, evaluates each node's trigger against the ingested
designation layers, resolves the governing rule VERBATIM for the council in force, and
returns the fired issues grouped for rendering. It NEVER outputs a grant/refuse verdict or
a design prescription (doc/PLANNING_SITING_DECISION_TREE.md §1; memory feedback_no_inference).

Honesty rules baked in:
  - A node whose source layer is not yet ingested is marked data_status="layer_missing"
    and fired=False — we do NOT claim "no issue here" when we have no data to say so.
  - "near" is approximate (degrees, no pyproj yet) and labelled as such.
  - Risk language is "likely", never "will" (§23.5).
"""

from __future__ import annotations

import string
from dataclasses import dataclass, field
from typing import Any, Callable

from . import rulebook
from .catalogue import Catalogue, Node, load_catalogue
from .council import CouncilResult, resolve_council
from .layers import LayerStore

# how close (m) counts as "near" a designation for the bats / heritage proximity triggers
NEAR_M = {"european_site": 2000, "bats": 1500, "protected_structure": 250}


@dataclass
class IssueResult:
    node_id: str
    title: str
    layer: str                       # A / B / C
    fired: bool
    data_status: str                 # ok | layer_missing | deep_link_only | partial
    mitigation_class: str
    mitigation_classes: frozenset[str]
    flag: str
    engage: tuple[str, ...]
    mitigates: str
    risk_note: str
    precedents: tuple[dict[str, Any], ...]
    rule: rulebook.ResolvedRule | None
    detail: dict[str, Any] = field(default_factory=dict)
    extra: dict[str, Any] = field(default_factory=dict)  # e.g. {"flood_link": ...}


@dataclass
class SitingResult:
    lon: float
    lat: float
    dev_type: str
    council: CouncilResult
    issues: list[IssueResult]
    disclaimer: str

    @property
    def fired(self) -> list[IssueResult]:
        return [i for i in self.issues if i.fired]

    @property
    def missing_layers(self) -> list[str]:
        return sorted({i.node_id for i in self.issues if i.data_status == "layer_missing"})


class _SafeDict(dict):
    def __missing__(self, key):  # leave unknown placeholders as a neutral blank
        return ""


def _fmt(template: str, detail: dict[str, Any]) -> str:
    try:
        return string.Formatter().vformat(template, (), _SafeDict(detail)).strip()
    except Exception:
        return template.strip()


# ── trigger functions: node_id -> (store, lon, lat, dev_type, council_slug) -> (fired, detail, status)
# Each returns whether the issue fires, the placeholder detail, and the data status. Layers not
# yet ingested return ("layer_missing").

def _t_aa(store, lon, lat, dev, slug):  # universal gate
    near = store.covering("npws_sac", lon, lat) or store.covering("npws_spa", lon, lat)
    extra = ("This site is in/near a European site, so screening is likely to escalate to a "
             "Natura Impact Statement.") if near else ""
    return True, {"aa_extra": extra}, "ok"


def _european_site(store, lon, lat, dev, slug):
    if not store.available() & {"npws_sac", "npws_spa"}:
        return False, {}, "layer_missing"
    inside = store.covering("npws_sac", lon, lat) + store.covering("npws_spa", lon, lat)
    near = store.near("npws_sac", lon, lat, NEAR_M["european_site"]) + \
        store.near("npws_spa", lon, lat, NEAR_M["european_site"])
    hit = inside or near
    if not hit:
        return False, {}, "ok"
    names = sorted({h.get("SITE_NAME") for h in hit if h.get("SITE_NAME")})
    return True, {"site_name": "; ".join(names) or "a European site",
                  "relation": "inside" if inside else "within ~2 km of"}, "ok"


def _bats(store, lon, lat, dev, slug):
    if not store.available() & {"npws_sac", "npws_spa"}:
        return False, {}, "layer_missing"
    near = store.near("npws_sac", lon, lat, NEAR_M["bats"]) + \
        store.near("npws_spa", lon, lat, NEAR_M["bats"])
    # NB: trees / watercourse / old-structure parts of the trigger need user input/OSM (pending)
    return bool(near), {}, "ok" if near else "partial"


def _peat_bog(store, lon, lat, dev, slug):
    if "epa_subsoils" not in store.available():
        # soils layer not ingested; weak NHA-bog signal only
        nha = store.covering("npws_nha", lon, lat) if "npws_nha" in store.available() else []
        if nha:
            return True, {}, "partial"
        return False, {}, "layer_missing"
    return False, {}, "ok"


def _monument(store, lon, lat, dev, slug):
    if "smr_zone" not in store.available():
        return False, {}, "layer_missing"
    hit = store.covering("smr_zone", lon, lat)
    if not hit:
        return False, {}, "ok"
    return True, {"monument_class": "a recorded monument", "townland": ""}, "ok"


def _floodplain(store, lon, lat, dev, slug):
    # OPW flood is CC-BY-NC-ND -> never ingested; always surface the floodinfo.ie deep-link
    e3857 = _to_3857(lon, lat)
    link = f"https://www.floodinfo.ie/map/floodmaps/?X={round(e3857[1])}&Y={round(e3857[0])}&Z=14"
    return True, {"flood_zone": "see floodinfo.ie"}, "deep_link_only"


def _septic(store, lon, lat, dev, slug):
    # antecedent (no public sewer) is approximated by "rural one-off" until the EPA
    # agglomeration layer (WMS-only, not yet ingested) lands; severity = GSI vulnerability
    # category at the point (+ nearby karst). VUL_CAT: X/E=Extreme, H=High, M/L lower.
    avail = store.available()
    if "gsi_vulnerability" not in avail:
        return False, {}, "layer_missing"
    cov = store.covering("gsi_vulnerability", lon, lat)
    if not cov:
        return False, {}, "ok"  # outside the (Galway-bbox) GSI coverage
    vul = (cov[0].get("VUL_CAT") or "").upper()
    vdesc = cov[0].get("VUL_DESC") or vul
    karst = store.near("gsi_karst", lon, lat, 1000) if "gsi_karst" in avail else []
    fired = vul in {"X", "E", "H"} or bool(karst)
    detail = {"vuln_class": vdesc + (" (karst nearby)" if karst else "")}
    return fired, detail, "ok"


def _road(store, lon, lat, dev, slug):
    if "osm_roads" not in store.available():
        return False, {}, "layer_missing"
    n = store.nearest("osm_roads", lon, lat)
    if not n:
        return False, {}, "ok"
    attrs, dist = n
    ms = attrs.get("maxspeed") or "unposted"
    hw = (attrs.get("highway") or "road").replace("_", " ")
    ref = str(attrs.get("ref") or "")
    is_national = ref[:1].upper() in {"M", "N"}
    # any new vehicular access has an entrance-sightline standard; fire when a road is within
    # ~150 m (i.e. the site fronts/accesses it). Detail carries the speed for the rule text.
    fired = dist <= 150
    detail = {"maxspeed": ms, "road_class": hw, "road_ref": ref,
              "is_national": "national road" if is_national else ""}
    return fired, detail, "ok"


def _landscape(store, lon, lat, dev, slug):
    from .dem import terrain

    landscape_layers = store.layers_matching("landscape")
    lca_hit: list[dict] = []
    for ln in landscape_layers:
        lca_hit += store.covering(ln, lon, lat)
    t = terrain(lon, lat)
    if not landscape_layers and not t.ok:
        return False, {}, "layer_missing"  # no landscape layer AND DEM unavailable
    fired = bool(lca_hit) or (t.ok and t.exposed)
    detail = {
        "lca_class": (lca_hit[0].get("NAME") if lca_hit else "open countryside"),
        "elevation_m": t.elevation_m if t.ok else "",
    }
    # full coverage needs the per-LA landscape-sensitivity layer; DEM-only exposure is partial
    status = "ok" if landscape_layers else "partial"
    return fired, detail, status


def _rural_need(store, lon, lat, dev, slug):
    if "zoning_gzt" not in store.available():
        return False, {}, "layer_missing"
    cov = store.covering("zoning_gzt", lon, lat)
    if not cov:
        # no zone at the point -> typically open countryside, where rural-housing policy bites
        return True, {"local_need_test": "This appears to be unzoned open countryside.",
                      "zone": "unzoned countryside"}, "ok"
    zone = cov[0]
    desc = " ".join(str(zone.get(k, "")) for k in ("ZONE_DESC", "ZONE_ORIG", "ZONE_GZT")).lower()
    agri = any(w in desc for w in ("agricult", "rural", "amenity", "open space", "green"))
    return agri, {"local_need_test": "", "zone": zone.get("ZONE_DESC") or zone.get("ZONE_GZT") or "zoned"}, "ok"


def _protected_structure(store, lon, lat, dev, slug):
    avail = store.available()
    rps_aca = store.layers_matching("rps") + store.layers_matching("aca")
    if "niah" not in avail and not rps_aca:
        return False, {}, "layer_missing"
    near: list[dict] = []
    if "niah" in avail:
        near += store.near("niah", lon, lat, NEAR_M["protected_structure"])
    for ln in rps_aca:
        near += store.covering(ln, lon, lat) + store.near(ln, lon, lat, NEAR_M["protected_structure"])
    # RPS is the statutory list; NIAH-only coverage is partial
    status = "ok" if rps_aca else "partial"
    return bool(near), {}, status


def _to_3857(lon: float, lat: float) -> tuple[float, float]:
    """WGS84 -> Web Mercator (easting, northing). Pure math, no pyproj."""
    import math
    R = 6378137.0
    x = R * math.radians(lon)
    y = R * math.log(math.tan(math.pi / 4 + math.radians(lat) / 2))
    return x, y


TRIGGERS: dict[str, Callable] = {
    "aa_screening": _t_aa,
    "bats": _bats,
    "european_site": _european_site,
    "peat_bog": _peat_bog,
    "monument": _monument,
    "floodplain": _floodplain,
    "septic_groundwater": _septic,
    "road_sightlines": _road,
    "landscape_siting": _landscape,
    "rural_need_zoning": _rural_need,
    "protected_structure": _protected_structure,
}


def evaluate(
    lon: float,
    lat: float,
    dev_type: str = "one_off_house",
    *,
    store: LayerStore | None = None,
    council_slug: str | None = None,
    catalogue_path: str | None = None,
) -> SitingResult:
    cat: Catalogue = load_catalogue(catalogue_path)
    store = store or LayerStore()
    council = (CouncilResult(council_slug, "", "", 0.0, False) if council_slug
               else resolve_council(lon, lat))
    slug = council.slug

    issues: list[IssueResult] = []
    for node in cat.nodes:
        if not node.applies(dev_type):
            continue
        trig = TRIGGERS.get(node.id)
        if trig is None:
            fired, detail, status = False, {}, "layer_missing"
        else:
            fired, detail, status = trig(store, lon, lat, dev_type, slug)

        extra: dict[str, Any] = {}
        if node.id == "floodplain":
            e = _to_3857(lon, lat)
            extra["flood_link"] = f"https://www.floodinfo.ie/map/floodmaps/?X={round(e[1])}&Y={round(e[0])}&Z=14"

        rule = rulebook.resolve(slug, node.id, catalogue_path) if slug else None
        issues.append(IssueResult(
            node_id=node.id, title=node.title, layer=node.layer, fired=fired,
            data_status=status, mitigation_class=node.mitigation_class,
            mitigation_classes=node.mitigation_classes,
            flag=_fmt(node.flag_template, detail), engage=node.engage,
            mitigates=node.mitigates, risk_note=node.risk_note,
            precedents=node.precedents, rule=rule, detail=detail, extra=extra,
        ))

    return SitingResult(lon=lon, lat=lat, dev_type=dev_type, council=council,
                        issues=issues, disclaimer=cat.disclaimer)
