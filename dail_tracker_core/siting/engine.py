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
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from . import rulebook
from .catalogue import Catalogue, load_catalogue
from .council import CouncilResult, resolve_council
from .layers import LayerStore

# how close (m) counts as "near" a designation for the bats / heritage proximity triggers
NEAR_M = {"european_site": 2000, "bats": 1500, "protected_structure": 250}
# an access within this of a road junction raises the crossroads / entrance-setback flag
JUNCTION_NEAR_M = 100.0
# The monument trigger is "within a Zone of Archaeological Notification" — the rule councils state
# verbatim (Fingal DP §14.19.2 / DMSO169) and the others defer to via the National Monuments Acts
# / RMP (Galway DM Std 62, Cork County Ch.16). No council states a metre radius; the geometry is the
# NMS smr_zone polygon, which we test by containment. This constant is ONLY a fallback for the ~8%
# of monument POINTS with no digitised zone: it is the modal zone radius MEASURED from smr_zone
# itself — sqrt(area/π) clusters at ~60 m (p50 60.3 m, p75 60.6 m) — reconstructing the absent zone.
MONUMENT_ZONE_RADIUS_M = 60.0


@dataclass
class IssueResult:
    node_id: str
    title: str
    layer: str  # A / B / C
    fired: bool
    data_status: str  # ok | layer_missing | deep_link_only | partial
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
    mitigation_path: tuple[dict[str, Any], ...] = ()  # static if/then cascade (from catalogue)
    # presentation tier (set from the catalogue node; see catalogue.Node): universal = "standard for
    # any rural one-off"; conditional = "a check that depends on site features we can't read";
    # elevated = a universal node a trigger promoted to site-specific by severity (e.g. extreme septic).
    universal: bool = False
    conditional: bool = False
    elevated: bool = False


@dataclass(frozen=True)
class Exclusion:
    """A statutory land designation whose polygon COVERS the point.

    This is a verifiable FACT about the land (the designation contains the point), surfaced as a
    hard exclusion — NOT a prediction of the planning decision (which stays the council's/ABP's).
    `mitigation` records the narrow, real legal route by which development can still be permitted:
    the exclusion is a presumption-against, not an absolute impossibility.
    """

    layer: str  # the source layer (e.g. "npws_sac")
    designation: str  # human label (e.g. "Special Area of Conservation (SAC)")
    site_name: str  # the specific designated site (e.g. "Lough Corrib SAC")
    mitigation: str = ""  # the narrow statutory route that could still permit development


@dataclass
class SitingResult:
    lon: float
    lat: float
    dev_type: str
    council: CouncilResult
    issues: list[IssueResult]
    disclaimer: str
    exclusions: list[Exclusion] = field(default_factory=list)

    @property
    def fired(self) -> list[IssueResult]:
        return [i for i in self.issues if i.fired]

    @property
    def excluded(self) -> bool:
        """True when the point lies inside a development-excluding statutory designation."""
        return bool(self.exclusions)

    @property
    def missing_layers(self) -> list[str]:
        return sorted({i.node_id for i in self.issues if i.data_status == "layer_missing"})

    @property
    def likely_rfi_reports(self) -> list[str]:
        """The reports the authority will likely seek via a Request for Further Information.

        Process axiom (§11/§12): the required assessments tied to the FIRED obligations. If
        they are not in the first submission, the authority issues an RFI listing them — the
        decision clock stops, and no response within 6 months = deemed withdrawn.
        """
        seen: set[str] = set()
        out: list[str] = []
        for i in self.fired:
            if not i.rule:
                continue
            for c in i.rule.checklist:
                if c.document not in seen:
                    seen.add(c.document)
                    out.append(c.document)
        return out


RFI_NOTE = (
    "If these reports are not in the first submission, the authority typically issues a Request "
    "for Further Information (RFI; s.33 PDA 2000 / Art.33 Regs): the decision clock stops until you "
    "respond, and no response within 6 months means the application is deemed withdrawn. Significant "
    "FI re-triggers public notice."
)


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
    extra = (
        ("This site is in/near a European site, so screening is likely to escalate to a Natura Impact Statement.")
        if near
        else ""
    )
    return True, {"aa_extra": extra}, "ok"


def _european_site(store, lon, lat, dev, slug):
    if not store.available() & {"npws_sac", "npws_spa"}:
        return False, {}, "layer_missing"
    inside = store.covering("npws_sac", lon, lat) + store.covering("npws_spa", lon, lat)
    near = store.near("npws_sac", lon, lat, NEAR_M["european_site"]) + store.near(
        "npws_spa", lon, lat, NEAR_M["european_site"]
    )
    hit = inside or near
    if not hit:
        return False, {}, "ok"
    names = sorted({h.get("SITE_NAME") for h in hit if h.get("SITE_NAME")})
    return (
        True,
        {"site_name": "; ".join(names) or "a European site", "relation": "inside" if inside else "within ~2 km of"},
        "ok",
    )


def _bats(store, lon, lat, dev, slug):
    if not store.available() & {"npws_sac", "npws_spa"}:
        return False, {}, "layer_missing"
    near = store.near("npws_sac", lon, lat, NEAR_M["bats"]) + store.near("npws_spa", lon, lat, NEAR_M["bats"])
    # NB: trees / watercourse / old-structure parts of the trigger need user input/OSM (pending)
    return bool(near), {}, "ok" if near else "partial"


def _peat_bog(store, lon, lat, dev, slug):
    avail = store.available()
    # preferred: GSI Quaternary Sediments carry the subsoil TYPE (incl. peat / blanket bog)
    if "gsi_quaternary" in avail and store.in_extent("gsi_quaternary", lon, lat):
        cov = store.covering("gsi_quaternary", lon, lat)
        if cov:
            desc = " ".join(str(cov[0].get(k, "")) for k in ("QSED_TYPE", "LEGENDDESC")).strip()
            if "peat" in desc.lower() or "bog" in desc.lower():
                return True, {"peat_type": desc}, "ok"
        return False, {}, "ok"  # quaternary mapped here, point is not on peat
    # fallback: weak designated-bog signal only
    nha = store.covering("npws_nha", lon, lat) if "npws_nha" in avail else []
    if nha:
        return True, {"peat_type": "designated bog (NHA)"}, "partial"
    return False, {}, "layer_missing"


def _monument(store, lon, lat, dev, slug):
    avail = store.available()
    # honesty: smr_zone (zones of notification) is NATIONAL, so a monument fires anywhere a zone
    # covers the point; smr_points (which NAMES the monument class/townland) is regional, so outside
    # its extent the monument still fires but is named generically. The in_extent guard keeps a point
    # outside a layer's ingested data as layer_missing, never a silent "ok" (which would mask data we
    # don't have) — it is moot for the national smr_zone but real for the regional smr_points.
    if "smr_zone" not in avail or not store.in_extent("smr_zone", lon, lat):
        return False, {}, "layer_missing"
    in_zone = bool(store.covering("smr_zone", lon, lat))
    # ALSO fire when the point sits ON a recorded-monument POINT (within MONUMENT_ON_M): not every
    # monument carries a zone-of-notification polygon, so a point placed directly on the monument
    # must still fire. The nearest point also names the monument's class/townland.
    n = None
    if "smr_points" in avail and store.in_extent("smr_points", lon, lat):
        n = store.nearest("smr_points", lon, lat)
    on_point = bool(n and n[1] <= MONUMENT_ZONE_RADIUS_M)
    if not in_zone and not on_point:
        return False, {}, "ok"
    mclass = (n[0].get("MONUMENT_CLASS") if n else "") or "a recorded monument"
    townland = (n[0].get("TOWNLAND") if n else "") or ""
    return True, {"monument_class": mclass, "townland": townland}, "ok"


def _national_park(store, lon, lat, dev, slug):
    if "national_parks" not in store.available():
        return False, {}, "layer_missing"
    inside = store.covering("national_parks", lon, lat)
    near = store.near("national_parks", lon, lat, 1000)
    hit = inside or near
    if not hit:
        return False, {}, "ok"
    name = next((h.get("SITE_NAME") for h in hit if h.get("SITE_NAME")), "a National Park")
    return True, {"park_name": name, "relation": "inside" if inside else "adjacent to"}, "ok"


def _gaeltacht(store, lon, lat, dev, slug):
    # Gaeltacht (Irish-speaking) area: the Irish language is a statutory material consideration here
    # (PDA s.10(2)) — Linguistic Impact Statement + (in stronger sub-areas) an Irish-language occupancy
    # condition. Layer is national (7 county polygons); containment only.
    if "gaeltacht" not in store.available():
        return False, {}, "layer_missing"
    cov = store.covering("gaeltacht", lon, lat)
    if not cov:
        return False, {}, "ok"
    name = (cov[0].get("GT_GAEILGE") or cov[0].get("GT_ENGLISH") or "").strip() or "this"
    return True, {"gt_name": name}, "ok"


def _floodplain(store, lon, lat, dev, slug):
    # OPW flood is CC-BY-NC-ND -> never ingested. We can't READ the zone, so this is a CHECK, not a
    # finding: deep_link_only status routes it to the "verify yourself" lane (the floodinfo.ie link
    # is built in evaluate()). Firing here just means "surface the check", never "you are in a flood
    # zone" — that false assertion on every site was the over-flagging bug.
    return True, {"flood_zone": "see floodinfo.ie"}, "deep_link_only"


def _septic(store, lon, lat, dev, slug):
    # antecedent (no public sewer) is approximated by "rural one-off" until the EPA
    # agglomeration layer (WMS-only, not yet ingested) lands; severity = GSI vulnerability
    # category at the point (+ nearby karst). VUL_CAT: X/E=Extreme, H=High, M/L lower.
    # A one-off house needs on-site wastewater UNLESS a public sewer is available. We have no
    # reliable sewer-extent layer — EPA UWWT agglomeration is WMS-only, and zoning proved an
    # unsound proxy (the GZT codes are inconsistent: N1.1="transport bypass", M5="greenbelt";
    # and free-text matched "town" inside "greenbelts around the main towns"). So we do NOT guess
    # sewered vs not (no-inference): we flag the on-site-wastewater consideration on poor ground
    # and STATE the assumption. severity = GSI vulnerability (X/E/H) + nearby karst.
    if "gsi_vulnerability" not in store.available() or not store.in_extent("gsi_vulnerability", lon, lat):
        return False, {}, "layer_missing"
    cov = store.covering("gsi_vulnerability", lon, lat)
    if not cov:
        return False, {}, "ok"  # mapped here, just not a high-vulnerability polygon
    vul = (cov[0].get("VUL_CAT") or "").upper()
    vdesc = cov[0].get("VUL_DESC") or vul
    karst = store.near("gsi_karst", lon, lat, 1000) if store.in_extent("gsi_karst", lon, lat) else []
    fired = vul in {"X", "E", "H"} or bool(karst)
    detail = {"vuln_class": vdesc + (" (karst nearby)" if karst else "")}
    # Septic is a UNIVERSAL consideration for any rural one-off, so it normally sits in the standard
    # tier. PROMOTE it to a site-specific constraint only where the ground is genuinely a viability
    # risk: EXTREME vulnerability (X/E) or karst at surface — the EPA percolation test can fail here.
    # High (H) alone is a normal design constraint and stays in the standard tier.
    if vul in {"X", "E"} or karst:
        detail["_elevate"] = True
    return fired, detail, "ok"


def _road(store, lon, lat, dev, slug):
    if "osm_roads" not in store.available() or not store.in_extent("osm_roads", lon, lat):
        return False, {}, "layer_missing"
    n = store.nearest("osm_roads", lon, lat)
    if not n:
        return False, {}, "ok"
    attrs, dist = n
    ms = attrs.get("maxspeed") or "unposted"
    hw = (attrs.get("highway") or "road").replace("_", " ")
    ref = str(attrs.get("ref") or "")
    is_national = ref[:1].upper() in {"M", "N"}
    # junction-proximity sub-check: an entrance opposite/near a junction forms a crossroads
    # (a traffic hazard) and must be set back / staggered (TII DN-GEO-03060). Detect the
    # nearest OSM road junction and flag if within JUNCTION_NEAR_M.
    jnote, jm = "", ""
    jn = store.nearest_junction("osm_roads", lon, lat, search_m=400)
    if jn and jn[0] <= JUNCTION_NEAR_M:
        jm = round(jn[0])
        shape = "road junction (possible crossroads)" if jn[1] >= 4 else "road junction"
        jnote = (
            f" A {shape} is ~{jm} m away: a new entrance must be SET BACK / STAGGERED from it "
            "(not directly opposite — that forms a crossroads), with the sight-triangle land in "
            "your control; a Road Safety Audit (#6) will assess the junction."
        )
    # any new vehicular access has an entrance-sightline standard; fire when a road is within
    # ~150 m (i.e. the site fronts/accesses it). Detail carries the speed for the rule text.
    fired = dist <= 150
    detail = {
        "maxspeed": ms,
        "road_class": hw,
        "road_ref": ref,
        "is_national": "national road" if is_national else "",
        "junction_note": jnote,
        "junction_m": jm,
    }
    # Entrance sightlines are UNIVERSAL for any new access (standard tier). PROMOTE to a
    # site-specific constraint where the access is near a junction/crossroads (the genuine hazard
    # needing set-back/staggering + a Road Safety Audit) or onto a national road.
    if fired and (jnote or is_national):
        detail["_elevate"] = True
    return fired, detail, "ok"


def _council_layers(store, slug, kind):
    """Per-LA layers (RPS/ACA/landscape) scoped to THIS council only.

    `layers_matching(kind)` returns every council's per-LA layer (they carry no bbox), so an
    unscoped match would let a Kerry point 'see' the Cork RPS layer and report status='ok' as if
    Kerry's statutory list had been checked. We only count a layer whose name stems from this
    council's slug (cork_county_council -> cork_county*); absent -> [] -> honest 'partial'/missing.
    """
    layers = store.layers_matching(kind)
    if not slug:
        return layers
    stem = slug.replace("_council", "")
    return [ln for ln in layers if ln.startswith(stem)]


def _landscape(store, lon, lat, dev, slug):
    from .dem import terrain

    landscape_layers = _council_layers(store, slug, "landscape")
    lca_hit: list[dict] = []
    for ln in landscape_layers:
        lca_hit += store.covering(ln, lon, lat)
    t = terrain(lon, lat)
    if not landscape_layers and not t.ok:
        return False, {}, "layer_missing"  # no landscape layer AND DEM unavailable
    fired = bool(lca_hit) or (t.ok and t.exposed)
    detail = {
        # per-council landscape schemas differ (Galway=NAME, Cork=TYPE, others vary)
        "lca_class": (
            next((lca_hit[0].get(k) for k in ("NAME", "TYPE", "CATEGORY") if lca_hit[0].get(k)), "designated landscape")
            if lca_hit
            else "open countryside"
        ),
        "elevation_m": t.elevation_m if t.ok else "",
    }
    # full coverage needs the per-LA landscape-sensitivity layer; DEM-only exposure is partial
    status = "ok" if landscape_layers else "partial"
    return fired, detail, status


# Zone families for the rural-housing-need test. Settlement/serviced zones are land the plan
# has already zoned FOR development, so the "urban-generated rural housing" local-need policy
# does not bite there (a house may face other issues, but not rural need). Restricted rural
# zones (agricultural / high-amenity / open countryside) are where the policy applies.
_SETTLEMENT_ZONE_WORDS = (
    "residential",
    "city centre",
    "town centre",
    "village centre",
    "mixed use",
    "mixed-use",
    "commercial",
    "retail",
    "enterprise",
    "employment",
    "industrial",
    "business",
    "office",
    "institution",
    "community",
    "education",
    "tourism",
    "transport",
    "utilit",
    "regeneration",
)
# NB deliberately NOT a bare "amenity" — that substring appears in R-zone descriptions
# ("protection of existing residential amenity") AND in urban "Recreation and Amenity"
# (parkland, e.g. Eyre Square); both previously mis-fired this node. Rural-housing-need is
# about agricultural / countryside / high-amenity-rural land, so match those specifically.
_RURAL_ZONE_WORDS = (
    "agricult",
    "high amenity",
    "open countryside",
    "countryside",
    "rural",
    "green belt",
    "greenbelt",
    "greenfield",
    "landscape",
)


def _rural_need(store, lon, lat, dev, slug):
    if "zoning_gzt" not in store.available():
        return False, {}, "layer_missing"
    cov = store.covering("zoning_gzt", lon, lat)
    if not cov:
        # no zone at the point -> typically open countryside, where rural-housing policy bites
        return (
            True,
            {"local_need_test": "This appears to be unzoned open countryside.", "zone": "unzoned countryside"},
            "ok",
        )
    zone = cov[0]
    orig = str(zone.get("ZONE_ORIG", "")).lower()
    blob = " ".join(str(zone.get(k, "")) for k in ("ZONE_ORIG", "ZONE_DESC", "ZONE_GZT")).lower()
    label = zone.get("ZONE_ORIG") or zone.get("ZONE_DESC") or zone.get("ZONE_GZT") or "zoned"
    # settlement/serviced land first: the local-need policy does not apply -> do not fire
    if any(w in orig for w in _SETTLEMENT_ZONE_WORDS):
        return False, {"local_need_test": "", "zone": label}, "ok"
    # restricted rural zoning (or agricultural/high-amenity) -> local-need policy applies
    rural = any(w in blob for w in _RURAL_ZONE_WORDS)
    return rural, {"local_need_test": "", "zone": label}, "ok"


def _protected_structure(store, lon, lat, dev, slug):
    avail = store.available()
    rps_aca = _council_layers(store, slug, "rps") + _council_layers(store, slug, "aca")
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


SLOPE_FIRE_DEG = 3.0  # above this, run-off must be actively managed (SuDS / re-grading)


def _surface_water(store, lon, lat, dev, slug):
    """Surface-water / drainage axiom (checklist #20, DM Std 68) — driven by DEM slope.

    A sloping site sheds run-off downhill; it must be retained/attenuated on-site. Escalates
    where the slope drains toward a European site (run-off rate AND quality must be controlled).
    """
    from .dem import terrain

    t = terrain(lon, lat)
    if not t.ok or t.slope_deg is None:
        return False, {}, "layer_missing"
    near_eur = store.near("npws_sac", lon, lat, 1000) + store.near("npws_spa", lon, lat, 1000)
    receptor = (
        (" — and it drains toward a European site, so run-off RATE and QUALITY must be controlled to protect it")
        if near_eur
        else ""
    )
    fired = t.slope_deg >= SLOPE_FIRE_DEG
    detail = {"slope_deg": t.slope_deg, "receptor_note": receptor}
    # SuDS/drainage is a UNIVERSAL requirement for any sloping site (standard tier). PROMOTE to a
    # site-specific constraint only where the run-off drains toward a European site (rate AND quality
    # must then be controlled to protect it) — the genuinely onerous case.
    if fired and near_eur:
        detail["_elevate"] = True
    return fired, detail, "ok"


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
    "national_park": _national_park,
    "gaeltacht": _gaeltacht,
    "floodplain": _floodplain,
    "septic_groundwater": _septic,
    "road_sightlines": _road,
    "landscape_siting": _landscape,
    "rural_need_zoning": _rural_need,
    "protected_structure": _protected_structure,
    "surface_water": _surface_water,
}


# ── hard-exclusion mask ─────────────────────────────────────────────────────────────────────
# Statutory designations whose CONTAINMENT excludes ordinary development. Each is national (or
# national+region) and carries a SITE_NAME, so the mask names the specific protected site. The
# mask reports a FACT about the land (the designation polygon covers the point); it does not
# assert the planning OUTCOME (a council/ABP can still grant in rare cases — IROPI etc.). NB peat
# *subsoil* alone (gsi_quaternary) is an engineering constraint, not a statutory exclusion — only
# DESIGNATED bog (inside an NHA/SAC) excludes, which is why peat_bog stays a hard constraint, not
# an exclusion here.
# Each row: (layer, label, mitigation route). The mitigation text is NOT invented — for European
# sites it is the EU Habitats Directive Art. 6 mechanism (transposed by the Birds & Natural Habitats
# Regs 2011, which the rulebook already cites): Art. 6(3) a development can proceed only where an
# Appropriate Assessment concludes no adverse effect on site integrity; failing that, Art. 6(4)
# IROPI + compensatory measures. These routes are exceptional and rarely open to a private house.
_AA_ROUTE = (
    "Only permissible where an Appropriate Assessment concludes no adverse effect on the "
    "site's integrity (Habitats Directive Art. 6(3)); failing that, only via imperative "
    "reasons of overriding public interest with compensatory measures (Art. 6(4)) — "
    "exceptional and rarely available for a private dwelling. Confirm with NPWS."
)
EXCLUSION_DESIGNATIONS: tuple[tuple[str, str, str], ...] = (
    ("npws_sac", "Special Area of Conservation (SAC)", _AA_ROUTE),
    ("npws_spa", "Special Protection Area (SPA)", _AA_ROUTE),
    (
        "npws_nha",
        "Natural Heritage Area (NHA) — incl. raised/blanket bog",
        "Activities Requiring Consent apply; some development can be consented by NPWS where it does "
        "not damage the conservation interest. Confirm with NPWS before relying on this.",
    ),
    (
        "national_parks",
        "National Park",
        "Generally only park-related / designation-compatible uses are permitted (NPWS-managed land); "
        "confirm any proposal with NPWS and the planning authority.",
    ),
)


def hard_exclusions(store: LayerStore, lon: float, lat: float) -> list[Exclusion]:
    """Statutory designations whose polygon COVERS the point (ordinary development excluded).

    Fact-based, not a verdict, and NOT an absolute: each carries the narrow real route by which
    development could still be permitted (it may be possible to mitigate — the user must check it).
    An empty list means no excluding designation was found in the ingested layers — which is NOT
    proof the land is developable. One entry per designation type.
    """
    out: list[Exclusion] = []
    for layer, label, mitigation in EXCLUSION_DESIGNATIONS:
        if layer not in store.available() or not store.in_extent(layer, lon, lat):
            continue
        hits = store.covering(layer, lon, lat)
        if hits:
            name = hits[0].get("SITE_NAME") or hits[0].get("DESIG") or "a designated site"
            out.append(Exclusion(layer=layer, designation=label, site_name=str(name), mitigation=mitigation))
    return out


def _ge(v, n) -> bool:
    return (v or 0) >= n


# Attribute / scale-gated axioms (§16 "always-on" + scale-gated): no spatial layer — they
# fire on development type, unit count and floor area. Returns (fired, detail). Thresholds
# from the required-assessments checklist (e.g. climate #19 = >10 units / >1,000 m² retail;
# EIA #15 ≈ Schedule 5, 500 dwellings). One-off houses clear every scale gate, so only the
# universal #4/#18 fire for them.
ATTRIBUTE_RULES: dict[str, Callable] = {
    "landscaping": lambda dev, u, fa: (True, {}),
    "energy_cert": lambda dev, u, fa: (True, {}),
    "design_statement": lambda dev, u, fa: (dev in ("multi_unit", "commercial") or _ge(u, 5), {}),
    "mobility_plan": lambda dev, u, fa: (dev in ("multi_unit", "commercial") or _ge(u, 10), {}),
    "climate_statement": lambda dev, u, fa: (_ge(u, 11) or (dev == "commercial" and _ge(fa, 1000)), {}),
    "waste_management": lambda dev, u, fa: (dev in ("multi_unit", "commercial"), {}),
    "noise_assessment": lambda dev, u, fa: (dev == "commercial", {}),
    "eia": lambda dev, u, fa: (_ge(u, 500) or _ge(fa, 50_000), {}),
}


def evaluate(
    lon: float,
    lat: float,
    dev_type: str = "one_off_house",
    *,
    num_units: int | None = None,
    floor_area_m2: float | None = None,
    store: LayerStore | None = None,
    council_slug: str | None = None,
    catalogue_path: str | None = None,
) -> SitingResult:
    cat: Catalogue = load_catalogue(catalogue_path)
    store = store or LayerStore()
    council = CouncilResult(council_slug, "", "", 0.0, False) if council_slug else resolve_council(lon, lat)
    slug = council.slug

    issues: list[IssueResult] = []
    for node in cat.nodes:
        if not node.applies(dev_type):
            continue
        trig = TRIGGERS.get(node.id)
        attr = ATTRIBUTE_RULES.get(node.id)
        if trig is not None:
            fired, detail, status = trig(store, lon, lat, dev_type, slug)
        elif attr is not None:
            fired, detail = attr(dev_type, num_units, floor_area_m2)
            status = "ok"
        else:
            fired, detail, status = False, {}, "layer_missing"

        # a trigger may PROMOTE a universal node to a site-specific constraint by severity
        # (e.g. septic on extreme-vulnerability ground); pop the marker so it never shows in detail.
        elevated = bool(detail.pop("_elevate", False))

        extra: dict[str, Any] = {}
        if node.id == "floodplain":
            e = _to_3857(lon, lat)
            extra["flood_link"] = f"https://www.floodinfo.ie/map/floodmaps/?X={round(e[1])}&Y={round(e[0])}&Z=14"

        rule = rulebook.resolve(slug, node.id, catalogue_path) if slug else None
        issues.append(
            IssueResult(
                node_id=node.id,
                title=node.title,
                layer=node.layer,
                fired=fired,
                data_status=status,
                mitigation_class=node.mitigation_class,
                mitigation_classes=node.mitigation_classes,
                flag=_fmt(node.flag_template, detail),
                engage=node.engage,
                mitigates=node.mitigates,
                risk_note=node.risk_note,
                precedents=node.precedents,
                rule=rule,
                detail=detail,
                extra=extra,
                mitigation_path=node.mitigation_path,
                universal=node.universal,
                conditional=node.conditional,
                elevated=elevated,
            )
        )

    return SitingResult(
        lon=lon,
        lat=lat,
        dev_type=dev_type,
        council=council,
        issues=issues,
        disclaimer=cat.disclaimer,
        exclusions=hard_exclusions(store, lon, lat),
    )
