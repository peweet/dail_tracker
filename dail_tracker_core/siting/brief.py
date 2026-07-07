"""Assemble a SitingResult into a structured site-planning brief (pure, deterministic).

The brief reorganises the fired issues into the shape a user actually reads: the hard
(pass/fail) constraints, the shaping constraints, a dedicated ACCESS & ENTRANCE section
(road class + the junction/crossroads finding), the universal/scale obligations, and the
required-report / RFI list. It adds no new logic — it only groups what engine.evaluate()
already produced, so it is as deterministic as the engine (same result -> same brief).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .engine import RFI_NOTE, SitingResult

# ── mitigation-cascade rendering ────────────────────────────────────────────────
# A leaf outcome maps to a short tag; we mirror the P/D/F vocabulary (clear/mitigable/fatal).
OUTCOME_TAG = {"clear": "[clear]", "mitigable": "[mitigable]", "fatal": "[often fatal]"}


def _render_path(steps: list[dict[str, Any]], lines: list[str], indent: str = "  ") -> None:
    """Render a static if/then mitigation cascade as an indented tree.

    Spine steps are numbered (1., 2., …); each step's findings branch with ├─/└─, and the
    follow-on steps inside a branch are bulleted (NOT numbered) so the two mutually-exclusive
    paths don't share a running counter.
    """
    for n, step in enumerate(steps, 1):
        who = f"  → {step['who']}" if step.get("who") else ""
        lines.append(f"{indent}{n}. {step['do']}{who}")
        branches = step.get("findings") or []
        for bi, br in enumerate(branches):
            last = bi == len(branches) - 1
            elbow = "└─" if last else "├─"
            tag = OUTCOME_TAG.get(br["outcome"], "")
            lines.append(f"{indent}   {elbow} if {br['if']} … {tag}")
            child_indent = indent + ("        " if last else "   │    ")
            for child in br.get("then") or []:
                if child.get("findings"):
                    _render_path([child], lines, child_indent)
                else:
                    cwho = f"  → {child['who']}" if child.get("who") else ""
                    lines.append(f"{child_indent}• {child['do']}{cwho}")


def cascade_text(path) -> str:
    """Public helper: render a mitigation_path cascade to monospace text (for the UI/brief)."""
    lines: list[str] = []
    _render_path(list(path or ()), lines)
    return "\n".join(lines)


# ── one-number road sightline (Feature B) ───────────────────────────────────────
# OSM `highway` class -> (assumed speed, the single key visibility figure). Used only when
# OSM carries no posted maxspeed; a posted speed wins. Figures are stopping-sight-distance
# approximations (TII DN-GEO-03060) — shown as "assumed"; the binding number is the council
# DM standard (the road node's risk_note already says OSM gives the road, not the splay).
ROAD_SPEED_MAP: dict[str, tuple[str, str]] = {
    "living street": ("~30 km/h", "~45 m"),
    "track": ("~50 km/h", "~70 m"),
    "service": ("~50 km/h", "~70 m"),
    "residential": ("~50 km/h", "~70 m"),
    "unclassified": ("80 km/h (unposted rural default)", "~160 m"),
    "tertiary": ("80 km/h (unposted rural default)", "~160 m"),
    "secondary": ("~80 km/h", "~160 m"),
    "primary": ("~80 km/h", "~160 m"),
}
# posted speed (km/h) -> sight distance, nearest band
SPEED_SIGHT = {30: "~45 m", 50: "~70 m", 60: "~90 m", 80: "~160 m", 100: "~215 m", 120: "~285 m"}


def _posted_kmh(maxspeed: Any) -> int | None:
    digits = "".join(c for c in str(maxspeed) if c.isdigit())
    return int(digits) if digits else None


def road_sightline_line(detail: dict[str, Any]) -> str:
    """One human line: the road, an assumed/posted speed, and the single key visibility figure."""
    if detail.get("is_national"):
        return (
            "national road — a new house is generally restricted to farm families and direct access is usually refused"
        )
    road_class = (detail.get("road_class") or "road").strip()
    posted = _posted_kmh(detail.get("maxspeed", ""))
    if posted is not None:
        band = min(SPEED_SIGHT, key=lambda s: abs(s - posted))
        speed, sight = f"{posted} km/h (posted)", SPEED_SIGHT[band]
    else:
        speed, sight = ROAD_SPEED_MAP.get(road_class, ("~50 km/h (assumed)", "~70 m"))
    return f"{road_class}, {speed} → prove {sight} visibility each way, on land you control"


@dataclass
class BriefItem:
    title: str
    why: str
    action: str
    reports: tuple[str, ...] = ()
    path: tuple[dict[str, Any], ...] = ()  # static if/then cascade (may be empty)
    node_id: str = ""
    passfail: bool = False  # F-class — lets the standard tier still flag the rural-need pass/fail gate


@dataclass
class Brief:
    site: dict[str, Any]
    headline: str
    exclusions: list[Any]  # statutory designations that exclude development (facts)
    hard_constraints: list[BriefItem]
    shaping_constraints: list[BriefItem]
    access: dict[str, Any]
    obligations: list[BriefItem]  # STANDARD requirements for any rural one-off (universal, non-elevated)
    to_verify: list[BriefItem]  # checks we can't read (flood deep-link + conditional, e.g. bats)
    required_reports: list[str]
    rfi_note: str
    not_assessed: list[str]
    disclaimer: str

    @property
    def excluded(self) -> bool:
        return bool(self.exclusions)


def _item(i) -> BriefItem:
    reports = tuple(c.document for c in i.rule.checklist) if i.rule else ()
    return BriefItem(
        title=i.title,
        why=i.flag,
        action=i.mitigates,
        reports=reports,
        path=tuple(i.mitigation_path),
        node_id=i.node_id,
        passfail="F" in i.mitigation_classes,
    )


@dataclass
class TieredIssues:
    """Fired issues grouped into the honest presentation tiers (raw IssueResult, so a UI can render
    rich cards). The SINGLE source of tiering truth — both build_brief() and the page use this, so
    they can never diverge. See catalogue.Node for the universal/conditional/elevated semantics."""

    site_specific: list  # notable at THIS location (non-universal, or a universal node elevated by severity)
    standard: list  # apply to essentially every rural one-off here (universal, not elevated)
    checks: list  # depend on a layer we can't read / site features we can't see (flood + bats)
    access: object | None  # the road node (its own access/entrance section), or None


def tier_issues(result: SitingResult) -> TieredIssues:
    """Group result.fired into tiers (pure, deterministic — catalogue order preserved)."""
    fired = result.fired
    checks = [i for i in fired if i.data_status == "deep_link_only" or i.conditional]
    rest = [
        i for i in fired if i.node_id != "road_sightlines" and i.data_status != "deep_link_only" and not i.conditional
    ]
    standard = [i for i in rest if i.universal and not i.elevated]
    site_specific = [i for i in rest if not (i.universal and not i.elevated)]
    access = next((i for i in result.issues if i.node_id == "road_sightlines" and i.fired), None)
    return TieredIssues(site_specific=site_specific, standard=standard, checks=checks, access=access)


def build_brief(result: SitingResult, terrain=None) -> Brief:
    # TIERING (restores site-specific signal without suppressing anything — see catalogue.Node):
    #  - CHECKS the user must confirm: deep-link layers we can't read (flood) + conditional nodes whose
    #    binding trigger is a site feature we can't see (bats: trees/old structures/watercourses). Never
    #    presented as confirmed constraints (the over-flag bug was asserting these on every site).
    #  - STANDARD requirements: universal nodes that apply to essentially every rural one-off (AA
    #    screening, rural-need test, on-site wastewater, surface-water, landscaping, BER) — grouped so
    #    they don't drown the real signal; a node a trigger ELEVATED by severity drops out of here.
    #  - SITE-SPECIFIC: what is actually notable about THIS location (in/near a European site, on peat,
    #    a monument, a National Park, near a protected structure, a sensitive landscape) + any elevated
    #    universal (e.g. septic on extreme ground). The road has its own ACCESS section.
    t = tier_issues(result)  # single source of tiering truth (shared with the page)
    hard = [_item(i) for i in t.site_specific if "F" in i.mitigation_classes]
    shaping = [_item(i) for i in t.site_specific if "F" not in i.mitigation_classes and "D" in i.mitigation_classes]
    # standard tier ordered pass/fail-first so the rural-housing-need gate leads, not the boilerplate
    obligations = [_item(i) for i in sorted(t.standard, key=lambda x: "F" not in x.mitigation_classes)]
    to_verify = [_item(i) for i in t.checks]

    # dedicated ACCESS & ENTRANCE section (road node, incl. the junction/crossroads finding)
    access: dict[str, Any] = {"applies": False}
    road = next((i for i in result.issues if i.node_id == "road_sightlines"), None)
    if road and road.fired:
        d = road.detail
        access = {
            "applies": True,
            "road_class": d.get("road_class", ""),
            "maxspeed": d.get("maxspeed", ""),
            "is_national": bool(d.get("is_national")),
            "sightline": road_sightline_line(d),
            "junction_m": d.get("junction_m", ""),
            "junction_note": d.get("junction_note", "").strip(),
            "summary": road.flag,
            "action": road.mitigates,
            "path": tuple(road.mitigation_path),
            "reports": [c.document for c in road.rule.checklist] if road.rule else [],
            "engage": list(road.engage),
        }

    n_site = len(hard) + len(shaping)  # site-specific constraints (the real signal)
    nF = len(hard)
    if result.excluded:
        sites = "; ".join(f"{e.site_name} ({e.designation})" for e in result.exclusions)
        headline = (
            f"EXCLUDED — this point lies inside {sites}. Ordinary development is presumed "
            "against on this statutorily protected land; it may still be possible only via "
            "the narrow statutory route below. (A fact about the designation, not the "
            "planning decision, which remains the authority's.)"
        )
    else:
        tightness = (
            "tight — several site-specific constraints stack"
            if nF >= 3
            else "moderate"
            if n_site >= 1
            else "no site-specific hard constraints surfaced"
        )
        n_std = len(obligations)
        headline = (
            f"{n_site} site-specific constraint(s) here ({nF} hard / pass-fail); the constraint box is "
            f"{tightness}. Plus {n_std} standard requirement(s) every rural one-off addresses, and "
            f"{len(to_verify)} check(s) to confirm yourself. (Listing what applies — not a grant/refuse view.)"
        )

    site = {
        "lat": result.lat,
        "lon": result.lon,
        "dev_type": result.dev_type,
        "council": result.council.council_name or result.council.authority,
        "council_slug": result.council.slug,
    }
    if terrain is not None and getattr(terrain, "ok", False):
        site["elevation_m"] = terrain.elevation_m
        site["slope_deg"] = terrain.slope_deg

    return Brief(
        site=site,
        headline=headline,
        exclusions=list(result.exclusions),
        hard_constraints=hard,
        shaping_constraints=shaping,
        access=access,
        obligations=obligations,
        to_verify=to_verify,
        required_reports=result.likely_rfi_reports,
        rfi_note=RFI_NOTE,
        not_assessed=result.missing_layers,
        disclaimer=result.disclaimer,
    )


def brief_text(result: SitingResult, terrain=None) -> str:
    """Plain-text rendering of the brief (deterministic)."""
    b = build_brief(result, terrain)
    out = [f"SITE BRIEF — {b.site['council']} | {b.site['lat']},{b.site['lon']} | {b.site['dev_type']}", b.headline, ""]
    if b.exclusions:
        out.append("⛔ EXCLUDED — STATUTORY PROTECTED LAND (presumption against development):")
        for e in b.exclusions:
            out.append(f"  - inside {e.site_name} ({e.designation})")
            if getattr(e, "mitigation", ""):
                out.append(f"      possible route: {e.mitigation}")
        out.append("")
    if b.hard_constraints:
        out.append("SITE-SPECIFIC HARD CONSTRAINTS (pass/fail, notable at THIS location):")
        for it in b.hard_constraints:
            out.append(f"  - {it.title}: {it.action}")
            if it.path:
                _render_path(list(it.path), out, indent="      ")
    if b.access.get("applies"):
        out += ["", "ACCESS & ENTRANCE:", f"  road: {b.access['road_class']} (speed {b.access['maxspeed']})"]
        if b.access.get("sightline"):
            out.append(f"  sightline: {b.access['sightline']}")
        if b.access.get("junction_note"):
            out.append(f"  junction: {b.access['junction_note']}")
        out.append(f"  action: {b.access['action']}")
        if b.access.get("path"):
            _render_path(list(b.access["path"]), out, indent="  ")
    if b.shaping_constraints:
        out += ["", "SITE-SPECIFIC SHAPING CONSTRAINTS:"]
        for it in b.shaping_constraints:
            out.append(f"  - {it.title}: {it.action}")
            if it.path:
                _render_path(list(it.path), out, indent="      ")
    if b.obligations:
        out += ["", "STANDARD REQUIREMENTS (apply to essentially every rural one-off here):"]
        out += [f"  - {'[pass/fail] ' if it.passfail else ''}{it.title}" for it in b.obligations]
    if b.to_verify:
        out += ["", "CHECKS TO CONFIRM YOURSELF (depend on your site / a layer we can't read):"]
        for it in b.to_verify:
            out.append(f"  - {it.title}: {it.why}")
    out += ["", "REQUIRED REPORTS (likely RFI):"]
    out += [f"  - {r}" for r in b.required_reports]
    out += ["", b.rfi_note]
    if b.not_assessed:
        out += ["", "Not assessed (layer pending): " + ", ".join(b.not_assessed)]
    out += ["", b.disclaimer]
    return "\n".join(out)
