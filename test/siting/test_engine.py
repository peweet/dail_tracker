"""Engine tests — trigger wiring, the no-inference guarantee, and layer honesty.

Most tests pass council_slug explicitly so they don't load the 495k spine; the council
resolver itself is covered by one integration test (skipped if the spine isn't built).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from dail_tracker_core.siting import engine
from dail_tracker_core.siting.engine import _to_3857, evaluate
from dail_tracker_core.siting.layers import LayerStore

GALWAY_CO = "galway_county_council"
LAYERS_DIR = LayerStore().dir
HAVE_NPWS = (LAYERS_DIR / "npws_sac.parquet").exists()
HAVE_SPINE = (
    Path(__file__).resolve().parents[2]
    / "pipeline_sandbox"
    / "_planning_output"
    / "planning_applications_silver.parquet"
).exists()

# language a triage tool must NEVER use (verdicts / prescriptions)
_FORBIDDEN = re.compile(r"\b(will be (refused|granted)|you will|must build|build a)\b", re.I)


def _result(**kw):
    return evaluate(-9.10, 53.20, dev_type="one_off_house", council_slug=GALWAY_CO, **kw)


def test_evaluate_returns_all_applicable_nodes_with_disclaimer():
    r = _result()
    assert r.disclaimer and "advice" in r.disclaimer.lower()
    assert len(r.issues) >= 10
    assert {i.node_id for i in r.issues} >= {"european_site", "rural_need_zoning", "floodplain"}


def _flatten_path(steps) -> str:
    parts: list[str] = []
    for step in steps or ():
        parts.append(str(step.get("do", "")))
        for br in step.get("findings") or ():
            parts.append(str(br.get("if", "")))
            parts.append(_flatten_path(br.get("then")))
    return " ".join(parts)


def test_no_inference_no_verdict_or_prescription_in_any_output():
    r = _result()
    for i in r.issues:
        blob = " ".join([i.flag, i.mitigates, i.risk_note, _flatten_path(i.mitigation_path)])
        assert not _FORBIDDEN.search(blob), f"{i.node_id}: forbidden language in {blob!r}"
    # the exclusion mask is fact-based too: it must not assert a refusal/grant verdict
    for e in r.exclusions:
        blob = " ".join([e.designation, e.site_name, e.mitigation])
        assert not _FORBIDDEN.search(blob), f"exclusion {e.layer}: forbidden language in {blob!r}"


def test_risk_language_is_likely_not_will():
    # european_site risk_note must hedge ("likely"/"hard"), never assert refusal
    r = _result()
    es = next(i for i in r.issues if i.node_id == "european_site")
    assert "refused" not in es.flag.lower() or "hard to grant" in es.flag.lower()


def test_missing_layer_is_honest_not_silent_no():
    # septic needs layers we have not ingested -> must be flagged, not reported as "no issue"
    r = _result()
    septic = next(i for i in r.issues if i.node_id == "septic_groundwater")
    if septic.data_status == "layer_missing":
        assert septic.fired is False


def test_applies_to_filters_by_dev_type():
    r_ext = evaluate(-9.10, 53.20, dev_type="extension", council_slug=GALWAY_CO)
    ids = {i.node_id for i in r_ext.issues}
    assert "septic_groundwater" not in ids  # one_off_house only
    assert "european_site" in ids  # applies to all


def test_floodplain_always_deep_links_never_ingests():
    r = _result()
    flood = next(i for i in r.issues if i.node_id == "floodplain")
    assert flood.data_status == "deep_link_only"
    assert flood.extra.get("flood_link", "").startswith("https://www.floodinfo.ie/map/floodmaps/")


def test_to_3857_galway_sane():
    x, y = _to_3857(-9.05, 53.30)
    # Galway is west (negative easting) and north (~7.0e6 northing) in Web Mercator
    assert -1_020_000 < x < -1_000_000
    assert 7_000_000 < y < 7_080_000


def test_engine_is_deterministic():
    """Same input -> identical output, every run (no memory/session/randomness in the loop)."""
    kw = dict(dev_type="one_off_house", council_slug=GALWAY_CO)
    a = evaluate(-9.0376579, 53.3150837, **kw)
    b = evaluate(-9.0376579, 53.3150837, **kw)

    def sig(r):
        return [(i.node_id, i.fired, i.data_status, i.flag) for i in r.issues]

    assert sig(a) == sig(b)
    assert a.likely_rfi_reports == b.likely_rfi_reports
    from dail_tracker_core.siting.brief import brief_text

    assert brief_text(a) == brief_text(b)


def test_mitigation_paths_authored_and_valid():
    """The four cascade nodes carry a well-formed mitigation_path (validated at load time)."""
    from dail_tracker_core.siting.catalogue import load_catalogue

    cat = load_catalogue()
    for nid in ("bats", "septic_groundwater", "floodplain", "road_sightlines"):
        assert cat.node(nid).mitigation_path, f"{nid} has no mitigation_path"
    # nodes without a cascade stay empty (unchanged behaviour)
    assert cat.node("energy_cert").mitigation_path == ()


def test_brief_renders_cascade_tree_with_outcomes():
    """The renderer emits a numbered spine, if/then branches, and the P/D/F-style leaf tags."""
    from dail_tracker_core.siting.brief import _render_path
    from dail_tracker_core.siting.catalogue import load_catalogue

    cat = load_catalogue()
    lines: list[str] = []
    _render_path(list(cat.node("septic_groundwater").mitigation_path), lines)
    text = "\n".join(lines)
    assert "1. Trial hole" in text  # numbered spine step
    assert "if percolation passes" in text.lower()  # branch
    assert "[often fatal]" in text  # the karst-fail leaf carries the F-class tag
    assert "[mitigable]" in text


def test_road_sightline_is_one_key_number():
    """One assumed/posted speed -> one visibility figure, derived from the OSM road class."""
    from dail_tracker_core.siting.brief import road_sightline_line

    assert "~70 m" in road_sightline_line({"road_class": "track", "maxspeed": "unposted"})
    assert "~160 m" in road_sightline_line({"road_class": "tertiary", "maxspeed": "unposted"})
    assert "~90 m" in road_sightline_line({"road_class": "tertiary", "maxspeed": "60"})  # posted wins
    nat = road_sightline_line({"is_national": "national road", "road_class": "primary"})
    assert "farm families" in nat


@pytest.mark.skipif(not (LAYERS_DIR / "osm_roads.parquet").exists(), reason="osm_roads layer not built")
def test_brief_access_section_carries_junction():
    from dail_tracker_core.siting.brief import build_brief

    r = evaluate(-9.0376579, 53.3150837, dev_type="one_off_house", council_slug=GALWAY_CO)
    b = build_brief(r)
    assert b.access["applies"]
    assert "STAGGERED" in b.access["junction_note"]
    assert 0 < b.access["junction_m"] <= 100


def test_attribute_always_on_for_one_off():
    r = _result()  # one_off_house
    ids = {i.node_id for i in r.fired}
    assert {"landscaping", "energy_cert"} <= ids  # universal #4/#18 fire
    all_ids = {i.node_id for i in r.issues}
    assert "design_statement" not in all_ids  # scale node doesn't apply to a one-off
    assert "mobility_plan" not in all_ids


def test_attribute_scale_gated_for_multi_unit():
    r = evaluate(-9.10, 53.20, dev_type="multi_unit", num_units=20, council_slug=GALWAY_CO)
    ids = {i.node_id for i in r.fired}
    assert {"design_statement", "mobility_plan", "climate_statement", "waste_management"} <= ids
    assert "eia" not in ids  # 20 units is below the Schedule-5 threshold


def test_eia_fires_only_at_large_scale():
    r = evaluate(-9.10, 53.20, dev_type="multi_unit", num_units=600, council_slug=GALWAY_CO)
    assert "eia" in {i.node_id for i in r.fired}


# ── 2026-06-16 polish batch: national parks, monument identity, peat, septic sewered-check ──


@pytest.mark.skipif(not (LAYERS_DIR / "national_parks.parquet").exists(), reason="national_parks layer not built")
def test_national_park_fires_inside_park():
    import polars as pl
    import shapely

    df = pl.read_parquet(LAYERS_DIR / "national_parks.parquet")
    pt = shapely.from_wkb(df["wkb"][0]).representative_point()  # any park interior
    r = evaluate(pt.x, pt.y, dev_type="one_off_house", council_slug=GALWAY_CO)
    npk = next(i for i in r.issues if i.node_id == "national_park")
    assert npk.fired and "F" in npk.mitigation_classes
    assert "National Park" in npk.flag


@pytest.mark.skipif(not (LAYERS_DIR / "smr_points.parquet").exists(), reason="smr_points layer not built")
def test_monument_names_class_inside_zone():
    import polars as pl
    import shapely

    zdf = pl.read_parquet(LAYERS_DIR / "smr_zone.parquet")
    pt = None
    for w in zdf["wkb"].to_list():
        rp = shapely.from_wkb(w).representative_point()
        if -9.25 < rp.x < -8.90 and 53.20 < rp.y < 53.42:  # within the Galway smr_points bbox
            pt = rp
            break
    assert pt is not None
    r = evaluate(pt.x, pt.y, dev_type="one_off_house", council_slug=GALWAY_CO)
    mon = next(i for i in r.issues if i.node_id == "monument")
    assert mon.fired
    assert mon.detail.get("monument_class") and mon.detail["monument_class"] != "a recorded monument"


@pytest.mark.skipif(not (LAYERS_DIR / "gsi_quaternary.parquet").exists(), reason="gsi_quaternary layer not built")
def test_peat_fires_on_blanket_bog():
    r = evaluate(-9.8812, 53.5309, dev_type="one_off_house", council_slug=GALWAY_CO)  # Connemara
    pb = next(i for i in r.issues if i.node_id == "peat_bog")
    assert pb.fired and pb.data_status == "ok"
    assert "peat" in pb.detail.get("peat_type", "").lower()


@pytest.mark.skipif(not (LAYERS_DIR / "gsi_vulnerability.parquet").exists(), reason="gsi_vulnerability layer not built")
def test_septic_fires_on_bad_ground_and_states_sewer_assumption():
    # No reliable sewer layer -> we do NOT guess sewered/not; septic fires on high-vuln ground
    # for a one-off and STATES the assumption (no-inference). Rural karst point must fire.
    rural = evaluate(-9.0376579, 53.3150837, dev_type="one_off_house", council_slug=GALWAY_CO)
    sep = next(i for i in rural.issues if i.node_id == "septic_groundwater")
    assert sep.fired is True
    assert "public sewer" in sep.flag.lower()  # the assumption is stated, not a fabricated verdict


@pytest.mark.skipif(not (LAYERS_DIR / "gsi_vulnerability.parquet").exists(), reason="gsi_vulnerability layer not built")
def test_septic_honest_status_outside_galway():
    # GSI vulnerability is now ingested NATIONALLY (bbox_subset=null), so a Cork point is in-extent
    # and gets a real assessment (ok/fired), never a silent layer_missing. The honesty contract is
    # the SAME either way: status is never a fabricated "no issue" — it is layer_missing only when
    # the point is genuinely outside every ingested extent.
    cork = evaluate(-8.90, 51.90, dev_type="one_off_house", council_slug="cork_county_council")
    sep = next(i for i in cork.issues if i.node_id == "septic_groundwater")
    assert sep.data_status in ("layer_missing", "ok")
    if sep.data_status == "layer_missing":
        assert sep.fired is False


# ── hard-exclusion mask + monument-on-point + flood-is-a-check (this session) ──


@pytest.mark.skipif(not HAVE_NPWS, reason="NPWS layers not ingested")
def test_exclusion_mask_fires_inside_sac_with_mitigation_route():
    """A point inside an SAC polygon is EXCLUDED — fact-based, with the narrow Art.6 route shown."""
    import polars as pl
    import shapely

    df = pl.read_parquet(LAYERS_DIR / "npws_sac.parquet")
    pt = shapely.from_wkb(df["wkb"][0]).representative_point()
    r = evaluate(pt.x, pt.y, council_slug=GALWAY_CO)
    assert r.excluded and r.exclusions
    e = r.exclusions[0]
    assert "SAC" in e.designation and e.site_name
    assert "Appropriate Assessment" in e.mitigation  # not an absolute "cannot build"
    assert "Art. 6" in e.mitigation


@pytest.mark.skipif(not HAVE_NPWS, reason="NPWS layers not ingested")
def test_exclusion_mask_empty_when_not_in_a_designation():
    # Menlo is NEAR (european_site fires) but NOT inside a designation polygon -> not excluded
    r = evaluate(-9.0520, 53.3062, dev_type="one_off_house")
    assert r.excluded is False and r.exclusions == []


@pytest.mark.skipif(not (LAYERS_DIR / "smr_points.parquet").exists(), reason="smr_points layer not built")
def test_monument_fires_when_point_is_on_a_recorded_monument():
    """The user's scenario: an XY placed ON a monument must fire (zone OR the ~60 m point fallback)."""
    import polars as pl
    import shapely

    pdf = pl.read_parquet(LAYERS_DIR / "smr_points.parquet")
    g = shapely.from_wkb(pdf["wkb"][0])  # a recorded-monument point in the Galway extent
    fired, detail, status = engine._monument(LayerStore(), g.x, g.y, "one_off_house", GALWAY_CO)
    assert fired is True and status == "ok"
    assert detail.get("monument_class") and detail["monument_class"] != "a recorded monument"


def test_monument_zone_radius_is_data_derived_not_invented():
    # the fallback radius is the measured modal Zone-of-Notification radius (~60 m), not a guess
    assert engine.MONUMENT_ZONE_RADIUS_M == 60.0


def test_flood_is_a_check_not_a_hard_constraint():
    """Flood is deep_link_only (we can't read OPW) -> a 'verify yourself' item, never a hard
    constraint asserted on every site (the over-flagging the user reported)."""
    from dail_tracker_core.siting.brief import build_brief

    r = evaluate(-9.0520, 53.3062, dev_type="one_off_house", council_slug=GALWAY_CO)
    b = build_brief(r)
    assert any(i.title.lower().startswith("flood") for i in b.to_verify)
    assert not any("flood" in it.title.lower() for it in b.hard_constraints)
    flood = next(i for i in r.issues if i.node_id == "floodplain")
    assert flood.data_status == "deep_link_only"
    assert "check" in flood.flag.lower()  # framed as a check, not "your site IS in a flood zone"


def test_rule_resolution_wired_to_council():
    r = _result()
    es = next(i for i in r.issues if i.node_id == "european_site")
    assert es.rule is not None
    assert "Galway" in es.rule.council_name
    assert any(d.number == 51 for d in es.rule.dm_standards)


def test_rfi_reports_deduplicated_across_rephrasings():
    """Re-phrasings of the same report (per-node checklist strings) must not stack in the RFI
    list: a string whose every component is already covered by a kept one is dropped whole.
    Kept strings stay verbatim (first occurrence wins)."""
    docs = [
        "Appropriate Assessment (AA) screening / Natura Impact Statement",
        "Ecological Impact Assessment (EcIA)",
        "Appropriate Assessment / Natura Impact Statement + Ecological Impact Assessment",
        "Flood Risk Assessment (FRA) + Justification Test",
    ]
    out = engine._dedupe_reports(docs)
    assert out == [
        "Appropriate Assessment (AA) screening / Natura Impact Statement",
        "Ecological Impact Assessment (EcIA)",
        "Flood Risk Assessment (FRA) + Justification Test",
    ]
    # a genuinely new report is never dropped
    assert engine._dedupe_reports(["Natura Impact Statement", "Traffic and Transport Assessment (TTA)"]) == [
        "Natura Impact Statement",
        "Traffic and Transport Assessment (TTA)",
    ]


@pytest.mark.skipif(
    not (LAYERS_DIR / "epa_uww_agglomeration.parquet").exists(), reason="EPA agglomeration layer not built"
)
def test_septic_not_fired_inside_sewered_agglomeration():
    """Inside an EPA UWWT agglomeration boundary a public collection network serves the area, so
    the on-site-wastewater node does not apply — the antecedent is now READ, not assumed."""
    import polars as pl
    import shapely

    df = pl.read_parquet(LAYERS_DIR / "epa_uww_agglomeration.parquet")
    pt = shapely.from_wkb(df["wkb"][0]).representative_point()
    fired, detail, status = engine._septic(LayerStore(), pt.x, pt.y, "one_off_house", GALWAY_CO)
    assert fired is False and status == "ok"
    assert "sewered" in detail.get("sewer_note", "").lower()


@pytest.mark.skipif(
    not (LAYERS_DIR / "epa_uww_agglomeration.parquet").exists(), reason="EPA agglomeration layer not built"
)
def test_septic_states_no_sewer_outside_agglomeration():
    # Killoughter (rural karst, outside any agglomeration): fires AND states the checked absence
    r = evaluate(-9.0376579, 53.3150837, dev_type="one_off_house", council_slug=GALWAY_CO)
    sep = next(i for i in r.issues if i.node_id == "septic_groundwater")
    assert sep.fired is True
    assert "no epa-mapped public-sewer agglomeration" in sep.flag.lower()


@pytest.mark.skipif(not (LAYERS_DIR / "epa_wfd_lakes.parquet").exists(), reason="EPA lakes layer not built")
def test_open_water_fires_inside_lake_and_not_on_land():
    import polars as pl
    import shapely

    df = pl.read_parquet(LAYERS_DIR / "epa_wfd_lakes.parquet")
    pt = shapely.from_wkb(df["wkb"][0]).representative_point()
    r = evaluate(pt.x, pt.y, dev_type="one_off_house", council_slug=GALWAY_CO)
    ow = next(i for i in r.issues if i.node_id == "open_water")
    assert ow.fired and "F" in ow.mitigation_classes
    assert "lake waterbody" in ow.flag
    # dry land (Menlo) must not fire it
    dry = evaluate(-9.0520, 53.3062, dev_type="one_off_house", council_slug=GALWAY_CO)
    assert next(i for i in dry.issues if i.node_id == "open_water").fired is False


@pytest.mark.skipif(not (LAYERS_DIR / "gsi_vulnerability.parquet").exists(), reason="GSI layer not built")
def test_cork_generalisation_concept_rulebook_and_rfi():
    """Cork (2nd council) resolves its concept-keyed rulebook verbatim + populates the RFI list."""
    r = evaluate(-8.90, 51.90, dev_type="one_off_house", council_slug="cork_county_council")
    rn = next(i for i in r.issues if i.node_id == "rural_need_zoning")
    assert rn.rule and rn.rule.dm_standards  # concept-keyed Cork rule resolves
    d = rn.rule.dm_standards[0]
    assert d.number == 0 and d.source_ref and "Cork" in d.source_ref  # council's own citation
    assert "NPO19" in d.text  # verbatim Cork text
    assert len(r.likely_rfi_reports) >= 3  # RFI populates (was the empty bug)


@pytest.mark.skipif(not HAVE_NPWS, reason="NPWS layers not ingested")
def test_integration_european_site_fires_inside_sac():
    # Lough Corrib SAC area (Galway) — a point inside the SAC must fire european_site
    LayerStore()
    # use the SAC layer's own first polygon centroid to guarantee an inside point
    import polars as pl
    import shapely

    df = pl.read_parquet(LAYERS_DIR / "npws_sac.parquet")
    g = shapely.from_wkb(df["wkb"][0])
    pt = g.representative_point()
    r = evaluate(pt.x, pt.y, council_slug=GALWAY_CO)
    es = next(i for i in r.issues if i.node_id == "european_site")
    assert es.fired and es.detail.get("relation") == "inside"


@pytest.mark.skipif(not HAVE_SPINE, reason="application spine not built")
def test_integration_council_resolver_dublin():
    from dail_tracker_core.siting.council import resolve_council

    c = resolve_council(-6.260, 53.349)
    assert c.slug == "dublin_city_council"


_HAVE_GALWAY_LAYERS = (LAYERS_DIR / "gsi_vulnerability.parquet").exists() and (
    LAYERS_DIR / "zoning_gzt.parquet"
).exists()


@pytest.mark.skipif(not (LAYERS_DIR / "osm_roads.parquet").exists(), reason="osm_roads layer not built")
def test_junction_proximity_flag_killoughter():
    """The road node flags a near junction (crossroads / entrance-setback) at Killoughter."""
    r = evaluate(-9.0376579, 53.3150837, dev_type="one_off_house", council_slug=GALWAY_CO)
    road = next(i for i in r.issues if i.node_id == "road_sightlines")
    assert road.fired
    jm = road.detail.get("junction_m")
    assert isinstance(jm, (int, float)) and 0 < jm <= 100  # a junction within 100 m
    assert "STAGGERED" in road.flag and "Road Safety Audit" in road.flag


def test_plan_name_to_slug_distinguishes_city_and_county():
    from dail_tracker_core.siting.council import _plan_name_to_slug

    assert _plan_name_to_slug("Galway City Development Plan 2023 - 2029") == "galway_city_council"
    assert _plan_name_to_slug("Galway County Development Plan 2022-2028") == "galway_county_council"


def test_plan_name_to_slug_handles_messy_national_names():
    from dail_tracker_core.siting.council import _plan_name_to_slug

    # real zoning_gzt PLAN_NAMEs (national layer) — messy but resolvable
    assert _plan_name_to_slug("The Fingal Development Plan 2023 – 2029") == "fingal_county_council"
    assert _plan_name_to_slug("South Dublin County Development Plan 2022-2028") == "south_dublin_county_council"
    assert _plan_name_to_slug("Cork City Development Plan 2022-2028") == "cork_city_council"
    assert _plan_name_to_slug("Cork County Development Plan 2022") == "cork_county_council"
    # "South Dublin County …" must NOT mis-resolve to Dublin City (token-count + type keyword)
    assert _plan_name_to_slug("South Dublin County Development Plan 2022-2028") != "dublin_city_council"
    # unrecognisable / ambiguous names return None -> caller falls back to nearest-application
    assert _plan_name_to_slug("Development Plan 2022-2028") is None
    assert _plan_name_to_slug("WCCC Development Plan 2022 - 2028") is None


@pytest.mark.skipif(not _HAVE_GALWAY_LAYERS, reason="Galway zoning layer not built")
def test_rural_need_does_not_fire_in_residential_zone():
    """The amenity-substring false-positive fix: a zoned RESIDENTIAL site is not rural-need."""
    from dail_tracker_core.siting.engine import _rural_need

    s = LayerStore()
    fired, _detail, status = _rural_need(s, -9.0857, 53.2607, "one_off_house", "galway_city_council")
    assert status == "ok" and fired is False  # Salthill = R3 Residential


@pytest.mark.skipif(not _HAVE_GALWAY_LAYERS, reason="Galway zoning layer not built")
def test_rural_need_fires_on_agricultural_zone():
    from dail_tracker_core.siting.engine import _rural_need

    s = LayerStore()
    fired, _detail, _status = _rural_need(s, -9.0520, 53.3062, "one_off_house", "galway_city_council")
    assert fired is True  # Menlo = Zoned Agriculture


@pytest.mark.skipif(not _HAVE_GALWAY_LAYERS, reason="Galway zoning layer not built")
def test_council_resolved_via_zoning_gets_city_county_line_right():
    from dail_tracker_core.siting.council import resolve_council

    city = resolve_council(-9.0520, 53.3062)  # Menlo -> City
    assert city.slug == "galway_city_council"
    assert city.resolved_via == "zoning" and city.on_boundary is False
    county = resolve_council(-9.3200, 53.4258)  # Oughterard -> County
    assert county.slug == "galway_county_council" and county.resolved_via == "zoning"


@pytest.mark.skipif(not (HAVE_SPINE and _HAVE_GALWAY_LAYERS), reason="spine or Galway layers not built")
def test_integration_menlo_golden():
    """Golden blind reconstruction of the Menlo (Galway) one-off-house site.

    Locks in the data-derived decision tree: governing council, and the issues that the
    designation layers + rulebook fire. NOT a hand-tuned outcome — if a layer or rule
    changes this should be re-reviewed, not silently 'fixed'.
    """
    r = evaluate(-9.0520, 53.3062, dev_type="one_off_house")
    assert r.council.slug == "galway_city_council"  # the subtle County->City case
    fired = {i.node_id for i in r.fired}
    # the load-bearing issues an honest tool must surface for this site
    assert {"european_site", "septic_groundwater", "rural_need_zoning", "landscape_siting"} <= fired
    septic = next(i for i in r.issues if i.node_id == "septic_groundwater")
    assert "karst" in septic.detail.get("vuln_class", "").lower()  # GSI returned karst ground
    rural = next(i for i in r.issues if i.node_id == "rural_need_zoning")
    assert "F" in rural.mitigation_classes  # often-fatal class
