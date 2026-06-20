"""Tests for the siting catalogue loader + per-council rule resolver (no network)."""

from __future__ import annotations

import pytest

from dail_tracker_core.siting import rulebook
from dail_tracker_core.siting.catalogue import load_catalogue

GALWAY_CO = "galway_county_council"
GALWAY_CITY = "galway_city_council"


# ── catalogue ─────────────────────────────────────────────────────────────────


def test_catalogue_loads_and_validates():
    cat = load_catalogue()
    assert len(cat.nodes) >= 10
    # every node validated in _validate(); spot-check known ids exist
    ids = {n.id for n in cat.nodes}
    assert {"european_site", "septic_groundwater", "rural_need_zoning", "landscape_siting"} <= ids


def test_node_source_layers_resolve_to_glossary():
    cat = load_catalogue()
    for n in cat.nodes:
        for sl in n.source_layers:
            assert sl in cat.source_layers


def test_mitigation_classes_parsed_from_ranges():
    cat = load_catalogue()
    eur = cat.node("european_site")  # mitigation_class "D->F"
    assert eur.mitigation_classes == frozenset({"D", "F"})
    flood = cat.node("floodplain")  # "F (Zone A) / D (Zone B)"
    assert flood.mitigation_classes == frozenset({"D", "F"})


def test_applies_to_filtering():
    cat = load_catalogue()
    septic = cat.node("septic_groundwater")  # applies_to [one_off_house]
    assert septic.applies("one_off_house")
    assert not septic.applies("commercial")
    eur = cat.node("european_site")  # applies_to [all]
    assert eur.applies("commercial")


def test_disclaimer_present():
    assert "advice" in load_catalogue().disclaimer.lower()


# ── council directory resolution ───────────────────────────────────────────────


def test_find_council_dir_across_subdirs():
    assert rulebook.find_council_dir(GALWAY_CO) is not None  # county_councils/
    assert rulebook.find_council_dir(GALWAY_CITY) is not None  # city_councils/
    assert rulebook.find_council_dir("limerick_city_and_county_council") is not None
    assert rulebook.find_council_dir("not_a_real_council") is None


# ── required_assessments.md parsing ────────────────────────────────────────────


def test_parse_checklist_galway_county():
    chk = rulebook.parse_required_assessments(GALWAY_CO)
    assert len(chk) >= 20
    # #12 = Appropriate Assessment screening / NIS, tied to SAC/SPA
    aa = chk[12]
    assert "Appropriate Assessment" in aa.document
    assert "SAC" in aa.layer or "SPA" in aa.layer
    # #25 = Site Suitability (septic)
    assert "Site Suitability" in chk[25].document


# ── dm_standards.md verbatim extraction ────────────────────────────────────────


def test_parse_dm_standard_verbatim_galway_county():
    dm = rulebook.parse_dm_standards(GALWAY_CO)
    assert 51 in dm and 9 in dm and 8 in dm
    s51 = dm[51]
    assert "Environmental Assessments" in s51.title
    # verbatim body must carry the actual AA wording, not a paraphrase
    assert "Appropriate Assessment" in s51.text
    assert "Habitats Directive" in s51.text


def test_dm_standard_bodies_do_not_bleed_into_next():
    dm = rulebook.parse_dm_standards(GALWAY_CO)
    # DM Standard 8 text must not contain DM Standard 9's heading
    assert "DM Standard 9" not in dm[8].text


# ── resolve(): node -> verbatim rule for the council in force ───────────────────


def test_resolve_european_site_galway_county():
    r = rulebook.resolve(GALWAY_CO, "european_site")
    assert r.council_name and "Galway" in r.council_name
    # checklist #12/#13/#14 + DM Standard 51 per the catalogue rule_ref
    assert any(c.number == 12 for c in r.checklist)
    assert any(d.number == 51 for d in r.dm_standards)
    assert "Habitats Directive" in r.dm_standards[0].text
    assert not r.missing  # all refs resolved for Galway


def test_resolve_applies_council_override_numbering():
    # galway_county override puts road_sightlines at DM Std [27, 29]
    r = rulebook.resolve(GALWAY_CO, "road_sightlines")
    nums = {d.number for d in r.dm_standards}
    assert nums <= {27, 29}
    assert r.override  # override dict populated


def test_resolve_city_override_plan_name():
    # galway_city override sets plan "Galway City DP 2023-2029" on rural_need_zoning
    r = rulebook.resolve(GALWAY_CITY, "rural_need_zoning")
    assert "2023-2029" in r.plan_name


def test_resolve_unknown_council_degrades_gracefully():
    r = rulebook.resolve("not_a_real_council", "european_site")
    assert r.checklist == () and r.dm_standards == ()
    assert r.missing  # records what could not be resolved, never raises


# ── hardened DM-heading parsing (markdown/bold prefixes, : . – separators) ──────


def test_dm_heading_tolerates_markdown_and_separators():
    for line in ["DM Standard 51: T", "## DM Standard 8 – T", "**DM Standard 5** - T", "DM Std 9. T"]:
        assert rulebook._DM_HEADING.match(line), f"should match: {line!r}"


def test_hardening_did_not_regress_galway_county():
    # the exemplar must still parse fully (70 standards, 26 checklist rows)
    assert len(rulebook.parse_dm_standards(GALWAY_CO)) == 70
    assert len(rulebook.parse_required_assessments(GALWAY_CO)) == 26
    assert rulebook.parse_dm_concepts(GALWAY_CO) == {}  # county is numbered, not concept-keyed


# ── concept-keyed councils (plans that don't use "DM Standard N") ───────────────


def test_concept_keyed_dm_resolution_galway_city():
    r = rulebook.resolve(GALWAY_CITY, "rural_need_zoning")
    assert r.dm_standards, "Galway City rural_need should resolve a concept-keyed standard"
    d = r.dm_standards[0]
    assert d.number == 0 and d.source_ref  # concept-keyed marker + the plan citation
    assert "0.2 hectares" in d.text  # VERBATIM Galway City text, not a paraphrase


def test_concept_keyed_checklist_galway_city():
    r = rulebook.resolve(GALWAY_CITY, "european_site")
    docs = " ".join(c.document for c in r.checklist)
    assert "Appropriate Assessment" in docs


def test_concept_checklist_skips_table_header_row():
    chk = rulebook.parse_checklist_concepts(GALWAY_CITY)
    assert "node" not in chk  # the "| node | … |" header must not become a fake node
    assert "aa_screening" in chk


def test_city_honest_about_uncaptured_chapters():
    # archaeology / septic standards live in city-plan chapters that were not captured ->
    # reported as missing, never fabricated (no-inference contract)
    r = rulebook.resolve(GALWAY_CITY, "monument")
    assert not r.dm_standards
    assert r.missing


# ── Dublin region: all four LAs concept-keyed and resolving verbatim ─────────────

DUBLIN_LAS = [
    "dublin_city_council",
    "fingal_county_council",
    "south_dublin_county_council",
    "dun_laoghaire_rathdown_county_council",
]


@pytest.mark.parametrize("slug", DUBLIN_LAS)
def test_dublin_la_concept_keyed_and_clean(slug):
    dm = rulebook.parse_dm_concepts(slug)
    chk = rulebook.parse_checklist_concepts(slug)
    assert dm, f"{slug} has no concept DM standards"
    assert chk, f"{slug} has no concept checklist"
    assert "node" not in dm and "node" not in chk  # header row never leaks as a node
    # the core environmental node every Dublin plan covers must resolve verbatim + cited
    r = rulebook.resolve(slug, "european_site")
    assert r.dm_standards or r.checklist
    if r.dm_standards:
        d = r.dm_standards[0]
        assert d.number == 0 and d.source_ref  # concept-keyed marker + plan citation


def test_dublin_city_urban_gaps_are_honest():
    # Dublin City is fully sewered + has no rural one-off policy -> these stay missing, not faked
    for nid in ("septic_groundwater", "rural_need_zoning"):
        r = rulebook.resolve("dublin_city_council", nid)
        assert not r.dm_standards and not r.checklist
        assert r.missing
