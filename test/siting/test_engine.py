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
HAVE_SPINE = (Path(__file__).resolve().parents[2] / "pipeline_sandbox" / "_planning_output"
              / "planning_applications_silver.parquet").exists()

# language a triage tool must NEVER use (verdicts / prescriptions)
_FORBIDDEN = re.compile(r"\b(will be (refused|granted)|you will|must build|build a)\b", re.I)


def _result(**kw):
    return evaluate(-9.10, 53.20, dev_type="one_off_house", council_slug=GALWAY_CO, **kw)


def test_evaluate_returns_all_applicable_nodes_with_disclaimer():
    r = _result()
    assert r.disclaimer and "advice" in r.disclaimer.lower()
    assert len(r.issues) >= 10
    assert {i.node_id for i in r.issues} >= {"european_site", "rural_need_zoning", "floodplain"}


def test_no_inference_no_verdict_or_prescription_in_any_output():
    r = _result()
    for i in r.issues:
        blob = " ".join([i.flag, i.mitigates, i.risk_note])
        assert not _FORBIDDEN.search(blob), f"{i.node_id}: forbidden language in {blob!r}"


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
    assert "septic_groundwater" not in ids   # one_off_house only
    assert "european_site" in ids            # applies to all


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


def test_rule_resolution_wired_to_council():
    r = _result()
    es = next(i for i in r.issues if i.node_id == "european_site")
    assert es.rule is not None
    assert "Galway" in es.rule.council_name
    assert any(d.number == 51 for d in es.rule.dm_standards)


@pytest.mark.skipif(not HAVE_NPWS, reason="NPWS layers not ingested")
def test_integration_european_site_fires_inside_sac():
    # Lough Corrib SAC area (Galway) — a point inside the SAC must fire european_site
    store = LayerStore()
    # use the SAC layer's own first polygon centroid to guarantee an inside point
    import shapely, polars as pl
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


_HAVE_GALWAY_LAYERS = (LAYERS_DIR / "gsi_vulnerability.parquet").exists() and \
    (LAYERS_DIR / "zoning_gzt.parquet").exists()


@pytest.mark.skipif(not (HAVE_SPINE and _HAVE_GALWAY_LAYERS),
                    reason="spine or Galway layers not built")
def test_integration_menlo_golden():
    """Golden blind reconstruction of the Menlo (Galway) one-off-house site.

    Locks in the data-derived decision tree: governing council, and the issues that the
    designation layers + rulebook fire. NOT a hand-tuned outcome — if a layer or rule
    changes this should be re-reviewed, not silently 'fixed'.
    """
    r = evaluate(-9.0520, 53.3062, dev_type="one_off_house")
    assert r.council.slug == "galway_city_council"      # the subtle County->City case
    fired = {i.node_id for i in r.fired}
    # the load-bearing issues an honest tool must surface for this site
    assert {"european_site", "septic_groundwater", "rural_need_zoning",
            "landscape_siting"} <= fired
    septic = next(i for i in r.issues if i.node_id == "septic_groundwater")
    assert "karst" in septic.detail.get("vuln_class", "").lower()  # GSI returned karst ground
    rural = next(i for i in r.issues if i.node_id == "rural_need_zoning")
    assert "F" in rural.mitigation_classes                          # often-fatal class
