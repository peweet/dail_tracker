"""Regression tests for the over-flagging fix (2026-06-22).

The engine fired ~7 issues / 3 "hard" pass-fail constraints on essentially every rural site, so
the output gave no site-specific signal ("everywhere is a floodplain or bat survey"). The fix
TIERS fired issues honestly — suppressing nothing — into: site-specific constraints (notable at
THIS location), standard requirements (apply to any rural one-off), and checks (depend on site
features / a layer we can't read). These tests lock that tiering in.
"""

from dail_tracker_core.siting.brief import build_brief
from dail_tracker_core.siting.engine import evaluate

GALWAY_CO = "galway_county_council"
MENLO = (-9.0520, 53.3062)  # near Lough Corrib SAC (not inside), karst ground, agric zoning


def _brief(lon, lat, **kw):
    return build_brief(evaluate(lon, lat, dev_type="one_off_house", council_slug=GALWAY_CO, **kw))


def test_universal_gates_are_standard_not_hard():
    """AA screening, landscaping, BER, rural-need are 'every rural one-off' items -> standard tier,
    never the site-specific hard/shaping tiers (that mis-weighting was the over-flagging)."""
    b = _brief(*MENLO)
    std = {o.node_id for o in b.obligations}
    hard = {h.node_id for h in b.hard_constraints}
    shaping = {s.node_id for s in b.shaping_constraints}
    assert {"aa_screening", "rural_need_zoning", "landscaping", "energy_cert"} <= std
    assert not ({"aa_screening", "rural_need_zoning", "landscaping", "energy_cert"} & (hard | shaping))


def test_rural_need_still_flagged_passfail_in_standard_tier():
    """rural-need is universal (so standard tier) BUT pass/fail — the standard tier must keep that
    signal, and order pass/fail items first, so the key rural gate isn't buried as boilerplate."""
    b = _brief(*MENLO)
    rn = next(o for o in b.obligations if o.node_id == "rural_need_zoning")
    assert rn.passfail is True
    assert b.obligations[0].passfail is True  # pass/fail items lead the standard tier


def test_european_site_near_is_shaping_not_hard():
    """Being NEAR (within ~2 km of) a European site triggers AA/NIS (mitigable, shaping) — it is not
    a pass/fail hard constraint. The pass/fail INSIDE case is the separate statutory exclusion mask."""
    b = _brief(*MENLO)
    assert "european_site" in {s.node_id for s in b.shaping_constraints}
    assert "european_site" not in {h.node_id for h in b.hard_constraints}


def test_bats_is_a_check_not_a_constraint():
    """A bat survey depends on site features we can't read (trees / old structures / watercourses),
    so bats is a CHECK to confirm, never a confirmed shaping/hard constraint."""
    b = _brief(*MENLO)
    assert "bats" in {v.node_id for v in b.to_verify}
    assert "bats" not in {i.node_id for i in (b.hard_constraints + b.shaping_constraints)}


def test_septic_elevates_to_site_specific_on_karst():
    """On extreme-vulnerability / karst ground septic is a genuine viability risk, so it is PROMOTED
    out of the standard tier into the site-specific tier (and the issue carries elevated=True)."""
    r = evaluate(*MENLO, dev_type="one_off_house", council_slug=GALWAY_CO)
    sep = next(i for i in r.issues if i.node_id == "septic_groundwater")
    assert sep.fired and sep.elevated is True
    b = build_brief(r)
    sitespec = {i.node_id for i in (b.hard_constraints + b.shaping_constraints)}
    assert "septic_groundwater" in sitespec
    assert "septic_groundwater" not in {o.node_id for o in b.obligations}


def test_headline_counts_site_specific_not_all_fired():
    """The headline must report the (small) site-specific count, not the ~7 total fired issues."""
    r = evaluate(*MENLO, dev_type="one_off_house", council_slug=GALWAY_CO)
    b = build_brief(r)
    n_site = len(b.hard_constraints) + len(b.shaping_constraints)
    assert f"{n_site} site-specific constraint" in b.headline
    assert "standard requirement" in b.headline  # the standard tier is named, not hidden
