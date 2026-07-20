"""T3 — the trust-tier weakest-link property suite (doc/SOURCE_CONFIDENCE_SYSTEM.md §12).

``derive_trust_tier`` is the one place the project turns scattered provenance metadata into
a single publishable claim about how much a figure can be trusted, so the failure mode is
**overclaiming**: a record whose grade is stronger than its weakest component justifies. A
handful of examples cannot cover that — the guarantee has to hold for arbitrary component
combinations, including missing and out-of-vocabulary values.

The properties asserted here are exactly the ones §12/T3 names:

  1. **weakest link** — the grade equals the minimum component ceiling, and ``binding``
     names precisely the components sitting at that minimum (a grade whose stated reason is
     wrong is as bad as a wrong grade — the popover renders it);
  2. **monotonicity** — degrading any single component can never *raise* the overall grade;
  3. **no disqualified Verified** — an OCR / weak-match / sandbox / stale / blocking-caveat
     record can never grade A, however clean the rest of it is;
  4. **footnote-1 asymmetry** — *no join attempted* must not cap the grade, but a join that
     was attempted and FAILED must.

Pure + fast — no markers, always runs.
"""

import sys
from pathlib import Path

from hypothesis import given
from hypothesis import strategies as st

sys.path.insert(0, str(Path(__file__).parents[2]))

from services.data_contracts import (  # noqa: E402
    CAVEAT_SEVERITY,
    EXTRACTION_METHOD,
    FRESHNESS_STATUS,
    MATCH_METHOD,
    MATCH_METHOD_ALIASES,
    PIPELINE_STATUS,
    TRUST_TIERS,
    assess_trust,
    derive_trust_tier,
)

# A pristine record: every component at its strongest documented value. Individual tests
# degrade exactly one field of this, so any grade drop is attributable to that field alone.
CLEAN: dict[str, object] = {
    "extraction_method": "official_api",
    "match_method": "exact",
    "pipeline_status": "live",
    "freshness_status": "ok",
    "caveat_severity": "none",
}

# In-vocabulary values, plus None (absent metadata) and a junk value the gate has never
# seen — the two cases a real un-backfilled frame actually contains.
_JUNK = [None, "", "UNKNOWN", "not_a_real_value"]
COMPONENT_VALUES: dict[str, list[object]] = {
    "extraction_method": sorted(EXTRACTION_METHOD) + _JUNK,
    "pipeline_status": sorted(PIPELINE_STATUS) + _JUNK,
    "freshness_status": sorted(FRESHNESS_STATUS) + _JUNK,
    "caveat_severity": sorted(CAVEAT_SEVERITY) + _JUNK,
    "match_method": sorted(MATCH_METHOD) + sorted(MATCH_METHOD_ALIASES) + _JUNK,
}

records = st.fixed_dictionaries(
    {name: st.sampled_from(values) for name, values in COMPONENT_VALUES.items()}
    | {"match_confidence": st.sampled_from([None, 0.0, 0.5, 0.9, "junk"])}
)


def _strength(tier: str) -> int:
    """Higher == stronger. TRUST_TIERS is ordered strongest-first, so negate the index."""
    return -TRUST_TIERS.index(tier)


@given(record=records)
def test_grade_is_the_minimum_component_ceiling(record):
    """The grade IS the weakest link, and `binding` explains it truthfully."""
    a = assess_trust(record)
    weakest = min(_strength(t) for t in a.components.values())
    assert _strength(a.tier) == weakest
    assert a.binding, "a grade must always name the component(s) that bound it"
    assert set(a.binding) == {c for c, t in a.components.items() if _strength(t) == weakest}


@given(record=records, component=st.sampled_from(sorted(COMPONENT_VALUES)), data=st.data())
def test_degrading_a_component_never_raises_the_grade(record, component, data):
    """T3 proper: weaken one component, and the headline grade must not improve.

    Compared on the *effective* ceiling `assess_trust` reports for that component rather
    than on the raw value, so the match_method/match_confidence interaction is covered too.
    """
    replacement = data.draw(st.sampled_from(COMPONENT_VALUES[component]))
    before = assess_trust(record)
    after = assess_trust({**record, component: replacement})
    if _strength(after.components[component]) <= _strength(before.components[component]):
        assert _strength(after.tier) <= _strength(before.tier)


@given(record=records)
def test_grade_is_always_a_known_tier(record):
    """Never raises, never invents a band — the §12 'no crash, no new vocabulary' floor."""
    assert derive_trust_tier(record) in TRUST_TIERS


def test_pristine_record_grades_verified():
    """The suite is only meaningful if A is reachable at all."""
    assert derive_trust_tier(CLEAN) == "A"


def test_disqualifying_components_can_never_grade_verified():
    """Each of these caps the grade below A no matter how clean everything else is."""
    for component, value in [
        ("extraction_method", "ocr_extracted"),
        ("extraction_method", "derived"),
        ("match_method", "weak"),
        ("match_method", "exact_ambiguous"),  # the stored dialect must cap too
        ("pipeline_status", "sandbox"),
        ("pipeline_status", "experimental"),
        ("freshness_status", "stale"),
        ("caveat_severity", "blocking"),
    ]:
        tier = derive_trust_tier({**CLEAN, component: value})
        assert tier != "A", f"{component}={value} must not grade Verified (got {tier})"


def test_no_join_attempted_does_not_cap_but_a_failed_join_does():
    """§3 footnote 1 — the asymmetry that stops single-source records being punished."""
    assert derive_trust_tier({**CLEAN, "match_method": None}) == "A"
    assert derive_trust_tier({**CLEAN, "match_method": "none"}) == "A"
    # Attempted and failed: the stored `no_match` carries match_confidence 0.0.
    assert derive_trust_tier({**CLEAN, "match_method": "no_match", "match_confidence": 0.0}) == "D"


def test_failed_join_caps_the_grade_even_when_confidence_is_null():
    """Regression — a spike found this grading **Verified** (2026-07-20).

    `no_match` aliased to `none`, and `none` only capped the grade when `match_confidence`
    was present and <= 0. So a failed join whose confidence column was NULL graded A: the
    precise overclaim the module exists to stop. The categorical value must decide on its
    own, never the nullable number.
    """
    for conf in (None, "", "junk"):
        rec = {**CLEAN, "match_method": "no_match", "match_confidence": conf}
        assert derive_trust_tier(rec) == "D", f"no_match with match_confidence={conf!r} must cap at D"
    # The canonical spelling must behave identically to the stored dialect.
    assert derive_trust_tier({**CLEAN, "match_method": "failed"}) == "D"


def test_unknown_metadata_floors_to_indicative_by_default():
    """An un-backfilled row must not grade Verified by virtue of having no metadata."""
    assert derive_trust_tier({}) == "D"
    # ...but a frame already established to carry the full envelope can opt out.
    assert derive_trust_tier({}, unknown_ceiling="A") == "A"
