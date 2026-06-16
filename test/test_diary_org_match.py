"""Unit test for diary × lobbying-org gazetteer matching guards.

``pipeline_sandbox/diary_org_match.py`` produces the explosion-prone
``diary_org_mentions`` table. The matching is deliberately conservative; this
test pins the normalisation, tiering, and the three precision guards the round-1
probe proved necessary (memory project_enrichment_round2_2026_06_12):

* SURNAME guard — a single-token org equal to the entry's own minister surname.
* PLACENAME guard — a single-token org that is an Irish county/town.
* PERSON-TITLE guard — a single-token org preceded by a title ("Minister Harris"
  is the person, not "Harris Group").
* the HIGH-key length floor that rejects over-stripped stubs ("bank of").

These guards are why the probe's naive count drops; if an edit removes one, a
known false-positive class silently re-enters the display tier.
"""

from __future__ import annotations

from extractors.diary_org_match import (
    anchor_tier,
    build_gazetteer,
    build_token_index,
    match_subject,
    norm,
)

# A tiny fixed gazetteer standing in for the lobbying register.
LOBBYISTS = [
    "Insurance Ireland",          # -> "insurance"  (single token, medium)
    "Harris Group",               # -> "harris"     (single token, medium)
    "Grow Remote Ireland CLG",    # -> "grow remote" (multi, high)
    "Bank of Ireland Group plc",  # -> "bank of"    (stub, REJECTED by length floor)
    "Aer Lingus",                 # -> "aer lingus" (multi, high)
]
CLIENTS: list[str] = []
GAZ = build_gazetteer(LOBBYISTS, CLIENTS)
IDX = build_token_index(GAZ)


def _orgs(subject: str, minister: str | None = None) -> set[str]:
    return {m["matched_org_name"] for m in match_subject(subject, minister, GAZ, IDX)}


def test_norm_strips_suffixes_and_accents() -> None:
    assert norm("Aer Língus Limited") == "aer lingus"
    assert norm("Bank of Ireland Group plc") == "bank of"


def test_high_tier_length_floor_rejects_stub() -> None:
    # "bank of" is <9 chars after stripping → never enters the gazetteer at all.
    assert anchor_tier("bank of") is None
    assert "Bank of Ireland Group plc" not in GAZ.get("bank of", ("",))[0:1]
    assert _orgs("Meeting with Central Bank of Ireland officials") == set()


def test_high_tier_multi_token_matches() -> None:
    assert _orgs("Meeting with Grow Remote") == {"Grow Remote Ireland CLG"}
    assert _orgs("Aer Lingus announces new Shannon schedule") == {"Aer Lingus"}


def test_medium_needs_engagement_cue() -> None:
    # single-token "insurance" only fires when an engagement cue is present
    assert _orgs("Insurance levy policy note") == set()        # no cue
    assert _orgs("Meeting re Insurance Ireland") == {"Insurance Ireland"}  # cue


def test_surname_guard_drops_own_minister() -> None:
    # entry's own minister is "Harris" → the token "harris" must not self-match
    assert _orgs("Meeting with Harris", minister="Harris") == set()


def test_person_title_guard_drops_third_party_person() -> None:
    # "Minister Harris" is the person Simon Harris, not Harris Group
    assert _orgs("Meeting with Minister Harris", minister="Varadkar") == set()


def test_placename_guard() -> None:
    # add a placename-shaped single-token org and confirm it is guarded
    gaz = build_gazetteer(["Limerick"], [])  # -> "limerick", but it's a placename
    idx = build_token_index(gaz)
    hits = match_subject("Visit to Limerick", None, gaz, idx)
    assert hits == []


def test_member_name_excluded_from_gazetteer() -> None:
    # a TD/Senator name must not enter the org gazetteer
    gaz = build_gazetteer(["Patrick Costello"], [], person_names={norm("Patrick Costello")})
    assert "patrick costello" not in gaz


def test_person_title_guard_multi_token() -> None:
    # "Prof Eamonn Murphy" is the person; "Meeting with Eamonn Murphy" (no title)
    # still matches — the guard keys on the preceding title, not the name.
    gaz = build_gazetteer(["Eamonn Murphy"], [])
    idx = build_token_index(gaz)
    assert match_subject("Prof Eamonn Murphy keynote", None, gaz, idx) == []
    assert {m["matched_org_name"] for m in match_subject("Meeting with Eamonn Murphy", None, gaz, idx)} == {
        "Eamonn Murphy"
    }


def test_ceo_does_not_drop_org() -> None:
    # "CEO" is org-ambiguous and must NOT trigger the person-title guard
    gaz = build_gazetteer(["Wind Energy Ireland"], [])
    idx = build_token_index(gaz)
    hits = {m["matched_org_name"] for m in match_subject("Meeting with CEO Wind Energy Ireland", None, gaz, idx)}
    assert hits == {"Wind Energy Ireland"}


def test_generic_topic_demoted_to_medium() -> None:
    # a generic industry phrase is demoted out of the HIGH display tier
    gaz = build_gazetteer(["Renewable Energy Ireland"], [])
    assert gaz["renewable energy"][2] == "medium"
