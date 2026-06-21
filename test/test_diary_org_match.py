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

import polars as pl

from extractors.diary_org_match import (
    anchor_tier,
    build_gazetteer,
    build_token_index,
    is_personal_name,
    match_subject,
    norm,
    reclassify_other_as_external,
)

# A tiny fixed gazetteer standing in for the lobbying register.
LOBBYISTS = [
    "Insurance Ireland",  # -> "insurance"  (single token, medium)
    "Harris Group",  # -> "harris"     (single token, medium)
    "Grow Remote Ireland CLG",  # -> "grow remote" (multi, high)
    "Bank of Ireland Group plc",  # -> "bank of"    (stub, REJECTED by length floor)
    "Aer Lingus",  # -> "aer lingus" (multi, high)
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
    assert _orgs("Insurance levy policy note") == set()  # no cue
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


# --- person_primarily_responsible exclusion (2026-06-19: the 'Dónall Geoghegan'
# trailing-attendee-tag FP class). is_personal_name decides which names drawn from
# the lobbying register's person field are INDIVIDUALS (excluded from the gazetteer)
# vs orgs that were mistyped into that field (rescued).
def test_personal_name_excludes_individuals() -> None:
    # the FP names that polluted the diary match — two-token person names
    assert is_personal_name("donall geoghegan", set())
    assert is_personal_name("david kelly", set())


def test_personal_name_rescues_org_token_names() -> None:
    # real orgs mistyped into the person field keep an org-indicator token → survive
    assert not is_personal_name("limerick chamber", set())
    assert not is_personal_name("irish hairdressers federation", set())  # 3 tokens anyway
    assert not is_personal_name("coalition 2030", set())


def test_personal_name_rescues_known_client_org() -> None:
    # a two-token org with no org-token survives if it is a known client
    assert is_personal_name("dave fallon", set())  # person
    assert not is_personal_name("dave fallon", {"dave fallon"})  # but not if it's a client org


def test_responsible_person_excluded_from_gazetteer() -> None:
    # end-to-end: a person name passed via person_names never enters the gazetteer,
    # so the diary subject that names them yields no org mention
    gaz = build_gazetteer(["Dónall Geoghegan"], [], person_names={norm("Dónall Geoghegan")})
    idx = build_token_index(gaz)
    assert "donall geoghegan" not in gaz
    assert match_subject("Pre-Cab - Donall Geoghegan", "Ryan", gaz, idx) == []


# --- gazetteer widening 2026-06-19: curated-acronym tier + stateboards tier ---
def _matches(subject: str):
    return match_subject(subject, None, GAZ, IDX)


def test_acronym_tier_matches_uppercase_wholeword() -> None:
    hits = _matches("IBEC Annual Conference keynote")
    assert hits and hits[0]["matched_org_name"] == "Irish Business and Employers Confederation"
    assert hits[0]["gaz_origin"] == "acronym"
    assert hits[0]["match_confidence"] == "high"


def test_acronym_is_case_sensitive_and_wholeword() -> None:
    # lowercase 'ibec' is NOT an acronym hit; 'IDA' inside 'Florida' must not match
    assert _matches("ibec lower case mention") == []
    assert _matches("Trip to Florida and back") == []
    # but a real whole-word uppercase acronym does fire
    assert {m["matched_org_name"] for m in _matches("Meeting with IDA Chair")} == {"IDA Ireland"}


def test_acronym_not_double_counted_with_full_name() -> None:
    # a subject naming the org in full AND by acronym yields ONE mention, not two
    gaz = build_gazetteer(["Land Development Agency"], [])
    idx = build_token_index(gaz)
    hits = match_subject("Launch of the Land Development Agency (LDA)", None, gaz, idx)
    assert [h["matched_org_name"] for h in hits].count("Land Development Agency") == 1


def test_stateboard_tier_added_to_gazetteer() -> None:
    gaz = build_gazetteer([], [], stateboards=["Low Pay Commission", "Marine Institute"])
    idx = build_token_index(gaz)
    assert gaz["low pay commission"][1] == "stateboard"
    hits = match_subject("Meeting with Chair of Low Pay Commission", None, gaz, idx)
    assert {h["matched_org_name"] for h in hits} == {"Low Pay Commission"}
    assert hits[0]["gaz_origin"] == "stateboard"


# --- org-evidence reclassification 2026-06-19: a terse 'other' entry that NAMES an org
# (no trigger verb -> keyword classifier missed it) is promoted to external_meeting ---
def test_reclassify_promotes_only_matched_other() -> None:
    e = pl.DataFrame(
        {
            "entry_id": ["a", "b", "c", "d"],
            "subject": ["Nestlé Ireland", "Private", "Meeting with X", "Bus Éireann"],
            "entry_class": ["other", "other", "external_meeting", "media"],
        }
    )
    # only 'a' and 'd' named an org; 'd' is media (must NOT be touched), 'b' had no org
    out, n = reclassify_other_as_external(e, {"a", "d"})
    assert n == 1  # only the 'other'+matched row 'a'
    cls = dict(zip(out["entry_id"], out["entry_class"], strict=True))
    assert cls == {"a": "external_meeting", "b": "other", "c": "external_meeting", "d": "media"}


def test_reclassify_is_noop_without_entry_class() -> None:
    e = pl.DataFrame({"entry_id": ["a"], "subject": ["x"]})
    out, n = reclassify_other_as_external(e, {"a"})
    assert n == 0 and out.equals(e)


# --- curated full-name org tier 2026-06-19 (Tier ⑥): well-known orgs the register +
# norm() miss. Matched on the accent-folded RAW subject (bypasses the suffix-strip trap). ---
def test_curated_org_recovers_suffix_stripped_name() -> None:
    # "Enterprise Ireland" norm()s to the stopword "enterprise" → the register tier can't
    # anchor it; the curated tier matches the full phrase on the folded subject.
    hits = _matches("Enterprise Ireland Site Visit")
    assert {m["matched_org_name"] for m in hits} == {"Enterprise Ireland"}
    assert hits[0]["gaz_origin"] == "curated_org"


def test_curated_org_accent_fold_and_single_token() -> None:
    assert {m["matched_org_name"] for m in _matches("Wyeth site Update - Nestlé Ireland")} == {"Nestlé"}
    assert {m["matched_org_name"] for m in _matches("Tesco Jobs announcement")} == {"Tesco"}


def test_curated_microsoft_teams_is_not_microsoft() -> None:
    # the video-platform tag must NOT count as a meeting with Microsoft
    assert _matches("Town Hall (Microsoft Teams Meeting)") == []
    assert {m["matched_org_name"] for m in _matches("Meeting with Microsoft re jobs")} == {"Microsoft"}


def test_curated_central_bank_is_not_bank_of_ireland() -> None:
    # "Central Bank of Ireland" (the regulator) must not match curated "Bank of Ireland"
    assert _matches("Meeting with Central Bank of Ireland officials") == []
    assert {m["matched_org_name"] for m in _matches("Meeting with Bank of Ireland")} == {"Bank of Ireland"}


# ── curated additions 2026-06-21 (doc/DIARY_GAZETTEER_CANDIDATES.md) — pin the adds AND the
#    deliberately-DROPPED false positives so a future re-add of a colliding key trips a test ──


def test_curated_cisco_matches_but_not_san_francisco() -> None:
    # Cisco was added; the word-boundary guard must keep "san francisco" from matching it
    assert {m["matched_org_name"] for m in _matches("Meeting w/Cisco re jobs")} == {"Cisco"}
    assert _matches("Travel to San Francisco for a tech mission") == []  # 'cisco' inside 'francisco' is guarded


def test_curated_merck_and_coca_cola_match() -> None:
    assert {m["matched_org_name"] for m in _matches("Announcement of Merck expansion")} == {"Merck"}
    assert {m["matched_org_name"] for m in _matches("Visit to Coca Cola plant")} == {"Coca-Cola"}


def test_curated_kerry_group_phrase_only_not_the_county() -> None:
    # "Kerry Group" (food multinational) added as a PHRASE; bare county "Kerry" must NOT match
    assert {m["matched_org_name"] for m in _matches("Meeting with Kerry Group")} == {"Kerry Group"}
    assert _matches("Interview with Radio Kerry") == []
    assert _matches("Kerry Babies Briefing") == []


def test_curated_drops_collision_prone_names() -> None:
    # these were SCANNED but DROPPED as false positives — must stay unmatched
    assert _matches("FYI - Stephen Roche San Fran Police - Dáil Tour") == []  # cyclist, not Roche pharma
    assert _matches("Fianna Fáil Think-In (Rochestown Park Hotel, Cork)") == []  # hotel, not Roche
    assert _matches("Peter Baxter (The Burnaby, Greystones)") == []  # a person, not Baxter healthcare


# --------------------------------------------------------------------------- platform denoise
# Outlook calendar exports tag the JOINING platform onto the subject. "Cisco Webex" / a trailing
# "- Cisco" is the video platform, NOT Cisco the company; "(Microsoft Teams Meeting)" is not
# Microsoft. denoise_subject strips these before matching so they cannot coin false engagements
# (2 "Cisco Webex" rows already leaked into gold; the Education export adds ~57 "- Cisco" tags).
from extractors.diary_org_match import denoise_subject  # noqa: E402


def test_denoise_strips_cisco_webex_phrase() -> None:
    assert "cisco" not in denoise_subject("Meeting with Min O'Gorman - Cisco Webex Budget Day").lower()


def test_denoise_strips_trailing_cisco_platform_tag() -> None:
    assert denoise_subject("Meeting with Minister Madigan - Cisco") == "Meeting with Minister Madigan"


def test_denoise_keeps_real_cisco_company_meeting() -> None:
    # the company (not a platform tag) must survive — it's a real engagement
    assert "cisco" in denoise_subject("MEETING WITH CISCO (The Board room, Clayton Hotel)").lower()


def test_denoise_strips_microsoft_teams_but_keeps_bare_teams() -> None:
    assert "teams" not in denoise_subject("Constituency Meeting (Microsoft Teams Meeting)").lower()
    assert denoise_subject("Special Education Teams catch-up") == "Special Education Teams catch-up"


def test_denoise_strips_status_prefix_and_meet_url() -> None:
    assert denoise_subject("scheduled: Meeting with NCSE").startswith("Meeting")
    assert "google.com" not in denoise_subject("Boherbue NS (https://meet.google.com/abc-defg-hij)")


def test_match_does_not_coin_cisco_from_webex_tag() -> None:
    # integration: the curated tier must NOT emit Cisco for a Webex-platform subject …
    assert {m["matched_org_name"] for m in _matches("Meeting with Minister Humphreys - Cisco Webex 09:00")} == set()
    # … but DOES for a genuine Cisco meeting
    assert "Cisco" in {m["matched_org_name"] for m in _matches("Pre-briefing in advance of Cisco meeting")}


def test_match_does_not_coin_microsoft_from_teams_venue() -> None:
    assert {m["matched_org_name"] for m in _matches("Constituency Meeting (Microsoft Teams Meeting)")} == set()
    assert "Microsoft" in {m["matched_org_name"] for m in _matches("Meeting with Microsoft at One Microsoft Place")}
