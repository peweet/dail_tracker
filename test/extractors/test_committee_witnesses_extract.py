"""Unit tests for the committee-evidence witness extractor's transform
(extractors/committee_witnesses_extract.py).

`extract_meeting(rec)` is *almost* pure: it navigates an Oireachtas-API-style record
and parses an AKN-XML transcript into witness orgs / persons / topics / committee-identity
reconciliation. Its ONLY side effect is one `fetch_text(uri)` call that pulls the XML — so
we monkeypatch that single network call with a synthetic transcript and assert the parse.
The genuinely pure helpers `_plain` and `_clean_heading_org` are exercised directly.

No real network, no real S3 — every transcript here is hand-built to mirror the real AKN-XML
shape (FRBRthis path, <heading>, <speech>, <from>) so we test WHAT the parser pulls and that
empty/missing sections degrade to sensible empties rather than crashing.

NOT covered (network/IO, per task scope): enumerate_meetings (paginates /v1/debates), run
(writes parquet), main (argparse). These are thin glue around extract_meeting and out of scope
for a pure-transform unit test.
"""

from __future__ import annotations

import extractors.committee_witnesses_extract as cwe
from extractors.committee_witnesses_extract import _clean_heading_org, _plain, extract_meeting

XML_URI = "https://data.oireachtas.ie/akn/ie/debateRecord/committee_of_public_accounts/2024-11-21/debate.xml"


def _rec(committee_code: str = "committee_of_public_accounts") -> dict:
    """A minimal Oireachtas-API debateRecord mirroring the keys extract_meeting navigates:
    house.{committeeCode,showAs,houseNo}, date, formats.xml.uri."""
    return {
        "house": {
            "committeeCode": committee_code,
            "showAs": "Committee of Public Accounts",
            "houseNo": "33",
        },
        "date": "2024-11-21",
        "formats": {"xml": {"uri": XML_URI}},
    }


def _patch_xml(monkeypatch, xml: str) -> None:
    """Stub the lone network call. fetch_text returns (text, status); only text is used."""
    monkeypatch.setattr(cwe, "fetch_text", lambda *_a, **_k: (xml, 200))


# --------------------------------------------------------------------------- _plain
def test_plain_strips_tags_and_collapses_whitespace():
    """_plain is the workhorse normaliser feeding every downstream match — it must strip AKN
    tags and collapse runs of whitespace/newlines to single spaces, else heading/welcome
    regexes (anchored on word boundaries) would miss real captures."""
    assert _plain("<b>Mr.</b>\n  John   Smith\t") == "Mr. John Smith"
    assert _plain("") == ""


# ------------------------------------------------------------------- _clean_heading_org
def test_clean_heading_org_unwraps_topic_prefix():
    """'Operations of <Body>' is a topic phrasing wrapping the witness body — the topic
    prefix must be stripped so we record the BODY, not the sentence."""
    assert _clean_heading_org("Operations of Tailte Éireann") == "Tailte Éireann"


def test_clean_heading_org_drops_format_tail_and_pure_topics():
    """A session-format tail (': Discussion') is noise to strip; a pure accounting artefact
    heading ('Appropriation Accounts ...') names no body and must return None (dropped),
    guarding against ingesting accounting boilerplate as a 'witness org'."""
    assert _clean_heading_org("Health Service Executive: Discussion") == "Health Service Executive"
    assert _clean_heading_org("Appropriation Accounts 2024") is None
    assert _clean_heading_org("Business of Committee") is None
    assert _clean_heading_org("") is None


# --------------------------------------------------------------------- extract_meeting
def test_extract_meeting_full_record(monkeypatch):
    """End-to-end on a record whose FRBR path code MATCHES the API committeeCode: assert the
    meeting metadata, reconciliation=True, the topics spine (deduped, housekeeping dropped,
    verbatim), witness orgs from headings (primary) + welcome (corroboration), and witness
    PERSONS = non-member honorific speakers only (TDs/chair excluded)."""
    xml = f"""
    <akomaNtoso>
      <FRBRthis value="/akn/ie/debateRecord/committee_of_public_accounts/2024-11-21/main@"/>
      <heading>Operations of Tailte Éireann</heading>
      <heading>Business of Committee</heading>
      <heading>Operations of Tailte Éireann</heading>
      <speech><from>An Cathaoirleach</from>
        <p>I welcome the representatives of the Health Service Executive. Thank you all for coming.</p>
      </speech>
      <speech><from>Mr. John Murphy</from><p>Thank you, Chair.</p></speech>
      <speech><from>Deputy Mary Lou McDonald</from><p>A question.</p></speech>
      <speech><from>Ms Aoife Byrne</from><p>To answer that.</p></speech>
      <speech><from>An Cathaoirleach</from><p>Thank you.</p></speech>
    </akomaNtoso>
    """
    _patch_xml(monkeypatch, xml)
    out = extract_meeting(_rec())

    # meeting metadata pulled straight from the API record
    assert out["committee_code"] == "committee_of_public_accounts"
    assert out["committee_name"] == "Committee of Public Accounts"
    assert out["house_no"] == "33"
    assert out["date"] == "2024-11-21"
    assert out["source_xml"] == XML_URI

    # committee-identity reconciliation: FRBR path code == API code → reconciled
    assert out["frbr_path_code"] == "committee_of_public_accounts"
    assert out["reconciled"] is True

    # topics = verbatim headings, deduped + housekeeping ('Business of') dropped
    assert out["topics"] == ["Operations of Tailte Éireann"]

    # witness orgs: heading-derived body, plus the welcome-named HSE
    orgs = {o["witness_org"]: o["org_source"] for o in out["orgs"]}
    assert orgs.get("Tailte Éireann") == "heading"
    assert orgs.get("Health Service Executive") == "welcome"

    # witness persons: honorific non-members only; Deputy + An Cathaoirleach excluded; sorted+unique
    assert out["persons"] == sorted(["Mr. John Murphy", "Ms Aoife Byrne"])


def test_extract_meeting_reconciliation_mismatch(monkeypatch):
    """If the AKN FRBR path's committee code DISAGREES with the API committeeCode, the meeting
    is flagged reconciled=False (run() drops these rather than guessing) — the committee
    conflation guard documented in the module header."""
    xml = """
    <akomaNtoso>
      <FRBRthis value="/akn/ie/debateRecord/joint_committee_on_housing/2024-11-21/main@"/>
      <heading>Some Topic: Discussion</heading>
    </akomaNtoso>
    """
    _patch_xml(monkeypatch, xml)
    out = extract_meeting(_rec(committee_code="committee_of_public_accounts"))
    assert out["frbr_path_code"] == "joint_committee_on_housing"
    assert out["reconciled"] is False


def test_extract_meeting_empty_sections_yield_empties(monkeypatch):
    """EDGE CASE: a transcript with no FRBRthis, no headings, no speeches, no <from> labels must
    NOT crash — it should yield empty topics/orgs/persons and reconciled=False (empty path code
    cannot match a real committee code). Mirrors a sparse/garbled transcript in the wild."""
    xml = "<akomaNtoso><body></body></akomaNtoso>"
    _patch_xml(monkeypatch, xml)
    out = extract_meeting(_rec())

    assert out["topics"] == []
    assert out["orgs"] == []
    assert out["persons"] == []
    assert out["frbr_path_code"] == ""
    assert out["reconciled"] is False
    # metadata still comes through intact from the record
    assert out["committee_code"] == "committee_of_public_accounts"
    assert out["date"] == "2024-11-21"


def test_extract_meeting_heading_takes_precedence_over_welcome(monkeypatch):
    """When the SAME body is named in both a heading and the welcome, the heading source wins
    (headings looped first; candidates.setdefault makes first-writer-wins). Confirms we attribute
    the stronger 'heading' provenance, not 'welcome', for the same org."""
    xml = """
    <akomaNtoso>
      <FRBRthis value="/akn/ie/debateRecord/committee_of_public_accounts/2024-11-21/main@"/>
      <heading>Health Service Executive: Discussion</heading>
      <speech><from>An Cathaoirleach</from>
        <p>I welcome the representatives of the Health Service Executive.</p>
      </speech>
    </akomaNtoso>
    """
    _patch_xml(monkeypatch, xml)
    out = extract_meeting(_rec())
    sources = {o["witness_org"]: o["org_source"] for o in out["orgs"]}
    assert sources == {"Health Service Executive": "heading"}
