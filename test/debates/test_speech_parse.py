"""Unit tests for debates/speech_parse.py.

``parse_akn`` turns an AKN debate transcript into one row per member
contribution (speech / question / answer). The tests build minimal AKN XML by
hand and assert: the namespace-agnostic tag matching (incl. the versioned
/CSD13 default namespace), deterministic member resolution via the
<TLCPerson> href tail, section attribution across a multi-section document,
speaker/body separation, and graceful handling of empty / malformed input.
"""

from __future__ import annotations

import pandas as pd

from debates.speech_parse import parse_akn

# Versioned default namespace — the suffix the parser must NOT hardcode.
_NS = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0/CSD13"


def _doc(chamber: str, date: str, sections: str, references: str) -> str:
    return f"""<akomaNtoso xmlns="{_NS}">
  <debate>
    <meta><identification><FRBRWork>
      <FRBRthis value="/akn/ie/debateRecord/{chamber}/{date}/debate/mul@/main"/>
    </FRBRWork></identification>
      <references>{references}</references>
    </meta>
    <debateBody>{sections}</debateBody>
  </debate>
</akomaNtoso>"""


def _speech(by: str, name: str, body: str, time: str | None = None) -> str:
    t = f'<recordedTime time="{time}"/>' if time else ""
    return f'<speech by="#{by}"><from>{name}{t}</from><p>{body}</p></speech>'


def test_basic_parse_resolution_and_section():
    refs = (
        '<TLCPerson eId="ErinMcGreehan" href="/ie/oireachtas/member/id/Erin-McGreehan.S.2020-06-29" showAs="Erin McGreehan"/>'
        '<TLCPerson eId="HelenMcEntee" href="/ie/oireachtas/member/id/Helen-McEntee.D.2013-03-27" showAs="Helen McEntee"/>'
    )
    sections = (
        '<debateSection eId="dbsect_30"><heading>Trade Relations</heading>'
        + _speech("ErinMcGreehan", "Deputy Erin McGreehan", "What is the plan?", time="2026-03-26T11:50:00+00:00")
        + _speech("HelenMcEntee", "Deputy Helen McEntee", "Here is the plan.")
        + "</debateSection>"
    )
    df = parse_akn(_doc("dail", "2026-03-26", sections, refs))

    assert len(df) == 2
    assert df["chamber"].unique().tolist() == ["dail"]
    assert df["date"].unique().tolist() == ["2026-03-26"]
    assert df["debate_section_id"].unique().tolist() == ["dbsect_30"]
    assert df["section_heading"].unique().tolist() == ["Trade Relations"]
    # Deterministic member resolution via the TLCPerson href tail.
    assert df.iloc[0]["unique_member_code"] == "Erin-McGreehan.S.2020-06-29"
    assert df.iloc[1]["unique_member_code"] == "Helen-McEntee.D.2013-03-27"
    # Speaker label and body are separated; body excludes the <from> name.
    assert df.iloc[0]["speaker_raw"] == "Deputy Erin McGreehan"
    assert df.iloc[0]["speech_text"] == "What is the plan?"
    assert df.iloc[0]["recorded_time"] == "2026-03-26T11:50:00+00:00"
    assert df.iloc[0]["contribution_type"] == "speech"
    # Order is assigned in document order.
    assert df["contribution_order"].tolist() == [1, 2]


def test_multi_section_attribution():
    refs = '<TLCPerson eId="A" href="/ie/oireachtas/member/id/Aoife-A.S.2020-01-01"/>'
    sections = (
        '<debateSection eId="dbsect_2"><heading>Messages from Dáil</heading>'
        + _speech("A", "Senator A", "Message one.")
        + "</debateSection>"
        '<debateSection eId="dbsect_3"><heading>Order of Business</heading>'
        + _speech("A", "Senator A", "Business item.")
        + "</debateSection>"
    )
    df = parse_akn(_doc("seanad", "2025-12-18", sections, refs))
    # Each speech attributed to its own enclosing section.
    assert dict(zip(df["debate_section_id"], df["section_heading"], strict=True)) == {
        "dbsect_2": "Messages from Dáil",
        "dbsect_3": "Order of Business",
    }
    assert df["chamber"].unique().tolist() == ["seanad"]


def test_nested_section_business_grouping():
    # A "Commencement Matters" parent section wraps a per-topic subsection that
    # holds the speech. nearest -> topic; outermost -> business grouping.
    refs = '<TLCPerson eId="A" href="/ie/oireachtas/member/id/Aoife-A.S.2020-01-01"/>'
    sections = (
        '<debateSection eId="dbsect_3"><heading>Commencement Matters</heading>'
        '<debateSection eId="dbsect_4"><heading>Litter Pollution</heading>'
        + _speech("A", "Senator A", "On litter in my county.")
        + "</debateSection></debateSection>"
    )
    df = parse_akn(_doc("seanad", "2025-06-25", sections, refs))
    assert len(df) == 1
    assert df.iloc[0]["debate_section_id"] == "dbsect_4"
    assert df.iloc[0]["section_heading"] == "Litter Pollution"  # nearest = topic
    assert df.iloc[0]["business"] == "Commencement Matters"  # outermost = grouping


def test_flat_section_business_equals_heading():
    refs = '<TLCPerson eId="A" href="/ie/oireachtas/member/id/Aoife-A.S.2020-01-01"/>'
    sections = (
        '<debateSection eId="dbsect_1"><heading>Order of Business</heading>'
        + _speech("A", "Senator A", "A point.")
        + "</debateSection>"
    )
    df = parse_akn(_doc("seanad", "2025-06-25", sections, refs))
    assert df.iloc[0]["section_heading"] == "Order of Business"
    assert df.iloc[0]["business"] == "Order of Business"  # un-nested: coincide


def test_question_and_answer_contribution_types():
    refs = '<TLCPerson eId="A" href="/ie/oireachtas/member/id/Aoife-A.S.2020-01-01"/>'
    sections = (
        '<debateSection eId="dbsect_5"><heading>Topical</heading>'
        '<question by="#A"><from>Senator A</from><p>My question?</p></question>'
        '<answer by="#A"><from>Senator A</from><p>The answer.</p></answer>'
        "</debateSection>"
    )
    df = parse_akn(_doc("dail", "2026-01-01", sections, refs))
    assert set(df["contribution_type"]) == {"question", "answer"}


def test_unresolved_member_ref_kept_as_blank():
    # A by-ref with no matching TLCPerson resolves to '' (kept, not dropped).
    sections = (
        '<debateSection eId="dbsect_1"><heading>X</heading>'
        + _speech("Ghost", "An Cathaoirleach", "Procedural note.")
        + "</debateSection>"
    )
    df = parse_akn(_doc("dail", "2026-01-01", sections, references=""))
    assert len(df) == 1
    assert df.iloc[0]["unique_member_code"] == ""
    assert df.iloc[0]["speaker_raw"] == "An Cathaoirleach"


def test_empty_and_malformed_input():
    assert parse_akn("").empty
    assert parse_akn("<not><valid").empty
    # Well-formed but no contributions → empty, schema-shaped frame.
    out = parse_akn(_doc("dail", "2026-01-01", "<debateSection eId='d'><heading>H</heading></debateSection>", ""))
    assert isinstance(out, pd.DataFrame)
    assert out.empty
