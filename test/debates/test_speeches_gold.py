"""Unit tests for debates/speeches_gold.py.

Covers the two gold enrichments: the Irish-language detector (precision against
fada-bearing English proper nouns is the tricky case) and the member-identity
join in build_speeches_fact (resolution, null-keep, house mapping, derived cols).
"""

from __future__ import annotations

import pandas as pd

from debates.speeches_gold import build_speeches_fact, irish_score, is_irish_speech

_ENGLISH = "I thank the Minister for being here today to discuss this important matter of housing policy."
_IRISH = "Gabhaim buíochas leis an Aire as bheith anseo inniu. Tá an-áthas orm an cheist seo a ardú agus go raibh maith agat."
# English turns that merely NAME fada-bearing proper nouns must NOT flag.
_ENGLISH_WITH_NAMES = "Deputy Ruairí Ó Murchú asked the Taoiseach to report on the work in Dáil Éireann this week."
_SHORT_PROCEDURAL = "An Cathaoirleach Gníomhach"


def test_irish_detector_precision():
    assert is_irish_speech(_IRISH) is True
    assert irish_score(_IRISH) >= 0.25
    assert is_irish_speech(_ENGLISH) is False
    assert irish_score(_ENGLISH) == 0.0
    # The discriminator: fada proper nouns in English do not flag.
    assert is_irish_speech(_ENGLISH_WITH_NAMES) is False
    # Sub-minimum length never flags (procedural Irish titles).
    assert is_irish_speech(_SHORT_PROCEDURAL) is False
    assert irish_score(_SHORT_PROCEDURAL) == 0.0


def _silver(code, text, chamber="dail"):
    return {
        "date": "2026-03-26",
        "chamber": chamber,
        "debate_section_id": "dbsect_1",
        "section_heading": "Trade",
        "contribution_type": "speech",
        "contribution_order": 1,
        "akn_eid": "spk_1",
        "unique_member_code": code,
        "speaker_raw": "Deputy X",
        "recorded_time": "",
        "speech_text": text,
    }


def test_build_joins_identity_and_derives_columns():
    speeches = pd.DataFrame(
        [
            _silver("Erin-McGreehan.S.2020-06-29", _IRISH, chamber="seanad"),
            _silver("", _ENGLISH),  # unresolved by-ref -> null identity, kept
        ]
    )
    members = pd.DataFrame(
        [
            {
                "unique_member_code": "Erin-McGreehan.S.2020-06-29",
                "member_name": "Erin McGreehan",
                "party": "FF",
                "constituency": "Louth",
            }
        ]
    )
    out = build_speeches_fact(speeches, members)

    assert len(out) == 2
    resolved = out[out["unique_member_code"] == "Erin-McGreehan.S.2020-06-29"].iloc[0]
    assert resolved["member_name"] == "Erin McGreehan"
    assert resolved["party"] == "FF"
    assert resolved["house"] == "Seanad"
    assert bool(resolved["is_irish"]) is True
    assert resolved["word_count"] > 0
    assert resolved["year"] == 2026
    # The blank-code row is kept with null identity (not dropped).
    unresolved = out[out["member_name"].isna()]
    assert len(unresolved) == 1
    assert bool(unresolved.iloc[0]["is_irish"]) is False


def test_build_empty_input():
    assert build_speeches_fact(pd.DataFrame(), pd.DataFrame()).empty
