"""Unit tests pinning the locator rules in ``pipeline_sandbox/etenders_criteria_pointer.py``.

Pure in-memory synthetic fixtures. No network, no document cache, no parquet write: the module
under test is READ-ONLY here and nothing imports ``main()`` or touches a path the extractor writes.

Each test names the real notice whose failure motivated the rule it pins.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pipeline_sandbox import etenders_criteria_pointer as m

# ------------------------------------------------------------------ fixtures
FILLER = "Clause paragraph {} concerning general contractual matters."


def lines(texts, source="doc.docx", styles=None):
    """Build the Line pool the matcher consumes, in document order."""
    styles = styles or {}
    return [m.Line(t, i, styles.get(i, ""), source, "para") for i, t in enumerate(texts)]


def doc(filename, texts, orig_idx=0, styles=None, status="ok"):
    """One published document with a single extracted part."""
    return {
        "filename": filename,
        "document_id": filename,
        "orig_idx": orig_idx,
        "parts": [(filename, lines(texts, filename, styles), status)],
        "n_lines": len(texts),
    }


def padded(insertions, total=100):
    """`insertions` = {index: text}; every other slot is inert filler."""
    out = [FILLER.format(i) for i in range(total)]
    for i, t in insertions.items():
        out[i] = t
    return out


# =================================================================== BEHAVIOUR 1
# document ranking: the specification document outranks the ESPD form.


def test_spec_document_outranks_espd_form():
    # notice 3509643 - `ESPD Template.docx` was ranked above the ITT that held the real requirements
    espd = doc(
        "ESPD Template.docx",
        ["Part IV: Selection Criteria", "Global indication for all selection criteria"],
        orig_idx=0,
    )
    itt = doc(
        "ITT - Main Tender Document.docx",
        ["3. Selection Criteria", "The tenderer shall hold employers liability cover of EUR 13,000,000."],
        orig_idx=1,
    )
    got = m.find_landing([espd, itt])
    assert got["doc"] == "ITT - Main Tender Document.docx"
    assert got["doc_class"] == "spec"
    assert got["heading"] == "3. Selection Criteria"


def test_doc_class_scores_espd_below_every_other_class():
    # notice 3509643 - the ESPD form must be the last-resort document class
    assert m.doc_class("ESPD Template.docx") == ("espd", 5)
    assert m.doc_class("ITT Volume 1.pdf")[0] == "spec"
    assert m.doc_class("Request for Tenders.pdf")[0] == "spec"
    assert m.doc_class("Call for Tender.pdf")[0] == "spec"
    assert m.doc_class("Suitability Assessment Questionnaire.docx")[0] == "criteria"
    espd_score = m.doc_class("ESPD Template.docx")[1]
    for other in ("ITT Volume 1.pdf", "Suitability Questionnaire.docx", "Random Attachment.pdf", "Appendix A.pdf"):
        assert m.doc_class(other)[1] > espd_score


def test_doc_class_normalises_underscores_so_word_boundaries_still_fire():
    # notice 3509643 - `_` is a word character, so `\bcft\b` would miss `CFT_Main.pdf`
    assert m.doc_class("CFT_Main_Document.pdf")[0] == "spec"
    assert m.doc_class("RFT_Volume_1.pdf")[0] == "spec"


def test_spec_check_runs_before_the_espd_check():
    # notice 3509643 - THE FIX. The prototype tested ESPD_RX first and unconditionally, so a
    # genuine specification whose FILENAME merely mentions the ESPD scored 5 - below `noise` (20)
    # and below `other` (50), which is precisely the mis-ranking correction 1 exists to prevent.
    assert m.doc_class("Invitation to Tender incorporating the ESPD.docx") == ("spec", 100)
    assert m.doc_class("RFT Volume 2 - ESPD Response.pdf") == ("spec", 100)
    # and the pure form is still the last-resort class
    assert m.doc_class("ESPD Template.docx") == ("espd", 5)
    assert m.doc_class("RFT Volume 2 - ESPD Response.pdf")[1] > m.doc_class("Appendix A Site Visit.pdf")[1]


def test_spec_named_after_the_espd_still_wins_the_landing():
    # notice 3509643 - the ordering fix must show up end-to-end, not only in doc_class
    noise = doc("Appendix A Site Visit.pdf", ["4. Suitability Assessment"], orig_idx=0)
    spec = doc("RFT Volume 2 - ESPD Response.pdf", ["4.1 Annual Turnover"], orig_idx=1)
    got = m.find_landing([noise, spec])
    assert got["doc"] == "RFT Volume 2 - ESPD Response.pdf"
    assert got["doc_class"] == "spec"


def test_part_iii_needs_the_word_exclusion_unlike_part_iv():
    # notice 3509643 - characterisation of an asymmetry in the added patterns
    assert m.PAT_BY_TAG["part_iv_espd"].search("Part IV")
    assert m.PAT_BY_TAG["part_iii_exclusion"].search("Part III") is None
    assert m.PAT_BY_TAG["part_iii_exclusion"].search("Part III: Exclusion Grounds")


def test_v1_filename_heuristic_scored_espd_top_which_is_the_bug_being_corrected():
    # notice 3509643 - v1 gave `espd` the top filename score of 10, above ITT's 9
    assert m.v1_fn_score("ESPD Template.docx") == 10
    assert m.v1_fn_score("ITT - Main Tender Document.docx") == 9
    # and the shipped classifier inverts exactly that ordering
    assert m.doc_class("ESPD Template.docx")[1] < m.doc_class("ITT - Main Tender Document.docx")[1]


# =================================================================== BEHAVIOUR 2
# pattern preference order.


def test_landing_pattern_preference_order_is_declared():
    # notice 3509643 - the ESPD's Part IV must sit below the three substantive patterns
    assert m.LAND_ORDER[:4] == [
        "selection_criteria",
        "economic_financial_standing",
        "suitability_assessment",
        "part_iv_espd",
    ]
    for added in ("technical_professional_ability", "part_iii_exclusion", "turnover"):
        assert m.LAND_RANK[added] > m.LAND_RANK["part_iv_espd"]


def test_best_pattern_wins_not_the_first_hit_encountered():
    # notice 3509643 - a weaker pattern occurring earlier must not capture the landing
    d = doc(
        "ITT.docx",
        ["2. Economic and Financial Standing", "The applicant shall submit audited accounts.", "5. Selection Criteria"],
    )
    got = m.find_landing([d])
    assert got["pattern"] == "selection_criteria"
    assert got["heading"] == "5. Selection Criteria"


def test_suitability_assessment_beats_part_iv_within_one_document():
    # notice 3509643 - Part IV is a cross-reference tick-box, never preferred over real text
    d = doc("ITT.docx", ["Part IV", "Section 4 Suitability Assessment"])
    assert m.find_landing([d])["pattern"] == "suitability_assessment"


def test_document_class_dominates_pattern_preference_across_documents():
    # notice 3509643 - characterisation: find_landing is document-dominant by design
    espd = doc("ESPD Template.docx", ["3. Selection Criteria"], orig_idx=0)
    itt = doc("ITT.docx", ["4.1 Annual Turnover"], orig_idx=1)
    got = m.find_landing([espd, itt])
    assert got["doc"] == "ITT.docx"
    assert got["pattern"] == "turnover"


# =================================================================== BEHAVIOUR 3
# section_3_3 demotion: corroboration only, never a landing target.


def test_section_3_3_is_not_a_landing_pattern():
    # notice 6234281 - v1 landed on `3.3 Reference 3:`
    assert "section_3_3" not in m.LAND_RANK
    assert ("section_3_3", m.PAT_BY_TAG["section_3_3"], "corrob") in m.PATTERNS


def test_bare_section_number_only_yields_no_landing_at_all():
    # notice 5455040 - the only hit was the literal line `3.3.` with no text after it
    d = doc("Tender Document.pdf", ["3.3.", "Please refer to the appendices."])
    hits = m.scan(d["parts"][0][1])
    assert "section_3_3" in hits, "the corroboration pattern should still fire on `3.3.`"
    assert m.find_landing([d]) is None


def test_section_3_3_corroborates_but_a_land_pattern_takes_the_landing():
    # notice 6234281 - `3.3 Reference 3:` must be demoted to corroboration
    d = doc("ITT.docx", ["3.3 Reference 3:", "Name of client organisation", "6. Selection Criteria"])
    got = m.find_landing([d])
    assert got["pattern"] == "selection_criteria"
    assert got["heading"] == "6. Selection Criteria"
    assert "section_3_3" in got["corroboration"]


def test_section_3_3_hits_inside_an_award_scoring_table_never_win():
    # notice 6234281 template family - `3.3.1`/`3.3.2` fire inside the AWARD scoring table
    d = doc("ITT.docx", ["3.3.1 Price - 40 marks", "3.3.2 Quality - 60 marks", "5. Selection Criteria"])
    assert m.find_landing([d])["pattern"] == "selection_criteria"


# =================================================================== BEHAVIOUR 4
# added patterns fire, but rank below the four named patterns.


@pytest.mark.parametrize(
    "tag,text",
    [
        ("technical_professional_ability", "Technical and Professional Ability"),
        ("technical_professional_ability", "Technical & Professional Abilities"),
        ("part_iii_exclusion", "Part III: Exclusion Grounds"),
        ("part_iii_exclusion", "Exclusion Grounds"),
        ("turnover", "Turnover"),
        ("turnover", "4.1 Annual Turnover"),
        ("turnover", "Minimum Turnover Requirement"),
        ("economic_financial_standing", "Economic and Financial Standing"),
        ("economic_financial_standing", "Economic & Financial Standing"),
        ("economic_financial_standing", "FINANCIAL & ECONOMIC STANDING"),
        ("economic_financial_standing", "Financial and Economic Standing"),
    ],
)
def test_added_patterns_fire(tag, text):
    # notice 3509643 - the v1 pattern set missed the ampersand and reversed variants
    assert tag in m.scan(lines([text])), f"{tag!r} did not fire on {text!r}"


def test_v1_pattern_set_missed_the_ampersand_form_that_the_shipped_set_catches():
    # notice 3509643 - the v1 `economic and financial standing` regex is "and"-only
    v1_rx = dict(m.V1_STRICT)["economic_financial_standing"]
    assert v1_rx.search("Economic & Financial Standing") is None
    assert m.PAT_BY_TAG["economic_financial_standing"].search("Economic & Financial Standing")


def test_added_patterns_rank_below_the_four_named_patterns_in_a_real_landing():
    # notice 3509643 - an added pattern must not outrank Part IV inside the same document
    d = doc("ITT.docx", ["Technical and Professional Ability", "Part IV"])
    assert m.find_landing([d])["pattern"] == "part_iv_espd"


def test_turnover_ranks_below_technical_professional_ability():
    # notice 3509643 - standalone Turnover is the weakest added landing pattern
    d = doc("ITT.docx", ["4.1 Annual Turnover", "Technical and Professional Ability"])
    assert m.find_landing([d])["pattern"] == "technical_professional_ability"


def test_ampersand_economic_standing_can_be_the_landing_point_end_to_end():
    # notice 3509643 - an ampersand heading must be reachable as a landing target
    d = doc("ITT.docx", ["3. FINANCIAL & ECONOMIC STANDING", "Tenderers shall demonstrate adequate resources."])
    got = m.find_landing([d])
    assert got["pattern"] == "economic_financial_standing"
    assert got["heading"] == "3. FINANCIAL & ECONOMIC STANDING"


# =================================================================== BEHAVIOUR 5
# table-of-contents avoidance.


def test_toc_dot_leader_echo_is_rejected_for_the_real_later_heading():
    # notice 3509643 - a contents-page echo teaches a bidder nothing
    texts = padded({3: "3. Selection Criteria ..................... 12", 40: "3. Selection Criteria"}, total=50)
    got = m.find_landing([doc("ITT.pdf", texts)])
    assert got["line_idx"] == 40
    assert got["heading"] == "3. Selection Criteria"
    assert got["toc_skipped"] >= 1


def test_toc_early_duplicate_echo_is_rejected():
    # notice 3509643 - a heading repeated near the front of the document is the contents page
    texts = padded({5: "Selection Criteria", 60: "Selection Criteria"}, total=100)
    assert m.find_landing([doc("ITT.pdf", texts)])["line_idx"] == 60


def test_toc_styled_paragraph_is_rejected():
    # notice 3509643 - Word contents entries carry a `TOC 1` style
    texts = padded({4: "Selection Criteria", 70: "Selection Criteria"}, total=100)
    got = m.find_landing([doc("ITT.docx", texts, styles={4: "TOC 1", 70: "Heading 2"})])
    assert got["line_idx"] == 70
    assert got["style"] == "Heading 2"


@pytest.mark.xfail(
    strict=True,
    reason=(
        "DEFECT: is_toc_line's early_duplicate rule is gated on ln.idx/total < 0.20, so a "
        "contents entry sitting past the first fifth of the line pool is not recognised as an "
        "echo. PDF text extraction routinely puts the page number on its own line, leaving the "
        "entry with no dot leader and no trailing number, so this positional gate is the only "
        "defence left. A long contents list in a 200-line document crosses 20% and wins."
    ),
)
def test_toc_echo_past_the_20_percent_gate_is_still_rejected():
    # notice 3509643 - rule 5 as stated is positional-free: a repeat-earlier echo must never win
    texts = padded({45: "5. Selection Criteria", 120: "5. Selection Criteria"}, total=200)
    assert m.find_landing([doc("ITT.pdf", texts)])["line_idx"] == 120


def test_is_toc_line_reasons_are_reported():
    # notice 3509643 - the reason must be auditable, not just a silent skip
    flag, why = m.is_toc_line(lines(["Selection Criteria ......... 4"])[0], 50, {})
    assert flag is True
    assert why == "dot_leader"


# =================================================================== BEHAVIOUR 6
# heading shape beats sentence fragments.

FRAGMENT = "for exclusion or the fulfilment of the selection criteria, has not"


def test_heading_shaped_rejects_the_real_mid_sentence_fragment():
    # notice 6234281 - this exact fragment passed the naive "<130 chars, no full stop" test
    assert m.headingish(FRAGMENT) is True, "the naive heading test still accepts it"
    assert m.heading_shaped(FRAGMENT) is False
    assert m.heading_shaped("5. Selection Criteria") is True


def test_numbered_heading_beats_sentence_fragment_of_the_same_pattern():
    # notice 6234281 - preferring selection_criteria first landed on the fragment
    got = m.find_landing([doc("ITT.docx", [FRAGMENT, "5. Selection Criteria"])])
    assert got["heading"] == "5. Selection Criteria"
    assert got["prose_skipped"] >= 1


def test_fragment_only_pattern_falls_through_to_the_next_best_pattern():
    # notice 6234281 - a pattern that only ever occurs mid-sentence is not a section to link to
    got = m.find_landing([doc("ITT.docx", [FRAGMENT, "4. Economic and Financial Standing"])])
    assert got["pattern"] == "economic_financial_standing"
    assert got["heading"] == "4. Economic and Financial Standing"
    assert "degraded" not in got


def test_fragment_only_document_loses_to_a_document_with_a_real_heading():
    # notice 6234281 - a real heading elsewhere beats a degraded fallback
    a = doc("ITT Volume 1.docx", [FRAGMENT], orig_idx=0)
    b = doc("ITT Volume 2.docx", ["5. Selection Criteria"], orig_idx=1)
    got = m.find_landing([a, b])
    assert got["doc"] == "ITT Volume 2.docx"
    assert "degraded" not in got


def test_fragment_only_everywhere_is_returned_but_flagged_degraded():
    # notice 6234281 - the fallback must announce that it is not a heading
    got = m.find_landing([doc("ITT.docx", [FRAGMENT])])
    assert got is not None
    assert got["degraded"] == "no_heading_shaped_occurrence"


# =================================================================== confidence + deep link


def test_confidence_is_high_only_for_a_styled_selection_criteria_landing():
    # the audit measured very different reliability per pattern: the top band needs BOTH signals
    assert m.confidence_band("selection_criteria", True) == "high"
    assert m.confidence_band("selection_criteria", False) == "medium"
    assert m.confidence_band("turnover", True) == "medium"
    assert m.confidence_band("turnover", False) == "low"


def test_landing_row_carries_the_band_and_the_style_flag():
    from datetime import UTC, datetime

    texts = padded({70: "3. Selection Criteria"}, total=100)
    landing = m.find_landing([doc("ITT.docx", texts, styles={70: "Heading 2"})])
    assert landing["has_style"] is True
    row = m.landing_row("3509643", landing, datetime.now(UTC))
    assert row["confidence"] == "high"
    assert row["has_style"] is True
    assert set(row) == set(m.ROW_SCHEMA)


def test_download_url_keeps_the_cft_segment():
    # verified by curl 2026-08-16: without `/cft/` the server returns HTTP 500, not a redirect
    url = m.document_url("3509643", "3520718")
    assert "/epps/cft/downloadContractDocument.do" in url
    assert url.endswith("?documentId=3520718&resourceId=3509643")


# =================================================================== determinism


def test_landing_is_deterministic_across_identical_runs():
    # notice 3509643 - a deep link must resolve to the same anchor on every rebuild
    def build():
        return [
            doc("ESPD Template.docx", padded({2: "Part IV: Selection Criteria"}, 40), 0),
            doc(
                "ITT - Main.docx",
                padded(
                    {
                        3: "3. Selection Criteria .......... 9",
                        25: "3. Selection Criteria",
                        30: "Technical and Professional Ability",
                    },
                    60,
                ),
                1,
            ),
        ]

    first = m.find_landing(build())
    assert m.find_landing(build()) == first
    assert m.find_landing(build()) == first


# =================================================================== no matches


def test_document_with_zero_matches_does_not_crash():
    # notice 5455040 - a notice whose documents hold nothing must yield None, not raise
    d = doc("Pricing Schedule.xlsx", [FILLER.format(i) for i in range(30)])
    assert m.scan(d["parts"][0][1]) == {}
    assert m.find_landing([d]) is None


def test_empty_document_list_and_empty_line_pool_do_not_crash():
    # notice 5455040 - an unresolved notice reaches the matcher with nothing extracted
    assert m.find_landing([]) is None
    assert m.find_landing([doc("Empty.pdf", [])]) is None


# =================================================================== importability


def test_module_is_importable_without_side_effects():
    # productionisation gate: `main()` must stay behind the __main__ guard
    src = Path(m.__file__).read_text(encoding="utf-8")
    assert 'if __name__ == "__main__":' in src
    assert callable(m.main)
    # heavy optional extractors are imported lazily inside functions, not at module scope
    for lazy in ("docx", "fitz", "openpyxl"):
        assert lazy not in vars(m), f"{lazy} imported at module scope"


def test_pick_line_raises_on_an_empty_candidate_list():
    # latent defect, unreachable from find_landing today: pick_line indexes scored[0] blind
    with pytest.raises(IndexError):
        m.pick_line([], 10)
