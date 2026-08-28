"""Fixture tests for tools/paper_findings.py -- Stage 1 of the paper-findings
pipeline. Exercises the deterministic assembly (band capping, figure/next-
paragraph passthrough, schema shape) against hand-written paragraph blocks
with known right answers. Never touches a real paper or an LLM -- the claim
text is supplied directly, standing in for whatever produced it.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "tools"))

import paper_findings as pf  # noqa: E402


def _paragraph(**overrides) -> dict:
    block = {
        "type": "paragraph",
        "page": 3,
        "bbox": [10, 20, 300, 40],
        "text": "Omission compliance falls from 73% at turn 5 to 20% at turn 25.",
        "band": "Verified",
        "related_figure": None,
        "possible_ocr_corruption": False,
        "next_paragraph_text": None,
    }
    block.update(overrides)
    return block


# --- build_finding ----------------------------------------------------------


def test_finding_with_claim_gets_extracted_band():
    block = _paragraph()
    result = pf.build_finding(block, "Omission compliance decays sharply with turn depth.", "srd.pdf")
    assert result["finding"] == "Omission compliance decays sharply with turn depth."
    assert result["band"] == "Extracted"
    assert result["page"] == 3
    assert result["citation"] == "srd.pdf p.3"
    assert result["paragraph_excerpt"].startswith("Omission compliance falls")


def test_no_claim_means_no_finding_and_no_band():
    block = _paragraph(text="References\n[1] Liu et al., 2024.")
    result = pf.build_finding(block, None, "srd.pdf")
    assert result["finding"] is None
    assert result["band"] is None


def test_ocr_corruption_downgrades_band_to_indicative():
    block = _paragraph(possible_ocr_corruption=True)
    result = pf.build_finding(block, "A claim read from a possibly-mangled paragraph.", "srd.pdf")
    assert result["band"] == "Indicative"


def test_related_figure_passes_through_unchanged():
    block = _paragraph(related_figure="p2_fig1")
    result = pf.build_finding(block, "A claim near a figure.", "srd.pdf")
    assert result["related_figure"] == "p2_fig1"


def test_no_related_figure_stays_none():
    block = _paragraph(related_figure=None)
    result = pf.build_finding(block, "A claim with no nearby figure.", "srd.pdf")
    assert result["related_figure"] is None


def test_next_paragraph_excerpt_passes_through():
    block = _paragraph(next_paragraph_text="The mechanism is attention dilution.")
    result = pf.build_finding(block, "A claim.", "srd.pdf")
    assert result["next_paragraph_excerpt"] == "The mechanism is attention dilution."


def test_rejects_non_paragraph_block():
    gap_block = {"type": "page_gap", "page": 9, "band": "Indicative", "reason": "scanned"}
    with pytest.raises(ValueError, match="paragraph"):
        pf.build_finding(gap_block, "shouldn't matter", "srd.pdf")


# --- extract_findings --------------------------------------------------------


def test_extract_findings_pairs_by_paragraph_position_not_block_index():
    blocks = {
        "source": "some/path/srd.pdf",
        "blocks": [
            {"type": "page_gap", "page": 1, "band": "Indicative", "reason": "scanned"},
            _paragraph(page=2, text="First real paragraph."),
            _paragraph(page=3, text="Second real paragraph.", possible_ocr_corruption=True),
        ],
    }
    claims = {"0": "Finding about the first paragraph.", "1": "Finding about the second."}

    findings = pf.extract_findings(blocks, claims, "srd.pdf")

    assert len(findings) == 2  # the page_gap block is not a paragraph and is skipped
    assert findings[0]["page"] == 2
    assert findings[0]["finding"] == "Finding about the first paragraph."
    assert findings[0]["band"] == "Extracted"
    assert findings[1]["page"] == 3
    assert findings[1]["band"] == "Indicative"  # corruption-flagged


def test_extract_findings_missing_claim_key_defaults_to_no_finding():
    blocks = {"source": "srd.pdf", "blocks": [_paragraph()]}
    findings = pf.extract_findings(blocks, claims={}, paper_name="srd.pdf")
    assert findings[0]["finding"] is None
    assert findings[0]["band"] is None
