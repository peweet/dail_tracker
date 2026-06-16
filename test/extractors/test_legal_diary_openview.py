"""Parser + privacy tests for the Legal Diary OpenView extractor.

Locks extractors/legal_diary_openview_extract.py: the HTML detail parser (three layouts
+ panels), the solicitor-tail strip, and — sharing the .docx anonymiser verbatim — the
residual-name privacy gate over the committed OpenView gold parquet. All names below are
invented; no real party data lives in this file.

Run:  pytest test/extractors/test_legal_diary_openview.py -v
"""

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))

from extractors.legal_diary_extract import anonymise, residual_name_tokens  # noqa: E402
from extractors.legal_diary_openview_extract import (  # noqa: E402
    court_and_meta,
    detail_lines,
    parse_detail,
    parse_meta,
)

OPENVIEW_GOLD = _ROOT / "data" / "gold" / "parquet" / "judicial_legal_diary_openview_cases.parquet"
CONTRACT_COLS = {
    "court",
    "venue",
    "diary_date",
    "judge",
    "courtroom",
    "time",
    "status",
    "list_type",
    "panel_size",
    "category",
    "case_anonymised",
    "plaintiff",
    "defendant",
    "plaintiff_kind",
    "source",
    "source_url",
}
FORBIDDEN_COLS = {"raw_case", "party", "parties", "solicitor", "solicitors"}


def _doc(meta_cells: str, content: str) -> str:
    return f'<div class="row alfresco-properties">{meta_cells}</div><div class="ld-content">{content}</div>'


def _cell(title: str, value: str) -> str:
    return f'<div class="cell"><span class="cell-title">{title}</span>{value}</div>'


# ───────────────────────────────────────────────── metadata
def test_parse_meta_reads_cells():
    html = _doc(_cell("Date", "17th June 2026") + _cell("Updated", "16th June 2026"), "x")
    meta = parse_meta(html)
    assert meta["Date"] == "17th June 2026"
    assert meta["Updated"] == "16th June 2026"


def test_court_and_meta_circuit_venue_and_date():
    html = _doc(
        _cell("Circuit Court", "Galway") + _cell("Category", "Criminal") + _cell("Date", "7th December 2026"), "x"
    )
    court, ctx = court_and_meta("circuit-court", parse_meta(html))
    assert court == "Circuit Court"
    assert ctx["venue"] == "Galway"
    assert ctx["diary_date"] == "2026-12-07"


def test_court_and_meta_appeal_criminal_split():
    html = _doc(_cell("Category", "Criminal") + _cell("Date", "1st May 2026"), "x")
    court, _ = court_and_meta("court-of-appeal", parse_meta(html))
    assert court == "Court of Appeal (Criminal)"


# ───────────────────────────────────────────────── packed (higher-court) layout
def test_packed_central_criminal_layout():
    content = (
        "Before Mr. Justice Inventedname in Courtroom 06 at 10:15 (For Mention)<br/>"
        "1\tCCDP0158/2022\tDPP -v- John Doefield<br/>"
    )
    rows = parse_detail(
        detail_lines(_doc(_cell("Date", "17th June 2026"), content)),
        "Central Criminal Court",
        {"diary_date": "2026-06-17"},
        "u",
    )
    assert len(rows) == 1
    r = rows[0]
    assert r["judge"] == "Mr. Justice Inventedname"
    assert r["courtroom"] == "06"
    assert r["status"] == "For Mention"
    # accused anonymised, no record reference survives
    assert residual_name_tokens(anonymise(r["raw_case"])) == []


# ───────────────────────────────────────────────── circuit civil + solicitor tail
def test_circuit_drops_solicitor_tail():
    content = (
        "Before Judge Inventedjudge<br/>At 10:30 Am<br/>Circuit Court<br/>"
        "1\t2024/00024\tMary Plaintiffield - V - Acme Gardenhouse Irl Limited\t : \tConnolly Solicitors\t / \tOgorman Sols<br/>"
    )
    rows = parse_detail(
        detail_lines(_doc(_cell("Circuit Court", "Ennis"), content)),
        "Circuit Court",
        {"venue": "Ennis", "list_type": "Civil Hearings"},
        "u",
    )
    assert len(rows) == 1
    r = rows[0]
    assert r["judge"] == "Judge Inventedjudge"
    # the party cell is isolated — solicitors must NOT appear in the raw case
    assert "Connolly" not in r["raw_case"] and "Ogorman" not in r["raw_case"]
    assert "Acme Gardenhouse" in r["raw_case"]


# ───────────────────────────────────────────────── panel (Supreme / Appeal)
def test_panel_joined_and_sized():
    content = (
        "(In the Supreme Court)<br/>Mr. Justice Aardvark<br/>Ms. Justice Beewell<br/>"
        "At 10.15 o'clock<br/>75/2025 Howley v McClean<br/>"
    )
    rows = parse_detail(
        detail_lines(_doc(_cell("Date", "17th June 2026"), content)), "Supreme Court", {"diary_date": "2026-06-17"}, "u"
    )
    assert len(rows) == 1
    r = rows[0]
    assert r["judge"] == "Mr. Justice Aardvark & Ms. Justice Beewell"
    assert r["panel_size"] == 2


# ───────────────────────────────────────────────── live gold parquet (integration)
@pytest.mark.integration
def test_openview_gold_contract_and_zero_residual_names():
    pl = pytest.importorskip("polars")
    if not OPENVIEW_GOLD.exists():
        pytest.skip(f"{OPENVIEW_GOLD} not built; run the OpenView poller + extractor first")
    df = pl.read_parquet(OPENVIEW_GOLD)
    cols = set(df.columns)
    assert cols == CONTRACT_COLS, f"openview gold columns drifted: {cols ^ CONTRACT_COLS}"
    assert not (FORBIDDEN_COLS & cols), f"raw-name column in gold: {FORBIDDEN_COLS & cols}"
    offenders = [
        (col, c, residual_name_tokens(c))
        for col in ("case_anonymised", "plaintiff", "defendant")
        for c in df.get_column(col).to_list()
        if residual_name_tokens(c)
    ]
    assert not offenders, f"{len(offenders)} openview gold cells leak names, e.g. {offenders[:5]}"
