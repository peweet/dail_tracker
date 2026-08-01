"""Tests for extractors/council_decisions_extract.py (graduated from the sandbox 2026-08-01).

Pins the motion extraction (proposer/seconder/outcome) and the filename-date parser the
gold agendas path also leans on.
"""

from __future__ import annotations

from extractors.council_decisions_extract import _fname_date, extract_doc

MINUTES = """
ITEM NO. 4 Housing Report
On the PROPOSAL of Cllr. Anne Aaa SECONDED by Cllr. Brian Bbb it was resolved that the
report be adopted. AGREED.
Result: 11 For, 7 Against
"""


def test_extract_doc_motion_fields() -> None:
    rows = extract_doc(MINUTES, {"local_authority": "Testshire", "meeting": "minutes_12_03_2024.pdf"})
    assert len(rows) == 1
    r = rows[0]
    assert r["proposer"].startswith("Anne Aaa")
    assert r["seconder"].startswith("Brian Bbb")
    assert r["outcome"] == "AGREED"
    assert (r["tally_for"], r["tally_against"]) == (11, 7)
    assert r["meeting_date"] == "2024-03-12"


def test_fname_date_formats() -> None:
    assert _fname_date("minutes_2025_01_20.pdf") == "2025-01-20"
    assert _fname_date("plenary_08_02_2024.pdf") == "2024-02-08"
    assert _fname_date("Minutes%20February%202026.pdf") == "2026 February"
    assert _fname_date("no_date_here.pdf") == ""
