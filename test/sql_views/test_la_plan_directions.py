"""Guards for the OPR/Ministerial-Direction fact (councillors' plan+zoning votes overruled).

The load-bearing rules this pins:
  * it is NOT an overrides counter — the `minister_declined` outcome (councillors UPHELD) must
    survive, or the feature would misrepresent the process;
  * the council key joins the canonical 31;
  * the classifier never regresses to the domain-match bug that once made every Minister's
    Direction look like an OPR document (every URL is on opr.ie).
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "extractors"))

from opr_plan_directions_extract import CANON_31, canon_council, classify, doc_date  # noqa: E402

CSV_PATH = ROOT / "data" / "_meta" / "opr_plan_directions.csv"
VALID_OUTCOMES = {"direction_issued", "minister_declined", "in_progress", "suspension_notice"}


# ── classifier (pure) ──────────────────────────────────────────────────────────────────────────
def test_classify_never_reads_the_domain():
    # REGRESSION: every document is hosted on www.opr.ie, so a naive "opr in url" test classified
    # every Minister's Direction as an OPR document and produced ZERO directions.
    assert (
        classify(
            "Minister's Direction Clare CDP 2023-2029",
            "https://www.opr.ie/wp-content/uploads/2023/08/2023.08.03-S.-31-Final-Direction-Letter-to-CE-Clare.pdf",
        )
        == "minister_final_direction"
    )


def test_classify_stages():
    o = "https://www.opr.ie/wp-content/uploads/2023/04/x.pdf"
    assert classify("OPR Proposed draft Direction Clare CDP", o) == "opr_proposed_draft"
    assert classify("Minister's draft Direction Clare CDP", o) == "minister_draft_direction"
    assert classify("OPR Proposed Direction Clare CDP", o) == "opr_proposed_final"
    assert classify("Minister's Direction Clare CDP", o) == "minister_final_direction"
    assert classify("Minister's Statement of Reasons Clare CDP", o) == "statement_of_reasons"


def test_classify_decline_beats_draft_direction():
    # "Decision not to issue draft Direction" also contains "draft Direction" — decline must win,
    # or a council the Minister REFUSED to overrule would be counted as overruled.
    o = "https://www.opr.ie/wp-content/uploads/2026/04/x.pdf"
    assert classify("Minister's Decision not to issue draft Direction Variation 5 Kilkenny", o) == "minister_declined"
    assert classify("Minister's decision not to agree with recommendation to issue direction Sligo", o) == "minister_declined"


def test_classify_suspension_notice():
    assert (
        classify(
            "Kilkenny City and County Development Plan 2021-2027 (Castlecomer)",
            "https://www.opr.ie/wp-content/uploads/2026/06/2026.04.22-OPR-Notice-pursuant-to-section-636-Kilkenny-CDP.pdf",
        )
        == "suspension_notice"
    )


def test_canon_council_from_plan_titles():
    assert canon_council("Clare County Development Plan 2023-2029") == "Clare"
    assert canon_council("Cork City Development Plan 2022-2028") == "Cork City"
    assert canon_council("Cork County Development Plan 2022-2028") == "Cork County"
    assert canon_council("Dún Laoghaire-Rathdown County Development Plan 2022-2028") == "Dun Laoghaire-Rathdown"
    assert canon_council("South Dublin County Development Plan 2022-2028") == "South Dublin"
    # Local Area Plans are named for a TOWN, not the council
    assert canon_council("Athenry Local Area Plan 2024-2030") == "Galway County"
    assert canon_council("Kenmare Municipal District Local Area Plan 2024-2030") == "Kerry"
    assert canon_council("Letterkenny Local Area Plan and Local Transport Plan 2023-2029") == "Donegal"


def test_doc_date_from_upload_path():
    assert doc_date("https://www.opr.ie/wp-content/uploads/2023/08/x.pdf") == "2023-08"
    assert doc_date("https://www.opr.ie/no-date/x.pdf") == ""


# ── the fact ───────────────────────────────────────────────────────────────────────────────────
@pytest.fixture(scope="module")
def rows() -> list[dict]:
    assert CSV_PATH.exists(), "opr_plan_directions.csv missing"
    with open(CSV_PATH, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def test_councils_are_canonical(rows):
    assert {r["local_authority"] for r in rows} <= set(CANON_31)


def test_outcomes_are_known(rows):
    assert {r["plan_outcome"] for r in rows} <= VALID_OUTCOMES


def test_not_an_overrides_counter(rows):
    """The honesty rail: the register records the Minister REFUSING to overrule councillors
    (Sligo CDP 2024-2030; Kilkenny Variation 5). If this ever hits zero, either the source
    changed or the classifier regressed — and the feature would be misrepresenting the process."""
    declined = {r["plan_name"] for r in rows if r["plan_outcome"] == "minister_declined"}
    assert declined, "no minister_declined outcomes — the feature would read as overrides-only"


def test_directions_present_and_plausible(rows):
    plans = {(r["local_authority"], r["plan_name"]): r["plan_outcome"] for r in rows}
    issued = [p for p, o in plans.items() if o == "direction_issued"]
    assert len(issued) >= 20  # 28 at build time; a big drop means the parse broke
    assert len(plans) >= 30


def test_every_plan_has_a_document_trail(rows):
    from collections import Counter

    per_plan = Counter((r["local_authority"], r["plan_name"]) for r in rows)
    assert all(n >= 1 for n in per_plan.values())
    assert all(r["doc_url"].startswith("https://www.opr.ie/") for r in rows)
