"""Unit tests for extractors/sipo_candidate_expenses_extract.py (the per-candidate
GE2024 SIPO election-expense parser) and its gold aggregation.

The extractor is two-stage: PaddleOCR renders each page to cell dicts
({text, score, x0, y0, x1, y1}); a PURE geometry parser turns cells → rows. The OCR
stage is irreducible I/O — everything tested here is pure and driven with synthetic
cells (no PDF, no OCR). The headline case is the OCR decimal-GLYPH bug: the recogniser
captures '€10,270.50' at >0.99 conf but renders the decimal mark as a hyphen
('10270-50'); parse_money must recover 10270.50 deterministically (NOT re-OCR, NOT a
/100 heuristic). See [[project_sipo_candidate_expenses_corpus]].
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT / "extractors"))

from sipo_candidate_expenses_extract import (  # noqa: E402
    CAT_LABELS,
    STATUTORY_MAX_EUR,
    canon_party_expr,
    page_type,
    parse_grid,
    parse_items,
    parse_money,
    party_from_p1,
)


def _cell(text, x0, y, *, score=1.0, w=80, h=40):
    return {"text": text, "score": score, "x0": x0, "y0": y, "x1": x0 + w, "y1": y + h}


# ── parse_money: the OCR decimal-glyph bug zone ──────────────────────────────


@pytest.mark.parametrize(
    "text,expected",
    [
        ("10270-50", 10270.50),       # decimal mark OCR'd as a hyphen — THE bug
        ("2327-90", 2327.90),         # ditto, verified vs raw cell O'Donoghue C1
        ("€2,799.48", 2799.48),       # Grealish → Galway Advertiser, clean
        ("€615.00", 615.0),
        ("1.845.00", 1845.00),        # comma read as period (OCR), 2dp tail wins
        ("€2,000", 2000.0),           # thousands sep, whole euros
        ("174.98", 174.98),
        ("4", 4.0),                   # bare integer
        ("€1,234-56", 1234.56),       # mixed thousands ',' + hyphen decimal
    ],
)
def test_parse_money_recovers_decimal_glyphs(text, expected):
    assert parse_money(text) == expected


def test_parse_money_none_for_non_numeric():
    assert parse_money("nil") is None
    assert parse_money("Galway Advertiser") is None
    assert parse_money("€") is None
    assert parse_money("") is None


def test_decimal_fix_pulls_value_under_statutory_cap():
    """The fix is what moves a high-confidence figure back under the cap so it is NOT
    flagged cost_suspect — whereas genuine multi-cell garble stays above the cap."""
    assert parse_money("10270-50") <= STATUTORY_MAX_EUR          # recovered → clean
    assert parse_money("28784724120") > STATUTORY_MAX_EUR        # garble → stays suspect


# ── page_type: twin summary page classification ──────────────────────────────


def test_page_type_distinguishes_public_twin():
    not_pub = [_cell("Expenses not met out of public funds", 300, 200)]
    pub = [_cell("Expenses met out of public funds", 300, 200)]
    assert page_type(not_pub) == "not_public"
    assert page_type(pub) == "public"
    assert page_type([_cell("Candidate details", 300, 200)]) is None


# ── parse_grid: 5A–5H label → value pairing + overall reconciliation ─────────


def test_parse_grid_pairs_categories_and_overall():
    cells = [
        _cell("5A -", 100, 100), _cell("€1,000.00", 1000, 100),
        _cell("5B -", 100, 200), _cell("€500.00", 1000, 200),
        _cell("Overall Expense total:", 100, 900), _cell("€1,500.00", 1000, 900),
    ]
    grid, overall = parse_grid(cells)
    assert grid["5A"] == 1000.0
    assert grid["5B"] == 500.0
    assert grid["5C"] is None
    assert overall == 1500.0


# ── parse_items: Part-5 line items (Ref | detail | cost) ─────────────────────


def test_parse_items_extracts_ref_detail_cost():
    pages = {
        "c003": [
            _cell("A1", 200, 100), _cell("Galway Advertiser", 600, 100), _cell("€2,799.48", 1600, 100),
            _cell("A2", 200, 200), _cell("Connacht Tribune", 600, 200), _cell("€2,103.30", 1600, 200),
            # a subtotal row: bare 'A' + Total — must NOT match the Ref regex → skipped
            _cell("A", 200, 300), _cell("Total:", 600, 300), _cell("€4,902.78", 1600, 300),
        ]
    }
    items = parse_items(pages)
    assert len(items) == 2  # subtotal row filtered out
    a1 = next(i for i in items if i["ref"] == "A1")
    assert a1["category"] == "5A"
    assert a1["detail"] == "Galway Advertiser"
    assert a1["cost_eur"] == 2799.48
    assert a1["source_page"] == "c003"


def test_parse_items_keeps_costless_detail_out():
    pages = {"c004": [_cell("B1", 200, 100), _cell("Brendan Carroll", 600, 100)]}  # no cost cell
    assert parse_items(pages) == []


# ── party_from_p1 + label table sanity ───────────────────────────────────────


def test_party_from_p1_reads_declared_party():
    p1 = [_cell("Political Party", 320, 1280), _cell("Non-Party", 900, 1280)]
    assert party_from_p1(p1) == "Non-Party"


def test_category_labels_cover_all_eight():
    assert set(CAT_LABELS) == {f"5{c}" for c in "ABCDEFGH"}
    assert CAT_LABELS["5A"] == "Advertising"
    assert CAT_LABELS["5C"] == "Election Posters"


# ── canon_party_expr: canonicalise OCR'd party, NULL for placeholder/garbage ──


def test_canon_party_canonicalises_and_nulls_unknown():
    import polars as pl

    raw = [
        "Fianna Fáil", "Fianna Fail", "FIANNA FAIL",   # accent/case variants -> one party
        "Fine Gael", "FINE GAEL.",
        "Sinn Féin", "Sinn Fein",
        "Independent Ireland",                          # a PARTY, not generic independent
        "Non party", "NON-PARTY", "Independant",        # all -> Non-Party
        "Aontu", "AONTÚ",
        "Click here to enter text.",                    # MS-Word placeholder -> NULL
        "Tel: (01) 639 5666",                           # SIPO footer -> NULL
        "Marie Sherlock",                               # mis-grabbed candidate name -> NULL
        None,
    ]
    out = (
        pl.DataFrame({"party_declared": raw})
        .with_columns(canon_party_expr())["party"]
        .to_list()
    )
    assert out == [
        "Fianna Fáil", "Fianna Fáil", "Fianna Fáil",
        "Fine Gael", "Fine Gael",
        "Sinn Féin", "Sinn Féin",
        "Independent Ireland",
        "Non-Party", "Non-Party", "Non-Party",
        "Aontú", "Aontú",
        None, None, None, None,
    ]


# ── data-quality invariants on the actual outputs (skip if not built) ────────

_GOLD = _ROOT / "data/gold/parquet"
_FACT = _GOLD / "sipo_candidate_expenses_fact.parquet"
_ITEMS = _GOLD / "sipo_candidate_expense_items.parquet"
_UNQUANT = _GOLD / "sipo_candidate_expenses_unquantified.parquet"


@pytest.mark.skipif(not _FACT.exists(), reason="gold candidate fact not built")
def test_gold_fact_within_statutory_cap_and_no_pii():
    import polars as pl

    df = pl.read_parquet(_FACT)
    # every served candidate total is plausible (decimal-lost pages are excluded)
    assert df.filter(pl.col("total_spend_eur") > STATUTORY_MAX_EUR).height == 0
    assert df.filter(pl.col("total_suspect")).height == 0
    # no address/PII column ever reaches committed gold
    assert [c for c in df.columns if "address" in c.lower()] == []


@pytest.mark.skipif(not (_UNQUANT.exists() and _FACT.exists()), reason="gold not built")
def test_filed_unquantified_carries_no_amount_and_partitions_the_fact():
    """The filed-but-unquantified fact must (a) never leak a spend figure (showing a
    corrupt/blank total would be a fabricated number), and (b) be DISJOINT from the
    quantified fact — together they are every filed candidate, counted once."""
    import polars as pl

    uq = pl.read_parquet(_UNQUANT)
    fact = pl.read_parquet(_FACT)
    # (a) no monetary / amount / address column escapes into this fact
    leak = [c for c in uq.columns
            if any(t in c.lower() for t in ("eur", "spend", "amount", "total", "address"))]
    assert leak == [], f"unquantified fact must carry no figure, found {leak}"
    # status is one of the two documented reasons, never blank
    assert set(uq["filed_status"].unique()) <= {"no_total_declared", "figures_unreadable"}
    # (b) disjoint from the served fact, on the candidate+constituency identity
    key = ["candidate_name", "constituency_name"]
    overlap = uq.select(key).join(fact.select(key), on=key, how="inner")
    assert overlap.height == 0, f"a candidate is in BOTH facts: {overlap.to_dicts()}"


@pytest.mark.skipif(not _ITEMS.exists(), reason="gold line items not built")
def test_gold_items_exclude_suspect_costs():
    import polars as pl

    df = pl.read_parquet(_ITEMS)
    assert df.filter(pl.col("cost_suspect")).height == 0  # suspects filtered from gold
    assert df["cost_eur"].max() <= STATUTORY_MAX_EUR


_SILVER_FACT = _ROOT / "data/silver/sipo_candidate/sipo_candidate_expenses.parquet"


@pytest.mark.skipif(not _SILVER_FACT.exists(), reason="silver candidate fact not built")
def test_no_same_constituency_double_filing():
    """A candidate runs in only one constituency, so two statements for the same
    (candidate, constituency) is a double-filing — the extractor must dedupe them."""
    import polars as pl

    df = pl.read_parquet(_SILVER_FACT)
    dups = df.group_by(["candidate_slug", "constituency_slug"]).len().filter(pl.col("len") > 1)
    assert dups.height == 0, f"same-constituency double-filings not deduped: {dups.to_dicts()}"


# ── roster_join: candidate -> sitting-TD linkage (pure, synthetic frames) ────


def test_roster_join_links_elected_and_keeps_unmatched():
    import polars as pl

    sys.path.insert(0, str(_ROOT / "extractors"))
    from sipo_candidate_expenses_aggregate import roster_join

    head = pl.DataFrame({
        "candidate_name": ["Grealish, Noel", "Codd, Jim", "Nobody, Random"],
        "party": ["Non-Party", None, "Aontú"],          # OCR-declared canonical (Codd unknown)
    })
    members = pl.DataFrame({
        "full_name": ["Noel Grealish", "Jim Codd"],
        "unique_member_code": ["Noel-Grealish.D.2002-06-06", "Jim-Codd.D.2024-11-29"],
        "member_party": ["Independent", "Aontú"],         # registry is authoritative
    })
    out = roster_join(head, members).sort("candidate_name")
    by_name = {r["candidate_name"]: r for r in out.iter_rows(named=True)}

    # elected: gets the member code + is_elected_td, and registry party (canonicalised:
    # 'Independent' -> 'Non-Party') fills the unknown OCR party for Codd.
    assert by_name["Codd, Jim"]["unique_member_code"] == "Jim-Codd.D.2024-11-29"
    assert by_name["Codd, Jim"]["is_elected_td"] is True
    assert by_name["Codd, Jim"]["party"] == "Aontú"
    assert by_name["Grealish, Noel"]["unique_member_code"] == "Noel-Grealish.D.2002-06-06"
    assert by_name["Grealish, Noel"]["party"] == "Non-Party"   # 'Independent' canon -> Non-Party
    # unmatched: no code, keeps its declared party (never guessed)
    assert by_name["Nobody, Random"]["unique_member_code"] is None
    assert by_name["Nobody, Random"]["is_elected_td"] is False
    assert by_name["Nobody, Random"]["party"] == "Aontú"
    # 1:1 — no row explosion
    assert out.height == 3


# ── SQL views execute against real gold (registration smoke test) ────────────

_VIEW_SQL = _ROOT / "sql_views/sipo/sipo_candidate_expenses.sql"
_VIEWS = [
    "v_sipo_candidate_expenses",
    "v_sipo_candidate_expenses_filed_unquantified",
    "v_sipo_candidate_expense_items",
    "v_sipo_candidate_expenses_by_party",
    "v_sipo_candidate_expenses_by_category",
    "v_sipo_candidate_top_details",
]


@pytest.mark.skipif(not _FACT.exists(), reason="gold not built")
def test_sipo_candidate_views_execute_and_have_rows():
    import duckdb

    # Resolve the views' relative read_parquet('data/gold/parquet/…') literals to the
    # real gold dir regardless of the test runner's CWD, then register the whole file.
    sql = _VIEW_SQL.read_text(encoding="utf-8").replace(
        "data/gold/parquet/", str(_GOLD).replace("\\", "/") + "/"
    )
    con = duckdb.connect()
    con.execute(sql)
    for v in _VIEWS:
        assert con.execute(f"SELECT COUNT(*) FROM {v}").fetchone()[0] > 0
    # the category rollup must surface exactly the 8 statutory categories
    n_cat = con.execute("SELECT COUNT(*) FROM v_sipo_candidate_expenses_by_category").fetchone()[0]
    assert n_cat == 8
