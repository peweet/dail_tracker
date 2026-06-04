"""DATA-QUALITY tests + scorecard for the SIPO GE2024 candidate-expenses fact (SANDBOX).

Covers the candidate-summary (Part 3) table produced by sipo_expenses_paddle_etl.py:
  - pipeline_sandbox/_sipo_output/sipo_expenses_fact.parquet   (combined, all parties)

Two layers in one file (same shape as test_procurement_data_quality.py):
  1. pytest invariant tests  — hard pass/fail data contracts. SKIPS if the parquet is
     absent (pre-promotion / regenerable). Run:
       ./.venv/Scripts/python.exe -m pytest pipeline_sandbox/test_sipo_data_quality.py -q
  2. `python this_file`       — prints a DQ SCORECARD (0-100 + grade) + an anomaly hunt
     and writes a JSON report to c:/tmp.

These assert the validity bounds the extractor promises (engine-independent QA layer in
doc/SIPO_OCR_INVESTIGATION.md): every constituency is in the closed set of 43; spend and
assigned amounts respect the GE2024 statutory candidate limit; the per-row `flag` honestly
marks the rows that break those bounds. NO OCR is run here — read-only on parquet.
"""

from __future__ import annotations

import contextlib
import json
import sys
from pathlib import Path

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parents[1]
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

FACT = ROOT / "pipeline_sandbox/_sipo_output/sipo_expenses_fact.parquet"
CONSTIT = ROOT / "data/gold/parquet/ec_constituency_pop_2022.parquet"
REPORT = Path("c:/tmp/sipo_dq_report.json")

# GE2024 statutory candidate spending limit per constituency seat-count
# (Electoral Act 1997 as amended; SIPO 2024 GE guidelines).
STATUTORY_LIMIT = {3: 38900, 4: 48600, 5: 58350}

REQUIRED_COLS = {
    "party", "candidate_name_raw", "constituency", "constituency_match_score",
    "amount_assigned_eur", "expenditure_eur", "expenditure_confidence",
    "row_min_confidence", "flag", "source_pdf", "source_page",
}
ALLOWED_FLAGS = {
    "ok", "no_amount", "over_limit_verify", "assigned_over_limit_verify",
    "spend_gt_assigned_verify", "low_confidence_verify",
}
# a party realistically runs at most ~3 candidates in one constituency; >5 (the max
# seat count) signals an OCR constituency mis-snap (the FF "Dublin Bay South"=5 bug).
MAX_CANDIDATES_PER_CONSTIT = 5


def _load() -> pl.DataFrame | None:
    if not FACT.exists():
        return None
    return pl.read_parquet(FACT)


def _seats() -> dict[str, int]:
    c = pl.read_parquet(CONSTIT)
    return dict(zip(c["constituency_name"], c["td_seats_2024"]))


def _valid_constituencies() -> set[str]:
    return set(pl.read_parquet(CONSTIT)["constituency_name"].to_list())


# ----------------------------------------------------------------------------- tests
@pytest.fixture(scope="module")
def df() -> pl.DataFrame:
    d = _load()
    if d is None or d.height == 0:
        pytest.skip("sipo_expenses_fact.parquet absent/empty (pre-extraction)")
    return d


def test_required_columns(df):
    missing = REQUIRED_COLS - set(df.columns)
    assert not missing, f"missing columns: {missing}"


def test_key_fields_not_null(df):
    for col in ("party", "candidate_name_raw", "constituency"):
        n = df.filter(pl.col(col).is_null() | (pl.col(col).cast(str).str.strip_chars() == "")).height
        assert n == 0, f"{n} rows have empty {col}"


def test_constituencies_in_closed_set(df):
    valid = _valid_constituencies()
    bad = df.filter(~pl.col("constituency").is_in(list(valid)))
    assert bad.height == 0, f"{bad.height} rows have off-set constituency: {bad['constituency'].unique().to_list()}"


def test_flags_allowed(df):
    bad = set(df["flag"].unique().to_list()) - ALLOWED_FLAGS
    assert not bad, f"unexpected flag values: {bad}"


def test_confidence_in_range(df):
    for col in ("expenditure_confidence", "row_min_confidence", "constituency_match_score"):
        sub = df.filter(pl.col(col).is_not_null())
        bad = sub.filter((pl.col(col) < 0) | (pl.col(col) > 1))
        assert bad.height == 0, f"{bad.height} rows have {col} outside [0,1]"


def test_expenditure_non_negative(df):
    """Expenditure may legitimately be 0/blank: parties that report spend at NATIONAL
    level (SF, Green) leave the per-candidate Expenditure column empty, which the parser
    records as €0. Only NEGATIVE values are invalid. (The 'is this real spend?' guard is
    test_expenditure_not_misread_assigned, not a >0 floor.)"""
    sub = df.filter(pl.col("expenditure_eur").is_not_null())
    bad = sub.filter(pl.col("expenditure_eur") < 0)
    assert bad.height == 0, f"{bad.height} rows have negative expenditure"


def test_expenditure_within_statutory_limit_unless_flagged(df):
    """Any spend over the seat-count's statutory limit MUST carry over_limit_verify —
    an unflagged over-limit row is a silent bad read."""
    seats = _seats()
    rows = df.filter(pl.col("expenditure_eur").is_not_null()).to_dicts()
    offenders = []
    for r in rows:
        lim = STATUTORY_LIMIT.get(int(seats.get(r["constituency"], 0)))
        if lim and r["expenditure_eur"] > lim and r["flag"] != "over_limit_verify":
            offenders.append((r["party"], r["candidate_name_raw"], r["constituency"], r["expenditure_eur"], lim))
    assert not offenders, f"unflagged over-limit rows: {offenders[:5]}"


def test_assigned_within_statutory_limit(df):
    """Any assigned amount over the seat-count's statutory limit MUST carry
    assigned_over_limit_verify — these are decimal-loss OCR mis-reads (e.g. FF
    Jim O'Callaghan €1,944,000 = €19,440) the parser honestly flags for manual
    verification rather than shipping as fact. An UNflagged over-limit assigned
    value is a silent bad read (mirrors test_expenditure_within_statutory_limit_unless_flagged)."""
    seats = _seats()
    rows = df.filter(pl.col("amount_assigned_eur").is_not_null()).to_dicts()
    offenders = [
        (r["party"], r["candidate_name_raw"], r["constituency"], r["amount_assigned_eur"])
        for r in rows
        if (lim := STATUTORY_LIMIT.get(int(seats.get(r["constituency"], 0))))
        and r["amount_assigned_eur"] > lim * 1.001
        and r["flag"] != "assigned_over_limit_verify"
    ]
    assert not offenders, f"unflagged assigned-amount over statutory limit: {offenders[:5]}"


def test_expenditure_not_misread_assigned(df):
    """The single biggest correctness risk. SIPO's per-candidate form has TWO money
    columns — 'Amount Assigned' and 'Total Expenditure by the national agent' — and for
    parties that report spend at NATIONAL level (SF, Labour) the Expenditure column is
    BLANK. When the parser captures only one money cell it wrongly stores the *assigned*
    value as expenditure. Signature: assigned=null + a ROUND expenditure (no cents),
    typically €5,000 / €10,000 (SF) or 15,560 / 19,440 / 23,340 = 40% of the statutory
    limit (FF-style). Verified against the SF born-digital page (titled 'Amount assigned
    by each candidate to the political party', expenditure column empty) and the Labour
    scan (e.g. Ciaran Ahern parsed €23,340 but the PDF shows assigned €23,340 / spend
    €4,233.05). These rows must be 0 — otherwise expenditure_eur is NOT real spend."""
    suspect = df.filter(
        pl.col("amount_assigned_eur").is_null()
        & pl.col("expenditure_eur").is_not_null()
        & (pl.col("expenditure_eur") == pl.col("expenditure_eur").round(0))
    )
    assert suspect.height == 0, (
        f"{suspect.height} rows look like the Amount-Assigned column misread as "
        f"expenditure (assigned=null + whole-€ value), Σ=€{suspect['expenditure_eur'].sum():,.0f}: "
        f"{suspect.group_by('party').len().sort('party').to_dicts()}"
    )


def test_no_duplicate_candidate_rows(df):
    dups = (
        df.group_by(["party", "candidate_name_raw", "constituency"])
        .len()
        .filter(pl.col("len") > 1)
    )
    assert dups.height == 0, f"{dups.height} duplicate (party,candidate,constituency) groups"


def test_candidates_per_constituency_plausible(df):
    """A party with > max-seats candidates in one constituency = constituency mis-snap."""
    over = (
        df.group_by(["party", "constituency"]).len()
        .filter(pl.col("len") > MAX_CANDIDATES_PER_CONSTIT)
    )
    assert over.height == 0, f"implausible candidate counts (OCR mis-snap?): {over.to_dicts()}"


def test_ok_rows_respect_spend_le_assigned(df):
    """The 'ok' flag promises spend <= assigned (within 2% tol). Violations must be flagged."""
    bad = df.filter(
        (pl.col("flag") == "ok")
        & pl.col("expenditure_eur").is_not_null()
        & pl.col("amount_assigned_eur").is_not_null()
        & (pl.col("expenditure_eur") > pl.col("amount_assigned_eur") * 1.02)
    )
    assert bad.height == 0, f"{bad.height} 'ok' rows have spend > assigned"


def test_source_page_valid(df):
    bad = df.filter(pl.col("source_page").is_null() | (pl.col("source_page") < 1))
    assert bad.height == 0, f"{bad.height} rows with invalid source_page"


# -------------------------------------------------------------------- Part 4 tests
ITEMS = ROOT / "pipeline_sandbox/_sipo_output/sipo_expense_items_fact.parquet"
CATS = ROOT / "pipeline_sandbox/_sipo_output/sipo_expense_categories_fact.parquet"
VALID_SECTIONS = {"4A", "4B", "4C", "4D", "4E", "4F", "4G", "4H"}


@pytest.fixture(scope="module")
def cats() -> pl.DataFrame:
    if not CATS.exists():
        pytest.skip("sipo_expense_categories_fact.parquet absent (Part 4 not built)")
    return pl.read_parquet(CATS)


@pytest.fixture(scope="module")
def items() -> pl.DataFrame:
    if not ITEMS.exists():
        pytest.skip("sipo_expense_items_fact.parquet absent (Part 4 not built)")
    return pl.read_parquet(ITEMS)


def test_part4_sections_valid(cats):
    bad = set(cats["section"].unique().to_list()) - (VALID_SECTIONS | {"TOTAL"})
    assert not bad, f"unexpected Part-4 sections: {bad}"


def test_part4_one_overall_per_party(cats):
    over = cats.filter(pl.col("is_overall")).group_by("party").len().filter(pl.col("len") != 1)
    assert over.height == 0, f"parties without exactly one Overall total: {over.to_dicts()}"


def test_part4_totals_non_negative(cats):
    assert cats.filter(pl.col("category_total_eur") < 0).height == 0


def test_part4_born_digital_fully_reconciles(cats):
    """Born-digital returns (SF, Aontú) have a clean text layer, so EVERY non-zero
    heading's line-item sum must reconcile to its printed total. (Scanned parties may
    carry OCR mismatches — those are flagged via `reconciles`, not asserted here.)"""
    bd = cats.filter(
        pl.col("party").is_in(["Sinn Féin", "Aontú"])
        & ~pl.col("is_overall")
        & (pl.col("category_total_eur") > 0)
        & (pl.col("items_sum_eur") > 0)  # Aontú has totals-only (no Ref'd items) -> skip
    )
    bad = bd.filter(~pl.col("reconciles"))
    assert bad.height == 0, f"born-digital headings that don't reconcile: {bad.select(['party','section','category_total_eur','items_sum_eur']).to_dicts()}"


def test_part4_item_costs_non_negative(items):
    assert items.filter(pl.col("cost_eur") < 0).height == 0


# ------------------------------------------------------------------------- scorecard
def scorecard() -> None:
    df = _load()
    if df is None or df.height == 0:
        print("no sipo_expenses_fact.parquet yet — nothing to score")
        return
    seats = _seats()
    valid = _valid_constituencies()

    checks: dict[str, bool] = {}
    checks["columns_complete"] = not (REQUIRED_COLS - set(df.columns))
    checks["no_null_keys"] = all(
        df.filter(pl.col(c).is_null()).height == 0 for c in ("party", "candidate_name_raw", "constituency")
    )
    checks["constituencies_in_set"] = df.filter(~pl.col("constituency").is_in(list(valid))).height == 0
    checks["flags_allowed"] = not (set(df["flag"].unique().to_list()) - ALLOWED_FLAGS)
    wa = df.filter(pl.col("expenditure_eur").is_not_null())
    checks["expenditure_non_negative"] = wa.filter(pl.col("expenditure_eur") < 0).height == 0
    suspect = df.filter(
        pl.col("amount_assigned_eur").is_null()
        & pl.col("expenditure_eur").is_not_null()
        & (pl.col("expenditure_eur") == pl.col("expenditure_eur").round(0))
    )
    checks["no_assigned_misread_as_expenditure"] = suspect.height == 0
    over = (df.group_by(["party", "constituency"]).len().filter(pl.col("len") > MAX_CANDIDATES_PER_CONSTIT))
    checks["constit_counts_plausible"] = over.height == 0
    dups = df.group_by(["party", "candidate_name_raw", "constituency"]).len().filter(pl.col("len") > 1)
    checks["no_duplicate_rows"] = dups.height == 0

    score = round(100 * sum(checks.values()) / len(checks))
    grade = "A" if score >= 90 else "B" if score >= 80 else "C" if score >= 70 else "D" if score >= 60 else "F"

    print("=" * 64)
    print("SIPO GE2024 candidate-expenses — DATA-QUALITY SCORECARD")
    print("=" * 64)
    print(f"rows: {df.height}   parties: {df['party'].n_unique()}   "
          f"with amount: {wa.height} ({wa.height/df.height:.0%})")
    print(f"Σ expenditure: €{wa['expenditure_eur'].sum():,.2f}   "
          f"median: €{wa['expenditure_eur'].median():,.2f}   max: €{wa['expenditure_eur'].max():,.2f}")
    print(f"constituencies covered: {df['constituency'].n_unique()}/43")
    print()
    print("per-party:")
    pp = (df.group_by("party").agg(
        pl.len().alias("rows"),
        pl.col("expenditure_eur").is_not_null().sum().alias("with_amt"),
        pl.col("expenditure_eur").sum().round(2).alias("total_eur"),
    ).sort("party"))
    for r in pp.to_dicts():
        print(f"  {r['party']:<32} {r['rows']:>3} rows  {r['with_amt']:>3} w/amt  €{r['total_eur'] or 0:>12,.2f}")
    print()
    print("flag distribution:")
    for r in df["flag"].value_counts().sort("count", descending=True).to_dicts():
        print(f"  {r['flag']:<28} {r['count']:>4}")
    print()
    print("invariant checks:")
    for k, ok in checks.items():
        print(f"  [{'PASS' if ok else 'FAIL'}] {k}")
    print()
    print(f"SCORE: {score}/100  (grade {grade})")

    # anomaly hunt
    print("\n--- anomalies / things to verify vs official PDF ---")
    fl = df.filter(pl.col("flag") != "ok")
    if fl.height:
        for r in fl.select(["party", "candidate_name_raw", "constituency", "expenditure_eur",
                            "amount_assigned_eur", "flag", "source_page"]).to_dicts():
            print(f"  [{r['flag']}] {r['party']} / {r['candidate_name_raw']} / {r['constituency']} "
                  f"spend=€{r['expenditure_eur']} assigned=€{r['amount_assigned_eur']} (p{r['source_page']})")
    else:
        print("  (none — all rows flagged ok)")

    if over.height:
        print("\n  over-count constituencies (possible OCR mis-snap):")
        for r in over.to_dicts():
            print(f"    {r['party']} / {r['constituency']}: {r['len']} candidates")

    REPORT.write_text(json.dumps({
        "rows": df.height, "parties": df["party"].n_unique(),
        "with_amount": wa.height, "total_eur": float(wa["expenditure_eur"].sum()),
        "constituencies": df["constituency"].n_unique(),
        "checks": checks, "score": score, "grade": grade,
    }, indent=2), encoding="utf-8")
    print(f"\nwrote {REPORT}")


if __name__ == "__main__":
    scorecard()
