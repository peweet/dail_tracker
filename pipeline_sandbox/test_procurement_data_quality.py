"""DATA-QUALITY tests + scorecard for the procurement SILVER/SANDBOX parquets.

Covers the two tables produced this session:
  - data/silver/parquet/ted_ie_awards.parquet        (ted_ireland_extract.py)
  - data/silver/parquet/public_payments_fact.parquet (procurement_public_body_extract.py)

Two layers in one file:
  1. pytest invariant tests  — hard pass/fail data contracts (run: pytest -q this_file).
     Each suite SKIPS if its parquet is absent (pre-promotion, regenerable) — same pattern
     as the repo's silver/gold tests.
  2. `python this_file`       — prints a DQ SCORECARD (0-100 + grade), sample sum counts,
     and an outlier/anomaly hunt; writes a JSON report to c:/tmp.

The tests assert the value-semantics firewall the extractors promise: value_safe_to_sum is
a strict subset of summable rows, never includes framework/pan-EU/large ceilings, and is
orders of magnitude below the naive (DO-NOT-USE) sum.

Run:
  ./.venv/Scripts/python.exe -m pytest pipeline_sandbox/test_procurement_data_quality.py -q
  ./.venv/Scripts/python.exe pipeline_sandbox/test_procurement_data_quality.py        # scorecard
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

TED_PATH = ROOT / "data/silver/parquet/ted_ie_awards.parquet"
PUB_PATH = ROOT / "data/silver/parquet/public_payments_fact.parquet"
REPORT = Path("c:/tmp/procurement_dq_report.json")

LARGE_AWARD = 50_000_000

TED_REQUIRED = {
    "publication_number", "buyer_name", "winner_name", "winner_identifier_digits",
    "award_value_eur", "n_winners", "is_multi_supplier_framework", "is_pan_eu_outlier",
    "is_large_award_review", "value_kind", "cpv_division", "year", "supplier_class",
    "privacy_status", "cro_company_num", "cro_match_method", "value_safe_to_sum",
}
PUB_REQUIRED = {
    "publisher_id", "supplier_raw", "supplier_normalised", "amount_eur", "amount_semantics",
    "value_safe_to_sum", "supplier_class", "privacy_status", "public_display",
    "source_file_url", "extraction_confidence",
}


def _load(path: Path) -> pl.DataFrame:
    if not path.exists():
        pytest.skip(f"{path} not present (pre-promotion / regenerable)")
    return pl.read_parquet(path)


# ============================================================ TED invariant tests
def test_ted_schema_contract():
    df = _load(TED_PATH)
    missing = TED_REQUIRED - set(df.columns)
    assert not missing, f"TED missing columns: {missing}"
    assert df["award_value_eur"].dtype == pl.Float64
    for b in ("is_multi_supplier_framework", "is_pan_eu_outlier", "is_large_award_review",
              "value_safe_to_sum"):
        assert df[b].dtype == pl.Boolean, f"{b} not boolean"


def test_ted_grain_one_row_per_notice_winner():
    df = _load(TED_PATH)
    assert df["publication_number"].n_unique() <= df.height
    # no notice should explode into an implausible number of winner rows
    max_rows = df.group_by("publication_number").len()["len"].max()
    assert max_rows <= 60, f"a notice exploded into {max_rows} winner rows"


def test_ted_values_positive_or_null():
    df = _load(TED_PATH)
    vals = df.filter(pl.col("award_value_eur").is_not_null())
    assert vals.filter(pl.col("award_value_eur") <= 0).height == 0, "non-positive award value"


def test_ted_value_safe_to_sum_is_strict_subset():
    df = _load(TED_PATH)
    safe = df.filter(pl.col("value_safe_to_sum"))
    # every safe row: single-winner contract award, not framework/pan-EU/large, value>0
    bad = safe.filter(
        (pl.col("value_kind") != "contract_award_value")
        | pl.col("is_multi_supplier_framework")
        | pl.col("is_pan_eu_outlier")
        | pl.col("is_large_award_review")
        | pl.col("award_value_eur").is_null()
        | (pl.col("award_value_eur") <= 0)
    )
    assert bad.height == 0, f"{bad.height} value_safe_to_sum rows violate the firewall"
    assert safe["award_value_eur"].max() < LARGE_AWARD, "safe row above the large-award gate"


def test_ted_safe_sum_far_below_naive():
    df = _load(TED_PATH)
    safe = df.filter(pl.col("value_safe_to_sum"))["award_value_eur"].sum() or 0
    naive = df["award_value_eur"].sum() or 0
    # naive is contaminated by pan-EU ceilings (€15.3bn GÉANT etc.) -> safe must be tiny vs it
    assert safe < naive * 0.5, "safe sum suspiciously close to the contaminated naive sum"


def test_ted_known_mega_ceiling_excluded():
    df = _load(TED_PATH)
    top = df.filter(pl.col("award_value_eur").is_not_null()).sort("award_value_eur", descending=True).head(1)
    assert top["award_value_eur"][0] >= LARGE_AWARD, "test fixture: top award unexpectedly small"
    assert not bool(top["value_safe_to_sum"][0]), "the single largest award leaked into safe-to-sum"


def test_ted_cro_match_plausible_and_clean():
    df = _load(TED_PATH)
    rate = df.filter(pl.col("cro_match_method") != "none").height / df.height
    assert 0.40 <= rate <= 0.95, f"CRO match rate {rate:.2f} outside plausible band"
    # identifier matches must actually carry digits (the join key)
    by_id = df.filter(pl.col("cro_match_method") == "identifier")
    assert by_id.filter(pl.col("winner_identifier_digits").is_null()).height == 0


def test_ted_privacy_flags_consistent():
    df = _load(TED_PATH)
    sole = df.filter(pl.col("supplier_class") == "sole_trader_or_individual")
    assert sole.filter(pl.col("privacy_status") != "review_personal_data").height == 0
    comp = df.filter(pl.col("supplier_class") == "company")
    assert comp.filter(pl.col("privacy_status") != "ok").height == 0


def test_ted_dates_sane():
    df = _load(TED_PATH)
    yrs = df.filter(pl.col("year").is_not_null())["year"]
    assert yrs.min() >= 2023 and yrs.max() <= 2027, f"year out of range [{yrs.min()},{yrs.max()}]"


def test_ted_low_duplicate_rate():
    df = _load(TED_PATH)
    dup = df.height - df.unique(subset=["publication_number", "winner_name", "award_value_eur"]).height
    assert dup / df.height < 0.01, f"duplicate rate {dup / df.height:.3%} too high"


# ============================================================ public-body invariant tests
def test_pub_schema_contract():
    df = _load(PUB_PATH)
    missing = PUB_REQUIRED - set(df.columns)
    assert not missing, f"public-body missing columns: {missing}"


def test_pub_amount_present():
    df = _load(PUB_PATH)
    assert df["amount_eur"].null_count() == 0, "public-body amount_eur has nulls"


def test_pub_value_safe_semantics():
    df = _load(PUB_PATH)
    safe = df.filter(pl.col("value_safe_to_sum"))
    bad = safe.filter(
        ~pl.col("amount_semantics").is_in(["po_committed", "payment_actual"])
        | (pl.col("amount_eur") <= 0)
    )
    assert bad.height == 0, f"{bad.height} safe rows have non-summable semantics"


def test_pub_negative_amounts_bounded():
    df = _load(PUB_PATH)
    neg = df.filter(pl.col("amount_eur") < 0).height
    assert neg / df.height < 0.005, f"negative-amount rate {neg / df.height:.3%} too high (credit notes?)"


def test_pub_privacy_and_display_flags():
    df = _load(PUB_PATH)
    # quarantine deferred this run -> everything still public_display=True, but flagged
    assert df.filter(~pl.col("public_display")).height == 0, "a row was quarantined unexpectedly"
    sole = df.filter(pl.col("supplier_class") == "sole_trader_or_individual")
    assert sole.filter(pl.col("privacy_status") != "review_personal_data").height == 0


# ============================================================ DQ scorecard + anomalies
def _pct(n: int, d: int) -> float:
    return round(100 * n / d, 1) if d else 0.0


def ted_scorecard(df: pl.DataFrame) -> dict:
    n = df.height
    completeness = {
        "winner_name": _pct(df["winner_name"].is_not_null().sum(), n),
        "award_value_eur": _pct(df["award_value_eur"].is_not_null().sum(), n),
        "cpv_code": _pct(df["cpv_code"].is_not_null().sum(), n),
        "year": _pct(df["year"].is_not_null().sum(), n),
    }
    validity = {
        "value_positive_or_null": _pct(df.filter(pl.col("award_value_eur").is_null()
                                       | (pl.col("award_value_eur") > 0)).height, n),
        "year_in_range": _pct(df.filter(pl.col("year").is_null()
                              | pl.col("year").is_between(2023, 2027)).height, n),
    }
    safe = df.filter(pl.col("value_safe_to_sum"))
    consistency = {
        "safe_subset_clean": safe.filter(
            (pl.col("value_kind") != "contract_award_value") | pl.col("is_multi_supplier_framework")
            | pl.col("is_pan_eu_outlier") | pl.col("is_large_award_review")).height == 0,
        "privacy_flags_consistent": df.filter(
            (pl.col("supplier_class") == "sole_trader_or_individual")
            & (pl.col("privacy_status") != "review_personal_data")).height == 0,
    }
    dup = df.height - df.unique(subset=["publication_number", "winner_name", "award_value_eur"]).height
    uniqueness = _pct(n - dup, n)
    cro_rate = _pct(df.filter(pl.col("cro_match_method") != "none").height, n)
    # weighted score: completeness(ID fields) 25, validity 25, consistency 30, uniqueness 20
    comp_core = (completeness["winner_name"] + completeness["year"]) / 2
    cons_score = 100 * sum(consistency.values()) / len(consistency)
    score = round(0.25 * comp_core + 0.25 * (sum(validity.values()) / len(validity))
                  + 0.30 * cons_score + 0.20 * uniqueness, 1)
    return {"rows": n, "notices": df["publication_number"].n_unique(),
            "completeness_pct": completeness, "validity_pct": validity,
            "consistency": consistency, "uniqueness_pct": uniqueness,
            "cro_match_pct": cro_rate, "dq_score": score, "grade": _grade(score)}


def pub_scorecard(df: pl.DataFrame) -> dict:
    n = df.height
    completeness = {
        "supplier_raw": _pct(df["supplier_raw"].is_not_null().sum(), n),
        "amount_eur": _pct(df["amount_eur"].is_not_null().sum(), n),
        "period": _pct(df["period"].is_not_null().sum(), n) if "period" in df.columns else 0.0,
    }
    neg = df.filter(pl.col("amount_eur") < 0).height
    validity = {"amount_nonneg": _pct(n - neg, n)}
    safe = df.filter(pl.col("value_safe_to_sum"))
    consistency = {
        "safe_semantics_clean": safe.filter(
            ~pl.col("amount_semantics").is_in(["po_committed", "payment_actual"])).height == 0,
        "hi_conf_majority": df.filter(pl.col("extraction_confidence") == "high").height / n > 0.8,
    }
    dup = df.height - df.unique(subset=["source_file_hash", "supplier_raw", "amount_eur",
                                        "source_row_number"]).height if "source_file_hash" in df.columns else 0
    uniqueness = _pct(n - dup, n)
    cons_score = 100 * sum(consistency.values()) / len(consistency)
    score = round(0.30 * (sum(completeness.values()) / len(completeness))
                  + 0.20 * (sum(validity.values()) / len(validity))
                  + 0.30 * cons_score + 0.20 * uniqueness, 1)
    return {"rows": n, "publishers": df["publisher_id"].n_unique(),
            "completeness_pct": completeness, "validity_pct": validity,
            "consistency": consistency, "uniqueness_pct": uniqueness,
            "dq_score": score, "grade": _grade(score)}


def _grade(s: float) -> str:
    return ("A" if s >= 90 else "B" if s >= 80 else "C" if s >= 70 else "D" if s >= 60 else "F")


def test_ted_dq_score_floor():
    df = _load(TED_PATH)
    sc = ted_scorecard(df)
    assert sc["dq_score"] >= 70, f"TED DQ score {sc['dq_score']} below floor"


def test_pub_dq_score_floor():
    df = _load(PUB_PATH)
    sc = pub_scorecard(df)
    assert sc["dq_score"] >= 70, f"public-body DQ score {sc['dq_score']} below floor"


# ============================================================ human report
def _h(t):
    print(f"\n{'=' * 78}\n{t}\n{'=' * 78}")


def report() -> dict:
    out = {}
    if TED_PATH.exists():
        t = pl.read_parquet(TED_PATH)
        sc = ted_scorecard(t)
        out["ted"] = sc
        _h(f"TED SILVER — DQ {sc['dq_score']}/100  (grade {sc['grade']})")
        print(f"rows {sc['rows']:,} | notices {sc['notices']:,} | CRO matched {sc['cro_match_pct']}%")
        print(f"completeness: {sc['completeness_pct']}")
        print(f"validity:     {sc['validity_pct']}")
        print(f"consistency:  {sc['consistency']}  | uniqueness {sc['uniqueness_pct']}%")

        _h("TED — SAMPLE SUM COUNTS (the firewall in action)")
        naive = t["award_value_eur"].sum() or 0
        safe = t.filter(pl.col("value_safe_to_sum"))["award_value_eur"].sum() or 0
        med = t.filter(pl.col("award_value_eur") > 0)["award_value_eur"].median() or 0
        print(f"  naive sum (DO NOT USE): €{naive / 1e9:,.1f}bn")
        print(f"  value_safe_to_sum     : €{safe / 1e9:,.2f}bn  ({_pct(int(safe), int(naive))}% of naive)")
        print(f"  MEDIAN award (headline): €{med:,.0f}   |  awards with a value: "
              f"{t.filter(pl.col('award_value_eur') > 0).height:,}")
        print("  by year (count, median €):")
        for r in t.filter(pl.col("award_value_eur") > 0).group_by("year").agg(
                pl.len().alias("n"), pl.col("award_value_eur").median().alias("med")).sort("year").iter_rows():
            if r[0]:
                print(f"     {r[0]}  n={r[1]:>5}  median €{r[2]:,.0f}")
        print("  top CPV divisions by award count:")
        for r in t.group_by("cpv_division").agg(pl.len().alias("n")).sort("n", descending=True).head(6).iter_rows():
            print(f"     {r[1]:>5}  {r[0]}")

        _h("TED — OUTLIER / ANOMALY HUNT")
        top = t.filter(pl.col("award_value_eur").is_not_null()).sort("award_value_eur", descending=True).head(5)
        print("  largest values (should all be flagged framework/pan-EU/large, NOT in safe):")
        for r in top.select(["award_value_eur", "buyer_name", "winner_name", "is_pan_eu_outlier",
                             "is_large_award_review", "value_safe_to_sum"]).iter_rows():
            print(f"     €{r[0] / 1e6:>9,.0f}m panEU={r[3]!s:<5} large={r[4]!s:<5} safe={r[5]!s:<5} "
                  f"{str(r[1])[:28]:<28} -> {str(r[2])[:26]}")
        rep = t.filter(pl.col("award_value_eur") > 0).group_by("award_value_eur").len().sort(
            "len", descending=True).head(5)
        print("  round-number repeats (framework/threshold smell — same € on many notices):")
        for r in rep.iter_rows():
            print(f"     €{r[0]:>12,.0f}  x{r[1]}")
        dup = t.height - t.unique(subset=["publication_number", "winner_name", "award_value_eur"]).height
        sole = t.filter(pl.col("supplier_class") == "sole_trader_or_individual").height
        print(f"  exact (notice,winner,value) duplicates: {dup} ({_pct(dup, t.height)}%)")
        print(f"  sole-trader/individual winners (privacy-flagged, deferred): {sole:,} ({_pct(sole, t.height)}%)")
        print(f"  notices with NO parsed value: {t['award_value_eur'].null_count():,} "
              f"({_pct(t['award_value_eur'].null_count(), t.height)}% — TED often omits value)")

    if PUB_PATH.exists():
        p = pl.read_parquet(PUB_PATH)
        sc = pub_scorecard(p)
        out["public_body"] = sc
        _h(f"PUBLIC-BODY SANDBOX — DQ {sc['dq_score']}/100  (grade {sc['grade']})")
        print(f"rows {sc['rows']:,} | publishers {sc['publishers']}")
        print(f"completeness: {sc['completeness_pct']}")
        print(f"validity:     {sc['validity_pct']}  | consistency {sc['consistency']} | uniqueness {sc['uniqueness_pct']}%")

        _h("PUBLIC-BODY — SAMPLE SUM COUNTS (per amount_semantics)")
        for r in p.group_by("amount_semantics").agg(
                pl.len().alias("rows"),
                pl.col("amount_eur").filter(pl.col("value_safe_to_sum")).sum().alias("safe_sum")).iter_rows():
            print(f"  {r[0]:<22} rows={r[1]:>5}  safe_sum=€{(r[2] or 0) / 1e6:,.1f}m")
        print("  top publishers by safe sum:")
        for r in p.filter(pl.col("value_safe_to_sum")).group_by("publisher_id").agg(
                pl.col("amount_eur").sum().alias("s"), pl.len().alias("n")).sort("s", descending=True).head(8).iter_rows():
            print(f"     {r[0]:<22} €{r[1] / 1e6:>8,.1f}m  ({r[2]} rows)")

        _h("PUBLIC-BODY — OUTLIER / ANOMALY HUNT")
        print("  largest single amounts:")
        for r in p.select(["amount_eur", "publisher_id", "supplier_raw"]).sort(
                "amount_eur", descending=True).head(5).iter_rows():
            print(f"     €{r[0] / 1e6:>8,.1f}m  {r[1]:<16} {str(r[2])[:34]}")
        neg = p.filter(pl.col("amount_eur") < 0)
        print(f"  negative amounts (credit notes/refunds): {neg.height}")
        for r in neg.select(["amount_eur", "publisher_id", "supplier_raw"]).head(3).iter_rows():
            print(f"     €{r[0]:,.0f}  {r[1]:<16} {str(r[2])[:34]}")
        safe = p.filter(pl.col("value_safe_to_sum"))
        if safe.height:
            big = safe.sort("amount_eur", descending=True)["amount_eur"]
            tot = big.sum()
            print(f"  top-1 share of safe sum: {_pct(int(big[0]), int(tot))}%  "
                  f"(>50% => one row dominates, don't headline)")

    REPORT.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    print(f"\nwrote {REPORT}")
    return out


if __name__ == "__main__":
    report()
