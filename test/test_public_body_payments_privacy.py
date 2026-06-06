"""Privacy-quarantine tests for the public-body payments sandbox fact.

Locks ``extractors/procurement_public_body_extract.py:classify_and_flag()`` — the
function that derives ``supplier_class`` / ``privacy_status`` / ``public_display`` for
``data/sandbox/parquet/public_payments_fact.parquet``. This fact is a gold-CANDIDATE one
promotion away from a procurement UI; if a sole-trader / individual supplier (personal
data) were left ``public_display=True`` it would be exposed on promotion (synthesis INC-4).

Invariant under test: NO ``public_display=True`` row may be ``sole_trader_or_individual``.
Classification errs toward over-quarantine (an org without a recognised company suffix is
treated as personal) — the privacy-safe direction. Supplier names below are invented.

Run:  pytest test/test_public_body_payments_privacy.py -v
"""

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "extractors"))

pl = pytest.importorskip("polars")
from procurement_public_body_extract import (  # noqa: E402
    DEDUP_SIG,
    canonicalise_supplier_raw,
    classify_and_flag,
    dedup_source_repeats,
    flag_unidentifiable_suppliers,
    period_from_url,
)


def _flag(suppliers):
    df = pl.DataFrame({
        "supplier_raw": suppliers,
        "amount_eur": [100_000.0] * len(suppliers),
        "amount_semantics": ["payment_actual"] * len(suppliers),
    })
    return classify_and_flag(df)


def test_sole_trader_is_quarantined():
    df = _flag(["Jonathan Oakfield"])  # no company suffix -> personal
    row = df.row(0, named=True)
    assert row["supplier_class"] == "sole_trader_or_individual"
    assert row["privacy_status"] == "review_personal_data"
    assert row["public_display"] is False


@pytest.mark.parametrize("name", [
    "Brightwater Solutions Limited",
    "Acme Engineering Ltd",
    "Northgate Holdings DAC",
])
def test_company_is_displayable(name):
    row = _flag([name]).row(0, named=True)
    assert row["supplier_class"] == "company"
    assert row["privacy_status"] == "ok"
    assert row["public_display"] is True


def test_public_body_is_displayable():
    row = _flag(["Cork County Council"]).row(0, named=True)
    assert row["supplier_class"] == "public_body"
    assert row["public_display"] is True


# ----------------------------------------------------------------------------------------
# value_safe_to_sum: intergovernmental transfers must NOT be summable (DQ audit 2026-06-05).
# A payment whose recipient is itself a public body (e.g. TII -> county-council road grants,
# €2.5bn / 32% of the fact) is a transfer/grant, not private procurement; totalling it inflates
# "procurement spend" and triple-counts the same euro down the grant -> council -> contractor
# chain. Such rows are RETAINED (public_display=True) but excluded from value_safe_to_sum.
# ----------------------------------------------------------------------------------------
def test_public_body_recipient_is_not_summable():
    row = _flag(["Cork County Council"]).row(0, named=True)
    assert row["supplier_class"] == "public_body"
    assert row["value_safe_to_sum"] is False  # transfer, not procurement
    assert row["public_display"] is True       # but still retained/displayable


def test_company_payment_is_summable():
    row = _flag(["Acme Engineering Ltd"]).row(0, named=True)
    assert row["supplier_class"] == "company"
    assert row["value_safe_to_sum"] is True


@pytest.mark.parametrize("name", [
    "Transport Infrastructure Ireland",   # "...Ireland" — COMPANY_SUFFIX 'ireland' would mis-hit
    "Uisce Éireann",
    "Irish Water",
    "Tailte Éireann",
])
def test_named_state_agency_is_public_body_not_summable(name):
    # State agencies named "X Ireland"/"X Éireann" must classify public_body (tested before the
    # company check) so their intergovernmental transfers never leak into value_safe_to_sum.
    row = _flag([name]).row(0, named=True)
    assert row["supplier_class"] == "public_body", f"{name} misclassified as {row['supplier_class']}"
    assert row["value_safe_to_sum"] is False


def test_no_public_body_row_is_summable_in_mix():
    # Mirrors the real transfer pattern: TII pays county councils (recipient = public body),
    # which is the €2.5bn of intergovernmental transfers the fix excludes from summing.
    df = _flag([
        "Cork County Council",                # public body -> transfer, not summable
        "Donegal County Council",             # public body -> transfer, not summable
        "Acme Engineering Ltd",               # company -> summable
        "Brightwater Solutions Limited",      # company -> summable
    ])
    leaked = df.filter(
        pl.col("value_safe_to_sum") & (pl.col("supplier_class") == "public_body")
    )
    assert leaked.height == 0, f"{leaked.height} public_body transfer rows left summable"
    assert df.filter(pl.col("value_safe_to_sum")).height == 2  # only the two companies


def test_invariant_no_personal_row_is_displayable():
    df = _flag([
        "Jonathan Oakfield",            # personal -> quarantined
        "Acme Engineering Ltd",         # company -> ok
        "Cork County Council",          # public body -> ok
        "Mary Quillfeather",            # personal -> quarantined
        "Brightwater Solutions Limited",
    ])
    leaked = df.filter(
        pl.col("public_display") & (pl.col("supplier_class") == "sole_trader_or_individual")
    )
    assert leaked.height == 0, f"{leaked.height} personal rows left displayable"
    # and the quarantine actually suppressed the two invented individuals
    assert df.filter(~pl.col("public_display")).height == 2


# ----------------------------------------------------------------------------------------
# dedup_source_repeats: drop within-file parser repeats (identical in EVERY extracted field)
# without collapsing genuinely-distinct payments (DQ audit 2026-06-05, A3). Errs toward
# under-deduping: any differing field — notably description — preserves the row.
# ----------------------------------------------------------------------------------------
def _rows(specs):
    """specs: list of (supplier, amount, description, po, page) -> a fact-shaped frame.
    All share one source_file_hash/period so dedup is judged within-file."""
    base = {"source_file_hash": "h1", "period": "2024-Q1", "paid_flag": None}
    return pl.DataFrame([
        {**base, "supplier_raw": s, "amount_eur": float(a),
         "description": d, "po_number": po, "source_page_number": pg}
        for (s, a, d, po, pg) in specs
    ])


def test_identical_rows_are_collapsed():
    df = _rows([("Acme Ltd", 1000, "Stationery", "PO1", 1)] * 4)  # 4 identical
    out, dropped = dedup_source_repeats(df)
    assert out.height == 1
    assert dropped == 3


def test_distinct_description_is_preserved():
    # The Courts pattern: same mis-parsed amount + same (truncated) supplier, but 3 DIFFERENT
    # descriptions = 3 real payment lines. None may be dropped.
    df = _rows([
        ("Ireland Ltd", 21613, "Court A repairs", None, 2),
        ("Ireland Ltd", 21613, "Court B IT",      None, 2),
        ("Ireland Ltd", 21613, "Court C legal",   None, 2),
    ])
    out, dropped = dedup_source_repeats(df)
    assert dropped == 0
    assert out.height == 3


def test_differing_any_field_preserves_row():
    df = _rows([
        ("Acme Ltd", 1000, "X", "PO1", 1),
        ("Acme Ltd", 1000, "X", "PO2", 1),   # different PO -> kept
        ("Acme Ltd", 1000, "X", "PO1", 2),   # different page -> kept
        ("Acme Ltd", 2000, "X", "PO1", 1),   # different amount -> kept
        ("Acme Ltd", 1000, "X", "PO1", 1),   # exact repeat of row 0 -> dropped
    ])
    out, dropped = dedup_source_repeats(df)
    assert dropped == 1
    assert out.height == 4


def test_same_payment_in_two_files_is_not_collapsed():
    # Different source files (different hash) are NOT deduped here — that is the small,
    # separately-handled cross-file republish case, not a within-file parser repeat.
    df = pl.concat([
        _rows([("Acme Ltd", 1000, "X", "PO1", 1)]).with_columns(pl.lit("hA").alias("source_file_hash")),
        _rows([("Acme Ltd", 1000, "X", "PO1", 1)]).with_columns(pl.lit("hB").alias("source_file_hash")),
    ])
    out, dropped = dedup_source_repeats(df)
    assert dropped == 0
    assert out.height == 2


def test_dedup_sig_excludes_volatile_provenance():
    # source_row_number (a running counter) must NOT be in the signature, else true repeats with
    # different counters would never collapse. Confirms the key is content, not emission order.
    assert "source_row_number" not in DEDUP_SIG
    assert "supplier_raw" in DEDUP_SIG and "description" in DEDUP_SIG and "amount_eur" in DEDUP_SIG


# ----------------------------------------------------------------------------------------
# A2/A4: unidentifiable + split supplier names (DQ audit 2026-06-05).
# ----------------------------------------------------------------------------------------
def _norm_conf(suppliers, conf="high"):
    """Frame with supplier_normalised + extraction_confidence for flag_unidentifiable_suppliers."""
    df = pl.DataFrame({"supplier_raw": suppliers,
                       "amount_eur": [1000.0] * len(suppliers),
                       "amount_semantics": ["payment_actual"] * len(suppliers),
                       "extraction_confidence": [conf] * len(suppliers)})
    return classify_and_flag(df)


def test_empty_normalised_name_downgraded_to_low():
    # Truncated to just a legal suffix -> normalises to '' -> not attributable.
    out = flag_unidentifiable_suppliers(_norm_conf(["IRELAND LTD", "LTD", "(IRELAND) LTD"]))
    assert out["extraction_confidence"].to_list() == ["low", "low", "low"]


def test_generic_word_name_downgraded_to_low():
    out = flag_unidentifiable_suppliers(_norm_conf(
        ["Construction Ltd", "Aircraft Ltd", "Ireland Energy Ltd", "Shipping Group"]))
    assert set(out["extraction_confidence"].to_list()) == {"low"}


def test_real_oneword_firm_not_downgraded():
    # Distinctive token survives normalisation -> must STAY high-confidence.
    out = flag_unidentifiable_suppliers(_norm_conf(
        ["Sodexo Ireland Ltd", "Fujitsu Ireland Ltd", "Adston Ltd", "Accenture Limited",
         "Atkins Ltd", "Marsh Ireland Ltd", "Capgemini Ireland Ltd"]))
    assert set(out["extraction_confidence"].to_list()) == {"high"}


def test_suffix_and_geographic_remnants_downgraded():
    # Bare legal-form / geographic remnants (a distinctive lead word was truncated at source).
    out = flag_unidentifiable_suppliers(_norm_conf(["Deloitte LLP", "Ltd", "Ireland"], conf="high"))
    # "Deloitte LLP" -> norm "DELOITTE LLP" (2 tokens, distinctive) stays high; "Ltd"->'' and
    # "Ireland"->'IRELAND' (single generic) drop to low.
    confs = dict(zip(out["supplier_raw"].to_list(), out["extraction_confidence"].to_list(), strict=True))
    assert confs["Deloitte LLP"] == "high"
    assert confs["Ltd"] == "low"
    assert confs["Ireland"] == "low"


def test_unidentifiable_rows_stay_summable():
    # The money is real; only attribution confidence drops. value_safe_to_sum must be untouched.
    df = classify_and_flag(pl.DataFrame({
        "supplier_raw": ["Construction Ltd"], "amount_eur": [1000.0],
        "amount_semantics": ["payment_actual"], "extraction_confidence": ["high"]}))
    out = flag_unidentifiable_suppliers(df)
    assert out.row(0, named=True)["value_safe_to_sum"] is True
    assert out.row(0, named=True)["extraction_confidence"] == "low"


def test_nbi_split_is_merged_to_identifiable_name():
    df = pl.DataFrame({
        "supplier_raw": ["Infrastructure DAC", "Infrastructure DAC NBP",
                         "NBI Infrastructure DAC", "Infrastructure DAC"],
        "po_number": ["NBI", "NBI", None, "12345"],   # last one: not NBI -> left alone
        "amount_eur": [1.0, 2.0, 3.0, 4.0],
    })
    out = canonicalise_supplier_raw(df)
    raws = out["supplier_raw"].to_list()
    assert raws[0] == "NBI Infrastructure DAC"   # po=NBI rewritten
    assert raws[1] == "NBI Infrastructure DAC"   # 'Infrastructure DAC NBP' po=NBI rewritten
    assert raws[2] == "NBI Infrastructure DAC"   # already canonical
    assert raws[3] == "Infrastructure DAC"       # po != NBI -> untouched (no over-merge)
    # and after normalisation the NBI rows share ONE identifiable id (not generic 'INFRASTRUCTURE')
    normed = classify_and_flag(out.with_columns(pl.lit("payment_actual").alias("amount_semantics")))
    nbi_norm = normed.filter(pl.col("po_number") == "NBI")["supplier_normalised"].unique().to_list()
    assert nbi_norm == ["NBI INFRASTRUCTURE"], nbi_norm
    # the canonical NBI name is NOT swept up by the generic-word downgrade
    flagged = flag_unidentifiable_suppliers(
        normed.with_columns(pl.lit("high").alias("extraction_confidence")))
    assert flagged.filter(pl.col("po_number") == "NBI")["extraction_confidence"].to_list() == ["high", "high"]


# ----------------------------------------------------------------------------------------
# Period precision: month-range filenames encode a quarter the Q\d patterns miss (DQ audit
# 2026-06-06). Without this, recurring quarterly payments share a year-only period and look
# like cross-file duplicates.
# ----------------------------------------------------------------------------------------
@pytest.mark.parametrize("fname,expected", [
    ("https://x.ie/jan-mar-2016.pdf", ("2016-Q1", 2016, 1)),
    ("https://x.ie/apr-jun-2018.pdf", ("2018-Q2", 2018, 2)),
    ("https://x.ie/jul-sep-2016.pdf", ("2016-Q3", 2016, 3)),
    ("https://x.ie/oct-dec-2016.pdf", ("2016-Q4", 2016, 4)),
    ("https://x.ie/Q3_2023.pdf", ("2023-Q3", 2023, 3)),        # existing Q-pattern still works
    ("https://x.ie/payments-2024.pdf", ("2024", 2024, None)),  # no quarter -> year only
])
def test_period_from_url_parses_month_ranges(fname, expected):
    assert period_from_url(fname) == expected


@pytest.mark.integration
def test_coverage_flag_is_applied():
    """The on-disk coverage JSON must record the quarantine as applied. Marked integration:
    requires a fresh extractor run (network crawl) — a stale pre-fix sandbox fails this,
    which is the intended signal to regenerate `public_payments_fact.parquet`."""
    import json
    cov = _ROOT / "data" / "_meta" / "public_payments_coverage.json"
    if not cov.exists():
        pytest.skip("coverage not generated; run the extractor first")
    data = json.loads(cov.read_text())
    assert data.get("privacy_quarantine_applied") is True
