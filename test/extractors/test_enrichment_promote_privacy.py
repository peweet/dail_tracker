"""Privacy + projection tests for the enrichment sandbox→gold promotion.

Locks extractors/enrichment_promote_to_gold.py — the helpers that DROP suspected
natural-person rows and refuse the write if a PII identifier column survives.
data/gold/parquet/ is COMMITTED to the public repo, so a private individual's
CBI enforcement record or EU-TAM grant reaching it is a PII incident. Firm names +
amounts are the public regulatory record and are intentionally kept.
([[feedback_personal_insolvency_privacy]], same stance as test_sipo_promote_privacy.)

Run:  pytest test/extractors/test_enrichment_promote_privacy.py -v
"""

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT / "extractors"))

pl = pytest.importorskip("polars")
from enrichment_promote_to_gold import (  # noqa: E402
    assert_no_individuals,
    assert_no_pii_columns,
    drop_individuals,
    pii_columns,
)

# ----------------------------- unit: drop_individuals -----------------------------


def test_drop_individuals_removes_flagged_rows():
    df = pl.DataFrame(
        {"party_name": ["Acme DAC", "John Smith"], "flag": [False, True]}
    )
    out = drop_individuals(df, "flag")
    assert out["party_name"].to_list() == ["Acme DAC"]


def test_drop_individuals_treats_null_flag_as_keep():
    # a null flag means the heuristic never fired — conservative KEEP (it is only ever
    # set True by a positive match), matching the docstring contract.
    df = pl.DataFrame({"party_name": ["Acme DAC", "Beta Ltd"], "flag": [None, False]})
    assert drop_individuals(df, "flag").height == 2


def test_drop_individuals_noop_when_flag_absent():
    df = pl.DataFrame({"investee_name": ["Fund A", "Fund B"]})
    assert drop_individuals(df, "nonexistent_flag").height == 2


# ----------------------------- unit: pii_columns -----------------------------


@pytest.mark.parametrize(
    "col",
    ["national_id", "donor_address", "home_town", "pps_number", "phone", "email_addr", "date_of_birth"],
)
def test_pii_columns_flags_identifier_names(col):
    df = pl.DataFrame({"beneficiary_name": ["Acme"], col: ["x"]})
    assert col in pii_columns(df)


def test_pii_columns_clean_frame():
    df = pl.DataFrame({"beneficiary_name": ["Acme"], "cro_company_num": ["123456"], "amount": [1.0]})
    assert pii_columns(df) == []


def test_company_name_not_flagged_as_pii():
    # 'name' alone is fine — it is the public company/party name, not a person identifier.
    df = pl.DataFrame({"party_name": ["Ulster Bank Ireland DAC"], "beneficiary_name": ["IDA"]})
    assert pii_columns(df) == []


# ----------------------------- unit: the runtime guards -----------------------------


def test_assert_no_individuals_raises_when_a_flagged_row_survives():
    df = pl.DataFrame({"x": [1], "flag": [True]})
    with pytest.raises(RuntimeError, match="suspected-individual"):
        assert_no_individuals(df, "flag")


def test_assert_no_individuals_passes_when_all_false():
    df = pl.DataFrame({"x": [1, 2], "flag": [False, False]})
    assert_no_individuals(df, "flag")  # no raise


def test_assert_no_pii_columns_raises():
    df = pl.DataFrame({"beneficiary_name": ["Acme"], "national_id": ["1234567T"]})
    with pytest.raises(RuntimeError, match="PII column"):
        assert_no_pii_columns(df)


# ----------------------------- integration: the committed gold -----------------------------

GOLD = _ROOT / "data" / "gold" / "parquet"


@pytest.mark.integration
def test_committed_cbi_gold_has_no_individuals_or_pii():
    f = GOLD / "cbi_enforcement_actions.parquet"
    if not f.exists():
        pytest.skip("gold not built; run extractors/enrichment_promote_to_gold.py first")
    df = pl.read_parquet(f)
    assert pii_columns(df) == []
    assert "party_is_individual_suspected" not in df.columns  # flag dropped at projection


@pytest.mark.integration
def test_committed_eu_tam_gold_has_no_individuals_or_pii():
    f = GOLD / "eu_tam_state_aid.parquet"
    if not f.exists():
        pytest.skip("gold not built; run extractors/enrichment_promote_to_gold.py first")
    df = pl.read_parquet(f)
    assert pii_columns(df) == []
    assert "national_id" not in df.columns  # raw national id never ships
    assert "beneficiary_is_individual_suspected" not in df.columns


@pytest.mark.integration
@pytest.mark.parametrize("fname", ["isif_portfolio", "cbi_enforcement_actions", "eu_tam_state_aid"])
def test_committed_gold_value_safe_to_sum_is_all_false(fname):
    """The whole point of value_safe_to_sum: these facts must never be summable."""
    f = GOLD / f"{fname}.parquet"
    if not f.exists():
        pytest.skip("gold not built; run extractors/enrichment_promote_to_gold.py first")
    df = pl.read_parquet(f)
    assert df["value_safe_to_sum"].all() is not True
    assert int(df["value_safe_to_sum"].sum()) == 0
