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
    assert_no_pii_columns,
    assert_no_private_individuals,
    assert_tam_no_named_person,
    drop_private_individuals,
    load_org_allowlist,
    pii_columns,
    tam_organisation_mask,
)

# ----------------------------- unit: drop_private_individuals -----------------------------


def test_drop_removes_flagged_rows():
    df = pl.DataFrame({"party_name": ["Acme DAC", "John Smith"], "flag": [False, True]})
    out = drop_private_individuals(df, "flag")
    assert out["party_name"].to_list() == ["Acme DAC"]


def test_drop_treats_null_flag_as_keep():
    # a null flag means the heuristic never fired — conservative KEEP (it is only ever
    # set True by a positive match), matching the docstring contract.
    df = pl.DataFrame({"party_name": ["Acme DAC", "Beta Ltd"], "flag": [None, False]})
    assert drop_private_individuals(df, "flag").height == 2


def test_drop_noop_when_flag_absent():
    df = pl.DataFrame({"investee_name": ["Fund A", "Fund B"]})
    assert drop_private_individuals(df, "nonexistent_flag").height == 2


# ------------- unit: EU-TAM organisation ALLOWLIST (national_id-driven, privacy-first) -------------


def _tam_frame(rows):
    # rows: list of (name, national_id, cro_company_num)
    return pl.DataFrame(
        {
            "beneficiary_name": [r[0] for r in rows],
            "national_id": [r[1] for r in rows],
            "cro_company_num": [r[2] for r in rows],
        }
    )


def _kept_names(df, allowlist=None):
    return df.filter(tam_organisation_mask(df, allowlist))["beneficiary_name"].to_list()


def test_tam_keeps_rows_with_a_parsed_cro_number():
    # Cartoon Saloon: name-flagged originally, but its National-ID is a CRO number → company KEPT.
    df = _tam_frame([("Cartoon Saloon", "6338348", "6338348")])
    assert _kept_names(df) == ["Cartoon Saloon"]


def test_tam_keeps_incorporation_and_institution_names():
    df = _tam_frame(
        [
            ("Foyle Food Group Ltd", "Herd Number", None),
            ("Kilkenny County Council", "Not applicable", None),
            ("Irish Seed Savers Association", "Business Name Registration", None),
        ]
    )
    assert set(_kept_names(df)) == {
        "Foyle Food Group Ltd",
        "Kilkenny County Council",
        "Irish Seed Savers Association",
    }


def test_tam_drops_named_persons_and_family_partnerships():
    df = _tam_frame(
        [
            ("Cornelius Trass", "Herd Number", None),
            ("Gerard Connolly & Ann Connolly", "Not_Available", None),
            ("David Keane T/A Cappoquin Estate", "Herd Number", None),
            ("MR PETER T FARRELL JNR", "Sole-Trader", None),
        ]
    )
    assert _kept_names(df) == []


def test_tam_person_label_overrides_a_coincidental_org_token():
    # 'Sole-Trader' National-ID is a HARD drop even if the trading name contains 'Company'.
    df = _tam_frame([("John Murphy & Company", "Sole-Trader", None)])
    assert _kept_names(df) == []


def test_tam_keeps_incorporation_suffix_variants_and_cro_prefix():
    # unlimited companies + foreign suffixes (Inc/GmbH) + a 'CRO 119570'-prefixed number (CIÉ)
    df = _tam_frame(
        [
            ("O'Shea Farms Unlimited Co", "Herd Number", None),
            ("Manna Drone Delivery Inc", "Tax identification Number TIN", None),
            ("Bus Eireann", "CRO 119570", None),
            ("Brady Family Farm", "Not_Available", None),  # no token / number → still dropped
        ]
    )
    assert set(_kept_names(df)) == {"O'Shea Farms Unlimited Co", "Manna Drone Delivery Inc", "Bus Eireann"}


def test_tam_allowlist_recovers_token_less_org_but_not_a_person():
    allow = {"abbott ireland", "horse sport ireland"}
    df = _tam_frame(
        [
            ("Abbott Ireland", "Business Name Registration", None),
            ("Horse Sport Ireland", "ABER", None),
            ("Cornelius Trass", "Herd Number", None),  # a farmer, NOT in allowlist → dropped
        ]
    )
    assert set(_kept_names(df, allow)) == {"Abbott Ireland", "Horse Sport Ireland"}


def test_tam_allowlist_match_is_case_and_whitespace_insensitive():
    df = _tam_frame([("  TEAGASC  ", "Business Name Registration", None)])
    assert _kept_names(df, {"teagasc"}) == ["  TEAGASC  "]


def test_org_allowlist_csv_loads_and_contains_known_bodies():
    allow = load_org_allowlist()
    assert {"abbott ireland", "an post", "teagasc", "horse sport ireland"} <= allow


def test_assert_tam_no_named_person_raises_on_surviving_sole_trader():
    df = _tam_frame([("X", "Sole-Trader", None)])
    with pytest.raises(RuntimeError, match="natural-person"):
        assert_tam_no_named_person(df)


def test_assert_tam_no_named_person_passes_for_companies():
    df = _tam_frame([("Acme Ltd", "123456", "123456")])
    assert_tam_no_named_person(df)  # must not raise


# ------------- unit: CBI flag-only drop path (no national_id classifier) -------------


def test_cbi_drop_removes_flagged_individual():
    df = pl.DataFrame({"party_name": ["A", "B"], "flag": [True, False]})
    assert drop_private_individuals(df, "flag")["party_name"].to_list() == ["B"]


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


def test_assert_raises_when_a_flagged_row_survives():
    df = pl.DataFrame({"x": [1], "flag": [True]})
    with pytest.raises(RuntimeError, match="private-individual"):
        assert_no_private_individuals(df, "flag")


def test_assert_passes_when_all_false():
    df = pl.DataFrame({"x": [1, 2], "flag": [False, False]})
    assert_no_private_individuals(df, "flag")  # no raise


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
def test_eu_tam_gold_is_exactly_the_provable_organisations():
    """End-to-end lock: committed gold equals the organisation allowlist applied to the sandbox
    — every provable org kept, every sole-trader / herd-number / personal-ID beneficiary dropped."""
    sandbox = _ROOT / "data" / "sandbox" / "enrichment" / "eu_tam_ireland_awards.parquet"
    gold = GOLD / "eu_tam_state_aid.parquet"
    if not (sandbox.exists() and gold.exists()):
        pytest.skip("sandbox or gold absent; run the extractor + promotion first")
    sb = pl.read_parquet(sandbox)
    gd = pl.read_parquet(gold)
    expected = sb.filter(tam_organisation_mask(sb, load_org_allowlist()))
    assert gd.height == expected.height
    # the CRO-numbered companies (incl. names like Cartoon Saloon) are all retained
    assert gd["cro_company_num"].is_not_null().sum() == expected["cro_company_num"].is_not_null().sum()
    # and gold carries no unambiguous natural-person trace (national_id already dropped, names too)
    assert_tam_no_named_person(gd) if "national_id" in gd.columns else None


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
