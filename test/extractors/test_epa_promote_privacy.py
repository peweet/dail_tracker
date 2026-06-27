"""Privacy + projection tests for the EPA supplier-compliance sandbox→gold promotion.

Locks extractors/epa_promote_to_gold.py — the projection that DROPS named-individual
(sole-trader) licence holders and non-CRO rows, ships ``company_num`` as the ONLY identity,
carries NO money, and refuses to let any name/address/location/facility column reach
committed gold. data/gold/parquet/ is COMMITTED to the public repo, so a private person's
EPA regulatory record reaching it is a PII incident.
([[feedback_personal_insolvency_privacy]], same stance as test_enrichment_promote_privacy.)

Run:  pytest test/extractors/test_epa_promote_privacy.py -v
"""

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT / "extractors"))

pl = pytest.importorskip("polars")
from epa_promote_to_gold import (  # noqa: E402
    gold_pii_columns,
    project_supplier_compliance,
)


def _sandbox_frame(rows):
    """Build a sandbox accountability-view frame; each dict in `rows` overrides the defaults
    for one row. Carries a decoy `facility_name` PII column that must never be selected to gold."""
    base = dict(
        cro_company_num=123456,
        looks_individual=False,
        licence_classes=["IPC"],
        licence_statuses=["Granted"],
        any_active_licence=True,
        is_public_body=False,
        uww_priority_site=False,
        enforcement_crawled=True,
        last_record_date="2024-01-01",
        n_licences=1,
        n_enforcement_events=0,
        n_incident=0,
        n_complaint=0,
        n_non_compliance=0,
        n_open=0,
        facility_name="Acme Plant, Co. Cork",  # decoy: present in the view, must NOT reach gold
    )
    return pl.DataFrame([{**base, **r} for r in rows])


# ----------------------------- unit: project_supplier_compliance -----------------------------


def test_project_drops_named_individuals():
    df = _sandbox_frame([
        {"cro_company_num": 111, "looks_individual": False},
        {"cro_company_num": 222, "looks_individual": True},  # sole-trader / named person → DROP
    ])
    assert project_supplier_compliance(df)["company_num"].to_list() == [111]


def test_project_drops_rows_without_a_cro_company():
    df = _sandbox_frame([
        {"cro_company_num": 111, "looks_individual": False},
        {"cro_company_num": None, "looks_individual": False},  # unmatched → not a known company → DROP
    ])
    assert project_supplier_compliance(df)["company_num"].to_list() == [111]


def test_project_ships_company_num_as_only_identity_no_pii():
    out = project_supplier_compliance(_sandbox_frame([{"cro_company_num": 555}]))
    assert gold_pii_columns(out.columns) == []          # the decoy facility_name was not selected
    assert "facility_name" not in out.columns
    assert "looks_individual" not in out.columns         # the drop flag itself never ships
    assert "company_num" in out.columns


def test_project_carries_no_money_column():
    out = project_supplier_compliance(_sandbox_frame([{"cro_company_num": 555}]))
    money = [c for c in out.columns if any(t in c.lower() for t in ("amount", "eur", "value", "spend", "paid"))]
    assert money == []  # EPA gold is licences + compliance only (no-inference rule)


def test_project_keeps_no_inference_crawled_flag():
    # enforcement_crawled distinguishes "assessed, no events" from "not assessed" — a 0 must never
    # read as a clean bill of health, so the flag has to survive to gold.
    out = project_supplier_compliance(_sandbox_frame([{"cro_company_num": 555, "enforcement_crawled": False}]))
    assert "enforcement_crawled" in out.columns
    assert out["enforcement_crawled"].to_list() == [False]


# ----------------------------- unit: gold_pii_columns -----------------------------


def test_gold_pii_columns_flags_identifier_names():
    cols = ["company_num", "facility_name", "site_address", "home_town", "geo_location"]
    assert set(gold_pii_columns(cols)) == {"facility_name", "site_address", "home_town", "geo_location"}


def test_gold_pii_columns_clean_projection():
    cols = ["company_num", "n_licences", "any_active_licence", "enforcement_crawled", "licence_classes"]
    assert gold_pii_columns(cols) == []


# ----------------------------- integration: the committed gold -----------------------------

GOLD = _ROOT / "data" / "gold" / "parquet"


@pytest.mark.integration
def test_committed_epa_gold_has_no_pii_and_keys_on_company_num():
    f = GOLD / "epa_supplier_compliance.parquet"
    if not f.exists():
        pytest.skip("gold not built; run extractors/epa_promote_to_gold.py first")
    df = pl.read_parquet(f)
    assert gold_pii_columns(df.columns) == []
    assert "looks_individual" not in df.columns
    assert "company_num" in df.columns
    assert df["company_num"].is_not_null().all()  # company_num is the identity; never null
    money = [c for c in df.columns if any(t in c.lower() for t in ("amount", "eur", "value"))]
    assert money == []  # licences + compliance only
