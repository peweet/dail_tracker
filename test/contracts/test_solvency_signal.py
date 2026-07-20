"""Contract for ``solvency_signal`` — the guard against calling a solvent company insolvent.

``notice_category == 'corporate_insolvency'`` is a SCOPE bucket, not a finding. Measured
2026-07-18 across its 44,581 rows: only 13,013 (29.2%) are verifiably insolvent, 17,553
(39.4%) are members' voluntary liquidations — **solvent by statute** — and 14,015 (31.4%)
are ``*_unspecified`` families whose solvency the gazette wording does not state. Counting
the category as "insolvencies" therefore overstates by ~3.4x and inverts a legal distinction.

An MVL requires a directors' Declaration of Solvency (Form E1-SAP/E1-41, ss.207/579/580
Companies Act 2014) certifying debts will be paid in full within 12 months, countersigned by
a statutory auditor; if that declaration is not properly made the winding-up automatically
becomes a creditors' voluntary winding-up. The Act itself uses MVL/CVL as the solvent/
insolvent boundary — so ``members_voluntary_liquidation ⇒ solvent`` is statute, not judgement.
  https://cro.ie/termination-restoration/company/winding-up/declaration-form/

What this pins:
  * the pure mapping (no data needed) — a silent reclassification fails here;
  * fail-OPEN: an unseen subtype resolves to 'unknown', never 'insolvent';
  * rescue processes (examinership / SCARP) are NOT insolvency findings — a company
    routinely survives them;
  * against real gold (@sql): the derived column agrees with the mapping, and — the
    invariant that actually matters — **no solvent subtype is ever marked insolvent.**
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parents[2]))

from iris.corporate_notices_enrichment import (  # noqa: E402
    SOLVENCY_BY_SUBTYPE,
    SOLVENCY_INSOLVENT,
    SOLVENCY_SOLVENT,
    SOLVENCY_UNKNOWN,
    solvency_signal_expr,
)

_GOLD = Path("data/gold/parquet/corporate_notices_enriched.parquet")

# The three bands are a closed set; nothing else may ever appear in the column.
_BANDS = {SOLVENCY_SOLVENT, SOLVENCY_INSOLVENT, SOLVENCY_UNKNOWN}

# Subtypes that must NEVER be reported as insolvent. This is the defamation-adjacent
# invariant: a solvent wind-up mislabelled as insolvency is a false statement about a
# real, trading company.
_MUST_NOT_BE_INSOLVENT = ("members_voluntary_liquidation", "companies_act_notice", "icav_voluntary_strike_off")


def _signal(subtypes: list[str | None]) -> list[str]:
    df = pl.DataFrame({"notice_subtype": subtypes}, schema={"notice_subtype": pl.Utf8})
    return df.select(solvency_signal_expr()).to_series().to_list()


# --------------------------------------------------------------------------- pure mapping
def test_mvl_is_solvent():
    """The statutory core of the contract."""
    assert _signal(["members_voluntary_liquidation"]) == [SOLVENCY_SOLVENT]


@pytest.mark.parametrize("subtype", ["creditors_voluntary_liquidation", "court_winding_up", "receivership"])
def test_creditor_and_court_processes_are_insolvent(subtype):
    assert _signal([subtype]) == [SOLVENCY_INSOLVENT]


@pytest.mark.parametrize("subtype", ["voluntary_liquidation_unspecified", "liquidation_unspecified"])
def test_unspecified_families_stay_unknown(subtype):
    """31.4% of the bucket. Forcing these into a binary would manufacture a finding."""
    assert _signal([subtype]) == [SOLVENCY_UNKNOWN]


@pytest.mark.parametrize("subtype", ["examinership", "scarp_process_adviser"])
def test_rescue_processes_are_not_insolvency_findings(subtype):
    """Examinership/SCARP are rescue processes — companies routinely survive them."""
    assert _signal([subtype]) == [SOLVENCY_UNKNOWN]


def test_unseen_subtype_fails_open_to_unknown():
    """A subtype the classifier has never emitted must never be asserted as insolvent."""
    assert _signal(["some_brand_new_subtype_2027", None]) == [SOLVENCY_UNKNOWN, SOLVENCY_UNKNOWN]


def test_mapping_only_emits_known_bands():
    assert set(SOLVENCY_BY_SUBTYPE.values()) <= _BANDS


# --------------------------------------------------------------------------- real gold
@pytest.mark.sql
def test_gold_carries_solvency_signal_and_agrees_with_mapping():
    if not _GOLD.exists():
        pytest.skip(f"{_GOLD} not found — run the iris + corporate_receiver_enrich chain first")
    df = pl.read_parquet(_GOLD, columns=["notice_subtype", "solvency_signal"])

    assert set(df["solvency_signal"].unique().drop_nulls()) <= _BANDS

    recomputed = df.select(solvency_signal_expr()).to_series()
    mismatches = (df["solvency_signal"] != recomputed).sum()
    assert mismatches == 0, f"{mismatches} rows where the stored solvency_signal != the mapping"


@pytest.mark.sql
def test_gold_never_marks_a_solvent_subtype_insolvent():
    """The invariant that protects a real company from a false statement."""
    if not _GOLD.exists():
        pytest.skip(f"{_GOLD} not found — run the iris + corporate_receiver_enrich chain first")
    df = pl.read_parquet(_GOLD, columns=["notice_subtype", "solvency_signal"])
    bad = df.filter(
        pl.col("notice_subtype").is_in(list(_MUST_NOT_BE_INSOLVENT))
        & (pl.col("solvency_signal") == SOLVENCY_INSOLVENT)
    )
    assert bad.height == 0, f"{bad.height} solvent-subtype rows marked insolvent: {bad.head(5).to_dicts()}"


@pytest.mark.sql
def test_insolvent_share_is_a_minority_of_the_insolvency_category():
    """Ratchet on the premise: if this ever flips, the category has been re-scoped and the
    'category != finding' reasoning in the view/docstrings needs revisiting (anchored
    2026-07-18: 29.2% insolvent / 39.4% solvent / 31.4% unknown of 44,581)."""
    if not _GOLD.exists():
        pytest.skip(f"{_GOLD} not found — run the iris + corporate_receiver_enrich chain first")
    df = pl.read_parquet(_GOLD, columns=["notice_category", "solvency_signal"]).filter(
        pl.col("notice_category") == "corporate_insolvency"
    )
    if not df.height:
        pytest.skip("no corporate_insolvency rows")
    solvent = int((df["solvency_signal"] == SOLVENCY_SOLVENT).sum())
    assert solvent / df["notice_category"].len() > 0.20, (
        "solvent share collapsed — either the classifier changed or MVLs left the category; "
        "the 'notice_category is a scope bucket, not a finding' premise must be re-verified"
    )
