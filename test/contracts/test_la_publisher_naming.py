"""Contract: local-authority publisher_name in gold must speak the canonical LA vocabulary.

THE DEFECT THIS GATES (live as of 2026-07-14)
---------------------------------------------
``procurement_payments_fact.publisher_name`` mixes two naming conventions for councils:
23 use the canonical short form ("Clare", "Cork City") and 8 use the official long form
("Carlow County Council", "Dublin City Council"). The constituency<->LA crosswalk
(data/_meta/constituency_la_crosswalk.csv) and the AFS council facts key on the SHORT form,
so those 8 councils join NOTHING — ~41.6% of local-authority procurement rows are orphaned
from every council/constituency rollup. Dublin City alone is ~40k rows.

It is not a double-count: no council appears under both spellings, so the money is
unreachable rather than inflated. (Deliberately no € total asserted here — the column
carries two grains, payment_actual and po_committed; summing across them would break the
never-union rule.)

THE REAL LESSON: ``publisher_id`` is already clean and consistent for all 31 councils
(ie_la_carlow, ie_la_dublin_city, ie_la_cork_city). The join should key on that stable id,
not on a display string. The fix is to normalise publisher_name off publisher_id.

WHY AN ALLOWLIST
----------------
Same convention as KNOWN_SHIP_GAPS in test/tools/test_runtime_manifest.py: the 8 known
offenders are recorded so the contract can gate TODAY without a red build, while any NEW
council drifting out of the vocabulary fails immediately. When the data is renormalised,
empty KNOWN_LA_NAME_DRIFT — the test then enforces the vocabulary with no exceptions and
this file becomes a plain contract.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from reference.ec_constituency_crosswalk_extract import _LOCAL_AUTHORITIES

pytestmark = pytest.mark.sql

_FACT = Path("data/gold/parquet/procurement_payments_fact.parquet")

# EMPTY since 2026-07-18 — the 8 long-form council names were renormalised upstream, so
# the vocabulary now holds with no exceptions. Do NOT add without a deliberate decision —
# a new entry here means a council's spend has fallen out of the crosswalk.
KNOWN_LA_NAME_DRIFT: set[str] = set()


@pytest.fixture(scope="module")
def la_publisher_names() -> set[str]:
    if not _FACT.exists():
        pytest.skip("procurement_payments_fact not present")
    con = duckdb.connect()
    try:
        rows = con.execute(
            f"""
            SELECT DISTINCT publisher_name
            FROM read_parquet('{_FACT.as_posix()}')
            WHERE publisher_type = 'local_authority'
              AND publisher_name IS NOT NULL
            """
        ).fetchall()
    finally:
        con.close()
    return {r[0] for r in rows}


def test_canonical_vocabulary_is_the_31_local_authorities() -> None:
    """Pins the source of truth the crosswalk and the AFS facts both key on."""
    assert len(_LOCAL_AUTHORITIES) == 31, f"expected 31 LAs, got {len(_LOCAL_AUTHORITIES)}"


def test_no_new_local_authority_name_drift(la_publisher_names: set[str]) -> None:
    """Every LA publisher_name is canonical, or a KNOWN (documented) drifted name."""
    canonical = set(_LOCAL_AUTHORITIES)
    drifted = la_publisher_names - canonical
    new_drift = drifted - KNOWN_LA_NAME_DRIFT

    assert not new_drift, (
        "NEW local-authority publisher_name(s) outside the canonical vocabulary — these "
        "councils' spend will not join the constituency/LA crosswalk and is orphaned from "
        f"every council rollup:\n  {sorted(new_drift)}\n"
        "Normalise publisher_name off the stable publisher_id (ie_la_*), or, if this rename "
        "is intended, add it to KNOWN_LA_NAME_DRIFT with a reason."
    )


def test_known_drift_list_has_not_gone_stale(la_publisher_names: set[str]) -> None:
    """If a drifted name is fixed, its allowlist entry must go — the list must not rot.

    This is what turns the allowlist into a RATCHET rather than a permanent excuse.
    """
    fixed = KNOWN_LA_NAME_DRIFT - la_publisher_names
    assert not fixed, (
        "These names are in KNOWN_LA_NAME_DRIFT but no longer appear in the fact — the drift "
        f"was fixed. Remove them from the allowlist:\n  {sorted(fixed)}"
    )
