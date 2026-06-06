"""Privacy test for SIPO silver→gold promotion (donor address must never reach gold).

Locks ``extractors/sipo_promote_to_gold.py:address_columns()`` — the helper that both
DROPS address columns before writing gold and backs the runtime guard that refuses the
write if any survive. gold/parquet/ is committed to the public repo, so a donor HOME
ADDRESS reaching it is a PII incident. Donor NAMES + AMOUNTS are the public SIPO record
and are intentionally kept. (Replaces a -O-strippable, post-write assert — synthesis INC-2.)

Run:  pytest test/test_sipo_promote_privacy.py -v
"""

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT / "extractors"))

pl = pytest.importorskip("polars")
from sipo_promote_to_gold import address_columns  # noqa: E402


def test_detects_address_columns():
    df = pl.DataFrame({
        "donor_name": ["A. Byrne"],
        "donor_address_raw": ["1 Main St"],
        "donor_address": ["Town"],
        "Address": ["X"],
        "value_eur": [500.0],
    })
    assert set(address_columns(df)) == {"donor_address_raw", "donor_address", "Address"}


def test_clean_frame_has_no_address_columns():
    df = pl.DataFrame({"donor_name": ["A. Byrne"], "value_eur": [500.0]})
    assert address_columns(df) == []


def test_donor_name_is_not_treated_as_address():
    # names + amounts are the public record and must NOT be flagged
    df = pl.DataFrame({"donor_name": ["Mary Quill"], "value_eur": [1000.0]})
    assert address_columns(df) == []


@pytest.mark.integration
def test_committed_gold_has_no_address_column():
    """The committed, public gold donations parquet must carry no address column."""
    gold = _ROOT / "data" / "gold" / "parquet" / "sipo_donations.parquet"
    if not gold.exists():
        pytest.skip("gold not built; run sipo_promote_to_gold.py first")
    assert address_columns(pl.read_parquet(gold)) == []
