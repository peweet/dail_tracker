"""Unit tests for reference/ec_constituency_pop_extract.integrity_check.

Pure gate (DataFrame -> verdict dict, no IO) that decides whether the Census
2022 constituency-population parquet is allowed to be written. It is the
tripwire for PDF-layout drift in the Electoral Commission report: if the
Appendix-2 table moves and the parse comes back wrong, these invariants must
go RED and block the --write. So the contract is worth locking:
  * exactly 43 constituency rows
  * population_2022 sums to the report national total (5,149,139)
  * derived seats sum to 174
  * no null populations
"""

import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from reference.ec_constituency_pop_extract import (  # noqa: E402
    _NATIONAL_TOTAL,
    _TOTAL_SEATS,
    integrity_check,
)


def _frame(populations, seats):
    return pl.DataFrame({"population_2022": populations, "td_seats_2024": seats})


def _green_inputs():
    """43 rows whose populations sum to the national total and seats to 174."""
    pops = [100_000] * 42 + [_NATIONAL_TOTAL - 42 * 100_000]
    seats = [4] * 42 + [_TOTAL_SEATS - 42 * 4]
    return pops, seats


def test_green_when_all_invariants_hold():
    pops, seats = _green_inputs()
    rpt = integrity_check(_frame(pops, seats))
    assert rpt["green"] is True
    assert all(rpt["checks"].values())
    assert rpt["pop_sum"] == _NATIONAL_TOTAL
    assert rpt["seat_sum"] == _TOTAL_SEATS


def test_wrong_row_count_blocks_write():
    rpt = integrity_check(_frame([100_000] * 10, [4] * 10))
    assert rpt["checks"]["row_count_43"] is False
    assert rpt["green"] is False


def test_population_off_by_one_blocks_write():
    pops, seats = _green_inputs()
    pops[-1] += 1  # 43 rows, seats fine, but total no longer matches the report
    rpt = integrity_check(_frame(pops, seats))
    assert rpt["checks"]["population_sums_to_national_total"] is False
    assert rpt["green"] is False


def test_wrong_seat_total_blocks_write():
    pops, seats = _green_inputs()
    seats[-1] += 1  # seats sum to 175, not 174
    rpt = integrity_check(_frame(pops, seats))
    assert rpt["checks"]["seats_sum_to_174"] is False
    assert rpt["green"] is False


def test_null_population_blocks_write():
    pops, seats = _green_inputs()
    pops[-1] = None  # a missing population must fail the no-null gate
    rpt = integrity_check(_frame(pops, seats))
    assert rpt["checks"]["no_null_population"] is False
    assert rpt["green"] is False
