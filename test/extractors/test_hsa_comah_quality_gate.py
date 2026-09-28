"""The COMAH quality ratchet must be able to FAIL (gate-trap rule: prove it before trusting
it). Synthetic frames only — no data files, CI-safe. Added 2026-09-04 with the ratchet."""

import polars as pl
import pytest

from extractors.hsa_comah_extract import (
    _MAX_UNGEOCODED,
    _MAX_UPPER_TIER_ADDRESS,
    _MAX_UPPER_TIER_TOWN,
    apply_pins,
    curated_pins,
    enforce_quality,
)


def _frame(upper_town: int, ungeocoded: int, upper_address: int = 0) -> pl.DataFrame:
    rows = []
    for i in range(upper_town):
        rows.append({"establishment": f"UT{i}", "tier": "upper", "geocode_precision": "town", "lat": 53.0})
    for i in range(upper_address):
        rows.append({"establishment": f"UA{i}", "tier": "upper", "geocode_precision": "address", "lat": 53.0})
    for i in range(ungeocoded):
        rows.append({"establishment": f"UG{i}", "tier": "lower", "geocode_precision": None, "lat": None})
    rows.append({"establishment": "OK", "tier": "upper", "geocode_precision": "site", "lat": 53.0})
    return pl.DataFrame(rows)


def test_passes_at_the_committed_baseline():
    enforce_quality(_frame(_MAX_UPPER_TIER_TOWN, _MAX_UNGEOCODED, _MAX_UPPER_TIER_ADDRESS))


def test_fails_when_upper_tier_address_rows_exceed_baseline():
    with pytest.raises(SystemExit, match="upper-tier address-precision"):
        enforce_quality(_frame(0, 0, _MAX_UPPER_TIER_ADDRESS + 1))


_REGISTER = pl.DataFrame(
    [
        {
            "establishment": "Circle K Galway Terminal",
            "address": "Galway Harbour Enterprise Park, Co. Galway",
            "lon": -9.0402,
            "lat": 53.2697,
            "geocode_source": "nominatim",
            "geocode_precision": "address",
            "geocode_note": "",
        },
        {
            "establishment": "Calor Teoranta",
            "address": "Tolka Quay Road, Dublin Port, Dublin 1",
            "lon": -6.2181,
            "lat": 53.3521,
            "geocode_source": "nominatim",
            "geocode_precision": "address",
            "geocode_note": "",
        },
        {
            "establishment": "Calor Teoranta",
            "address": "Whitegate Filling Plant, Midleton, Co. Cork",
            "lon": -8.1757,
            "lat": 51.9212,
            "geocode_source": "nominatim",
            "geocode_precision": "town",
            "geocode_note": "",
        },
    ]
)


def _pin(name, address=None, lat=1.0, lon=2.0):
    row = {
        "establishment": name,
        "method": "manual",
        "epa_reg_cd": None,
        "lat": lat,
        "lon": lon,
        "source": "test",
        "address": address,
    }
    (pin,) = curated_pins(pl.DataFrame([row]), {})
    return pin


def test_name_only_pin_on_a_single_site_upgrades_it_to_site_grade():
    out = apply_pins(_REGISTER, [_pin("Circle K Galway Terminal", lat=53.268963, lon=-9.040658)])
    row = out.row(0, named=True)
    assert (row["lat"], row["lon"], row["geocode_precision"]) == (53.268963, -9.040658, "site")
    assert out.filter(pl.col("establishment") == "Calor Teoranta")["geocode_precision"].to_list() == ["address", "town"]


def test_name_only_pin_on_a_multi_site_name_fails_rather_than_moving_every_site():
    with pytest.raises(SystemExit, match="matches 2 register sites"):
        apply_pins(_REGISTER, [_pin("Calor Teoranta")])


def test_address_qualified_pin_moves_only_that_site():
    out = apply_pins(_REGISTER, [_pin("Calor Teoranta", "Whitegate  Filling Plant, Midleton, Co. Cork", lat=51.83)])
    calor = out.filter(pl.col("establishment") == "Calor Teoranta")
    assert calor["lat"].to_list() == [53.3521, 51.83]
    assert calor["geocode_precision"].to_list() == ["address", "site"]


def test_unmatched_pin_fails_loudly():
    with pytest.raises(SystemExit, match="matched no register row"):
        apply_pins(_REGISTER, [_pin("Renamed Establishment Ltd")])


def test_csv_without_an_address_column_still_loads_as_name_only_pins():
    curated = pl.DataFrame(
        [
            {
                "establishment": "Circle K Galway Terminal",
                "tier": "upper",
                "method": "manual",
                "epa_reg_cd": None,
                "lat": 53.268963,
                "lon": -9.040658,
                "source": "test",
                "verified_date": "2026-09-27",
                "note": "",
            }
        ]
    )
    (pin,) = curated_pins(curated, {})
    assert pin["address"] is None and pin["name"] == "circle k galway terminal"


def test_fails_when_upper_tier_town_rows_exceed_baseline():
    with pytest.raises(SystemExit, match="upper-tier town-precision"):
        enforce_quality(_frame(_MAX_UPPER_TIER_TOWN + 1, 0))


def test_fails_when_ungeocoded_rows_exceed_baseline():
    with pytest.raises(SystemExit, match="ungeocoded"):
        enforce_quality(_frame(0, _MAX_UNGEOCODED + 1))
