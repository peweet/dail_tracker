"""A shared operator name cannot place a COMAH establishment at another facility.

The Dublin22/Cork address counterexample comes from the independently reviewed
publisher rows retained in the planning robustness audit, not a production-parquet fixture.
"""

import polars as pl
import pytest

from extractors import hsa_comah_extract as extractor
from services.geometry import points_to_wkb
from services.parquet_io import save_parquet


def _epa(monkeypatch, tmp_path, rows):
    frame = pl.DataFrame(rows).with_columns(
        pl.Series("wkb", points_to_wkb([row["lon"] for row in rows], [row["lat"] for row in rows]), dtype=pl.Binary)
    )
    path = tmp_path / "small-publisher-records.parquet"
    save_parquet(frame.drop("lon", "lat"), path)
    monkeypatch.setattr(extractor, "_EPA_LAYER", path)


def _register(name, address):
    return pl.DataFrame([{"establishment": name, "address": address, "tier": "upper", "region": "fixture"}])


@pytest.mark.parametrize(
    "name,address",
    [
        ("Irish Distillers Pernod Ricard", "Robinhood Road, Fox & Geese, Clondalkin, Dublin22"),
        ("Irish Distillers Limited", "Robinhood Road, Fox & Geese, Clondalkin, Co. Dublin"),
    ],
)
def test_name_overlap_does_not_override_conflicting_site(monkeypatch, tmp_path, name, address):
    _epa(
        monkeypatch,
        tmp_path,
        [
            {
                "Name": "Irish Distillers Limited",
                "Address": "Midleton Distilleries, Midleton, Cork, P25X589",
                "RegCD": "P0442",
                "lon": -8.16182949,
                "lat": 51.9169917,
            }
        ],
    )
    row = extractor.match_epa(_register(name, address)).row(0, named=True)
    assert row["epa_reg_cd"] is None
    assert row["lat"] is None


def test_same_county_operator_still_requires_site_identity(monkeypatch, tmp_path):
    _epa(
        monkeypatch,
        tmp_path,
        [
            {
                "Name": "Example Chemicals Limited",
                "Address": "South Road, Cork City, Cork",
                "RegCD": "fixture-P1",
                "lon": -8.5,
                "lat": 51.9,
            }
        ],
    )
    row = extractor.match_epa(_register("Example Chemicals Limited", "North Road, Cork City, Cork")).row(0, named=True)
    assert row["epa_reg_cd"] is None


def test_exact_publisher_address_and_name_is_an_admissible_match(monkeypatch, tmp_path):
    _epa(
        monkeypatch,
        tmp_path,
        [
            {
                "Name": "Example Chemicals Ltd",
                "Address": "North Road, Cork City, Cork",
                "RegCD": "fixture-P1",
                "lon": -8.5,
                "lat": 51.9,
            }
        ],
    )
    row = extractor.match_epa(_register("Example Chemicals Limited", "North Road, Cork City, Cork")).row(0, named=True)
    assert row["epa_reg_cd"] == "fixture-P1"
    assert row["epa_match_status"] == "matched_publisher_address"


def test_ambiguous_equal_matches_do_not_take_first_row(monkeypatch, tmp_path):
    _epa(
        monkeypatch,
        tmp_path,
        [
            {
                "Name": "Example Chemicals Ltd",
                "Address": "North Road, Cork City, Cork",
                "RegCD": "fixture-P1",
                "lon": -8.5,
                "lat": 51.9,
            },
            {
                "Name": "Example Chemicals Ltd",
                "Address": "North Road, Cork City, Cork",
                "RegCD": "fixture-P2",
                "lon": -8.6,
                "lat": 51.8,
            },
        ],
    )
    row = extractor.match_epa(_register("Example Chemicals Limited", "North Road, Cork City, Cork")).row(0, named=True)
    assert row["epa_reg_cd"] is None
    assert row["epa_match_status"] == "ambiguous_site_identity"
