"""Polygon retention contracts for the two official historic-site sources."""

from __future__ import annotations

import pytest
import shapely

from planning.civic.extractors import planning_appeal_outcomes as appeals
from planning.civic.extractors import planning_applications_ingest as applications

POLYGON = {
    "type": "Polygon",
    "coordinates": [
        [
            [-6.30, 53.30],
            [-6.29, 53.30],
            [-6.29, 53.31],
            [-6.30, 53.31],
            [-6.30, 53.30],
        ]
    ],
}


def test_application_site_fetch_retains_wkb_bbox_dates_and_provenance(monkeypatch):
    feature = {
        "type": "Feature",
        "geometry": POLYGON,
        "properties": {
            "PlanningAuthority": "Carlow County Council",
            "ApplicationNumber": "24/1",
            "Decision": "GRANT PERMISSION",
            "DecisionDate": 1_720_000_000_000,
            "ETL_DATE": 1_721_000_000_000,
            "AreaofSite": 0,
        },
    }

    monkeypatch.setattr(
        applications,
        "_query",
        lambda layer_url, **params: {"type": "FeatureCollection", "features": [feature]},
    )
    rows, reasons = applications.fetch_sites("1=1", max_pages=None)
    frame = applications.transform_sites(rows)

    assert reasons == {"ok": 1}
    assert frame.height == 1
    row = frame.row(0, named=True)
    assert shapely.from_wkb(row["wkb"]).equals(shapely.geometry.shape(POLYGON))
    assert (row["bbox_minx"], row["bbox_miny"], row["bbox_maxx"], row["bbox_maxy"]) == (
        -6.30,
        53.30,
        -6.29,
        53.31,
    )
    assert row["DecisionDate"] is not None
    assert row["ETL_DATE"] is not None
    assert row["AreaofSite"] is None
    assert row["source_licence"] == "CC BY 4.0"


@pytest.mark.parametrize(
    ("label", "malformed"),
    [
        ("unknown type -> GeometryTypeError", {"type": "NotAThing", "coordinates": "x"}),
        ("no coordinates -> KeyError", {"type": "Polygon"}),
        ("no type -> AttributeError", {"coordinates": [[[0, 0], [1, 1], [1, 0], [0, 0]]]}),
        ("bad coordinates -> ValueError", {"type": "Polygon", "coordinates": "x"}),
    ],
)
def test_malformed_geometry_is_counted_unreadable_not_raised(label, malformed):
    # Regression, 2026-08-29: the handler caught only (TypeError, ValueError), so three of these
    # four escaped and aborted the whole national ingest instead of being counted and skipped.
    # shapely's GeometryTypeError subclasses ShapelyError, not ValueError.
    geometry, reason = applications._polygonal_geometry(malformed)
    assert (geometry, reason) == (None, "unreadable"), label


def test_geometry_validation_rejects_non_irish_or_non_polygonal_shapes():
    geometry, reason = applications._polygonal_geometry({"type": "Point", "coordinates": [-6.3, 53.3]})
    assert geometry is None and reason == "not_polygonal"

    geometry, reason = applications._polygonal_geometry(
        {
            "type": "Polygon",
            "coordinates": [[[10, 10], [11, 10], [11, 11], [10, 11], [10, 10]]],
        }
    )
    assert geometry is None and reason == "bounds_escape"


def test_acp_site_fetch_retains_polygon_and_does_not_invent_a_reuse_licence(monkeypatch):
    feature = {
        "type": "Feature",
        "geometry": POLYGON,
        "properties": {
            "ABPCASEID": "123456",
            "PLANINGATY": "Carlow County Council",
            "CATEGORY": "Planning appeal",
            "DECISION": "Grant permission",
            "LODGEDON": 1_720_000_000_000,
            "DECIDED_ON": 1_721_000_000_000,
            "UPDATED_ON": 1_722_000_000_000,
            "DEVDESC": "Published description",
            "LINKABPWEB": "https://www.pleanala.ie/en-ie/case/123456",
        },
    }

    monkeypatch.setattr(
        appeals,
        "fetch_json",
        lambda *args, **kwargs: (
            {"type": "FeatureCollection", "features": [feature]},
            object(),
        ),
    )
    frame, reasons, pulled = appeals._fetch_acp_sites()

    assert pulled == 1
    assert reasons == {"ok": 1}
    row = frame.row(0, named=True)
    assert row["abp_case"] == "123456"
    assert shapely.from_wkb(row["wkb"]).equals(shapely.geometry.shape(POLYGON))
    assert row["updated_date"] is not None
    assert row["source_licence"].startswith("No reuse licence stated")
