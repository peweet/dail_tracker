"""Restartable ArcGIS acquisition contracts for national planning metadata."""

# Runtime import intentionally precedes Polars; native thread caps are load-order sensitive.
# ruff: noqa: I001

from __future__ import annotations

import re

# Keep native Polars import safe in direct pytest collection on Windows.
import services.runtime_env as _runtime_env  # noqa: F401

import polars as pl
import pytest
from planning.civic import acquisition
from planning.civic.extractors import planning_applications_ingest as ingest


def _source(ids=(1, 2, 3), generation=10, *, wrong_page_ids=None):
    calls = []
    metadata = {
        "objectIdField": "OBJECTID",
        "fields": [
            {"name": "OBJECTID", "type": "esriFieldTypeOID"},
            {"name": "PlanningAuthority", "type": "esriFieldTypeString"},
        ],
        "maxRecordCount": 2,
        "editingInfo": {"lastEditDate": generation},
    }

    def request(url, params):
        calls.append((url, dict(params)))
        if not url.endswith("/query"):
            return metadata
        if params.get("returnIdsOnly") == "true":
            return {"objectIds": list(ids)}
        if params.get("returnCountOnly") == "true":
            return {"count": len(ids)}
        match = re.search(r"OBJECTID >= (\d+) AND OBJECTID <= (\d+)", params["where"])
        low, high = int(match.group(1)), int(match.group(2))
        selected = list(range(low, high + 1))
        if wrong_page_ids is not None and low == min(ids):
            selected = wrong_page_ids
        return {
            "features": [
                {"attributes": {"OBJECTID": value, "PlanningAuthority": "Test Council"}, "geometry": {}}
                for value in selected
            ]
        }

    return request, calls


def test_inventory_errors_fail_closed_before_any_page_is_staged(tmp_path):
    def request(_url, params):
        if params.get("f") == "json" and params.get("returnIdsOnly") is None:
            return {
                "objectIdField": "OBJECTID",
                "fields": [{"name": "OBJECTID", "type": "esriFieldTypeOID"}],
                "maxRecordCount": 2,
            }
        if params.get("returnIdsOnly") == "true":
            return {"error": {"message": "upstream unavailable"}}
        return {"count": 0}

    collector = acquisition.ArcGISStagedCollector(
        request=request,
        checkpoint_dir=tmp_path,
        resume=False,
        page_size=2,
    )

    with pytest.raises(acquisition.AcquisitionError, match="error"):
        collector.collect_layer(
            "https://example.test/FeatureServer/0",
            where="1=1",
            layer_name="points",
            transform=lambda rows: rows,
        )

    assert not list(tmp_path.glob("*.parquet"))


def test_interrupted_collection_reuses_valid_pages_and_fetches_only_missing(tmp_path):
    request, calls = _source()
    with acquisition.ArcGISStagedCollector(request, tmp_path, page_size=2, max_pages=1) as collector:
        partial = collector.collect_layer(
            "https://example.test/FeatureServer/0", layer_name="points", transform=lambda rows: pl.DataFrame(rows)
        )
        assert partial.complete is False
        assert partial.fetched_pages == 1
    first_page_requests = [params for _, params in calls if "resultRecordCount" in params]

    request2, calls2 = _source()
    with acquisition.ArcGISStagedCollector(request2, tmp_path, page_size=2, resume=True) as collector:
        finished = collector.collect_layer(
            "https://example.test/FeatureServer/0", layer_name="points", transform=lambda rows: pl.DataFrame(rows)
        )
        assert finished.complete is True
        assert finished.reused_pages == 1
        assert finished.fetched_pages == 1
    second_page_requests = [params for _, params in calls2 if "resultRecordCount" in params]
    assert len(first_page_requests) == 1
    assert len(second_page_requests) == 1
    assert "OBJECTID >= 3" in second_page_requests[0]["where"]


def test_any_capped_run_stays_incomplete_even_when_cap_covers_inventory(tmp_path):
    request, _ = _source(ids=(1, 2))
    with acquisition.ArcGISStagedCollector(request, tmp_path, page_size=2, max_pages=1) as collector:
        result = collector.collect_layer(
            "https://example.test/FeatureServer/0", layer_name="points", transform=lambda rows: pl.DataFrame(rows)
        )
    assert result.complete is False
    with (
        acquisition.ArcGISStagedCollector(request, tmp_path, page_size=2, resume=True) as collector,
        pytest.raises(acquisition.AcquisitionError, match="incomplete"),
    ):
        collector.assemble(result, tmp_path / "canonical.parquet")


def test_resume_rejects_changed_generation_and_corrupt_page(tmp_path):
    request, _ = _source(ids=(1, 2), generation=10)
    with acquisition.ArcGISStagedCollector(request, tmp_path, page_size=2) as collector:
        result = collector.collect_layer(
            "https://example.test/FeatureServer/0", layer_name="points", transform=lambda rows: pl.DataFrame(rows)
        )
        page = result.page_paths[0]
    page.write_bytes(b"corrupt")
    changed, _ = _source(ids=(1, 2), generation=11)
    with (
        acquisition.ArcGISStagedCollector(changed, tmp_path, page_size=2, resume=True) as collector,
        pytest.raises(acquisition.AcquisitionError, match="identity changed"),
    ):
        collector.collect_layer(
            "https://example.test/FeatureServer/0", layer_name="points", transform=lambda rows: pl.DataFrame(rows)
        )

    same, _ = _source(ids=(1, 2), generation=10)
    with (
        acquisition.ArcGISStagedCollector(same, tmp_path, page_size=2, resume=True) as collector,
        pytest.raises(acquisition.AcquisitionError, match="corrupt staged page"),
    ):
        collector.collect_layer(
            "https://example.test/FeatureServer/0", layer_name="points", transform=lambda rows: pl.DataFrame(rows)
        )


def test_returned_page_ids_must_match_planned_chunk(tmp_path):
    request, _ = _source(ids=(1, 2), wrong_page_ids=[1, 3])
    with (
        acquisition.ArcGISStagedCollector(request, tmp_path, page_size=2) as collector,
        pytest.raises(acquisition.AcquisitionError, match="does not match"),
    ):
        collector.collect_layer(
            "https://example.test/FeatureServer/0", layer_name="points", transform=lambda rows: pl.DataFrame(rows)
        )


def test_point_adapter_keeps_transform_schema_when_source_page_is_all_null(tmp_path):
    request, _ = _source(ids=(1, 2))
    with acquisition.ArcGISStagedCollector(request, tmp_path, page_size=2) as collector:
        result = collector.collect_layer(
            "https://example.test/FeatureServer/0",
            layer_name="points",
            adapt_page=ingest._adapt_point_page,
            transform=ingest.transform,
        )
        frame = pl.read_parquet(result.page_paths[0])
    assert result.complete is True
    assert frame.height == 2
    assert {"decision_category", "decision_normalised", "dq_flags", "geo_in_bounds"} <= set(frame.columns)
    assert "OBJECTID" not in frame.columns


def _capped_source(ids, cap, *, ignore_window=False):
    """A source whose returnIdsOnly stops at `cap` IDs, as IrishPlanningApplications L0 did on 2026-09-29."""
    request, calls = _source(ids=ids)
    window = re.compile(r"OBJECTID >= (\d+) AND OBJECTID <= (\d+)")

    def capped(url, params):
        if params.get("outStatistics"):
            calls.append((url, dict(params)))
            return {"features": [{"attributes": {"OID_MIN": min(ids), "OID_MAX": max(ids)}}]}
        if params.get("returnIdsOnly") == "true":
            calls.append((url, dict(params)))
            match = window.search(params["where"])
            selected = list(ids)
            if match:
                low, high = int(match.group(1)), int(match.group(2))
                # ignore_window: the server answers every window with the first IDs, whatever was asked.
                selected = list(ids)[: high - low + 1] if ignore_window else [i for i in ids if low <= i <= high]
            if len(selected) > cap:
                return {"objectIds": selected[:cap], "exceededTransferLimit": True}
            return {"objectIds": selected}
        return request(url, params)

    return capped, calls


def test_capped_id_inventory_is_rebuilt_from_objectid_windows(tmp_path):
    ids = tuple(range(1, 8))
    request, calls = _capped_source(ids, cap=4)
    with acquisition.ArcGISStagedCollector(request, tmp_path, page_size=2) as collector:
        result = collector.collect_layer(
            "https://example.test/FeatureServer/0", layer_name="points", transform=lambda rows: pl.DataFrame(rows)
        )
    assert result.complete is True
    assert result.expected_count == result.fetched_count == 7
    windows = [p["where"] for _, p in calls if p.get("returnIdsOnly") == "true" and "OBJECTID >=" in p["where"]]
    one_pass = [f"(1=1) AND OBJECTID >= {lo} AND OBJECTID <= {min(lo + 1, 7)}" for lo in (1, 3, 5, 7)]
    # Once to plan the pull, once more in the pre-publication revalidation of the same inventory.
    assert windows == one_pass * 2


def test_window_returning_ids_outside_its_range_fails_closed(tmp_path):
    request, _ = _capped_source(tuple(range(1, 8)), cap=4, ignore_window=True)
    with (
        acquisition.ArcGISStagedCollector(request, tmp_path, page_size=2) as collector,
        pytest.raises(acquisition.AcquisitionError, match="outside the requested window"),
    ):
        collector.collect_layer(
            "https://example.test/FeatureServer/0", layer_name="points", transform=lambda rows: pl.DataFrame(rows)
        )
    assert not list(tmp_path.glob("*.parquet"))


def test_collector_request_seam_preserves_metadata_and_query_urls(monkeypatch):
    seen = []

    def fake_fetch(url, *, params, headers, timeout):
        seen.append((url, params, headers, timeout))
        return {"ok": True}, object()

    monkeypatch.setattr(ingest, "fetch_json", fake_fetch)
    assert ingest._arcgis_request("https://example.test/FeatureServer/0", {"f": "json"}) == {"ok": True}
    assert seen[0][0] == "https://example.test/FeatureServer/0"
    assert ingest._arcgis_request("https://example.test/FeatureServer/0/query", {"where": "1=1"}) == {"ok": True}
    assert seen[1][0].endswith("/query")
