"""The Phase-4 API contract: uniform envelope, typed error kinds, one pagination
convention. These tests lock the cross-cutting guarantees (not per-resource data).
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

pytest.importorskip("fastapi")
import duckdb  # noqa: E402
import pandas as pd  # noqa: E402
from fastapi import FastAPI, HTTPException  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from api.deps import Page, get_cursor  # noqa: E402
from api.main import _PUBLIC_UNAVAILABLE_DETAIL, _http_error, _source_unavailable, app  # noqa: E402
from api.routers import health, procurement  # noqa: E402
from dail_tracker_core import caveats  # noqa: E402
from dail_tracker_core.results import QueryResult, SourceUnavailable  # noqa: E402


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


def test_error_bodies_carry_machine_readable_kind(client):
    body = client.get("/v1/members/__no_such_code__/dossier")
    assert body.status_code == 404
    assert body.json()["kind"] == "not_found"

    # Validation failures (limit over cap) are bad_request, not a bare 422 shape.
    body = client.get("/v1/votes", params={"limit": 999999})
    assert body.status_code == 422
    assert body.json()["kind"] == "bad_request"


def test_enveloped_lists_are_self_dating(client):
    body = client.get("/v1/members", params={"limit": 1}).json()
    head = body["head"]
    assert set(head) >= {"limit", "offset", "truncated", "generated_at"}
    assert head["generated_at"].startswith("20")  # ISO-8601 UTC stamp


def test_pagination_convention_defaults(client):
    # The shared dependency's floor: default 50, cap 500 (deviations are declared
    # per-endpoint via pagination(default=, cap=) and visible in OpenAPI).
    spec = client.get("/openapi.json").json()
    votes_params = {p["name"]: p for p in spec["paths"]["/v1/votes"]["get"]["parameters"]}
    assert votes_params["limit"]["schema"]["default"] == 50
    assert votes_params["limit"]["schema"]["maximum"] == 500


def test_health_is_not_ready_without_registered_views():
    probe = FastAPI()
    probe.include_router(health.router, prefix="/v1")
    conn = duckdb.connect()
    probe.dependency_overrides[get_cursor] = lambda: conn
    try:
        with TestClient(probe) as client:
            response = client.get("/v1/health")
        assert response.status_code == 503
        assert response.json()["detail"].startswith("database missing required data views: ")
        assert "v_payments_base" in response.json()["detail"]
    finally:
        probe.dependency_overrides.clear()
        conn.close()


def test_health_requires_a_data_backed_core_view():
    probe = FastAPI()
    probe.include_router(health.router, prefix="/v1")
    conn = duckdb.connect()
    conn.execute("CREATE VIEW v_payments_sources AS SELECT 'metadata only' AS source")
    probe.dependency_overrides[get_cursor] = lambda: conn
    try:
        with TestClient(probe) as client:
            assert client.get("/v1/health").status_code == 503
            for view in health._LIVENESS_VIEWS:
                conn.execute(f"CREATE VIEW {view} AS SELECT 1 AS value")
            response = client.get("/v1/health")
            assert client.get("/v1/readiness").status_code == 503
            for view in health._REQUIRED_VIEWS - health._LIVENESS_VIEWS:
                conn.execute(f"CREATE VIEW {view} AS SELECT 1 AS value")
            ready = client.get("/v1/readiness")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"
        assert response.json()["views_registered"] == len(health._LIVENESS_VIEWS) + 1
        assert ready.status_code == 200
        assert ready.json()["views_registered"] == len(health._REQUIRED_VIEWS) + 1
    finally:
        probe.dependency_overrides.clear()
        conn.close()


def test_snapshot_cache_returns_a_validator_and_conditional_304(client):
    first = client.get("/v1/votes", params={"limit": 1})
    assert first.status_code == 200
    assert first.headers["cache-control"] == "public, max-age=60, stale-while-revalidate=300"
    assert len(first.headers["x-data-snapshot"]) == 64
    etag = first.headers["etag"]

    unchanged = client.get("/v1/votes", params={"limit": 1}, headers={"If-None-Match": etag})
    assert unchanged.status_code == 304
    assert unchanged.content == b""
    assert unchanged.headers["etag"] == etag
    assert unchanged.headers["x-data-snapshot"] == first.headers["x-data-snapshot"]


def test_unavailable_http_errors_do_not_expose_internal_detail():
    response = asyncio.run(_http_error(None, HTTPException(status_code=503, detail="DuckDB at /private/path failed")))
    assert response.status_code == 503
    assert response.body == (f'{{"detail":"{_PUBLIC_UNAVAILABLE_DETAIL}","kind":"unavailable"}}'.encode())

    source_response = asyncio.run(_source_unavailable(None, SourceUnavailable("DuckDB at /private/path failed")))
    assert source_response.status_code == 503
    assert _PUBLIC_UNAVAILABLE_DETAIL.encode() in source_response.body


def test_pre_tender_api_helpers_preserve_the_portable_caveat_and_failure_states():
    result = QueryResult.success(pd.DataFrame([{"lead_id": "lead-1", "amount_is_not_aggregable": True}]))
    count = QueryResult.success(pd.DataFrame([{"total": 3}]))
    payload = procurement._pre_tender_list(result, count, page=Page(skip=1, limit=2))

    assert payload["head"]["caveat"] == caveats.PRE_TENDER
    assert payload["head"]["truncated"] is True
    assert payload["head"]["offset"] == 1
    assert payload["head"]["total"] == 3
    assert payload["results"] == [{"lead_id": "lead-1", "amount_is_not_aggregable": True}]

    with pytest.raises(HTTPException, match="pre-tender observation not found") as missing:
        procurement._pre_tender_detail(QueryResult.success(pd.DataFrame()))
    assert missing.value.status_code == 404

    with pytest.raises(HTTPException, match="pre-tender source is unavailable") as unavailable:
        procurement._pre_tender_list(QueryResult.unavailable("missing view"), count, page=Page(skip=0, limit=2))
    assert unavailable.value.status_code == 503


def test_pre_tender_catalogue_contract_requires_both_backing_views(client):
    from api.routers.catalog import required_catalog_views

    assert {
        "v_procurement_pre_tender_leads",
        "v_procurement_pre_tender_work_packages",
    }.issubset(required_catalog_views())
    resource = next(
        r for r in client.get("/v1/catalog").json()["resources"] if r["resource"] == "procurement-pre-tender"
    )
    assert resource["count"] is None or isinstance(resource["count"], int)
    assert "required_views" not in resource
