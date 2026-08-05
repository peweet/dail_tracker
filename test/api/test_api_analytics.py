"""Contract tests for the analytical core-query adapters."""

from __future__ import annotations

import pandas as pd
import pytest

pytest.importorskip("fastapi")
pytest.importorskip("duckdb")

from api.main import app, root
from api.routers import analytics
from dail_tracker_core.results import QueryResult


def test_every_new_analytics_route_has_a_response_schema() -> None:
    schema = app.openapi()
    expected = {
        "/v1/analytics/attendance/taa-compliance",
        "/v1/analytics/attendance/year-ranking",
        "/v1/analytics/entities/cross-register",
        "/v1/analytics/interests",
        "/v1/analytics/judiciary/appointing-authorities",
        "/v1/analytics/legislation/circular-si-crosswalk",
        "/v1/analytics/legislation/introduced-year-counts",
        "/v1/analytics/lobbying/summary",
        "/v1/analytics/local-government/derelict-levy",
        "/v1/analytics/ministerial/access-to-contracts",
        "/v1/analytics/ministerial/company-influence",
        "/v1/analytics/procurement/nphdb-bam-disclosures",
        "/v1/analytics/payments/member-year",
        "/v1/analytics/payments/summary",
        "/v1/analytics/procurement/afs/coverage",
        "/v1/analytics/procurement/afs/national-by-year",
        "/v1/analytics/procurement/afs/council-by-year",
        "/v1/analytics/procurement/afs/po-coverage",
        "/v1/analytics/votes/member-year",
    }
    assert expected <= set(schema["paths"])
    for path in expected:
        response = schema["paths"][path]["get"]["responses"]["200"]
        assert response["content"]["application/json"]["schema"]

    analytics_routes = [
        route
        for route in app.routes
        if getattr(getattr(route, "endpoint", None), "__module__", None) == analytics.__name__
    ]
    assert analytics_routes
    assert all(route.response_model_exclude_unset for route in analytics_routes)


def test_adapter_requires_query_and_returns_uniform_envelope(monkeypatch) -> None:
    result = QueryResult.success(pd.DataFrame([{"member": "Example", "days": 120}]))
    monkeypatch.setattr(analytics.attendance, "taa_compliance_summary", lambda *_a, **_k: result)
    response = analytics.attendance_taa_summary(year=2025, house="Dáil", cur=object())
    assert response["head"]["total"] == 1
    assert response["results"] == [{"member": "Example", "days": 120}]


def test_root_discovery_is_derived_from_every_registered_get_route() -> None:
    expected = sorted(
        {
            route.path
            for route in app.routes
            if route.path.startswith("/v1")
            and getattr(route, "include_in_schema", True)
            and "GET" in (getattr(route, "methods", None) or set())
        }
    )

    resources = root()["resources"]
    assert resources == expected
    assert "/v1/analytics/payments/summary" in resources
    assert "/v1/procurement/real-trends" not in resources
