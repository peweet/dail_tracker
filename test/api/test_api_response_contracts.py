"""OpenAPI response-contract coverage for every versioned JSON GET route."""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")

from fastapi.responses import FileResponse  # noqa: E402
from fastapi.routing import APIRoute  # noqa: E402

from api.main import app  # noqa: E402

_FILE_ROUTES = {"/v1/data/{resource}"}
_ERROR_SCHEMA_REFS = {
    "400": "#/components/schemas/BadRequestErrorResponse",
    "404": "#/components/schemas/NotFoundErrorResponse",
    "422": "#/components/schemas/BadRequestErrorResponse",
    "503": "#/components/schemas/UnavailableErrorResponse",
}


def _versioned_get_routes() -> list[APIRoute]:
    return [
        route
        for route in app.routes
        if isinstance(route, APIRoute)
        and "GET" in route.methods
        and route.path.startswith("/v1/")
        and route.include_in_schema
    ]


def _public_json_get_routes() -> list[APIRoute]:
    return [
        route
        for route in app.routes
        if isinstance(route, APIRoute)
        and "GET" in route.methods
        and (route.path == "/" or route.path.startswith("/v1/"))
        and route.include_in_schema
    ]


def test_every_public_json_get_has_a_named_response_schema() -> None:
    """Prevent a new endpoint from silently reverting to a bare ``dict`` contract."""
    spec = app.openapi()
    failures: list[str] = []

    for route in _public_json_get_routes():
        if route.path in _FILE_ROUTES:
            continue
        success = spec["paths"][route.path]["get"]["responses"]["200"]
        schema = success.get("content", {}).get("application/json", {}).get("schema")
        if route.response_model in (None, dict, list) or not schema:
            failures.append(route.path)

    assert failures == [], f"JSON GET routes without named response contracts: {failures}"


def test_bulk_download_is_a_file_response_not_a_json_model() -> None:
    route = next(route for route in _versioned_get_routes() if route.path in _FILE_ROUTES)
    assert route.response_model is None
    assert route.response_class is FileResponse


def test_router_error_envelopes_are_documented_consistently() -> None:
    """Pin the status-to-kind models used by the global exception handlers."""
    spec = app.openapi()
    failures: list[str] = []

    for route in _versioned_get_routes():
        responses = spec["paths"][route.path]["get"]["responses"]
        for status, expected_ref in _ERROR_SCHEMA_REFS.items():
            schema = responses.get(status, {}).get("content", {}).get("application/json", {}).get("schema", {})
            if schema.get("$ref") != expected_ref:
                failures.append(f"{route.path} {status}")

    assert failures == [], f"routes with missing or inconsistent error contracts: {failures}"


def test_error_schema_kinds_remain_machine_readable() -> None:
    schemas = app.openapi()["components"]["schemas"]
    assert schemas["BadRequestErrorResponse"]["properties"]["kind"]["const"] == "bad_request"
    assert schemas["NotFoundErrorResponse"]["properties"]["kind"]["const"] == "not_found"
    assert schemas["UnavailableErrorResponse"]["properties"]["kind"]["const"] == "unavailable"
