"""The Phase-4 API contract: uniform envelope, typed error kinds, one pagination
convention. These tests lock the cross-cutting guarantees (not per-resource data).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from api.main import app  # noqa: E402


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
