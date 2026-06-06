"""TestClient tests for the read-only API (api.main:app).

Exercises the full path: FastAPI route → core dossier composition → serializer →
envelope, against the real registered views. Skips data-dependent assertions if
the gold/silver parquet isn't built in this env (health reports 0 views).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # repo root → import `api`

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from api.main import app  # noqa: E402


@pytest.fixture(scope="module")
def client():
    # `with` triggers the lifespan → builds the read-only connection once.
    with TestClient(app) as c:
        yield c


@pytest.fixture(scope="module")
def has_data(client) -> bool:
    return client.get("/v1/health").json().get("views_registered", 0) > 0


def test_root(client):
    r = client.get("/")
    assert r.status_code == 200
    body = r.json()
    assert body["version"] == "v1"
    assert body["licence"] == "CC-BY-4.0"


def test_health(client):
    r = client.get("/v1/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_openapi_generated(client):
    r = client.get("/openapi.json")
    assert r.status_code == 200
    paths = r.json()["paths"]
    assert "/v1/members" in paths
    assert "/v1/members/{code}/dossier" in paths


def test_members_list_envelope(client, has_data):
    r = client.get("/v1/members", params={"limit": 5})
    assert r.status_code == 200
    body = r.json()
    # Envelope contract
    assert set(body.keys()) == {"head", "results"}
    head = body["head"]
    assert head["limit"] == 5 and head["offset"] == 0
    assert "total" in head and "truncated" in head
    if not has_data:
        pytest.skip("no member data in this env")
    assert len(body["results"]) <= 5
    row = body["results"][0]
    assert {"unique_member_code", "member_name", "house"}.issubset(row.keys())


def test_members_limit_cap_rejected(client):
    # limit > 500 must be rejected by validation (422), never silently served.
    r = client.get("/v1/members", params={"limit": 9999})
    assert r.status_code == 422


def test_members_house_filter(client, has_data):
    if not has_data:
        pytest.skip("no member data")
    r = client.get("/v1/members", params={"house": "Seanad", "limit": 500})
    assert r.status_code == 200
    rows = r.json()["results"]
    if rows:
        assert all(row["house"] == "Seanad" for row in rows)


def test_dossier_roundtrip(client, has_data):
    if not has_data:
        pytest.skip("no member data")
    first = client.get("/v1/members", params={"limit": 1}).json()["results"]
    if not first:
        pytest.skip("empty roster")
    code = first[0]["unique_member_code"]
    r = client.get(f"/v1/members/{code}/dossier")
    assert r.status_code == 200
    d = r.json()
    assert d["member"]["unique_member_code"] == code
    # composed sections present (typed shape)
    assert "headline" in d and "votes_cast" in d["headline"]
    for section in ("attendance_by_year", "payments_by_year", "legislation_sponsored"):
        assert isinstance(d[section], list)
    # JSON-safe: no NaN leaked (would have failed json parse, but assert types)
    assert isinstance(d["headline"]["payments_total_eur"], (int, float))


def test_dossier_unknown_code_404(client):
    r = client.get("/v1/members/Not-A-Real-Code.D.2099-01-01/dossier")
    assert r.status_code == 404
