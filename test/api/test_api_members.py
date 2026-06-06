"""TestClient tests for the member resource (list + composed dossier)."""

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


@pytest.fixture(scope="module")
def has_data(client) -> bool:
    return client.get("/v1/health").json().get("views_registered", 0) > 0


def test_root(client):
    body = client.get("/").json()
    assert body["version"] == "v1"
    assert body["licence"] == "CC-BY-4.0"


def test_health(client):
    assert client.get("/v1/health").json()["status"] == "ok"


def test_openapi_generated(client):
    paths = client.get("/openapi.json").json()["paths"]
    assert "/v1/members" in paths
    assert "/v1/members/{code}/dossier" in paths


def test_members_list_envelope(client, has_data):
    body = client.get("/v1/members", params={"limit": 5}).json()
    assert set(body.keys()) == {"head", "results"}
    assert body["head"]["limit"] == 5 and body["head"]["offset"] == 0
    if not has_data:
        pytest.skip("no member data")
    assert len(body["results"]) <= 5
    assert {"unique_member_code", "member_name", "house"}.issubset(body["results"][0].keys())


def test_members_limit_cap_rejected(client):
    assert client.get("/v1/members", params={"limit": 9999}).status_code == 422


def test_members_house_filter(client, has_data):
    if not has_data:
        pytest.skip("no member data")
    rows = client.get("/v1/members", params={"house": "Seanad", "limit": 500}).json()["results"]
    if rows:
        assert all(r["house"] == "Seanad" for r in rows)


def test_dossier_roundtrip(client, has_data):
    if not has_data:
        pytest.skip("no member data")
    first = client.get("/v1/members", params={"limit": 1}).json()["results"]
    if not first:
        pytest.skip("empty roster")
    code = first[0]["unique_member_code"]
    d = client.get(f"/v1/members/{code}/dossier").json()
    assert d["member"]["unique_member_code"] == code
    assert "votes_cast" in d["headline"]
    for section in ("attendance_by_year", "payments_by_year", "legislation_sponsored"):
        assert isinstance(d[section], list)
    assert isinstance(d["headline"]["payments_total_eur"], (int, float))


def test_dossier_unknown_code_404(client):
    assert client.get("/v1/members/Not-A-Real-Code.D.2099-01-01/dossier").status_code == 404
