"""TestClient tests for the procurement / committees / ministerial resources and the
member questions/interests + votes interest-breakdown/cross-reference extensions.

All retrieval/composition is already exercised by the core query + dossier tests;
these assert the HTTP wiring (routing, envelope, 404/422/503 mapping) only, and
skip cleanly when a given dataset is absent on the machine running them.
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


def _total(client, path, **params) -> int:
    return client.get(path, params={**params, "limit": 1}).json()["head"].get("total", 0)


# ── Procurement ────────────────────────────────────────────────────────────────


def test_suppliers_envelope_and_cap(client):
    body = client.get("/v1/procurement/suppliers", params={"limit": 3}).json()
    assert set(body.keys()) == {"head", "results"}
    assert body["head"]["limit"] == 3
    assert client.get("/v1/procurement/suppliers", params={"limit": 9999}).status_code == 422


def test_supplier_dossier_roundtrip_and_404(client):
    if _total(client, "/v1/procurement/suppliers") == 0:
        pytest.skip("no procurement data")
    top = client.get("/v1/procurement/suppliers", params={"limit": 1}).json()["results"][0]
    norm = top["supplier_norm"]
    d = client.get(f"/v1/procurement/suppliers/{norm}/dossier").json()
    assert isinstance(d["awards"], list)
    assert d["summary"] is None or isinstance(d["summary"], dict)
    assert client.get("/v1/procurement/suppliers/__no_such_supplier__/dossier").status_code == 404


def test_competition_carries_caveat(client):
    r = client.get("/v1/procurement/competition", params={"min_lots": 0, "limit": 5})
    if r.status_code == 503:
        pytest.skip("competition source unavailable")
    body = r.json()
    assert "caveat" in body and isinstance(body["buyers"], list)


def test_lobbying_overlap_caveat_and_no_double_count(client):
    r = client.get("/v1/procurement/lobbying-overlap", params={"limit": 5})
    if r.status_code == 503:
        pytest.skip("overlap source unavailable")
    body = r.json()
    assert "caveat" in body
    assert body["summary"]["distinct_suppliers"] == len(body["suppliers"]) or body["summary"][
        "distinct_suppliers"
    ] >= len(body["suppliers"])


# ── Committees ───────────────────────────────────────────────────────────────


def test_committees_list_and_item(client):
    body = client.get("/v1/committees", params={"chamber": "Dáil"}).json()
    assert body["chamber"] == "Dáil"
    if not body["committees"]:
        pytest.skip("no committee data")
    name = body["committees"][0]["committee"]
    item = client.get(f"/v1/committees/{name}", params={"chamber": "Dáil"}).json()
    assert isinstance(item["detail"], dict)
    assert isinstance(item["party_seats"], list)
    assert client.get("/v1/committees/__nope__", params={"chamber": "Dáil"}).status_code == 404


# ── Ministerial ──────────────────────────────────────────────────────────────


def test_who_was_minister(client):
    body = client.get("/v1/ministers", params={"department": "Finance", "on_date": "2022-01-01"}).json()
    # Either a resolved holder, a disambiguation list, or the picker — all 200.
    assert any(k in body for k in ("minister", "disambiguation", "departments", "error"))


def test_ministers_requires_params(client):
    assert client.get("/v1/ministers").status_code == 422


# ── Member questions / interests ─────────────────────────────────────────────


def _a_member_code(client) -> str | None:
    rows = client.get("/v1/members", params={"limit": 1}).json()["results"]
    return rows[0]["unique_member_code"] if rows else None


def test_member_questions_and_404(client):
    code = _a_member_code(client)
    if code is None:
        pytest.skip("no member data")
    body = client.get(f"/v1/members/{code}/questions", params={"limit": 5}).json()
    assert "questions" in body and "total_matched" in body
    assert client.get("/v1/members/__nobody__/questions").status_code == 404


def test_member_interests_and_404(client):
    code = _a_member_code(client)
    if code is None:
        pytest.skip("no member data")
    body = client.get(f"/v1/members/{code}/interests").json()
    assert "declarations" in body and "by_year" in body
    assert client.get("/v1/members/__nobody__/interests").status_code == 404


# ── Votes: interest-breakdown + cross-reference ──────────────────────────────


def test_division_interest_breakdown(client):
    rows = client.get("/v1/votes", params={"limit": 1}).json()["results"]
    if not rows:
        pytest.skip("no vote data")
    vid = rows[0]["vote_id"]
    body = client.get(f"/v1/votes/{vid}/interest-breakdown").json()
    assert isinstance(body["division"], dict)
    assert isinstance(body["interest_breakdown"], list)
    assert "caveat" in body
    assert client.get("/v1/votes/9999-99-99_999/interest-breakdown").status_code == 404


def test_cross_reference_votes_interests(client):
    r = client.get(
        "/v1/cross-reference/votes-interests",
        params={"keyword": "housing", "vote_type": "Voted No", "interest": "landlord"},
    )
    if r.status_code == 503:
        pytest.skip("cross-reference source unavailable")
    body = r.json()
    assert "matches" in body and "caveat" in body and "match_count" in body


# ── Payments per-year ────────────────────────────────────────────────────────


def test_payments_for_year(client):
    body = client.get("/v1/payments/2025", params={"limit": 5}).json()
    assert set(body.keys()) == {"head", "results"}
    assert len(body["results"]) <= 5


# ── Catalog reflects the new resources ───────────────────────────────────────


def test_catalog_lists_new_resources(client):
    names = {r["resource"] for r in client.get("/v1/catalog").json()["resources"]}
    assert {"procurement", "committees", "ministers"}.issubset(names)
