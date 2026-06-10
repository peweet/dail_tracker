"""TestClient tests for the bulk export endpoints (/v1/data).

The allow-list IS the security boundary, so these tests pin three properties:
  1. Default-deny — anything not in EXPORTS (and every hard-excluded PII
     dataset) 404s, and the manifest lists only allow-listed names.
  2. Privacy filters are IN THE FILE — a generated snapshot re-read with duckdb
     contains zero natural-person rows.
  3. The manifest carries the commercial-credibility metadata (licence,
     attribution, caveat, two-clock data_currency).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

pytest.importorskip("fastapi")
import duckdb  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from api.main import app  # noqa: E402
from api.routers.exports import EXPORTS  # noqa: E402


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


def test_manifest_lists_only_allowlisted_resources(client):
    body = client.get("/v1/data").json()
    names = {r["name"] for r in body["resources"]}
    assert names == set(EXPORTS)
    for r in body["resources"]:
        # every entry must carry the credibility metadata, available or not
        assert r["licence"] and r["attribution"] and r["caveat"]
        if r["available"]:
            assert r["n_rows"] >= 0
            assert "latest_record" in r["data_currency"]
            assert "source_fetched_at" in r["data_currency"]


@pytest.mark.parametrize(
    "blocked",
    [
        "sipo_donations",  # donor addresses = PII
        "corporate_notices",  # personal-insolvency quarantine lives at view level
        "member_interests",
        "judiciary_appointments",
        "../config",  # path-shaped names must also be plain 404s
    ],
)
def test_hard_excluded_resources_404(client, blocked):
    assert client.get(f"/v1/data/{blocked}").status_code == 404


def test_parquet_download_roundtrip(client):
    spec = EXPORTS["procurement_lobbying_overlap"]  # smallest allow-listed file
    if not spec.source.exists():
        pytest.skip("overlap gold not built on this machine")
    r = client.get("/v1/data/procurement_lobbying_overlap")
    assert r.status_code == 200
    assert r.content[:4] == b"PAR1"  # parquet magic bytes


def test_csv_download_has_header(client):
    spec = EXPORTS["procurement_lobbying_overlap"]
    if not spec.source.exists():
        pytest.skip("overlap gold not built on this machine")
    r = client.get("/v1/data/procurement_lobbying_overlap", params={"format": "csv"})
    assert r.status_code == 200
    assert "supplier_norm" in r.text.splitlines()[0]


def test_bad_format_rejected(client):
    assert client.get("/v1/data/procurement_awards", params={"format": "xlsx"}).status_code == 422


@pytest.mark.parametrize(
    ("resource", "person_predicate"),
    [
        ("procurement_awards", "supplier_class = 'sole_trader_or_individual'"),
        ("procurement_payments_fact", "public_display = FALSE OR supplier_class = 'sole_trader_or_individual'"),
        ("ted_awards", "supplier_class = 'sole_trader_or_individual' OR privacy_status = 'review_personal_data'"),
        ("ted_winner_history", "supplier_class = 'sole_trader_or_individual' OR privacy_status = 'review_personal_data'"),
    ],
)
def test_snapshot_contains_no_natural_persons(client, resource, person_predicate):
    """The filter must be in the generated FILE, not just the docs."""
    spec = EXPORTS[resource]
    if not spec.source.exists():
        pytest.skip(f"{resource} source not built on this machine")
    r = client.get(f"/v1/data/{resource}")
    assert r.status_code == 200
    snapshot = Path("data/_export_cache") / f"{resource}.parquet"
    assert snapshot.exists()
    con = duckdb.connect()
    try:
        leaked = con.execute(
            f"SELECT count(*) FROM read_parquet('{snapshot.as_posix()}') WHERE {person_predicate}"
        ).fetchone()[0]
        kept = con.execute(f"SELECT count(*) FROM read_parquet('{snapshot.as_posix()}')").fetchone()[0]
    finally:
        con.close()
    assert leaked == 0
    assert kept > 0  # the filter removed persons, not everything
