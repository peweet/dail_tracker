"""The EXPERIMENTAL real-terms procurement endpoints must stay gated: on the deployed API
(DAIL_EXPERIMENTAL unset — the default here) they 404 AND are hidden from the OpenAPI schema, so
the local-only inflation lens never leaks publicly. The ON-path response shape is covered by the
core query tests (test_core_procurement_queries) and the Streamlit Playwright checks.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from api.main import app  # noqa: E402

# The gate is read at import time; this file asserts the flag-OFF (deployed) behaviour.
pytestmark = pytest.mark.skipif(
    os.getenv("DAIL_EXPERIMENTAL") == "1",
    reason="gating test asserts the deployed flag-off behaviour",
)


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


@pytest.mark.parametrize(
    "path",
    [
        "/v1/procurement/inflation/indices",
        "/v1/procurement/inflation/cpv",
        "/v1/procurement/inflation/spend-trend",
    ],
)
def test_inflation_endpoints_are_404_when_gated(client, path):
    assert client.get(path).status_code == 404


def test_inflation_endpoints_hidden_from_openapi_when_gated(client):
    paths = client.get("/openapi.json").json()["paths"]
    assert not [p for p in paths if "inflation" in p]
