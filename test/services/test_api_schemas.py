"""Tests for validate-at-fetch of Oireachtas API responses (DAIL-019 / DAIL-020).

Three layers:
  1. Every registered schema validates a real (trimmed) bronze sample.
  2. Deliberately manipulated samples (renamed/dropped keys) are rejected —
     this is the drift the validation exists to catch.
  3. The fetch path (services.http_engine.fetch_json) validates Oireachtas
     responses and passes other hosts through untouched.

No real network is hit; the fetch-path tests use the `responses` library.
"""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

import pytest
import responses

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from services.http_engine import fetch_json
from services.schema_validation import (
    SchemaValidationError,
    endpoint_from_url,
    validate_if_known,
    validate_response,
)

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "api"
ENDPOINTS = ["members", "legislation", "questions", "votes", "debates"]

# The per-endpoint result-item wrapper key that the schema requires.
WRAPPER_KEY = {
    "members": "member",
    "legislation": "bill",
    "questions": "question",
    "votes": "division",
    "debates": "debateRecord",
}


def _load_sample(endpoint: str) -> dict:
    with (FIXTURE_DIR / f"{endpoint}_sample.json").open(encoding="utf-8") as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# 1. Real samples validate
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("endpoint", ENDPOINTS)
def test_real_sample_validates(endpoint: str):
    """A genuine recent response must pass its own schema. If this fails, the
    schema is stricter than reality, not the other way round."""
    validate_response(endpoint, _load_sample(endpoint))


# ---------------------------------------------------------------------------
# 2. Manipulated samples are rejected
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("endpoint", ENDPOINTS)
def test_dropped_result_count_fails(endpoint: str):
    """Dropping head.counts.resultCount must fail — every paginator reads it to
    detect truncation, so its disappearance is the highest-value drift to catch."""
    sample = _load_sample(endpoint)
    del sample["head"]["counts"]["resultCount"]
    with pytest.raises(SchemaValidationError):
        validate_response(endpoint, sample)


@pytest.mark.parametrize("endpoint", ENDPOINTS)
def test_renamed_results_key_fails(endpoint: str):
    """results -> data is the classic envelope rename."""
    sample = _load_sample(endpoint)
    sample["data"] = sample.pop("results")
    with pytest.raises(SchemaValidationError):
        validate_response(endpoint, sample)


@pytest.mark.parametrize("endpoint", ENDPOINTS)
def test_renamed_item_wrapper_fails(endpoint: str):
    """A renamed per-item wrapper (e.g. member -> memberData) must fail when at
    least one result is present."""
    sample = _load_sample(endpoint)
    if not sample["results"]:
        pytest.skip("sample has no result items to rename")
    for item in sample["results"]:
        item["_renamed"] = item.pop(WRAPPER_KEY[endpoint])
    with pytest.raises(SchemaValidationError):
        validate_response(endpoint, sample)


@pytest.mark.parametrize("endpoint", ENDPOINTS)
def test_new_optional_field_is_tolerated(endpoint: str):
    """A new optional field anywhere must NOT fail — the schema is loose on
    purpose so additive API changes don't page us at 4am."""
    sample = _load_sample(endpoint)
    sample["head"]["counts"]["someNewCount"] = 5
    sample["brandNewTopLevelField"] = {"anything": True}
    if sample["results"]:
        sample["results"][0]["brandNewItemField"] = "ok"
    validate_response(endpoint, sample)  # must not raise


def test_result_count_must_be_integer():
    """resultCount as a string (a real way APIs drift) is rejected."""
    sample = _load_sample("members")
    sample["head"]["counts"]["resultCount"] = "174"
    with pytest.raises(SchemaValidationError):
        validate_response("members", sample)


def test_unregistered_endpoint_raises():
    """validate_response on an unknown endpoint is a programming error."""
    with pytest.raises(SchemaValidationError):
        validate_response("not_an_endpoint", {"head": {}, "results": []})


# ---------------------------------------------------------------------------
# 3. URL -> endpoint resolution
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://api.oireachtas.ie/v1/members?chamber_id=x&limit=200", "members"),
        ("https://api.oireachtas.ie/v1/legislation?date_start=2014-01-01&skip=0", "legislation"),
        ("https://api.oireachtas.ie/v1/questions?member_id=x&skip=0", "questions"),
        ("https://api.oireachtas.ie/v1/votes?chamber=dail&limit=1000", "votes"),
        ("https://api.oireachtas.ie/v1/debates?date_start=2025-01-01", "debates"),
        # Unknown host -> not validated.
        ("https://api.example.com/v1/members/123", None),
        ("https://jsonplaceholder.typicode.com/posts/1", None),
        # Known host, endpoint with no registered schema -> not validated.
        ("https://api.oireachtas.ie/v1/committees?x=1", None),
        ("https://api.oireachtas.ie/health", None),
    ],
)
def test_endpoint_from_url(url: str, expected):
    assert endpoint_from_url(url) == expected


def test_validate_if_known_skips_unknown_host():
    """A non-Oireachtas payload that does not match any schema must pass through
    validate_if_known untouched."""
    validate_if_known("https://api.example.com/anything", {"totally": "different"})


# ---------------------------------------------------------------------------
# 4. Fetch-path integration (services.http_engine.fetch_json)
# ---------------------------------------------------------------------------


@responses.activate
def test_fetch_json_validates_oireachtas_response():
    url = "https://api.oireachtas.ie/v1/members?limit=200"
    responses.add(responses.GET, url, json=_load_sample("members"), status=200)

    data, raw_bytes = fetch_json(url)

    assert data["head"]["counts"]["resultCount"] >= 0
    assert raw_bytes > 0


@responses.activate
def test_fetch_json_raises_on_drifted_oireachtas_response():
    """A real-looking Oireachtas URL returning a drifted envelope must raise out
    of fetch_json (loud failure), not return silently."""
    url = "https://api.oireachtas.ie/v1/votes?chamber=dail"
    drifted = copy.deepcopy(_load_sample("votes"))
    del drifted["head"]["counts"]["resultCount"]
    responses.add(responses.GET, url, json=drifted, status=200)

    with pytest.raises(SchemaValidationError):
        fetch_json(url)


@responses.activate
def test_fetch_json_passes_through_non_oireachtas_host():
    """Generic fetch_json usage (test doubles, other APIs) is unaffected by
    validation — the response shape here matches no schema yet must return."""
    url = "https://api.example.com/posts/1"
    body = {"unrelated": "payload", "id": 1}
    responses.add(responses.GET, url, json=body, status=200)

    data, _ = fetch_json(url)

    assert data == body
