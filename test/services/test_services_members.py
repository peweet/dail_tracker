"""
Tests for services/members.py and services/storage.py.

services/members.py is the driving table for every downstream API fetch
(legislation, questions, debates are all per-member). A regression here
silently invalidates the entire pipeline because the member list is empty
or malformed.

services/storage.py is the JSON read/write layer used by all bronze
scrapers. Easy to test, no network, surfaces path/encoding bugs.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
import responses

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from services.members import (
    _extract_member_uri,
    fetch_members_payload,
    get_or_create_members_payload,
    load_members_payload,
    members_payload_to_df,
)
from services.storage import load_json, members_file_path, output_exists, result_file_path, save_json

# ---------------------------------------------------------------------------
# _extract_member_uri — pure dict transformer
# ---------------------------------------------------------------------------


def test_extract_member_uri_from_nested_member_object():
    """API responses nest the uri inside row['member']['uri']."""
    row = {"member": {"uri": "/ie/oireachtas/member/id/abc-123"}}
    assert _extract_member_uri(row) == "/ie/oireachtas/member/id/abc-123"


def test_extract_member_uri_falls_back_to_member_id_field():
    """Some payload shapes use member_id instead of uri."""
    row = {"member": {"member_id": "/ie/oireachtas/member/id/legacy-456"}}
    assert _extract_member_uri(row) == "/ie/oireachtas/member/id/legacy-456"


def test_extract_member_uri_falls_back_to_top_level_uri():
    """If `member` wrapper is absent the top-level uri/member_id is used."""
    row = {"uri": "/top-level-uri"}
    assert _extract_member_uri(row) == "/top-level-uri"


def test_extract_member_uri_normalises_absolute_to_relative():
    """Absolute https://data.oireachtas.ie/... URIs get stripped to relative
    form so downstream callers always see the same shape regardless of which
    field the payload used.
    """
    row = {"member": {"uri": "https://data.oireachtas.ie/ie/oireachtas/member/id/abc"}}
    assert _extract_member_uri(row) == "/ie/oireachtas/member/id/abc"


def test_extract_member_uri_returns_none_when_no_uri_present():
    """A row with no usable URI must return None — callers filter on this."""
    row = {"member": {"name": "Anon"}}
    assert _extract_member_uri(row) is None


def test_extract_member_uri_handles_empty_dict():
    assert _extract_member_uri({}) is None


# ---------------------------------------------------------------------------
# members_payload_to_df — payload reshaper
# ---------------------------------------------------------------------------


def test_members_payload_to_df_returns_one_row_per_unique_uri():
    """Duplicate URIs across the payload collapse via `.unique()`."""
    payload = {
        "results": [
            {"member": {"uri": "/uri-1"}},
            {"member": {"uri": "/uri-2"}},
            {"member": {"uri": "/uri-1"}},  # duplicate
        ]
    }
    df = members_payload_to_df(payload)
    assert df.height == 2
    assert set(df["member_uri"].to_list()) == {"/uri-1", "/uri-2"}


def test_members_payload_to_df_drops_rows_with_no_uri():
    """A row with no extractable URI is silently dropped — these are usually
    placeholder entries returned by the API.
    """
    payload = {
        "results": [
            {"member": {"uri": "/uri-1"}},
            {"member": {"name": "no uri here"}},
        ]
    }
    df = members_payload_to_df(payload)
    assert df.height == 1
    assert df["member_uri"][0] == "/uri-1"


def test_members_payload_to_df_returns_empty_dataframe_for_empty_payload():
    """An empty results list must produce an empty (but schema-valid) DataFrame.
    Downstream URL builders rely on `.is_empty()` returning True.
    """
    df = members_payload_to_df({"results": []})
    assert df.is_empty()
    assert "member_uri" in df.columns


def test_members_payload_to_df_sorts_by_uri():
    """Output is sorted — deterministic across runs so downstream URL
    construction produces stable diffs.
    """
    payload = {
        "results": [
            {"member": {"uri": "/zebra"}},
            {"member": {"uri": "/alpha"}},
            {"member": {"uri": "/beta"}},
        ]
    }
    df = members_payload_to_df(payload)
    assert df["member_uri"].to_list() == ["/alpha", "/beta", "/zebra"]


# ---------------------------------------------------------------------------
# fetch_members_payload — mocked HTTP
# ---------------------------------------------------------------------------


@responses.activate
def test_fetch_members_payload_returns_api_response():
    """Single GET against the members endpoint with Dáil 34 chamber filter."""
    body = {"head": {"counts": {"resultCount": 2}}, "results": [{"member": {"uri": "/m1"}}, {"member": {"uri": "/m2"}}]}
    # Match by URL prefix — actual call adds query params for chamber_id etc.
    responses.add(
        responses.GET,
        "https://api.oireachtas.ie/v1/members",
        json=body,
        status=200,
        match=[responses.matchers.query_param_matcher({}, strict_match=False)],
    )

    payload = fetch_members_payload()

    assert payload == body
    assert len(payload["results"]) == 2


# ---------------------------------------------------------------------------
# load_members_payload — backwards-compatible deserialisation
# ---------------------------------------------------------------------------


def test_load_members_payload_handles_new_dict_format(tmp_path):
    """New format: payload saved as a dict directly."""
    payload = {"head": {"counts": {"resultCount": 1}}, "results": [{"member": {"uri": "/m"}}]}
    path = tmp_path / "members.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with patch("services.members.members_file_path", return_value=path):
        loaded = load_members_payload()

    assert loaded == payload


def test_load_members_payload_unwraps_legacy_list_wrapper(tmp_path):
    """Backward-compat: old files saved as [payload_dict] must unwrap cleanly.
    Without this branch, downstream `.get('results')` calls would crash on a list.
    """
    payload = {"head": {"counts": {"resultCount": 1}}, "results": [{"member": {"uri": "/m"}}]}
    path = tmp_path / "members.json"
    path.write_text(json.dumps([payload]), encoding="utf-8")  # legacy list wrapper

    with patch("services.members.members_file_path", return_value=path):
        loaded = load_members_payload()

    assert loaded == payload


# ---------------------------------------------------------------------------
# get_or_create_members_payload — orchestration logic
# ---------------------------------------------------------------------------


def test_get_or_create_uses_cached_payload_when_present(tmp_path):
    """When the cached file exists and overwrite=False, no HTTP call fires —
    the cache is reused. Important: prevents accidentally re-hitting the
    Oireachtas API on every pipeline run.
    """
    payload = {"results": [{"member": {"uri": "/cached"}}]}
    path = tmp_path / "members.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    # If fetch_members_payload is called, this test should fail loudly.
    with (
        patch("services.members.members_file_path", return_value=path),
        patch("services.members.fetch_members_payload") as mock_fetch,
    ):
        mock_fetch.side_effect = AssertionError("fetch must not be called when cache exists")
        loaded = get_or_create_members_payload(overwrite=False)

    assert loaded == payload


@responses.activate
def test_get_or_create_fetches_and_saves_when_no_cache(tmp_path):
    """No cache file → fetch from API + save to disk."""
    path = tmp_path / "members.json"
    body = {"head": {"counts": {"resultCount": 1}}, "results": [{"member": {"uri": "/fresh"}}]}
    responses.add(
        responses.GET,
        "https://api.oireachtas.ie/v1/members",
        json=body,
        status=200,
        match=[responses.matchers.query_param_matcher({}, strict_match=False)],
    )

    with patch("services.members.members_file_path", return_value=path):
        loaded = get_or_create_members_payload(overwrite=False)

    assert loaded == body
    # Cache file was created so subsequent runs use it.
    assert path.exists()
    assert json.loads(path.read_text(encoding="utf-8")) == body


# ---------------------------------------------------------------------------
# services/storage.py — file I/O layer
# ---------------------------------------------------------------------------


def test_save_and_load_json_roundtrip_preserves_data(tmp_path):
    """The JSON write/read cycle must preserve unicode (Irish names, accents)
    exactly. ensure_ascii=False is the contract.
    """
    data = {"member": "Aoife Ní Bhroin", "constituency": "Galway West", "irish": "Seán Ó Briain"}
    path = tmp_path / "subdir" / "test.json"

    save_json(data, path)
    loaded = load_json(path)

    assert loaded == data
    # Verify the on-disk file actually contains UTF-8 characters, not \uXXXX escapes.
    raw = path.read_text(encoding="utf-8")
    assert "Aoife" in raw
    assert "Ní" in raw


def test_save_json_creates_parent_directories(tmp_path):
    """A nested path with non-existent parents must be created (mkdir parents=True)."""
    path = tmp_path / "deep" / "nested" / "structure" / "out.json"
    save_json({"x": 1}, path)
    assert path.exists()


def test_output_exists_returns_true_when_path_exists_and_no_overwrite(tmp_path):
    path = tmp_path / "exists.json"
    path.write_text("{}", encoding="utf-8")
    assert output_exists(path, overwrite=False) is True


def test_output_exists_returns_false_when_overwrite_requested(tmp_path):
    """overwrite=True forces re-fetch even when the file is present.
    Without this, --force-refresh CLI flags would silently no-op.
    """
    path = tmp_path / "exists.json"
    path.write_text("{}", encoding="utf-8")
    assert output_exists(path, overwrite=True) is False


def test_output_exists_returns_false_when_path_missing(tmp_path):
    assert output_exists(tmp_path / "missing.json", overwrite=False) is False


# --- DAIL-160: staleness-aware caching -------------------------------------------------
# A bare path.exists() check let a daily cron skip every Oireachtas API pull forever once
# the bronze JSON existed (members/questions/votes/legislation/debates froze at first-run
# values). max_age_hours refetches a too-old cache even with overwrite=False.
import os  # noqa: E402
import time  # noqa: E402


def _age_file(path, hours):
    """Backdate a file's mtime by ``hours`` hours."""
    past = time.time() - hours * 3600
    os.utime(path, (past, past))


def test_output_exists_refetches_when_cache_older_than_max_age(tmp_path):
    path = tmp_path / "stale.json"
    path.write_text("{}", encoding="utf-8")
    _age_file(path, 25)  # 25h old
    assert output_exists(path, overwrite=False, max_age_hours=18) is False


def test_output_exists_reuses_when_cache_within_max_age(tmp_path):
    path = tmp_path / "fresh.json"
    path.write_text("{}", encoding="utf-8")
    _age_file(path, 2)  # 2h old
    assert output_exists(path, overwrite=False, max_age_hours=18) is True


def test_output_exists_max_age_none_is_backward_compatible(tmp_path):
    """max_age_hours=None must preserve the original exists-only behaviour."""
    path = tmp_path / "old.json"
    path.write_text("{}", encoding="utf-8")
    _age_file(path, 1000)
    assert output_exists(path, overwrite=False, max_age_hours=None) is True


def test_api_main_defines_a_live_freshness_default():
    """Regression guard against silently reverting DAIL-160: the API entrypoint must
    carry a positive, finite refetch threshold (not None/0/inf), so a cron refetches."""
    from services.oireachtas_api_main import DATA_MAX_AGE_HOURS

    assert isinstance(DATA_MAX_AGE_HOURS, float)
    assert 0 < DATA_MAX_AGE_HOURS < float("inf")


@pytest.mark.parametrize(
    "scenario,expected_segment",
    [
        ("legislation", "legislation_results.json"),
        ("questions", "questions_results.json"),
        ("votes", "votes_results.json"),
        ("debates_listings", "debates_listings_results.json"),
        ("legislation_unscoped", "legislation_results_unscoped.json"),
        ("custom_scenario", "custom_scenario_results.json"),  # fallback path
    ],
)
def test_result_file_path_maps_scenario_to_filename(scenario: str, expected_segment: str):
    """Each scraper writes to a deterministic path based on scenario name.
    A regression here means files land in the wrong directory and downstream
    loaders silently see "no data."
    """
    path = result_file_path(scenario)
    assert expected_segment in str(path)


def test_members_file_path_points_to_members_subdirectory():
    path = members_file_path()
    assert path.name == "members.json"
    assert "members" in str(path).lower()
