"""Mocked tests for pdf_infra/pdf_downloader.endpoint_downloader — the shared PDF pull
primitive (every Oireachtas PDF source flows through it), previously at 0% coverage.

What this pins:
  - URL → destination-subdir routing (attendance / payments / interests / other);
  - a 200 streams to disk; a non-200 writes nothing (no half/empty file shipped);
  - an already-downloaded file is skipped (no needless re-fetch / overwrite);
  - a connection error is caught, not crashed (one dead URL can't abort the batch).

No real network: `responses` intercepts the shared session.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import requests
import responses

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pdf_infra.pdf_downloader as dl  # noqa: E402


@pytest.fixture
def download_root(tmp_path, monkeypatch):
    """Point the module's download_path at a temp dir so nothing touches real bronze."""
    monkeypatch.setattr(dl, "download_path", tmp_path)
    return tmp_path


@responses.activate
@pytest.mark.parametrize(
    ("url", "subdir"),
    [
        ("https://oireachtas.ie/recordAttendanceForTaa/2024.pdf", "attendance"),
        ("https://oireachtas.ie/parliamentaryAllowances/2024.pdf", "payments"),
        ("https://oireachtas.ie/registerOfMembersInterests/2024.pdf", "interests"),
        ("https://oireachtas.ie/somethingElse/2024.pdf", "other"),
    ],
)
def test_routes_url_to_correct_subdir_and_downloads(download_root, url, subdir):
    responses.add(responses.GET, url, body=b"%PDF-1.7 bytes", status=200)

    dl.endpoint_downloader([url], session=requests.Session())

    dest = download_root / subdir / "2024.pdf"
    assert dest.exists(), f"expected download at {dest}"
    assert dest.read_bytes() == b"%PDF-1.7 bytes"


def test_skips_already_downloaded_file(download_root):
    # Pre-existing file in the routed subdir → must be left untouched, no fetch.
    url = "https://oireachtas.ie/somethingElse/already.pdf"
    existing = download_root / "other" / "already.pdf"
    existing.parent.mkdir(parents=True, exist_ok=True)
    existing.write_bytes(b"ORIGINAL")

    # No responses registered: a fetch here would raise, proving the skip.
    dl.endpoint_downloader([url], session=requests.Session())

    assert existing.read_bytes() == b"ORIGINAL"


@responses.activate
def test_non_200_writes_no_file(download_root):
    url = "https://oireachtas.ie/somethingElse/missing.pdf"
    responses.add(responses.GET, url, status=404)

    dl.endpoint_downloader([url], session=requests.Session())

    assert not (download_root / "other" / "missing.pdf").exists()


@responses.activate
def test_connection_error_is_caught_not_raised(download_root):
    url = "https://oireachtas.ie/somethingElse/blip.pdf"
    responses.add(responses.GET, url, body=requests.exceptions.ConnectionError("boom"))

    # Must not raise — a dead URL is logged and skipped, the batch survives.
    dl.endpoint_downloader([url], session=requests.Session())

    assert not (download_root / "other" / "blip.pdf").exists()
