from __future__ import annotations

import sys
from pathlib import Path

import pytest
import requests

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "extractors"))

pytest.importorskip("polars")

import ted_ireland_tenders_extract as ted_tenders  # noqa: E402

from extractors import housing_construction_pipeline_extract as housing_pipeline  # noqa: E402
from extractors.public_body_payments import harvest as public_body_harvest  # noqa: E402
from services.http_engine import BROWSER_UA  # noqa: E402

try:  # PyMuPDF is optional in the fast/unit-test environment.
    import fitz  # noqa: F401
except ImportError:
    la_payments = None
    pdf_sample = None
else:
    import procurement_la_payments_extract as la_payments  # noqa: E402
    import sample_extract_procurement_pdf as pdf_sample  # noqa: E402

HARVEST_MODULES = [public_body_harvest, *([la_payments] if la_payments is not None else [])]


@pytest.mark.parametrize("module", HARVEST_MODULES)
def test_harvest_fetch_wrapper_preserves_failure_diagnostics(module, monkeypatch):
    response = requests.Response()
    response.status_code = 403
    error = requests.HTTPError(response=response)

    def fail(_url, **kwargs):
        kwargs["on_failure"](error)
        return None

    monkeypatch.setattr(module, "resilient_fetch_bytes", fail)
    module.LAST_ERR["stale"] = True

    assert module.fetch_bytes("https://publisher.example/file.pdf") is None
    assert module.LAST_ERR == {"error_class": "http_403", "http_status": 403}


@pytest.mark.parametrize("module", HARVEST_MODULES)
def test_harvest_fetch_wrapper_clears_old_error_after_success(module, monkeypatch):
    monkeypatch.setattr(module, "resilient_fetch_bytes", lambda *_args, **_kwargs: b"downloaded")
    module.LAST_ERR["error_class"] = "old_failure"

    assert module.fetch_bytes("https://publisher.example/file.pdf") == b"downloaded"
    assert module.LAST_ERR == {}


def test_pdf_sample_keeps_disk_cache_while_using_shared_fetch(tmp_path, monkeypatch):
    if pdf_sample is None:
        pytest.skip("PyMuPDF is not installed")
    monkeypatch.setattr(pdf_sample, "TMP", tmp_path)
    calls: list[str] = []

    def download(url, **kwargs):
        calls.append(url)
        body = b"%PDF" + b"x" * 2100
        assert kwargs["validate"](body)
        return body

    monkeypatch.setattr(pdf_sample, "fetch_bytes", download)
    url = "https://publisher.example/sample.pdf"

    expected = b"%PDF" + b"x" * 2100
    assert pdf_sample.fetch(url) == expected
    assert pdf_sample.fetch(url) == expected
    assert calls == [url]


def test_ted_pull_posts_json_through_shared_engine(monkeypatch):
    calls: list[dict] = []

    def post(_url, payload, **_kwargs):
        calls.append(payload)
        return {"notices": [{"publication-number": "one"}]}, 20

    monkeypatch.setattr(ted_tenders, "post_json", post)

    assert ted_tenders.pull(max_pages=3) == [{"publication-number": "one"}]
    assert calls[0]["page"] == 1
    assert calls[0]["limit"] == 250


def test_ted_pull_treats_post_failure_as_end_of_pagination(monkeypatch, capsys):
    def fail(*_args, **_kwargs):
        raise requests.ConnectionError("offline")

    monkeypatch.setattr(ted_tenders, "post_json", fail)

    assert ted_tenders.pull(max_pages=3) == []
    assert "ConnectionError" in capsys.readouterr().out


def test_housing_govie_fetch_uses_browser_headers(monkeypatch):
    class RequestObserved(RuntimeError):
        pass

    def observe(_url, **kwargs):
        assert kwargs["headers"]["User-Agent"] == BROWSER_UA
        raise RequestObserved

    monkeypatch.setattr(housing_pipeline, "http_fetch_bytes", observe)

    with pytest.raises(RequestObserved):
        housing_pipeline.fetch_csv()
