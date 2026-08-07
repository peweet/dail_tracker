"""Portability and containment tests for the ad-hoc PDF text batch."""

from __future__ import annotations

import argparse

import pytest

pytest.importorskip("fitz")

from pipeline_sandbox import scratch_batch


@pytest.mark.parametrize(
    "value",
    ["../escape::https://example.test/a.pdf", "name::file:///tmp/a.pdf", "missing-separator"],
)
def test_parse_pair_rejects_unsafe_or_non_http_inputs(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        scratch_batch._parse_pair(value)


def test_main_writes_under_configured_absolute_scratch(tmp_path, monkeypatch) -> None:
    scratch = (tmp_path / "scratch").resolve()
    monkeypatch.setattr(scratch_batch, "SCRATCH", scratch)
    monkeypatch.setattr(scratch_batch, "_extract_pdf_text", lambda _url: "page text")

    result = scratch_batch.main(["report::https://example.test/report.pdf"])

    assert result == 0
    assert (scratch / "report.txt").read_text(encoding="utf-8") == "page text"
