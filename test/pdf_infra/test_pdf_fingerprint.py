"""Unit tests for pdf_infra/pdf_fingerprint.py — the pure supersession decision.

These lock the conservative verdict table that backs DAIL-162 detection: a never-seen
file is baselined, a hidden remote size is UNKNOWN (never a false alarm), an equal size
is UNCHANGED, and only a differing size is SUPERSEDED.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pdf_infra.pdf_fingerprint import (
    NEW_BASELINE,
    SUPERSEDED,
    UNCHANGED,
    UNKNOWN,
    compare,
    load_index,
    save_index,
    sha256_bytes,
    sha256_file,
)


def test_compare_no_baseline_is_new_baseline():
    assert compare(None, 123) == NEW_BASELINE
    assert compare(None, None) == NEW_BASELINE


def test_compare_missing_remote_size_is_unknown():
    """Server hid Content-Length -> we must NOT cry supersession."""
    assert compare(100, None) == UNKNOWN


def test_compare_equal_size_is_unchanged():
    assert compare(100, 100) == UNCHANGED


def test_compare_different_size_is_superseded():
    assert compare(100, 250) == SUPERSEDED


def test_compare_coerces_numeric_strings_consistently():
    # Content-Length arrives as a string from headers; equal values must not false-flag.
    assert compare(100, int("100")) == UNCHANGED


def test_sha256_bytes_and_file_agree(tmp_path):
    blob = b"%PDF-1.4 hello"
    p = tmp_path / "f.pdf"
    p.write_bytes(blob)
    assert sha256_file(p) == sha256_bytes(blob)


def test_index_round_trip(tmp_path):
    path = tmp_path / "_meta" / "fp.json"
    idx = {"a.pdf": {"sha256": "deadbeef", "bytes": 10}}
    save_index(path, idx)
    assert load_index(path) == idx


def test_load_index_missing_file_is_empty():
    assert load_index(Path("does/not/exist.json")) == {}


def test_load_index_corrupt_file_is_empty(tmp_path):
    p = tmp_path / "bad.json"
    p.write_text("{not json", encoding="utf-8")
    assert load_index(p) == {}
