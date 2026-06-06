"""Tests for the eTenders CSV cache TTL in extractors/procurement_etenders_extract.py.

The old `ensure_csv` reused c:/tmp/etenders_opendata.csv whenever it merely existed, so a
recurring `procurement` chain run silently re-built from a months-old CSV and never pulled a
new OGP publication (the same silent-staleness class as DAIL-160/162). These lock the TTL +
--force behaviour: stale / too-small / forced caches are NOT reused.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "extractors"))

pytest.importorskip("polars")
import procurement_etenders_extract as pe  # noqa: E402


@pytest.fixture
def cache(tmp_path, monkeypatch):
    """Point the module's CACHE at a tmp file so tests never touch c:/tmp."""
    p = tmp_path / "etenders_opendata.csv"
    monkeypatch.setattr(pe, "CACHE", p)
    return p


def _write_big(p: Path, age_days: float = 0.0) -> None:
    p.write_bytes(b"x" * 1_000_001)  # just over the 1MB plausibility floor
    if age_days:
        past = time.time() - age_days * 86400
        os.utime(p, (past, past))


def test_missing_cache_is_not_fresh(cache):
    assert pe._cache_is_fresh(force=False, max_age_days=7) is False


def test_recent_cache_is_fresh(cache):
    _write_big(cache, age_days=1)
    assert pe._cache_is_fresh(force=False, max_age_days=7) is True


def test_stale_cache_is_not_fresh(cache):
    _write_big(cache, age_days=30)
    assert pe._cache_is_fresh(force=False, max_age_days=7) is False


def test_force_ignores_a_recent_cache(cache):
    _write_big(cache, age_days=1)
    assert pe._cache_is_fresh(force=True, max_age_days=7) is False


def test_too_small_cache_is_not_fresh(cache):
    cache.write_bytes(b"truncated")  # < 1MB -> treat as unusable
    assert pe._cache_is_fresh(force=False, max_age_days=7) is False


def test_default_ttl_is_positive_finite():
    """Guard against reverting to the old exists-only (effectively infinite) cache."""
    assert 0 < pe.CACHE_MAX_AGE_DAYS < 3650
