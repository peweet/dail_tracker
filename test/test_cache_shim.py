"""The framework-neutral cache shim (utility/data_access/_cache.py).

Verifies the shim is a faithful drop-in for st.cache_data / st.cache_resource in
both directions: it delegates to Streamlit when present, and memoises correctly
when Streamlit is absent (the API-only / test path).
"""

from __future__ import annotations

import importlib
import sys

import pytest

# The app runs from utility/, so data_access is importable as a top-level package.
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent / "utility"))

from data_access import _cache  # noqa: E402


def test_bare_and_called_forms_both_work():
    calls = {"n": 0}

    @_cache.cache_data
    def bare():
        calls["n"] += 1
        return calls["n"]

    @_cache.cache_data(ttl=300)
    def called():
        return "ok"

    @_cache.cache_resource
    def bare_resource():
        return object()

    assert bare() == bare()          # memoised: second call returns cached
    assert calls["n"] == 1
    assert called() == "ok"
    assert bare_resource() is bare_resource()  # singleton


def test_memoisation_keys_on_arguments():
    calls = []

    @_cache.cache_data(ttl=300)
    def square(x: int) -> int:
        calls.append(x)
        return x * x

    assert square(2) == 4
    assert square(2) == 4  # cached, not recomputed
    assert square(3) == 9
    assert calls == [2, 3]  # each distinct arg computed exactly once


def test_delegates_to_streamlit_when_present():
    """When streamlit imports, the shim must use st.cache_data, not the fallback."""
    if not _cache._HAS_STREAMLIT:
        pytest.skip("streamlit not installed in this environment")

    import streamlit as st

    @_cache.cache_data(ttl=300)
    def f():
        return 1

    # st.cache_data wraps the function in a CachedFunc exposing .clear();
    # the neutral fallback exposes .cache_clear instead. Presence of .clear
    # proves delegation to the real Streamlit decorator.
    assert hasattr(f, "clear")


def test_fallback_when_streamlit_absent(monkeypatch):
    """Simulate an API-only runtime: streamlit not importable -> lru_cache path."""
    real = _cache._HAS_STREAMLIT
    monkeypatch.setattr(_cache, "_HAS_STREAMLIT", False)
    try:
        calls = []

        @_cache.cache_data(ttl=300)
        def f(x):
            calls.append(x)
            return x

        assert f(1) == 1
        assert f(1) == 1
        assert calls == [1]  # memoised without any Streamlit runtime
        assert hasattr(f, "cache_clear")  # the neutral marker
    finally:
        monkeypatch.setattr(_cache, "_HAS_STREAMLIT", real)


def test_unhashable_arg_passes_through_uncached(monkeypatch):
    """A list arg would TypeError under lru_cache; the shim must call through."""
    monkeypatch.setattr(_cache, "_HAS_STREAMLIT", False)

    @_cache.cache_data(ttl=300)
    def f(items):
        return sum(items)

    assert f([1, 2, 3]) == 6  # no crash on an unhashable argument


def test_signature_and_name_preserved():
    @_cache.cache_data(ttl=300)
    def named_fn(a, b):
        """docstring."""
        return a + b

    # functools.wraps / st both preserve __name__ so pages keep readable stacks.
    assert named_fn.__name__ == "named_fn"
