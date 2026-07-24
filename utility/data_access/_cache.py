"""Framework-neutral caching shim for the data-access layer.

The data-access wrappers used Streamlit for one thing only: memoisation
(`@st.cache_data` / `@st.cache_resource`). That was the single reason 27 of the
28 modules imported streamlit at all — see doc/FRAMEWORK_DECOUPLING_PLAN.md §2.1.

This shim removes that coupling without changing behaviour:

  * **Streamlit installed** (the live app): delegate verbatim to
    `st.cache_data` / `st.cache_resource`. Identical semantics — per-session
    scope, TTL expiry, defensive copy on return, spinner control. Zero risk.
  * **Streamlit absent** (tests, a future FastAPI-only deploy): fall back to
    `functools.lru_cache`, an unbounded process-global memo.

The seam is IMPORT availability, not runtime activity, because that is exactly
the deployment split: the Streamlit app ships streamlit; an API-only box need
not. Detecting a live ScriptRunContext would be more fragile and buy nothing.

Two honest differences in the fallback path (both harmless off-Streamlit):
  * `ttl` and `show_spinner` are ignored — there is no session clock or spinner
    without a Streamlit runtime; freshness there is a process-lifecycle concern.
  * `lru_cache` returns the SAME object each call; `st.cache_data` returns a
    COPY. These wrappers return read-only frames, so callers must not mutate a
    returned frame in place — which was already true under Streamlit.

Usage — a one-line import swap in each wrapper:

    from data_access._cache import cache_data, cache_resource

    @cache_resource
    def get_conn(): ...

    @cache_data(ttl=300)
    def fetch(...): ...
"""

from __future__ import annotations

import functools
from typing import Any, Callable, TypeVar

F = TypeVar("F", bound=Callable[..., Any])

try:  # the live Streamlit app
    import streamlit as _st

    _HAS_STREAMLIT = True
except ModuleNotFoundError:  # tests / API-only runtime
    _st = None  # type: ignore[assignment]
    _HAS_STREAMLIT = False


def _neutral_memo(func: F, *, max_entries: int | None = None) -> F:
    """lru_cache with a graceful pass-through on unhashable arguments.

    `st.cache_data` hashes common containers by value; `lru_cache` rejects
    unhashable args with TypeError. The data-access wrappers only ever take
    scalar args, so this path is defensive — on an unhashable arg it calls
    through uncached rather than raising, keeping the fallback a strict
    superset of what Streamlit accepted.
    """
    memo = functools.lru_cache(maxsize=max_entries)(func)

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return memo(*args, **kwargs)
        except TypeError:
            return func(*args, **kwargs)

    wrapper.cache_clear = memo.cache_clear  # type: ignore[attr-defined]
    return wrapper  # type: ignore[return-value]


def cache_data(
    func: F | None = None,
    *,
    ttl: Any = None,
    show_spinner: Any = True,
    max_entries: int | None = None,
    **kwargs: Any,
) -> Any:
    """Drop-in for `st.cache_data`, usable bare or called with keywords.

    Accepts the same keyword surface the wrappers use (`ttl`, `show_spinner`);
    any further `st.cache_data` kwargs pass straight through when Streamlit is
    present and are ignored by the neutral fallback.
    """
    if _HAS_STREAMLIT:
        deco = _st.cache_data(
            ttl=ttl, show_spinner=show_spinner, max_entries=max_entries, **kwargs
        )
        return deco(func) if func is not None else deco

    def wrap(f: F) -> F:
        return _neutral_memo(f, max_entries=max_entries)

    return wrap(func) if func is not None else wrap


def cache_resource(
    func: F | None = None,
    *,
    show_spinner: Any = True,
    **kwargs: Any,
) -> Any:
    """Drop-in for `st.cache_resource` (singletons: connections, models).

    Usable bare (`@cache_resource`) or called (`@cache_resource(show_spinner=...)`).
    The neutral fallback memoises to a process-global singleton — the same shape
    a resource cache guarantees.
    """
    if _HAS_STREAMLIT:
        deco = _st.cache_resource(show_spinner=show_spinner, **kwargs)
        return deco(func) if func is not None else deco

    def wrap(f: F) -> F:
        return _neutral_memo(f, max_entries=None)

    return wrap(func) if func is not None else wrap
