"""Data/text utilities: search normalisation + tolerant substring matching.

Split out of the former monolithic ``ui/components.py`` (2026-07 package
split, C4 of doc/REFACTORING_CANDIDATES.md). Pure Move-Function — no body
changes.
"""

from __future__ import annotations

import re


def clean_meta(*parts: str) -> str:
    """Join non-empty, non-NaN string parts with ' · '."""
    return " · ".join(p for p in parts if p and p.lower() not in ("nan", ""))


_SEARCH_WS_RE = re.compile(r"\s+")


def search_normalise(value):
    """Normalise text for tolerant, crash-proof substring search.

    Lowercases, turns hyphens into spaces, and collapses runs of whitespace,
    so a plain term like "Dublin South West" matches stored "Dublin
    South-West" (and vice versa), and a stray double space never blocks a
    match. Accepts a ``str`` or a pandas ``Series`` and returns the same type.

    Always pair the result with ``str.contains(..., regex=False)`` (or use
    :func:`text_search_mask`): user input is treated as a literal, so a term
    containing regex metacharacters ("(", "*", "+") never raises.
    """
    if isinstance(value, str):
        return _SEARCH_WS_RE.sub(" ", value.replace("-", " ")).strip().lower()
    # pandas Series — normalise element-wise without importing pandas here.
    return (
        value.astype(str)
        .str.replace("-", " ", regex=False)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
        .str.lower()
    )


def search_matches(query: str, *fields: str) -> bool:
    """True if the normalised ``query`` is a substring of any normalised field.

    Scalar counterpart to :func:`text_search_mask` for list-comprehension
    filters. Blank/whitespace query matches everything.
    """
    q = search_normalise(query or "")
    if not q:
        return True
    return any(q in search_normalise(f or "") for f in fields)


def text_search_mask(df, query, columns):
    """Boolean mask selecting rows where any of ``columns`` contains ``query``.

    Tolerant (hyphen/space/case-insensitive) and regex-safe — see
    :func:`search_normalise`. Returns an all-True mask when ``query`` is blank,
    so callers can apply it unconditionally.
    """
    import pandas as pd

    q = search_normalise(query or "")
    if not q:
        return pd.Series(True, index=df.index)
    mask = pd.Series(False, index=df.index)
    for col in columns:
        mask |= search_normalise(df[col]).str.contains(q, na=False, regex=False)
    return mask
