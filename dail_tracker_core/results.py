"""QueryResult — the single return type for dail_tracker_core query functions.

Every ``queries/<domain>.py`` function returns a ``QueryResult`` instead of a
bare DataFrame. This lets *every* interface (Streamlit today, a dossier-pack
builder or FastAPI later) distinguish two things the old data-access layer
silently conflated by returning an empty DataFrame on failure:

  - **success with no rows** — the query ran; there is genuinely nothing to show
    (``ok=True``, ``data`` empty). The UI should render an empty state, not an error.
  - **source unavailable** — the query could not run: a missing parquet, an
    unregistered view, a DuckDB error (``ok=False``, ``unavailable_reason`` set).
    The UI should say so, not pretend "no results".

This is deliberately a THREE-state model (rows / no-rows / unavailable). The
richer states discussed in planning — ``not-checked`` (e.g. SI legal-state NULL)
and ``manual-review`` (e.g. SIPO OCR) — are real but map to specific datasets;
they are added per-surface when those pages migrate, not pre-built here.

The dataclass is frozen and holds only inert fields (a DataFrame + plain
scalars) so it is safe to cache and to pickle: it carries no live connection or
generator. Streamlit ``@st.cache_data`` caches the *return* value, so the wrapper
may cache a QueryResult directly; the unhashable ``conn`` never crosses the cache
boundary because query functions take it as an argument fetched inside the wrapper.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


class SourceUnavailable(RuntimeError):
    """A REQUIRED source could not be queried (missing parquet / unregistered view).

    Raised by ``QueryResult.require()`` at gates where treating an outage as
    "no rows" would lie to the caller (a member dossier rendering as "not found"
    because the registry view failed to register). Interfaces map it once:
    FastAPI has a global handler → 503; MCP tools catch it → an ``{"error"}``
    dict. Optional per-section reads should keep degrading softly instead —
    see ``dossiers._section``.
    """


@dataclass(frozen=True)
class QueryResult:
    """Outcome of a single core query.

    Construct via the classmethods rather than the raw initialiser so the
    ok/unavailable invariant is enforced in one place.
    """

    data: pd.DataFrame
    ok: bool = True
    unavailable_reason: str | None = None

    def __post_init__(self) -> None:
        # Invariant: an unavailable result must carry a reason and no rows; an ok
        # result must not carry a reason. Frozen dataclasses can still raise here.
        if self.ok and self.unavailable_reason is not None:
            raise ValueError("ok QueryResult must not carry an unavailable_reason")
        if not self.ok and not self.unavailable_reason:
            raise ValueError("unavailable QueryResult must carry an unavailable_reason")

    @classmethod
    def success(cls, data: pd.DataFrame) -> QueryResult:
        """A query that ran. ``data`` may be empty (success with no rows)."""
        return cls(data=data, ok=True, unavailable_reason=None)

    @classmethod
    def unavailable(cls, reason: str) -> QueryResult:
        """A query that could not run (missing source / view / DuckDB error)."""
        return cls(data=pd.DataFrame(), ok=False, unavailable_reason=reason)

    @property
    def is_empty(self) -> bool:
        """True when the query ran but returned no rows. Always True when not ok.

        Defensive: a raw-constructed result carrying ``data=None`` counts as
        empty rather than raising — pages branch on ``ok``/``is_empty`` and must
        never crash on a malformed result.
        """
        return self.data is None or self.data.empty

    def require(self) -> pd.DataFrame:
        """The DataFrame, or ``SourceUnavailable`` when the query could not run.

        Use at GATES (identity lookups, list indexes) where an outage must not
        collapse into "empty"/"not found" — the exact conflation this class
        exists to prevent. For optional enrichment sections, read ``.data``
        and record the degradation instead.
        """
        if not self.ok:
            raise SourceUnavailable(self.unavailable_reason or "source unavailable")
        return self.data
