"""DataFrame → JSON-safe serialization for dail_tracker_core.

Single chokepoint that turns a pandas DataFrame (what every core query returns
inside a ``QueryResult``) into plain JSON-serializable Python, handling the exact
type traps the migration parity harnesses surfaced:

  - NaN / NaT / pd.NA           → None
  - pandas.Timestamp / date     → ISO-8601 string
  - numpy scalar (int64, etc.)  → native int/float/bool
  - Decimal                     → float
  - numpy array / list-valued   → list (e.g. lobbying ``flags``, ``beneficiary_tags``)
  - bytes                       → utf-8 string

It also owns the response envelope. This is deliberately in CORE (not ``api/``)
so the same serializer powers both the live API and any future file-based
"dossier pack" product — and so the one place to attach caveat metadata / suppress
PII columns is shared. (PII suppression hook: pass ``drop_cols`` to ``to_records``
for surfaces that carry sensitive columns, e.g. SIPO donor addresses — the member
dossier has none.)
"""

from __future__ import annotations

import datetime as _dt
import math
from decimal import Decimal
from typing import Any

import numpy as np
import pandas as pd


def _coerce(v: Any) -> Any:
    # Containers first (pd.isna on an array raises / returns an array).
    if isinstance(v, np.ndarray):
        return [_coerce(x) for x in v.tolist()]
    if isinstance(v, (list, tuple, set)):
        return [_coerce(x) for x in v]
    if isinstance(v, dict):
        return {str(k): _coerce(x) for k, x in v.items()}
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    # Scalar missing values (guard the ambiguous-truth-value error).
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, (pd.Timestamp, _dt.datetime, _dt.date)):
        return v.isoformat()
    if isinstance(v, np.generic):
        return _coerce(v.item())
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, bytes):
        return v.decode("utf-8", "replace")
    return v


def value(v: Any) -> Any:
    """Coerce a single scalar/container to a JSON-safe value."""
    return _coerce(v)


def to_records(df: pd.DataFrame | None, *, drop_cols: list[str] | None = None) -> list[dict[str, Any]]:
    """DataFrame → list of JSON-safe row dicts. Empty/None → []."""
    if df is None or df.empty:
        return []
    cols = [c for c in df.columns if not (drop_cols and c in drop_cols)]
    keep = df[cols]
    out: list[dict[str, Any]] = []
    for row in keep.itertuples(index=False, name=None):
        out.append({c: _coerce(v) for c, v in zip(cols, row, strict=True)})
    return out


def first_record(df: pd.DataFrame | None) -> dict[str, Any] | None:
    """First row as a JSON-safe dict, or None when empty."""
    recs = to_records(df.head(1) if df is not None else None)
    return recs[0] if recs else None


def envelope(
    results: list[Any],
    *,
    limit: int | None = None,
    offset: int | None = None,
    total: int | None = None,
    truncated: bool = False,
    mart_version: str | None = None,
    generated_at: str | None = None,
    caveat: str | None = None,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """The standard ``{head, results}`` envelope (mirrors the Oireachtas API).

    ``head`` carries pagination + freshness metadata; ``results`` is the array.
    ``caveat`` attaches the canonical provenance string (from
    ``dail_tracker_core.caveats``) so a list response carries the same qualifier the
    composed dossiers do; ``meta`` merges any extra scope keys (e.g. the resolved
    ``year``/``house`` for a list that was filtered to a default) into ``head``.
    """
    head: dict[str, Any] = {}
    if limit is not None:
        head["limit"] = limit
    if offset is not None:
        head["offset"] = offset
    if total is not None:
        head["total"] = total
    head["truncated"] = truncated
    if mart_version is not None:
        head["mart_version"] = mart_version
    if generated_at is not None:
        head["generated_at"] = generated_at
    if meta:
        head.update(meta)
    if caveat is not None:
        head["caveat"] = caveat
    return {"head": head, "results": results}
