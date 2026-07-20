"""The generic list-response contract: ``{head, results}`` for every list endpoint.

Row fields stay view-driven (``list[dict]``) by design — the per-resource column
sets are the views' contract and typing all ~45 of them now would freeze churn we
still want. Typed per-resource models are added only when a resource gains an
external consumer (the suppliers/buyers surface first, per the uncoupling plan).
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class Head(BaseModel):
    """Pagination + provenance metadata. ``extra`` scope keys (resolved year/house
    filters etc.) pass through — the fixed fields are the stable floor."""

    model_config = ConfigDict(extra="allow")

    limit: int | None = None
    offset: int | None = None
    total: int | None = None
    truncated: bool = False
    generated_at: str | None = None
    caveat: str | None = None


class ListEnvelope(BaseModel):
    """``{head, results}`` — the uniform shape of every list response."""

    head: Head
    results: list[dict[str, Any]] = Field(default_factory=list)
