"""Legislation resource models. Bill list + SI list are pass-through records
(the published view columns are the contract); the composed bill dossier is typed."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class BillDossier(BaseModel):
    """A bill's composed record: detail + lifecycle + amendments + the SIs made under it."""

    bill: dict[str, Any]
    timeline: list[dict[str, Any]] = Field(default_factory=list)
    amendment_intensity: dict[str, Any] | None = None
    sources: dict[str, Any] | None = None
    pdfs: list[dict[str, Any]] = Field(default_factory=list)
    debates: list[dict[str, Any]] = Field(default_factory=list)
    si_composition: list[dict[str, Any]] = Field(default_factory=list)
    statutory_instruments: list[dict[str, Any]] = Field(default_factory=list)
