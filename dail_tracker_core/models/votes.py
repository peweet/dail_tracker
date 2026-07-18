"""Votes resource model: the composed division dossier (lists are pass-through)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class DivisionDossier(BaseModel):
    """One Dáil/Seanad division: the vote + party breakdown + every member's vote + sources."""

    division: dict[str, Any]
    party_breakdown: list[dict[str, Any]] = Field(default_factory=list)
    members: list[dict[str, Any]] = Field(default_factory=list)
    sources: dict[str, Any] | None = None
    # Present only when a section's source was down (outage ≠ empty).
    unavailable_sections: list[dict[str, str]] | None = None
