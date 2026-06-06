"""Member resource models: the list summary + the composed dossier."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class MemberSummary(BaseModel):
    """One row of the member registry (the list endpoint + dossier identity)."""

    unique_member_code: str
    member_name: str | None = None
    party_name: str | None = None
    constituency: str | None = None
    house: str


class HeadlineStats(BaseModel):
    """The cross-dataset headline numbers shown above the fold."""

    latest_year: int | None = None
    days_in_chamber_latest: int | None = None
    votes_cast: int = 0
    divisions: int = 0
    payments_total_eur: float = 0.0


class MemberDossier(BaseModel):
    """A member's full public accountability record, composed server-side.

    Section bodies are typed as record lists (the published projection is the
    identity + headline contract; section detail passes through as rows so the
    contract doesn't churn every time a view gains a column).
    """

    member: MemberSummary
    is_minister: bool = False
    headline: HeadlineStats
    attendance_by_year: list[dict[str, Any]] = Field(default_factory=list)
    payments_by_year: list[dict[str, Any]] = Field(default_factory=list)
    legislation_sponsored: list[dict[str, Any]] = Field(default_factory=list)
    ministerial_roles: list[dict[str, Any]] = Field(default_factory=list)
    statutory_instruments_signed: list[dict[str, Any]] = Field(default_factory=list)
    revolving_door: list[dict[str, Any]] = Field(default_factory=list)
    questions_profile: dict[str, Any] | None = None
    external_links: dict[str, Any] = Field(default_factory=dict)
    constituency_context: dict[str, Any] | None = None
