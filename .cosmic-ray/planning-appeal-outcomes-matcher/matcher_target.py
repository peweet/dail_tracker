# COPIED-FROM: planning/civic/extractors/planning_appeal_outcomes.py @ sha256:4e4107d23455a9c4d902d5c869606486d9176d7dcb276491f9cd76f7d31fd065
"""Mutation target for the current pure outcomes compatibility seam.

The spatial algorithm now lives in planning_appeal_vector.py; this target retains
only outcomes.py's authority/decision helpers and its delegation wrapper. The
vector session remains the authoritative mutation audit for matching behaviour.
"""

from __future__ import annotations

import re
import unicodedata

import polars as pl

from planning.civic.extractors.planning_appeal_vector import spatial_temporal_matches


def _auth_key(name: str | None) -> str:
    """Normalise a planning-authority name to a join key: drop any ' - … Section' suffix
    (only 'Cork County Council - West Cork Section' exists), strip accents/punctuation.
    Maps ACP's PLANINGATY onto the application feed's PlanningAuthority (e.g. West Cork
    folds into Cork County) so the spatial fallback only matches within the same council."""
    s = re.sub(r"\s*-\s*.*$", "", name or "")
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z]", "", s.lower())


def _council_decision(d: str | None) -> str:
    if d in ("Granted", "Granted-Conditional"):
        return "GRANT"
    return "REFUSE" if d == "Refused" else "OTHER"


def _spatial_temporal_matches(residual: pl.DataFrame, apps: pl.DataFrame) -> pl.DataFrame:
    """Compatibility entry point for the native, order-preserving matcher."""
    return spatial_temporal_matches(residual, apps)
