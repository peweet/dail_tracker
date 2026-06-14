"""Shared display-cleaning for public-buyer names from eTenders / TED.

eTenders — and the TED (EU Official Journal) notices that mirror the same OGP source —
append the OGP internal organisation id to the buyer name ("Cork County Council_424")
and, for schools, a roll number ("Scoil Ailbhe - (18030I)"). Both are identifiers, not
part of the display name.

A parenthetical that does NOT start with a digit ("…Authority (HIQA)", "School (Navan)")
is a real acronym / place-name and is PRESERVED — the digit-leading test is what tells an
id apart from a name. This is the single source of truth used by both the live-tenders
extractor (which also lifts the id into its own column) and the TED extractors (clean only).
"""

from __future__ import annotations

import polars as pl

# Trailing _<digits> = the OGP org id; trailing " - (<digit-led code>)" = a school roll number.
ORG_ID_RX = r"_(\d+)$"
ROLL_RX = r"\s*[-–]\s*\(\d[0-9A-Za-z]*\)$"


def org_id_expr(col: str = "buyer") -> pl.Expr:
    """The trailing _<digits> OGP org id lifted off the name (null when absent) — a stable join key."""
    return pl.col(col).str.extract(ORG_ID_RX, 1)


def clean_name_expr(col: str = "buyer") -> pl.Expr:
    """The display name with the org id + school roll number stripped (acronyms/place-names kept)."""
    return pl.col(col).str.replace(ORG_ID_RX, "").str.replace(ROLL_RX, "").str.strip_chars()


def clean_buyer_display(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """Strip id/roll debris from a buyer-name column in place. Idempotent — safe to re-run
    (the patterns simply no longer match an already-cleaned name)."""
    if col not in df.columns:
        return df
    return df.with_columns(clean_name_expr(col).alias(col))
