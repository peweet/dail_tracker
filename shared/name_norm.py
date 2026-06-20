"""Company-name normalisation — the shared join key for matching organisations
across the company register (CRO), procurement suppliers, lobbying registrants,
and TED award winners.

This is the COMPANY-name rule. It is deliberately distinct from
[[normalise_join_key]] (``shared/normalise_join_key.py``), which is the PERSON
(TD) name rule — the CRO/organisation universe is too dissimilar to member
names to share one key (different legal-suffix stripping, no sorted-letter
anagram key).

Extracted from ``cro_normalise.py`` because the same rule is depended on by
~18 call sites across the procurement / TED / corporate-xref / lobbying
enrichers: a notice's ``entity_name`` and a supplier's name must normalise by
the IDENTICAL rule as the CRO ``company_name`` for the exact-name join to land.
Keeping it in one place (with a direct unit test) is what guarantees that.
"""

from __future__ import annotations

import polars as pl

# Legal suffixes / corporate fillers / connectors dropped so "ACME HOLDINGS LIMITED"
# and "Acme" collapse to the same key. Word-bounded so it never eats a substring.
# AND is dropped for the SAME reason '&' is (replaced with space below): otherwise
# "Turner & Townsend"->TURNER TOWNSEND but "Turner And Townsend"->TURNER AND TOWNSEND
# miss each other and the CRO register.
LEGAL_SUFFIX_PATTERN = (
    r"\b(?:THE|AND|LIMITED|LTD|DAC|PLC|CLG|UC|COMPANY|"
    r"DESIGNATED ACTIVITY COMPANY|"
    r"COMPANY LIMITED BY GUARANTEE|"
    r"UNLIMITED COMPANY|GROUP|HOLDINGS|IRELAND|IRL|OF)\b"
)


def name_norm_expr(col: str) -> pl.Expr:
    """Normalise a company-name column: accent-fold, upper-case, strip punctuation,
    drop legal suffixes / corporate fillers, drop non-alphanumerics, collapse
    whitespace. Returns a Polars expression (pure — no IO).

    The NFD + combining-mark strip is the house accent-fold standard (shared with
    normalise_join_key, diary_org_match.norm, cbi_registers_extract._norm_firm).
    WITHOUT it, accented Irish/foreign company names get mangled, spelling-asymmetric
    keys that silently fail to join their CRO/ASCII counterpart: "Tirlán" → "TIRL N"
    (not "TIRLAN"), "Telefónica" → "TELEF NICA", "Gaelchultúr" → "GAELCHULT R". Both
    sides must fold é→E for the exact-name join to land regardless of fada usage."""
    return (
        pl.col(col)
        .str.normalize("NFD")  # decompose accents: e-acute -> e + combining mark
        .str.replace_all(r"[̀-ͯ]", "")  # drop combining marks -> ASCII letter
        .str.to_uppercase()
        .str.replace_all(r"[\.,&'\"]", " ")
        .str.replace_all(LEGAL_SUFFIX_PATTERN, " ")
        .str.replace_all(r"[^A-Z0-9 ]", " ")
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
    )
