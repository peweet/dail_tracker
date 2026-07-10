"""Unit tests for shared/name_norm.name_norm_expr.

The company-name join key. ~18 enrichers (procurement awards, TED winners,
corporate-notice xref, lobbying registrants) normalise an organisation name
with THIS rule and exact-join it against the CRO company_name normalised by the
SAME rule. If the rule drifts, those joins silently miss — a supplier stops
matching its CRO record, a registrant stops matching its company. So the
contract is worth locking directly (it was previously only exercised
indirectly via test_cro_corporate_xref).

Rule: upper-case -> strip punctuation -> drop legal suffixes / corporate
fillers -> drop non-alphanumerics -> collapse whitespace.
"""

import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from shared.name_norm import name_norm_expr, name_norm_str  # noqa: E402


def _norm(name: str) -> str:
    """Apply the expression to a one-row frame and return the normalised value."""
    return pl.DataFrame({"n": [name]}).select(name_norm_expr("n").alias("out"))["out"][0]


def test_legal_suffix_and_case_collapse():
    assert _norm("Acme Holdings Limited") == "ACME"


def test_the_prefix_and_company_dropped():
    assert _norm("The Foo Company") == "FOO"


def test_punctuation_becomes_space_and_collapses():
    assert _norm("O'Brien & Sons, Ltd.") == "O BRIEN SONS"


def test_same_entity_variants_produce_identical_key():
    # the whole point: a CRO record and a supplier listing of the same company
    # must land on the same key despite suffix/case differences.
    assert _norm("ACME LIMITED") == _norm("Acme Ltd") == "ACME"


def test_ampersand_and_word_and_collapse_identically():
    # '&' and the word 'and' are BOTH dropped so the two spellings of a connector
    # land on one key (and match the CRO register). Regression guard for the
    # Turner & Townsend / Turner And Townsend dedup gap.
    assert _norm("Turner & Townsend") == _norm("Turner And Townsend") == "TURNER TOWNSEND"
    assert _norm("Black and Decker") == "BLACK DECKER"


def test_accents_folded_to_ascii():
    # accents are NFD-folded to their base letter (é -> E) so an accented name and
    # its ASCII spelling land on ONE key — both sides of the CRO join must agree
    # regardless of fada usage ("Tirlán" awards vs "TIRLAN" payments). Other
    # symbols ('#') still become spaces.
    assert _norm("Café & Bar #1") == "CAFE BAR 1"
    assert _norm("Tirlán Ltd") == _norm("TIRLAN") == "TIRLAN"
    assert _norm("Telefónica") == "TELEFONICA"


def test_empty_string():
    assert _norm("") == ""


# ── name_norm_str is byte-identical to name_norm_expr ─────────────────────────
# The row-loop extractors (CBI, receiver, diary) use the str twin; it MUST agree
# with the Polars expr or those keys stop joining the supplier/CRO universe (the
# very bug this unification fixes). Sweep a corpus of the shapes that actually
# appear: accents/fadas, connectors, legal suffixes, trading-as tails, digits,
# punctuation, non-Latin, whitespace, empty.
_CORPUS = [
    "Acme Holdings Limited", "The Foo Company", "O'Brien & Sons, Ltd.",
    "ACME LIMITED", "Acme Ltd", "Turner & Townsend", "Turner And Townsend",
    "Black and Decker", "Café & Bar #1", "Tirlán Ltd", "TIRLAN", "Telefónica",
    "Gaelchultúr Teoranta", "Óglaigh na hÉireann", "Bank of Ireland Group plc",
    "PFH Technology Group", "Ernst & Young", "Deloitte Ireland LLP",
    "Uisce Éireann", "An Post", "Bord Gáis Energy", "SSE Airtricity Ltd",
    "Designated Activity Company", "X Unlimited Company", "Y CLG",
    "  spaced   out   name  ", "123 Numbers Ltd", "ABC / DEF Ltd",
    "Naomh Séamas Teoranta t/a St James", "MÜLLER & CO", "", "A",
]


def test_name_norm_str_matches_expr_across_corpus():
    for name in _CORPUS:
        assert name_norm_str(name) == _norm(name), f"drift on {name!r}: str={name_norm_str(name)!r} expr={_norm(name)!r}"


def test_name_norm_str_none_is_empty():
    assert name_norm_str(None) == ""


def test_name_norm_str_accent_fold_joins():
    # the headline fix: the str twin folds fadas the same way, so a CBI/charity
    # firm name lands on the same key as its CRO/supplier record.
    assert name_norm_str("Tirlán Ltd") == name_norm_str("TIRLAN") == "TIRLAN"
