"""Unit tests for wikidata/ministerial_tenure_build._norm_name.

Pure name normaliser — no network, no IO. It is the join key between Wikidata
person labels and flattened_members.full_name; a bug here silently drops the
member_code link on a minister's SIs (the minister resolves to a name, not a
clickable profile). So the contract is worth locking: accents stripped,
lower-cased, punctuation→space, whitespace collapsed — and crucially the
fada/no-fada spelling of the SAME name must normalise identically.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from wikidata.ministerial_tenure_build import _norm_name  # noqa: E402


def test_strips_accents_and_lowercases():
    assert _norm_name("Éamon Ó Cuív") == "eamon o cuiv"


def test_punctuation_becomes_space_and_collapses():
    assert _norm_name("O'Brien, Seán.") == "o brien sean"


def test_collapses_runs_of_whitespace_and_trims():
    assert _norm_name("  Mary   Lou  McDonald  ") == "mary lou mcdonald"


def test_already_normalised_is_stable():
    assert _norm_name("leo varadkar") == "leo varadkar"


def test_empty_string():
    assert _norm_name("") == ""


def test_fada_and_no_fada_match():
    # the whole point: Wikidata "Micheál Martin" must join to members "Micheal Martin"
    assert _norm_name("Micheál Martin") == _norm_name("Micheal Martin")
