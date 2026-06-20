"""Unit tests for the diary×public-money fold join key.

The fold key is the only thing connecting a diary org to the procurement
registers. A regression here silently DROPS money from the output (the exact
class of bug the missing accent-fold caused: "Tirlán Ltd"→"tirl n" on the awards
side never matched "TIRLAN"→"tirlan" on the payments side, zeroing €210k of
displayable payments). These tests pin the invariants that keep the join honest.
No file I/O — pure function.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))
from extractors.diary_company_influence import fold


def test_accent_folded_to_ascii():
    # the regression that dropped Tirlán's payments: accented vs ASCII must agree
    assert fold("Tirlán Ltd") == fold("TIRLAN") == "tirlan"


def test_accent_variants_converge():
    assert fold("Nestlé") == fold("Nestle") == "nestle"
    assert fold("Bord Gáis Energy Ltd") == fold("Bord Gais Energy")
    assert fold("Uisce Éireann") == fold("Uisce Eireann")


def test_legal_and_geographic_suffixes_stripped():
    assert fold("Vodafone Ireland Limited") == fold("Vodafone") == "vodafone"
    assert fold("Acme Group Holdings") == "acme"


def test_case_insensitive_and_space_collapsed():
    assert fold("MASON  HAYES & Curran LLP") == fold("mason hayes curran")


def test_digits_preserved():
    # company names carry meaningful digits (Havbell No.2) — fold must keep them
    assert "2" in fold("Havbell No.2 DAC")


def test_empty_and_none_safe():
    assert fold(None) == ""
    assert fold("") == ""
    assert fold("Ltd") == ""  # nothing but a stripped suffix
