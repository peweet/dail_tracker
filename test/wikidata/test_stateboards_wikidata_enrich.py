"""Unit tests for the State Boards Wikidata enrichment (pure logic, no network).

Covers the name-cleaning that decides WHAT we look up, and the match policy
that decides WHEN a candidate counts — both are the privacy/accuracy load-
bearing parts (false positive = wrong person publicly tagged with a role).

Run:  pytest test/wikidata/test_stateboards_wikidata_enrich.py -v
"""

from __future__ import annotations

from wikidata.stateboards_wikidata_enrich import (
    _sparql_literal,
    clean_name,
    decide,
    fold_name,
    is_person_name,
    match_query,
)


def test_clean_name_strips_stacked_honorifics():
    # Register style for judges stacks them; caught live on Courts Service Board.
    assert clean_name("The Hon. Mr. Justice Seamus Woulfe") == "Seamus Woulfe"
    assert clean_name("The Hon. Ms. Justice Aileen Donnelly") == "Aileen Donnelly"
    assert clean_name("Dr Anne Cusack") == "Anne Cusack"
    assert clean_name("Cllr. Joe Lynch") == "Joe Lynch"


def test_clean_name_strips_qualification_suffix_keeps_accents():
    assert clean_name("Adrienne Cawley, B.L.") == "Adrienne Cawley"
    # Accents are KEPT — WDQS label lookup is exact.
    assert clean_name("Seán Ó Foghlú") == "Seán Ó Foghlú"


def test_fold_name_accent_and_case_insensitive():
    assert fold_name("Seán Ó Foghlú") == "sean o foghlu"
    assert fold_name("Dr SEÁN Ó FOGHLÚ") == "sean o foghlu"


def test_is_person_name_rejects_placeholders():
    assert is_person_name("Denis Drennan")
    assert not is_person_name("Vacant")
    assert not is_person_name("Vacancy x2")
    assert not is_person_name("")


def test_sparql_literal_escapes_quotes():
    assert _sparql_literal('John "Jack" O\'Carroll') == '"John \\"Jack\\" O\'Carroll"'


def test_match_query_uses_preferred_label_only():
    q = match_query(["Aidan Murphy"])
    # alias matching pulled in people known under a DIFFERENT name (e.g. the
    # register's "Aidan Murphy" hit actor Aidan Gillen via birth-name alias).
    assert "altLabel" not in q
    assert "rdfs:label ?name" in q
    assert '"Aidan Murphy"@en' in q


def test_decide_policy():
    irish_cit = {"qid": "Q1", "label": "A B", "desc": None, "irish_citizen": True}
    irish_desc = {"qid": "Q2", "label": "A B", "desc": "Irish hurler", "irish_citizen": False}
    foreign = {"qid": "Q3", "label": "A B", "desc": "American politician", "irish_citizen": False}

    assert decide([])["match"] == "none"
    assert decide([foreign])["match"] == "none"

    one = decide([irish_cit, foreign])
    assert one["match"] == "matched"
    assert one["qid"] == "Q1"

    two = decide([irish_cit, irish_desc])
    assert two["match"] == "ambiguous"
    assert two["n_candidates"] == 2
    assert "qid" not in two

    # Same item arriving twice (two language tags) is ONE candidate, not ambiguous.
    assert decide([irish_cit, dict(irish_cit)])["match"] == "matched"
