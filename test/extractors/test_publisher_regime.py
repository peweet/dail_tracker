"""Unit tests for the publisher disclosure-regime registry (extractors/_publisher_regime.py).

Pure-function, no data files — runs in CI. Locks the structure that makes the PO/payments corpus
self-describing: every publisher resolves to a legal basis + threshold + VAT basis + procurement
legal class, defaulting sensibly by publisher_type and overriding only where a body genuinely
differs (CHI €25k, HSE incl-VAT, utilities as contracting entities, commercial state bodies).
"""

from __future__ import annotations

import pytest

from extractors._publisher_regime import (
    _OVERRIDES,
    BASIS_LABEL,
    BODY_PROCUREMENT_CLASS,
    CLASS_LABEL,
    DISCLOSURE_BASIS,
    regime_for,
)

_KEYS = {
    "disclosure_basis",
    "disclosure_threshold_eur",
    "threshold_vat",
    "body_procurement_class",
    "regime_note",
}


# --------------------------------------------------------------------------- shape / defaults
def test_returns_all_keys():
    r = regime_for("anything", "department")
    assert set(r) == _KEYS


def test_default_is_foi_scheme_20k():
    r = regime_for("dept_unknown_new_body", "department")
    assert r["disclosure_basis"] == "foi_s8_model_scheme"
    assert r["disclosure_threshold_eur"] == 20000
    assert r["threshold_vat"] == "unknown"
    assert r["body_procurement_class"] == "contracting_authority"
    assert r["regime_note"] == ""


def test_threshold_is_always_int():
    for pid in [None, "ie_chi", "ie_opw", "dept_health", "ie_esbnetworks"]:
        assert isinstance(regime_for(pid, "agency")["disclosure_threshold_eur"], int)


@pytest.mark.parametrize(
    "ptype,expected",
    [
        ("local_authority", "contracting_authority"),
        ("department", "contracting_authority"),
        ("agency", "contracting_authority"),
        ("state_body", "contracting_authority"),
        ("education_body", "contracting_authority"),
        ("hospital", "contracting_authority"),
        ("semi_state", "contracting_authority"),  # non-commercial default; utilities overridden
    ],
)
def test_body_class_default_by_type(ptype, expected):
    assert regime_for("brand_new_id", ptype)["body_procurement_class"] == expected


def test_unknown_type_falls_back_to_contracting_authority():
    assert regime_for("x", None)["body_procurement_class"] == "contracting_authority"
    assert regime_for("x", "made_up_type")["body_procurement_class"] == "contracting_authority"


def test_none_publisher_id_is_safe():
    r = regime_for(None, None)
    assert r["disclosure_basis"] in DISCLOSURE_BASIS
    assert r["body_procurement_class"] in BODY_PROCUREMENT_CLASS


# --------------------------------------------------------------------------- specific overrides
def test_chi_threshold_is_25k_incl_vat():
    r = regime_for("ie_chi", "state_body")
    assert r["disclosure_threshold_eur"] == 25000
    assert r["threshold_vat"] == "incl_vat"
    assert r["regime_note"]  # has an explanatory note


def test_hse_and_tusla_are_incl_vat():
    assert regime_for("ie_hse", "state_body")["threshold_vat"] == "incl_vat"
    assert regime_for("ie_tusla", "agency")["threshold_vat"] == "incl_vat"


def test_hse_note_mentions_100k_model_threshold():
    # The honesty point: HSE's model-scheme threshold is €100k even though the file used is 'above 20k'.
    assert "100,000" in regime_for("ie_hse", "state_body")["regime_note"]


def test_utilities_are_contracting_entities_outside_the_scheme():
    # The original misleading conflation, locked: ESB/ESB Networks/EirGrid/Uisce are NOT
    # contracting authorities and NOT under the FOI €20k scheme.
    for pid in ["ie_esb", "ie_esbnetworks", "ie_eirgrid", "ie_gni", "ie_uisce", "ie_daa"]:
        r = regime_for(pid, "semi_state")
        assert r["disclosure_basis"] == "utilities_regime", pid
        assert r["body_procurement_class"] == "contracting_entity_utility", pid


def test_commercial_state_bodies_publish_voluntarily():
    assert regime_for("ie_rte", "semi_state")["body_procurement_class"] == "commercial_state"
    assert regime_for("ie_rte", "semi_state")["disclosure_basis"] == "voluntary"


def test_aie_only_bodies():
    for pid in ["ie_bnm", "ie_coillte"]:
        assert regime_for(pid, "semi_state")["disclosure_basis"] == "aie_only"


# --------------------------------------------------------------------------- vocab integrity
def test_every_override_uses_valid_vocab():
    for pid in _OVERRIDES:
        r = regime_for(pid, "semi_state")
        assert r["disclosure_basis"] in DISCLOSURE_BASIS, pid
        assert r["body_procurement_class"] in BODY_PROCUREMENT_CLASS, pid
        assert r["disclosure_threshold_eur"] in {20000, 25000, 100000}, pid
        assert r["threshold_vat"] in {"incl_vat", "excl_vat", "unknown"}, pid


def test_labels_cover_every_vocab_value():
    assert set(BASIS_LABEL) == DISCLOSURE_BASIS
    assert set(CLASS_LABEL) == BODY_PROCUREMENT_CLASS


def test_utilities_regime_label_signals_outside_foi():
    assert "Utilities" in BASIS_LABEL["utilities_regime"]
    assert CLASS_LABEL["contracting_entity_utility"] != CLASS_LABEL["contracting_authority"]
