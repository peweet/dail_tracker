"""Single source of truth for a public-money publisher's DISCLOSURE REGIME.

WHY THIS EXISTS
---------------
The over-€20,000 PO/payments corpus mixes publishers that fall under *different*
publication obligations, with *different* thresholds and *different* VAT bases — but the
data (and the UI copy) used to treat them all as one regime ("Circular 07/2012 / €20,000"),
which is misleading:

  * The €20k requirement originated in Circular FIN 07/2012 and is now carried by the
    FOI Act 2014 s.8 *model publication scheme* — but the HSE's model threshold is €100,000,
    and Children's Health Ireland publishes paid invoices over €25,000 incl-VAT, not €20k.
  * Commercial state bodies (RTÉ) publish *voluntarily*, not under that scheme.
  * Utilities (ESB, ESB Networks, EirGrid, Uisce Éireann) are "contracting ENTITIES" under
    the EU Utilities Directive (2014/25), NOT public-sector "contracting authorities"
    (2014/24) — a different legal class, and they are outside the €20k FOI scheme.

So a body is NOT well-described by one blanket label. This module attaches four structured
fields, keyed by publisher_id, so the gold fact is self-describing and the UI renders each
body's *actual* basis + threshold rather than a hard-coded string. New publishers inherit a
sensible default by ``publisher_type`` and only need an override when they genuinely differ.

It is imported by BOTH the consolidator (to backfill gold without re-fetching) and the
extractor cfg (so future runs bake the fields into silver). One registry, no drift.
"""

from __future__ import annotations

# --- controlled vocabularies ---------------------------------------------------------------
DISCLOSURE_BASIS = {
    "foi_s8_model_scheme",  # FOI Act 2014 s.8 model publication scheme (origin: Circular FIN 07/12)
    "circular_fin_0712",  # explicitly cited Circular FIN 07/2012 (departments/agencies, pre-2014)
    "voluntary",  # body publishes a list with no statutory obligation (commercial state bodies)
    "utilities_regime",  # EU Utilities Directive 2014/25 contracting entity; publishes voluntarily
    "aie_only",  # data reachable only via Access to Information on the Environment requests
}
BODY_PROCUREMENT_CLASS = {
    "contracting_authority",  # public-sector body, EU Dir. 2014/24 (departments, councils, agencies)
    "contracting_entity_utility",  # utility, EU Dir. 2014/25 (ESB, EirGrid, Uisce Éireann, daa, ports)
    "commercial_state",  # commercial state company outside the €20k scheme (RTÉ, Coillte)
    "foi_body",  # an FOI body that isn't itself a contracting authority (rare)
}

# Default threshold / VAT for the €20k FOI model scheme.
_DEFAULT_BASIS = "foi_s8_model_scheme"
_DEFAULT_THRESHOLD_EUR = 20000
_DEFAULT_VAT = "unknown"

# Default body class by publisher_type. Bodies "governed by public law" (departments, councils,
# agencies, universities, voluntary/Section-38 hospitals, non-commercial state bodies) are all
# contracting AUTHORITIES. semi_state defaults to contracting_authority (the non-commercial ones —
# Teagasc, Bord Bia, BIM, Marine, Enterprise Ireland, IDA, Fáilte); the commercial utilities /
# broadcasters among them are flipped by an explicit override below.
_CLASS_BY_TYPE = {
    "local_authority": "contracting_authority",
    "department": "contracting_authority",
    "agency": "contracting_authority",
    "state_body": "contracting_authority",
    "education_body": "contracting_authority",
    "hospital": "contracting_authority",
    "semi_state": "contracting_authority",
}

# Per-publisher OVERRIDES — only where a body differs from the type default. Each value may set any
# of: basis / threshold_eur / vat / body_class / note. Everything unspecified falls back to the
# default. Keep this list grounded in the body's OWN published page (the threshold/VAT it states).
_OVERRIDES: dict[str, dict] = {
    # --- thresholds / VAT that differ from the €20k incl/excl default ----------------------
    "ie_chi": {
        "threshold_eur": 25000,
        "vat": "incl_vat",
        "note": "Publishes PAID invoices over €25,000 incl-VAT (operator side of the children's hospital).",
    },
    "ie_hse": {
        "vat": "incl_vat",
        "note": "HSE model-scheme threshold is €100,000; the file ingested here is the 'PO payments above €20,000' return.",
    },
    "ie_tusla": {"vat": "incl_vat"},
    "ie_prisons": {
        "vat": "incl_vat",
        "note": "Annual (not quarterly) PO list, incl-VAT; security redactions possible.",
    },
    "ie_setu": {"vat": "incl_vat"},
    # --- utilities: contracting ENTITIES (Dir. 2014/25), outside the €20k FOI scheme -------
    "ie_esb": {
        "basis": "utilities_regime",
        "body_class": "contracting_entity_utility",
        "note": "Commercial electricity utility; publishes voluntarily, not under the FOI €20k scheme.",
    },
    "ie_esbnetworks": {
        "basis": "utilities_regime",
        "body_class": "contracting_entity_utility",
        "note": "Regulated network utility; publication-scheme financial-information page (voluntary).",
    },
    "ie_eirgrid": {"basis": "utilities_regime", "body_class": "contracting_entity_utility"},
    "ie_gni": {"basis": "utilities_regime", "body_class": "contracting_entity_utility"},
    "ie_uisce": {"basis": "utilities_regime", "body_class": "contracting_entity_utility"},
    "ie_daa": {"basis": "utilities_regime", "body_class": "contracting_entity_utility"},
    "ie_dublinport": {"basis": "utilities_regime", "body_class": "contracting_entity_utility"},
    "ie_shannonfoynes": {"basis": "utilities_regime", "body_class": "contracting_entity_utility"},
    "ie_portofcork": {"basis": "utilities_regime", "body_class": "contracting_entity_utility"},
    # --- commercial state companies publishing voluntarily / via AIE -----------------------
    "ie_rte": {"basis": "voluntary", "body_class": "commercial_state"},
    "ie_tg4": {"basis": "voluntary", "body_class": "commercial_state"},
    "ie_bnm": {
        "basis": "aie_only",
        "body_class": "commercial_state",
        "note": "Bord na Móna — financial detail reachable mainly via AIE/OCEI, not a clean PO list.",
    },
    "ie_coillte": {
        "basis": "aie_only",
        "body_class": "commercial_state",
        "note": "Coillte — financial detail reachable mainly via AIE/OCEI, not a clean PO list.",
    },
}


def regime_for(publisher_id: str | None, publisher_type: str | None = None) -> dict:
    """Return the 4-field disclosure regime for a publisher.

    Keys: ``disclosure_basis``, ``disclosure_threshold_eur``, ``threshold_vat``,
    ``body_procurement_class`` (+ ``regime_note`` free text, may be "").
    Falls back to the €20k FOI model-scheme default + the publisher_type body class.
    """
    ov = _OVERRIDES.get(publisher_id or "", {})
    body_class = ov.get("body_class") or _CLASS_BY_TYPE.get(publisher_type or "", "contracting_authority")
    return {
        "disclosure_basis": ov.get("basis", _DEFAULT_BASIS),
        "disclosure_threshold_eur": int(ov.get("threshold_eur", _DEFAULT_THRESHOLD_EUR)),
        "threshold_vat": ov.get("vat", _DEFAULT_VAT),
        "body_procurement_class": body_class,
        "regime_note": ov.get("note", ""),
    }


# Human-readable labels for the UI / methodology copy (kept here so the vocab has ONE home).
BASIS_LABEL = {
    "foi_s8_model_scheme": "FOI Act 2014 s.8 model publication scheme (origin: Circular FIN 07/12)",
    "circular_fin_0712": "Circular FIN 07/2012",
    "voluntary": "voluntary publication scheme",
    "utilities_regime": "EU Utilities Directive (2014/25) — published voluntarily",
    "aie_only": "Access to Information on the Environment (AIE)",
}
CLASS_LABEL = {
    "contracting_authority": "contracting authority",
    "contracting_entity_utility": "utility (contracting entity)",
    "commercial_state": "commercial state body",
    "foi_body": "FOI body",
}
