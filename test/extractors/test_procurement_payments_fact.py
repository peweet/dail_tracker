"""Gold-quality contract for the consolidated public-body payment fact
(extractors/procurement_payments_consolidate.py → procurement_payments_fact.parquet).

Skips if the gold parquet hasn't been built. Guards the invariants the page relies on:
the canonical 2-axis taxonomy is present, tiers map correctly, VAT bases are tagged, and the
owner privacy decision (suppliers named, incl. individuals) holds with no address/PII column.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[2]
FACT = ROOT / "data/gold/parquet/procurement_payments_fact.parquet"


@pytest.fixture(scope="module")
def con():
    if not FACT.exists():
        pytest.skip("procurement_payments_fact.parquet not built")
    c = duckdb.connect()
    yield c
    c.close()


def _q(con, sql):
    return con.execute(sql.replace("FACT", f"read_parquet('{FACT.as_posix()}')")).fetchall()


def test_canonical_taxonomy_present(con):
    cols = {r[0] for r in _q(con, "DESCRIBE SELECT * FROM FACT")}
    assert {"value_kind", "realisation_tier", "vat_status"}.issubset(cols)


def test_tier_mapping_is_consistent(con):
    # Every payment_actual row is SPENT; every po_committed row is COMMITTED. No blend.
    bad = _q(
        con,
        "SELECT COUNT(*) FROM FACT WHERE (value_kind='payment_actual' AND realisation_tier<>'SPENT')"
        " OR (value_kind='po_committed' AND realisation_tier<>'COMMITTED')",
    )[0][0]
    assert bad == 0


def test_vat_inclusive_publishers_tagged(con):
    # HSE / Tusla are VAT-inclusive; the rest are left 'unknown' (never falsely 'exclusive').
    hse = _q(con, "SELECT DISTINCT vat_status FROM FACT WHERE publisher_name LIKE '%Health Service%'")
    assert hse == [("incl_vat",)]
    assert {"incl_vat", "unknown"}.issuperset({r[0] for r in _q(con, "SELECT DISTINCT vat_status FROM FACT")})


def test_suppliers_named_including_individuals(con):
    # Owner decision: individuals/sole traders ARE named (published-source data).
    n = _q(con, "SELECT COUNT(*) FROM FACT WHERE supplier_class='sole_trader_or_individual'")[0][0]
    assert n > 0  # they are present, not quarantined out


def test_no_address_or_pii_column(con):
    # Guardrail: only published name/amount/description — never an address/PII field.
    cols = {r[0].lower() for r in _q(con, "DESCRIBE SELECT * FROM FACT")}
    assert not (cols & {"address", "supplier_address", "eircode", "ppsn", "dob", "home_address"})


def test_public_body_transfers_mostly_excluded_from_sum(con):
    # Intergovernmental transfers (public_body recipients) must not inflate spend totals:
    # the overwhelming majority are value_safe_to_sum = false.
    safe, total = _q(
        con,
        "SELECT COUNT(*) FILTER (WHERE value_safe_to_sum), COUNT(*) FROM FACT WHERE supplier_class='public_body'",
    )[0]
    assert total > 0
    assert safe / total < 0.05  # <5% leak (the rest correctly non-summable)


def test_org_form_firms_reclassified_to_company(con):
    # The consolidation reclassifies suffix-less firms (Bros / & Sons / Solicitors / Partners …)
    # that the source name-suffix regex mis-binned as sole traders. None of those org-form names
    # may remain a sole trader (they are firms, not private individuals).
    stragglers = _q(
        con,
        r"SELECT COUNT(*) FROM FACT WHERE supplier_class='sole_trader_or_individual'"
        r" AND regexp_matches(supplier_normalised, '\b(BROS|BROTHERS|SOLICITORS|PARTNERS|CONTRACTORS)\b')",
    )[0][0]
    assert stragglers == 0


def test_cro_matched_rows_are_company_class(con):
    # A CRO company-number match is only ever attached to a company-class row: the reclassifier
    # upgrades any sole-trader with an exact CRO match BEFORE the fact is written, so no
    # sole_trader_or_individual row may carry a cro_company_num.
    leaked = _q(
        con,
        "SELECT COUNT(*) FROM FACT WHERE cro_company_num IS NOT NULL AND supplier_class='sole_trader_or_individual'",
    )[0][0]
    assert leaked == 0


def test_reclassified_companies_are_displayable(con):
    # Privacy invariant after reclassification: every row that is now company-class is displayable
    # and never carries the personal-data review flag (the upgrade re-derives both).
    bad = _q(
        con,
        "SELECT COUNT(*) FROM FACT WHERE supplier_class='company'"
        " AND (NOT public_display OR privacy_status='review_personal_data')",
    )[0][0]
    assert bad == 0


def test_leading_ref_prefixes_mostly_stripped(con):
    # The council parsers bled a row index / date / PO number into the supplier name
    # ("36 Ward Bros Plant Hire"), fragmenting one firm across many keys. The cleaner strips them,
    # so a normalised key beginning with a plain row-index-then-name is gone (only fused
    # number-brands like "2CQR" / pure-number junk rows may still start with a digit).
    bad = _q(
        con,
        # a digit-run followed by a SPACE then a letter = an un-stripped row-index prefix
        r"SELECT COUNT(DISTINCT supplier_normalised) FROM FACT"
        r" WHERE regexp_matches(supplier_normalised, '^[0-9]{1,4} [A-Z]')",
    )[0][0]
    assert bad <= 20  # a tiny residue of punctuation-separated markers is tolerated


# --------------------------------------------------------------------------- #
# Keystone sum-safe contract (2026-06-13). The project's #1 hazard is summing the
# wrong euros. These assert the value_safe_to_sum flag means exactly what every view/page
# relies on: an identifiable, single-tier, positive, non-transfer spend line. The
# consolidation enforces these at the fold (defense-in-depth); if any regress, a view
# total silently inflates — so they are hard ==0 assertions, not tolerances.
# --------------------------------------------------------------------------- #
def test_sum_safe_rows_have_identifiable_supplier(con):
    # A row whose supplier normalised to empty (category/subtotal lines, "& COMPANY",
    # "(IRELAND) LTD") is never identifiable spend and must not be summable. Caught the
    # 53 stale LA rows / €8.7m and the OPW €155.8m blank-supplier total-row (DQ 2026-06-13).
    bad = _q(
        con,
        "SELECT COUNT(*) FROM FACT WHERE value_safe_to_sum"
        " AND (supplier_normalised IS NULL OR TRIM(supplier_normalised) = '')",
    )[0][0]
    assert bad == 0


def test_no_public_body_recipient_is_sum_safe(con):
    # Intergovernmental transfers (public_body recipient) are never summable — strict ==0
    # at the fold (the broader <5% guard above documents the pre-fold source state).
    bad = _q(con, "SELECT COUNT(*) FROM FACT WHERE value_safe_to_sum AND supplier_class='public_body'")[0][0]
    assert bad == 0


def test_sum_safe_rows_are_single_known_tier(con):
    # Every summable row sits in exactly one summable lifecycle tier. A sum-safe row with an
    # UNKNOWN/other tier would let a view blend lifecycle stages (the OCDS 'never sum across
    # stages' rule). value_kind and realisation_tier must agree on a summable pair.
    bad = _q(
        con,
        "SELECT COUNT(*) FROM FACT WHERE value_safe_to_sum AND ("
        " realisation_tier NOT IN ('SPENT','COMMITTED')"
        " OR value_kind NOT IN ('payment_actual','po_committed'))",
    )[0][0]
    assert bad == 0


def test_sum_safe_rows_have_positive_amount(con):
    bad = _q(con, "SELECT COUNT(*) FROM FACT WHERE value_safe_to_sum AND (amount_eur IS NULL OR amount_eur <= 0)")[0][0]
    assert bad == 0


def test_core_money_flow_bodies_present_and_visible(con):
    # Regression guard for the 2026-06-13 ingest: a partial extractor run once silently dropped
    # 12 bodies (incl. Dept Defence ~€1.1bn, Dept Climate ~€1.3bn) and omitted the central
    # departments from gold. Each of these must be present AND have public-displayable summable
    # spend, so a future partial run / listing-rot can never make them vanish from the citizen view.
    required = [
        "dept_defence",
        "dept_climate",
        "dept_culture",
        "dept_social_protection",
        "dept_health",
        "dept_education",
        "ie_beaumont",
    ]
    present = {
        r[0]
        for r in _q(
            con, "SELECT publisher_id FROM FACT WHERE value_safe_to_sum AND public_display GROUP BY publisher_id"
        )
    }
    missing = [p for p in required if p not in present]
    assert not missing, f"core money-flow bodies absent or fully non-displayable in gold: {missing}"


# --------------------------------------------------------------------------- #
# spend_category contract (2026-06-13). A source-grounded category derived ONLY from the
# publisher's published `description` (canonicalised for truncation/casing) — never invented.
# Guards that it stays verifiable: every category traces to a published description, carries no
# leaked amounts, and the canonicaliser is a stable pure function.
# --------------------------------------------------------------------------- #
def test_spend_category_present(con):
    cols = {r[0] for r in _q(con, "DESCRIBE SELECT * FROM FACT")}
    assert "spend_category" in cols


def test_spend_category_only_from_published_description(con):
    # No-inference guard: a category may NEVER exist on a row with no source description — it is the
    # department's own words, not a derived label.
    invented = _q(
        con,
        "SELECT COUNT(*) FROM FACT WHERE spend_category IS NOT NULL AND (description IS NULL OR TRIM(description)='')",
    )[0][0]
    assert invented == 0


def test_spend_category_has_no_leaked_amounts(con):
    # The canonicaliser strips amounts/€ that some publishers (Education, councils) bled into the
    # description column. A tiny residue of legitimate digit-leading names ("24/7", "3D") is tolerated.
    money = _q(
        con,
        r"SELECT COUNT(DISTINCT spend_category) FROM FACT"
        r" WHERE spend_category IS NOT NULL AND regexp_matches(spend_category, '^€|^[0-9][0-9.,]*([ €]|$)')",
    )[0][0]
    assert money <= 20


def test_spend_category_is_letters_only_when_present(con):
    # Every non-null category contains at least one letter (a pure-number/symbol residue is nulled).
    bad = _q(
        con,
        r"SELECT COUNT(*) FROM FACT WHERE spend_category IS NOT NULL AND NOT regexp_matches(spend_category, '[A-Za-z]')",
    )[0][0]
    assert bad == 0


def test_category_supplier_drill_reconciles(con):
    # The transparent drill v_payments_category_suppliers must account for EVERY euro the category
    # rollup v_payments_by_category reports — i.e. summing the vendor rows per (category × tier)
    # equals the category total. A gap would mean a vendor (and its money) silently dropped.
    from dail_tracker_core.db import connect_with_views

    c = connect_with_views(["procurement_payments_by_category.sql"])
    gap = c.execute(
        """
        WITH cat AS (SELECT spend_category, realisation_tier, total_safe_eur FROM v_payments_by_category),
             sup AS (SELECT spend_category, realisation_tier, SUM(total_safe_eur) AS t
                     FROM v_payments_category_suppliers GROUP BY 1, 2)
        SELECT COUNT(*) FROM cat JOIN sup USING (spend_category, realisation_tier)
        WHERE abs(cat.total_safe_eur - sup.t) > 1.0
        """
    ).fetchone()[0]
    # Categories where the two differ should only be those with NO named-supplier rows excluded by the
    # supplier view's non-blank filter; in practice the rollup also excludes blank suppliers from the
    # safe sum, so they must reconcile to the euro.
    assert gap == 0


def test_canon_spend_category_unit():
    # Pure-function contract: department's exact words, canonicalised ONLY for truncation + casing.
    from extractors.procurement_payments_consolidate import canon_spend_category as c

    # leaked leading amount / bare € dropped:
    assert c("€80,000,000.00 Third Level Building and Infrastructure") == "Third Level Building and Infrastructure"
    assert c("€ Construction Costs") == "Construction Costs"
    # trailing dangling truncation tail dropped (connectors/punctuation only — never content words):
    assert c("Ukraine Accommodation and/or") == "Ukraine Accommodation"
    assert c("Asylum Seeker Accommodation,") == "Asylum Seeker Accommodation"
    # casing normalised but acronyms preserved (so "IT software"/"IT Software" merge):
    assert c("IT software") == "IT Software"
    assert c("IM&T Maintenance and Support") == "IM&T Maintenance and Support"
    assert c("SUPPORT AND MAINTENANCE (I.T.)") == "Support and Maintenance (I.T.)"
    assert c("PASSPORT BOOKLETS") == "Passport Booklets"
    # content words are NEVER stripped (no semantic re-grouping):
    assert c("ICT Services") == "ICT Services"
    # no-letter residue / empty -> None (not a purpose label):
    assert c("0 0 0") is None
    assert c("") is None
    assert c(None) is None


# --------------------------------------------------------------------------- #
# Disclosure-regime contract (2026-06-19). The corpus mixes publishers under DIFFERENT
# publication obligations / thresholds; treating it as one "€20,000 / Circular 07/2012 /
# contracting authority" regime is misleading. These assert every row is self-describing
# (basis + threshold + VAT + legal class) so the page can render each body's real regime.
# --------------------------------------------------------------------------- #
def test_disclosure_regime_columns_present(con):
    cols = {r[0] for r in _q(con, "DESCRIBE SELECT * FROM FACT")}
    assert {
        "disclosure_basis",
        "disclosure_threshold_eur",
        "threshold_vat",
        "body_procurement_class",
    }.issubset(cols)


def test_every_row_has_a_regime(con):
    # No row may be missing its basis / threshold / class — the whole point is self-description.
    bad = _q(
        con,
        "SELECT COUNT(*) FROM FACT WHERE disclosure_basis IS NULL"
        " OR disclosure_threshold_eur IS NULL OR body_procurement_class IS NULL",
    )[0][0]
    assert bad == 0


def test_regime_values_are_in_vocab(con):
    from extractors._publisher_regime import BODY_PROCUREMENT_CLASS, DISCLOSURE_BASIS

    bases = {r[0] for r in _q(con, "SELECT DISTINCT disclosure_basis FROM FACT")}
    classes = {r[0] for r in _q(con, "SELECT DISTINCT body_procurement_class FROM FACT")}
    thresholds = {r[0] for r in _q(con, "SELECT DISTINCT disclosure_threshold_eur FROM FACT")}
    assert bases.issubset(DISCLOSURE_BASIS), f"unknown basis: {bases - DISCLOSURE_BASIS}"
    assert classes.issubset(BODY_PROCUREMENT_CLASS), f"unknown class: {classes - BODY_PROCUREMENT_CLASS}"
    assert thresholds.issubset({20000, 25000, 100000}), f"unexpected threshold: {thresholds}"


def test_utilities_are_not_labelled_contracting_authorities(con):
    # The original misleading conflation: a utility (ESB Networks, a "contracting ENTITY" under the
    # EU Utilities Directive, outside the €20k FOI scheme) must NEVER be class contracting_authority.
    bad = _q(
        con,
        "SELECT COUNT(*) FROM FACT WHERE disclosure_basis='utilities_regime'"
        " AND body_procurement_class<>'contracting_entity_utility'",
    )[0][0]
    assert bad == 0


def test_per_body_thresholds_are_honoured(con):
    # CHI publishes over €25,000 (not €20k); guards that a real per-body threshold survives to gold
    # rather than being flattened to a blanket €20,000.
    chi = _q(con, "SELECT DISTINCT disclosure_threshold_eur FROM FACT WHERE publisher_id='ie_chi'")
    if chi:  # only assert when CHI is in the build
        assert chi == [(25000,)]


def test_strip_leading_ref_unit():
    # Pure-function contract: bled-in leading references are removed; fused number-brands and
    # whitespace-less names are preserved; a pure-number / empty residue is left untouched.
    from extractors.procurement_payments_consolidate import _strip_leading_ref as s

    assert s("36 Ward Bros Plant Hire") == "Ward Bros Plant Hire"
    assert s("03-607 O'Shaugnessy and Associates") == "O'Shaugnessy and Associates"
    assert s("2.4E+08 349849 Patrick Mc Caffrey & Sons Ltd") == "Patrick Mc Caffrey & Sons Ltd"
    assert s("02/08/2023 Martin Heffernan & Associates") == "Martin Heffernan & Associates"
    assert s("#### M H Associates Ltd") == "M H Associates Ltd"
    # Preserved: fused number-brands (no whitespace after the digits) and clean names.
    assert s("3M Ireland") == "3M Ireland"
    assert s("247meeting (Ireland) Ltd") == "247meeting (Ireland) Ltd"
    assert s("2CQR Limited") == "2CQR Limited"
    assert s("AECOM Ireland Ltd") == "AECOM Ireland Ltd"
    # ETB accounting-code prefix (LOETB: 3 digits + 2 letters, optional -seq) IS stripped …
    assert s("020OF 348 DOWNEYS AUTO STOP") == "DOWNEYS AUTO STOP"
    assert s("143AP 39 MIDLAND ENERGY") == "MIDLAND ENERGY"
    assert s("020IT-213 PFH Technology") == "PFH Technology"
    # … but a real 2-digit-prefixed NAME is never mistaken for that code (regression: 24HR was
    # over-stripped to "CARE SERVICES" and wrongly merged with other CARE SERVICES suppliers).
    assert s("24HR CARE SERVICES") == "24HR CARE SERVICES"
    # Untouched: a pure-number / no-name residue is not emptied out.
    assert s("100") == "100"
    assert s("56,143.35") == "56,143.35"
    assert s(None) is None
