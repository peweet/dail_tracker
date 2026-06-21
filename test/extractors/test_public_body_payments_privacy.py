"""Privacy-quarantine tests for the public-body payments sandbox fact.

Locks ``extractors/procurement_public_body_extract.py:classify_and_flag()`` — the
function that derives ``supplier_class`` / ``privacy_status`` / ``public_display`` for
``data/silver/parquet/public_payments_fact.parquet``. This fact is a gold-CANDIDATE one
promotion away from a procurement UI; if a sole-trader / individual supplier (personal
data) were left ``public_display=True`` it would be exposed on promotion (synthesis INC-4).

Invariant under test: NO ``public_display=True`` row may be ``sole_trader_or_individual``.
Classification errs toward over-quarantine (an org without a recognised company suffix is
treated as personal) — the privacy-safe direction. Supplier names below are invented.

Run:  pytest test/test_public_body_payments_privacy.py -v
"""

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT / "extractors"))

pl = pytest.importorskip("polars")
from procurement_public_body_extract import (  # noqa: E402
    DEDUP_SIG,
    canonicalise_supplier_raw,
    classify_and_flag,
    dedup_source_repeats,
    detect_roles_tab,
    flag_unidentifiable_suppliers,
    period_from_url,
)


# ----------------------------------------------------------------------------------------
# detect_roles_tab: a "Payment Type"/"Order Type" category column is claimed by the `paid`
# role (its regex includes "payment type") while `description` — which has no "type" keyword —
# is left empty, discarding the published category text. When that column holds free-text
# categories (not a Y/N flag), promote it to description (regression for 2026-06-13 fix).
# ----------------------------------------------------------------------------------------
def test_payment_type_category_column_maps_to_description():
    header = ["TRANSACTION", "SUPPLIER", "PAYMENT TYPE", "PAYMENT TOTAL"]
    rows = [
        ["0040007232", "PJ Hegarty", "Building Maintenance", "23836.36"],
        ["0040007234", "Acme Ltd", "Roofworks", "24970"],
        ["0040007240", "Beta Ltd", "Surveying Services", "28335.82"],
        ["0040007252", "Gamma Ltd", "Software", "74953.2"],
    ]
    roles = detect_roles_tab(header, rows)
    assert roles["description"] == 2, "PAYMENT TYPE category column must become description"
    assert roles["amount"] == 3
    assert roles["supplier"] == 1
    assert roles["po"] == 0
    assert roles["paid"] is None, "category text must not be left in the paid-flag role"


def test_genuine_paid_flag_is_not_promoted_to_description():
    # A real Paid Y/N flag (≤2 distinct values) must stay a paid flag, not become description.
    header = ["Reference", "Payee Name", "Tran Value", "Paid Y/N"]
    rows = [
        ["R1", "Acme Ltd", "21000", "Y"],
        ["R2", "Beta Ltd", "33000", "N"],
        ["R3", "Gamma Ltd", "45000", "Y"],
    ]
    roles = detect_roles_tab(header, rows)
    assert roles["paid"] == 3
    assert roles["description"] is None


def _flag(suppliers):
    df = pl.DataFrame(
        {
            "supplier_raw": suppliers,
            "amount_eur": [100_000.0] * len(suppliers),
            "amount_semantics": ["payment_actual"] * len(suppliers),
        }
    )
    return classify_and_flag(df)


# The consolidate reclassifier is the uniform last-gate net (catches inflections the source
# may miss, and folds the LA fact through the same rules). Unit-test its firm-indicator regex.
from procurement_payments_consolidate import _reclassify_missed_companies  # noqa: E402


def _reclass(specs):
    """specs: list of (supplier_normalised, cro_company_num) -> a frame starting all-sole-trader."""
    return _reclassify_missed_companies(
        pl.DataFrame(
            {
                "supplier_normalised": [s for s, _ in specs],
                "cro_company_num": [c for _, c in specs],
                "cro_company_status": [None] * len(specs),
                "supplier_class": ["sole_trader_or_individual"] * len(specs),
                "privacy_status": ["review_personal_data"] * len(specs),
                "public_display": [False] * len(specs),
            }
        )
    )


def test_reclassifier_upgrades_firm_words_and_cro_but_not_people():
    out = _reclass(
        [
            ("arup consulting engineers", None),  # activity stem (inflection) -> company
            ("alliance medical", None),  # activity word -> company
            ("mazars", "123456"),  # no firm word but CRO match (≥5 chars) -> company
            ("mary obrien", None),  # bare person, no signal -> stays sole trader
        ]
    ).sort("supplier_normalised")
    by = {r["supplier_normalised"]: r for r in out.iter_rows(named=True)}
    assert by["arup consulting engineers"]["supplier_class"] == "company"
    assert by["alliance medical"]["supplier_class"] == "company"
    assert by["mazars"]["supplier_class"] == "company"
    assert by["mary obrien"]["supplier_class"] == "sole_trader_or_individual"
    # upgraded rows become displayable; the person stays quarantined
    assert by["arup consulting engineers"]["public_display"] is True
    assert by["mary obrien"]["public_display"] is False


@pytest.mark.parametrize(
    "name,expect",
    [
        # truncation-tolerant legal/activity forms (source column-width cut the word)
        ("eamonn costello kerry limite", "company"),  # "LIMITED" cut to "LIMITE"
        ("joseph mcmenamin son con l", "company"),  # "& SON" whole-word
        ("ganson building civil engine", "company"),  # "ENGINEERING" cut to "ENGINE"
        ("o sheas builders", "company"),  # "BUILD" stem
        ("stewart tracey joint venture", "company"),  # "VENTUR" stem
        # foreign legal forms (END-anchored so "as"/"sa" can't match mid-name)
        ("novavax cz as", "company"),  # Norwegian/Czech AS
        ("seqirus netherlands b v", "company"),  # Dutch B.V. -> "b v"
        ("defence aerospace as", "company"),  # Kongsberg AS
        # privacy: people / mid-name collisions must NOT upgrade
        ("sonia murphy", "sole_trader_or_individual"),  # "son" must be whole-word, not a stem
        ("mary obrien", "sole_trader_or_individual"),
        ("as roofing limerick", "sole_trader_or_individual"),  # "as" only matches at END
    ],
)
def test_reclassifier_truncation_and_foreign_forms(name, expect):
    out = _reclass([(name, None)])
    assert out.row(0, named=True)["supplier_class"] == expect, name


import re as _re  # noqa: E402

from procurement_payments_consolidate import (  # noqa: E402
    _JUNK_SUPPLIER_RE,
    _classify_id_codes,
    _conform,
    _surface_sole_trader_contractors,
)


def _surface(rows):
    """rows: list of (supplier_normalised, spend_category) -> all start hidden sole_trader_or_individual."""
    df = pl.DataFrame(
        {
            "supplier_normalised": [s for s, _ in rows],
            "spend_category": [c for _, c in rows],
            "description": [c for _, c in rows],
            "amount_eur": [50_000.0] * len(rows),
            "supplier_class": ["sole_trader_or_individual"] * len(rows),
            "privacy_status": ["review_personal_data"] * len(rows),
            "public_display": [False] * len(rows),
        }
    )
    return {
        r["supplier_normalised"]: r
        for r in _surface_sole_trader_contractors(df).unique(subset=["supplier_normalised"]).iter_rows(named=True)
    }


def test_commercial_sole_trader_is_surfaced_private_stays_hidden():
    out = _surface(
        [
            ("terry rea", "Minor Contracts Trade Services"),  # commercial -> surface
            ("noel cunningham", "Roofworks"),  # commercial -> surface
            ("enda ocarroll", "House Purchase"),  # property -> hidden
            ("aoife buckley", "Croi Conaithe Top Up Grant"),  # grant -> hidden
            ("stephen lambe", "Land Purchase Compensation"),  # CPO -> hidden
            ("john noname", None),  # uncategorised -> hidden
        ]
    )
    assert out["terry rea"]["supplier_class"] == "sole_trader"
    assert out["terry rea"]["public_display"] is True
    assert out["noel cunningham"]["supplier_class"] == "sole_trader"
    for hidden in ("enda ocarroll", "aoife buckley", "stephen lambe", "john noname"):
        assert out[hidden]["supplier_class"] == "sole_trader_or_individual", hidden
        assert out[hidden]["public_display"] is False


def test_any_private_payment_keeps_whole_supplier_hidden():
    # A contractor who ALSO sold land under CPO must stay hidden — any private row vetoes surfacing.
    df = pl.DataFrame(
        {
            "supplier_normalised": ["sean moore", "sean moore"],
            "spend_category": ["Road Construction", "Rent Johns Green House"],
            "description": ["Road Construction", "Rent"],
            "amount_eur": [50_000.0, 50_000.0],
            "supplier_class": ["sole_trader_or_individual"] * 2,
            "privacy_status": ["review_personal_data"] * 2,
            "public_display": [False] * 2,
        }
    )
    out = _surface_sole_trader_contractors(df)
    assert out["supplier_class"].unique().to_list() == ["sole_trader_or_individual"]
    assert not out["public_display"].any()


def test_id_codes_become_id_code_class_hidden():
    df = pl.DataFrame(
        {
            "supplier_normalised": ["JOH260ZZ", "DUG001ZZ", "ARUP", "MARY OBRIEN"],
            "supplier_class": ["sole_trader_or_individual"] * 4,
            "privacy_status": ["review_personal_data"] * 4,
            "public_display": [False] * 4,
        }
    )
    out = {r["supplier_normalised"]: r for r in _classify_id_codes(df).iter_rows(named=True)}
    assert out["JOH260ZZ"]["supplier_class"] == "id_code"
    assert out["JOH260ZZ"]["public_display"] is False  # anonymised code stays hidden
    assert out["DUG001ZZ"]["supplier_class"] == "id_code"
    assert out["ARUP"]["supplier_class"] == "sole_trader_or_individual"  # a real name is untouched
    assert out["MARY OBRIEN"]["supplier_class"] == "sole_trader_or_individual"


@pytest.mark.parametrize(
    "name,is_junk",
    [
        ("PURCHASE ORDERS OVER", True),
        ("NOTICE ON PUBLICATION PURCHASE ORDERS OVER", True),
        ("SLIGO CO COUNCIL PURCHASE ORDERS OVER 20000", True),
        # NOT junk: real spend to an un-named/aggregate vendor must STAY summable (no understatement)
        ("IT SERVICE PROVIDER", False),
        ("SUNDRY SUPPLIER", False),
        ("KILKENNY ABBEY QUARTER DEVELOPMENT PARTNERSHIP", False),  # "quarter" is a place, not a header
        ("ARUP", False),
    ],
)
def test_junk_supplier_regex_is_page_furniture_only(name, is_junk):
    assert bool(_re.search(_JUNK_SUPPLIER_RE, name)) is is_junk, name


from procurement_payments_consolidate import (  # noqa: E402
    _canonicalise_split_entities,
    _clean_supplier_names,
)


def test_split_entity_variants_merge_to_one_key():
    # Airbus Defence & Space SAU is published 3 ways by Dept of Defence; all must collapse to one
    # name so the firm stops fragmenting (and the merged name ends in the SAU foreign form).
    df = pl.DataFrame(
        {
            "publisher_name": ["Department of Defence"] * 3 + ["Other Body"],
            "supplier_raw": [
                "AIRBUS DEFENCE & SPACE SAU SPAIN",
                "DEFENCE & SPACE SAU SPAIN",
                "& SPACE SAU SPAIN",
                "Space Cadets Ltd",  # unrelated firm, different body -> untouched
            ],
        }
    )
    out = _canonicalise_split_entities(df)["supplier_raw"].to_list()
    assert out[:3] == ["Airbus Defence and Space SAU"] * 3, out
    assert out[3] == "Space Cadets Ltd"


@pytest.mark.parametrize(
    "name,expect",
    [
        ("airbus defence space sau", "company"),  # ends in SAU
        ("novartis pharma gmbh germany", "company"),  # foreign form + trailing country
        ("carroceros s l", "company"),  # Spanish SL
        ("seqirus netherlands b v", "company"),
        ("bavarian nordic a s", "company"),  # Danish A/S -> "a s"
        ("sasta builders portugal", "company"),  # "build" stem (not the country)
        ("john sasportas", "sole_trader_or_individual"),  # "sa" must not match mid-name
    ],
)
def test_foreign_form_with_trailing_country(name, expect):
    assert _reclass([(name, None)]).row(0, named=True)["supplier_class"] == expect, name


@pytest.mark.parametrize(
    "name,expect",
    [
        ("premier recruitment int", "company"),  # "recruit" stem
        ("harrington concrete quarries ulc", "company"),  # Irish Unlimited Company
        ("noel cunningham", "sole_trader_or_individual"),  # bare person stays hidden
    ],
)
def test_recruit_stem_and_ulc(name, expect):
    assert _reclass([(name, None)]).row(0, named=True)["supplier_class"] == expect, name


def test_ernst_young_variants_merge_to_one_key():
    df = pl.DataFrame(
        {
            "publisher_name": ["Revenue Commissioners"] * 4,
            "supplier_raw": [
                "ERNST & YOUNG BUSINESS ADVISORY",
                "Ernst and Young",
                "& Young",  # orphaned tail (the "Ernst &" was cut at the column edge)
                "Young Brothers Plant",  # NOT EY — "young" mid-name must be untouched
            ],
        }
    )
    out = _canonicalise_split_entities(df)["supplier_raw"].to_list()
    assert out[:3] == ["Ernst and Young"] * 3, out
    assert out[3] == "Young Brothers Plant"


def test_pobal_scheme_code_prefix_is_stripped():
    # Pobal prefixes a "LLL###" scheme code that fragments the real vendor across keys.
    df = pl.DataFrame(
        {
            "supplier_raw": ["CMO005 Logicalis", "CER002 Ergo", "Acme Ltd"],
            "po_number": [None, None, None],
            "supplier_normalised": ["", "", ""],
        }
    )
    out = _clean_supplier_names(df)["supplier_raw"].to_list()
    assert out == ["Logicalis", "Ergo", "Acme Ltd"]


def test_conform_desums_page_furniture_but_keeps_unnamed_vendor():
    df = pl.DataFrame(
        {
            "amount_semantics": ["payment_actual"] * 3,
            "publisher_name": ["X"] * 3,
            "value_safe_to_sum": [True] * 3,
            "supplier_class": ["sole_trader_or_individual"] * 3,
            "supplier_normalised": ["PURCHASE ORDERS OVER 20000", "IT SERVICE PROVIDER", "ARUP"],
            "public_display": [False, False, True],
            "privacy_status": ["ok"] * 3,
        }
    )
    out = {r["supplier_normalised"]: r for r in _conform(df).iter_rows(named=True)}
    assert out["PURCHASE ORDERS OVER 20000"]["value_safe_to_sum"] is False  # page furniture excluded
    assert out["IT SERVICE PROVIDER"]["value_safe_to_sum"] is True  # real spend, un-named: kept
    assert out["ARUP"]["value_safe_to_sum"] is True


def test_sole_trader_is_quarantined():
    df = _flag(["Jonathan Oakfield"])  # no company suffix -> personal
    row = df.row(0, named=True)
    assert row["supplier_class"] == "sole_trader_or_individual"
    assert row["privacy_status"] == "review_personal_data"
    assert row["public_display"] is False


@pytest.mark.parametrize(
    "name",
    [
        "Brightwater Solutions Limited",
        "Acme Engineering Ltd",
        "Northgate Holdings DAC",
    ],
)
def test_company_is_displayable(name):
    row = _flag([name]).row(0, named=True)
    assert row["supplier_class"] == "company"
    assert row["privacy_status"] == "ok"
    assert row["public_display"] is True


@pytest.mark.parametrize(
    "name",
    [
        # Suffix-less firms that the OLD trailing-\b stem pattern misclassed as sole traders (regression
        # for the 2026-06-13 over-quarantine fix): the activity word is an INFLECTION of a stem
        # (engineerS / consultING / technologY / propertIES) that \b(stem)\b could never match.
        "Arup Consulting Engineers",
        "RPS Consulting Engineers",
        "Ganson Building and Civil Engineering",
        "Creative Technology Audio",
        "Version 1 Software",
        "Alliance Medical",
        "Willis Towers Watson Insurance",
        "Goldman Sachs Asset Management",
        "United Drug Distributors Ireland",
        "Primary Health Properties ICAV",
        "Vector Workplace & Facility",  # ampersand firm
        "McCullough Mulvin Architects",
    ],
)
def test_suffixless_activity_firm_is_company(name):
    row = _flag([name]).row(0, named=True)
    assert row["supplier_class"] == "company", f"{name} misclassed as {row['supplier_class']}"
    assert row["public_display"] is True


@pytest.mark.parametrize(
    "name",
    [
        # The fix must NOT sweep bare personal names in: no legal form, no activity word.
        "Mary O'Brien",
        "Sean Kelly",
        "M Fitzgibbon",
        "David Slattery",
    ],
)
def test_bare_personal_name_still_quarantined(name):
    row = _flag([name]).row(0, named=True)
    assert row["supplier_class"] == "sole_trader_or_individual", f"{name} should stay quarantined"
    assert row["public_display"] is False


def test_public_body_is_displayable():
    row = _flag(["Cork County Council"]).row(0, named=True)
    assert row["supplier_class"] == "public_body"
    assert row["public_display"] is True


# ----------------------------------------------------------------------------------------
# value_safe_to_sum: intergovernmental transfers must NOT be summable (DQ audit 2026-06-05).
# A payment whose recipient is itself a public body (e.g. TII -> county-council road grants,
# €2.5bn / 32% of the fact) is a transfer/grant, not private procurement; totalling it inflates
# "procurement spend" and triple-counts the same euro down the grant -> council -> contractor
# chain. Such rows are RETAINED (public_display=True) but excluded from value_safe_to_sum.
# ----------------------------------------------------------------------------------------
def test_public_body_recipient_is_not_summable():
    row = _flag(["Cork County Council"]).row(0, named=True)
    assert row["supplier_class"] == "public_body"
    assert row["value_safe_to_sum"] is False  # transfer, not procurement
    assert row["public_display"] is True  # but still retained/displayable


def test_company_payment_is_summable():
    row = _flag(["Acme Engineering Ltd"]).row(0, named=True)
    assert row["supplier_class"] == "company"
    assert row["value_safe_to_sum"] is True


@pytest.mark.parametrize(
    "name",
    [
        "Transport Infrastructure Ireland",  # "...Ireland" — COMPANY_SUFFIX 'ireland' would mis-hit
        "Uisce Éireann",
        "Irish Water",
        "Tailte Éireann",
    ],
)
def test_named_state_agency_is_public_body_not_summable(name):
    # State agencies named "X Ireland"/"X Éireann" must classify public_body (tested before the
    # company check) so their intergovernmental transfers never leak into value_safe_to_sum.
    row = _flag([name]).row(0, named=True)
    assert row["supplier_class"] == "public_body", f"{name} misclassified as {row['supplier_class']}"
    assert row["value_safe_to_sum"] is False


def test_no_public_body_row_is_summable_in_mix():
    # Mirrors the real transfer pattern: TII pays county councils (recipient = public body),
    # which is the €2.5bn of intergovernmental transfers the fix excludes from summing.
    df = _flag(
        [
            "Cork County Council",  # public body -> transfer, not summable
            "Donegal County Council",  # public body -> transfer, not summable
            "Acme Engineering Ltd",  # company -> summable
            "Brightwater Solutions Limited",  # company -> summable
        ]
    )
    leaked = df.filter(pl.col("value_safe_to_sum") & (pl.col("supplier_class") == "public_body"))
    assert leaked.height == 0, f"{leaked.height} public_body transfer rows left summable"
    assert df.filter(pl.col("value_safe_to_sum")).height == 2  # only the two companies


def test_invariant_no_personal_row_is_displayable():
    df = _flag(
        [
            "Jonathan Oakfield",  # personal -> quarantined
            "Acme Engineering Ltd",  # company -> ok
            "Cork County Council",  # public body -> ok
            "Mary Quillfeather",  # personal -> quarantined
            "Brightwater Solutions Limited",
        ]
    )
    leaked = df.filter(pl.col("public_display") & (pl.col("supplier_class") == "sole_trader_or_individual"))
    assert leaked.height == 0, f"{leaked.height} personal rows left displayable"
    # and the quarantine actually suppressed the two invented individuals
    assert df.filter(~pl.col("public_display")).height == 2


# ----------------------------------------------------------------------------------------
# dedup_source_repeats: drop within-file parser repeats (identical in EVERY extracted field)
# without collapsing genuinely-distinct payments (DQ audit 2026-06-05, A3). Errs toward
# under-deduping: any differing field — notably description — preserves the row.
# ----------------------------------------------------------------------------------------
def _rows(specs, parser_name="public_body_pdf"):
    """specs: list of (supplier, amount, description, po, page) -> a fact-shaped frame.
    All share one source_file_hash/period so dedup is judged within-file. Defaults to a PDF
    parser because dedup is now PDF-ONLY (the word-row clusterer is the only reader that can
    emit a true repeat); pass parser_name="public_body_xlsx" to model a tabular source."""
    base = {"source_file_hash": "h1", "period": "2024-Q1", "paid_flag": None, "parser_name": parser_name}
    return pl.DataFrame(
        [
            {
                **base,
                "supplier_raw": s,
                "amount_eur": float(a),
                "description": d,
                "po_number": po,
                "source_page_number": pg,
            }
            for (s, a, d, po, pg) in specs
        ]
    )


def test_identical_rows_are_collapsed():
    df = _rows([("Acme Ltd", 1000, "Stationery", "PO1", 1)] * 4)  # 4 identical
    out, dropped = dedup_source_repeats(df)
    assert out.height == 1
    assert dropped == 3


def test_distinct_description_is_preserved():
    # The Courts pattern: same mis-parsed amount + same (truncated) supplier, but 3 DIFFERENT
    # descriptions = 3 real payment lines. None may be dropped.
    df = _rows(
        [
            ("Ireland Ltd", 21613, "Court A repairs", None, 2),
            ("Ireland Ltd", 21613, "Court B IT", None, 2),
            ("Ireland Ltd", 21613, "Court C legal", None, 2),
        ]
    )
    out, dropped = dedup_source_repeats(df)
    assert dropped == 0
    assert out.height == 3


def test_differing_any_field_preserves_row():
    df = _rows(
        [
            ("Acme Ltd", 1000, "X", "PO1", 1),
            ("Acme Ltd", 1000, "X", "PO2", 1),  # different PO -> kept
            ("Acme Ltd", 1000, "X", "PO1", 2),  # different page -> kept
            ("Acme Ltd", 2000, "X", "PO1", 1),  # different amount -> kept
            ("Acme Ltd", 1000, "X", "PO1", 1),  # exact repeat of row 0 -> dropped
        ]
    )
    out, dropped = dedup_source_repeats(df)
    assert dropped == 1
    assert out.height == 4


def test_same_payment_in_two_files_is_not_collapsed():
    # Different source files (different hash) are NOT deduped here — that is the small,
    # separately-handled cross-file republish case, not a within-file parser repeat.
    df = pl.concat(
        [
            _rows([("Acme Ltd", 1000, "X", "PO1", 1)]).with_columns(pl.lit("hA").alias("source_file_hash")),
            _rows([("Acme Ltd", 1000, "X", "PO1", 1)]).with_columns(pl.lit("hB").alias("source_file_hash")),
        ]
    )
    out, dropped = dedup_source_repeats(df)
    assert dropped == 0
    assert out.height == 2


def test_tabular_identical_rows_are_kept():
    # PDF-ONLY dedup (2026-06-21). A tabular reader iterates each source cell exactly once and
    # CANNOT manufacture a duplicate, so identical xlsx/csv rows are genuinely distinct published
    # payments and must all survive — e.g. CHI lists 6 separate €45,398.38 rent invoices in one
    # quarter (no invoice-ref column), and its published Total reconciles to the UN-deduped sum.
    df = _rows([("Liffeyview Property Holdings Ltd", 45398.38, None, None, None)] * 6, parser_name="public_body_xlsx")
    out, dropped = dedup_source_repeats(df)
    assert dropped == 0, "tabular identical rows must never be collapsed"
    assert out.height == 6


def test_pdf_and_tabular_mixed_only_pdf_repeats_dropped():
    df = pl.concat(
        [
            _rows([("Acme Ltd", 1000, "X", None, 1)] * 3, parser_name="public_body_pdf"),  # 3 -> 1
            _rows([("Beta Ltd", 2000, None, None, None)] * 3, parser_name="public_body_csv"),  # 3 -> 3
        ]
    )
    out, dropped = dedup_source_repeats(df)
    assert dropped == 2, "only the 2 PDF repeats drop; the 3 tabular rows stay"
    assert out.filter(pl.col("supplier_raw") == "Beta Ltd").height == 3
    assert out.filter(pl.col("supplier_raw") == "Acme Ltd").height == 1


def test_amount_role_skips_period_column():
    # TII regression (2026-06-21): header "Period Paid, PAYMENT, Vendor" — 'Period Paid' matches
    # the amount regex via 'paid', and its 'Jan-21' cells parse to 21, tying the numeric-density
    # test and stealing the amount role from the real 'PAYMENT' column (every amount became €21).
    # A column better classified as period must be dropped from amount candidates.
    header = ["Period Paid", "PAYMENT", "Vendor", "Description"]
    rows = [
        ["Jan-21", "959848.79", "Alstom", "Luas Trams"],
        ["Jan-21", "1800000", "Alstom", "Luas Trams"],
        ["Feb-21", "46224.15", "Causeway Geotech", "Fieldworks"],
    ]
    roles = detect_roles_tab(header, rows)
    assert roles["amount"] == 1, "PAYMENT must win the amount role over the 'Period Paid' date column"
    assert roles["supplier"] == 2


def test_dedup_sig_excludes_volatile_provenance():
    # source_row_number (a running counter) must NOT be in the signature, else true repeats with
    # different counters would never collapse. Confirms the key is content, not emission order.
    assert "source_row_number" not in DEDUP_SIG
    assert "supplier_raw" in DEDUP_SIG and "description" in DEDUP_SIG and "amount_eur" in DEDUP_SIG


# ----------------------------------------------------------------------------------------
# A2/A4: unidentifiable + split supplier names (DQ audit 2026-06-05).
# ----------------------------------------------------------------------------------------
def _norm_conf(suppliers, conf="high"):
    """Frame with supplier_normalised + extraction_confidence for flag_unidentifiable_suppliers."""
    df = pl.DataFrame(
        {
            "supplier_raw": suppliers,
            "amount_eur": [1000.0] * len(suppliers),
            "amount_semantics": ["payment_actual"] * len(suppliers),
            "extraction_confidence": [conf] * len(suppliers),
        }
    )
    return classify_and_flag(df)


def test_empty_normalised_name_downgraded_to_low():
    # Truncated to just a legal suffix -> normalises to '' -> not attributable.
    out = flag_unidentifiable_suppliers(_norm_conf(["IRELAND LTD", "LTD", "(IRELAND) LTD"]))
    assert out["extraction_confidence"].to_list() == ["low", "low", "low"]


def test_generic_word_name_downgraded_to_low():
    out = flag_unidentifiable_suppliers(
        _norm_conf(["Construction Ltd", "Aircraft Ltd", "Ireland Energy Ltd", "Shipping Group"])
    )
    assert set(out["extraction_confidence"].to_list()) == {"low"}


def test_real_oneword_firm_not_downgraded():
    # Distinctive token survives normalisation -> must STAY high-confidence.
    out = flag_unidentifiable_suppliers(
        _norm_conf(
            [
                "Sodexo Ireland Ltd",
                "Fujitsu Ireland Ltd",
                "Adston Ltd",
                "Accenture Limited",
                "Atkins Ltd",
                "Marsh Ireland Ltd",
                "Capgemini Ireland Ltd",
            ]
        )
    )
    assert set(out["extraction_confidence"].to_list()) == {"high"}


def test_suffix_and_geographic_remnants_downgraded():
    # Bare legal-form / geographic remnants (a distinctive lead word was truncated at source).
    out = flag_unidentifiable_suppliers(_norm_conf(["Deloitte LLP", "Ltd", "Ireland"], conf="high"))
    # "Deloitte LLP" -> norm "DELOITTE LLP" (2 tokens, distinctive) stays high; "Ltd"->'' and
    # "Ireland"->'IRELAND' (single generic) drop to low.
    confs = dict(zip(out["supplier_raw"].to_list(), out["extraction_confidence"].to_list(), strict=True))
    assert confs["Deloitte LLP"] == "high"
    assert confs["Ltd"] == "low"
    assert confs["Ireland"] == "low"


def test_unidentifiable_rows_stay_summable():
    # The money is real; only attribution confidence drops. value_safe_to_sum must be untouched.
    df = classify_and_flag(
        pl.DataFrame(
            {
                "supplier_raw": ["Construction Ltd"],
                "amount_eur": [1000.0],
                "amount_semantics": ["payment_actual"],
                "extraction_confidence": ["high"],
            }
        )
    )
    out = flag_unidentifiable_suppliers(df)
    assert out.row(0, named=True)["value_safe_to_sum"] is True
    assert out.row(0, named=True)["extraction_confidence"] == "low"


def test_nbi_split_is_merged_to_identifiable_name():
    df = pl.DataFrame(
        {
            "supplier_raw": [
                "Infrastructure DAC",
                "Infrastructure DAC NBP",
                "NBI Infrastructure DAC",
                "Infrastructure DAC",
            ],
            "po_number": ["NBI", "NBI", None, "12345"],  # last one: not NBI -> left alone
            "amount_eur": [1.0, 2.0, 3.0, 4.0],
        }
    )
    out = canonicalise_supplier_raw(df)
    raws = out["supplier_raw"].to_list()
    assert raws[0] == "NBI Infrastructure DAC"  # po=NBI rewritten
    assert raws[1] == "NBI Infrastructure DAC"  # 'Infrastructure DAC NBP' po=NBI rewritten
    assert raws[2] == "NBI Infrastructure DAC"  # already canonical
    assert raws[3] == "Infrastructure DAC"  # po != NBI -> untouched (no over-merge)
    # and after normalisation the NBI rows share ONE identifiable id (not generic 'INFRASTRUCTURE')
    normed = classify_and_flag(out.with_columns(pl.lit("payment_actual").alias("amount_semantics")))
    nbi_norm = normed.filter(pl.col("po_number") == "NBI")["supplier_normalised"].unique().to_list()
    assert nbi_norm == ["NBI INFRASTRUCTURE"], nbi_norm
    # the canonical NBI name is NOT swept up by the generic-word downgrade
    flagged = flag_unidentifiable_suppliers(normed.with_columns(pl.lit("high").alias("extraction_confidence")))
    assert flagged.filter(pl.col("po_number") == "NBI")["extraction_confidence"].to_list() == ["high", "high"]


# ----------------------------------------------------------------------------------------
# Period precision: month-range filenames encode a quarter the Q\d patterns miss (DQ audit
# 2026-06-06). Without this, recurring quarterly payments share a year-only period and look
# like cross-file duplicates.
# ----------------------------------------------------------------------------------------
@pytest.mark.parametrize(
    "fname,expected",
    [
        ("https://x.ie/jan-mar-2016.pdf", ("2016-Q1", 2016, 1)),
        ("https://x.ie/apr-jun-2018.pdf", ("2018-Q2", 2018, 2)),
        ("https://x.ie/jul-sep-2016.pdf", ("2016-Q3", 2016, 3)),
        ("https://x.ie/oct-dec-2016.pdf", ("2016-Q4", 2016, 4)),
        ("https://x.ie/Q3_2023.pdf", ("2023-Q3", 2023, 3)),  # existing Q-pattern still works
        ("https://x.ie/payments-2024.pdf", ("2024", 2024, None)),  # no quarter -> year only
    ],
)
def test_period_from_url_parses_month_ranges(fname, expected):
    assert period_from_url(fname) == expected


@pytest.mark.integration
def test_coverage_flag_is_applied():
    """The on-disk coverage JSON must record the quarantine as applied. Marked integration:
    requires a fresh extractor run (network crawl) — a stale pre-fix sandbox fails this,
    which is the intended signal to regenerate `public_payments_fact.parquet`."""
    import json

    cov = _ROOT / "data" / "_meta" / "public_payments_coverage.json"
    if not cov.exists():
        pytest.skip("coverage not generated; run the extractor first")
    data = json.loads(cov.read_text())
    assert data.get("privacy_quarantine_applied") is True
