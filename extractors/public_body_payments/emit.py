"""Emit + classify for the public-body payments extractor package (split out
of extractors/procurement_public_body_extract.py, doc/REFACTORING_CANDIDATES.md
C7, pure move-function -- no logic changes).

Contains the privacy-quarantine block (classify_and_flag) MOVED VERBATIM: it
is one-way and safety-critical (supplier_class -> privacy_status ->
public_display), so its logic, comments and thresholds are untouched.

emit_rows() dispatches to a bespoke reading-order reader by NAME via
``globals()[spec["fn"]]`` (see _RO_SPECS) -- this is why every read_* used
there is imported here BY NAME (not module-qualified): globals() resolves
against THIS module's namespace, so a module-qualified import
(``from . import readers`` + ``readers.read_courts``) would silently break
the dispatch.
"""

from __future__ import annotations

import hashlib
import re

import polars as pl

from shared.name_norm import name_norm_expr

from .config import PARSER_VERSION
from .readers import (
    clean_supplier,
    detect_roles_tab,
    period_from_url,
    read_csv,
    read_pdf,
    read_pdf_reading_order_fallback,
    read_xls,
    read_xlsx,
    to_eur,
)
# The bespoke reading-order readers below are never called by NAME in this file's own
# source text -- _RO_SPECS (below) resolves them via ``globals()[spec["fn"]]`` at run
# time, so ruff's static F401 check cannot see the use. Each MUST stay imported under
# its bare name (not module-qualified) for that dispatch to find it; do not remove.
from .readers import read_courts  # noqa: F401
from .readers import read_culture  # noqa: F401
from .readers import read_defence  # noqa: F401
from .readers import read_dper  # noqa: F401
from .readers import read_housing  # noqa: F401
from .readers import read_reading_order  # noqa: F401
from .readers import read_revenue  # noqa: F401
from .readers import read_tailte  # noqa: F401

# ----------------------------------------------------------------------------- regexes
# A multi-word alphabetic string (e.g. "AN POST", "AIRNAV IRELAND") — used to recover a supplier
# name that a mis-mapped column put into po_number while the supplier cell came out blank.
NAME_LIKE = re.compile(r"[A-Za-z]{2,}[A-Za-z .,&'/-]*\s[A-Za-z&]", re.I)
NUMERIC_NOISE = re.compile(r"\d{4,}|\d,\d{3}")  # a big/grouped number => category total, not a name
CAVEAT_RE = re.compile(r"\bvat\b|exclud|inclus|indicativ|not (a )?payment|net of|estimate|note:|please note", re.I)
# A company / organisation indicator: a legal form, a plurality word, or a business-activity
# stem that a lone private individual's name never carries. Two arms because the matching differs:
#   • WHOLE words (\b…\b) for short tokens that would over-match as prefixes ("uc" inside "UCD",
#     "co" inside "Connolly"): the legal forms and bare plurality words.
#   • STEMS (leading \b only, NO trailing \b) so an inflection matches: "engineer" must catch
#     "engineerING"/"engineerS", "consult" → "consultING/consultANTS/consultANCY", "technolog" →
#     "technologY/technologIES". The old single \b(…)\b pattern silently failed on every such
#     inflection (the trailing \b needs a word→non-word edge, which "engineerS" doesn't have), so
#     ARUP CONSULTING ENGINEERS / CREATIVE TECHNOLOGY etc. were misclassed sole-trader. Fixed
#     2026-06-13; the reclassifier in procurement_payments_consolidate.py carries the same vocab.
_CO_WORDS = (
    "ltd",
    "limited",
    "dac",
    "plc",
    "clg",
    "llp",
    "teo",
    "teoranta",
    "t/a",
    "uc",
    "inc",
    "llc",
    "gmbh",
    "co",
    "company",
    "companies",
    "group",
    "sons",
    "bros",
    "university",
    "college",
    "council",
    "hse",
    "board",
    "media",
    "hotel",
    "ireland",
    "jv",
    "ppp",
)
_CO_STEMS = (
    "servic",
    "solution",
    "consult",
    "engineer",
    "architect",
    "surveyor",
    "solicitor",
    "barrister",
    "accountant",
    "advis",
    "contract",
    "construct",
    "develop",
    "enterprise",
    "industr",
    "technolog",
    "system",
    "software",
    "logistic",
    "distribut",
    "manufactur",
    "pharma",
    "biotech",
    "diagnostic",
    "laborator",
    "healthcare",
    "medical",
    "insuranc",
    "assuranc",
    "management",
    "communicat",
    "telecom",
    "propert",
    "holding",
    "internation",
    "institut",
    "foundation",
    "partner",
    "associat",
    "incorporat",
    "corporat",
    "centre",
)
COMPANY_SUFFIX = re.compile(
    r"\b(?:" + "|".join(_CO_WORDS) + r")\b|&|\b(?:" + "|".join(_CO_STEMS) + r")",
    re.I,
)
FOREIGN_FORM = re.compile(
    r"\b(gmbh|s\.?a\.?|n\.?v\.?|s\.?a\.?s|s\.?p\.?a|inc|llc|\bpty\b|\bab\b|\bbv\b|\boy\b|srl|sl|sarl|aps|kft|ltda)\b",
    re.I,
)
# NOTE: national state agencies named "X Ireland" / "X Éireann" (e.g. Transport
# Infrastructure Ireland, Uisce Éireann) must be caught HERE — _pub is tested before
# _co, otherwise COMPANY_SUFFIX's bare "ireland" token misclassifies them as companies
# and their intergovernmental transfers leak into value_safe_to_sum. Add agencies as
# they surface as transfer recipients.
PUBLIC_BODY = re.compile(
    r"\b(county council|city council|university|institute of technology|department of|office of|\bHSE\b|health service|an garda|údarás|udaras|education and training board|\bETB\b|local authority|national \w+ authority|\bOPW\b|hospital|transport infrastructure ireland|\bTII\b|uisce éireann|irish water|tailte éireann)\b",
    re.I,
)
# Drops title/threshold rows that masquerade as a supplier (the page heading bleeds into the
# supplier column with the literal €20,000 threshold as its amount). Plural "Purchase Orders"
# and "Payments greater than/over €20,000" headings leaked through the singular-only pattern.
CATEGORY_WORD = re.compile(
    r"^\s*(total|category total|sum|subtotal|grand total|all suppliers|various|publication of|purchase orders?|payments? (greater|over|to suppliers)|payments? greater than)\b",
    re.I,
)
# Non-anchored variant: a page-title that bleeds into the supplier column may LEAD with the
# body name (e.g. "TU Dublin Payments and Purchase Orders over €20,000") so ^-anchoring misses
# it. These rows carry the literal threshold as their amount; the phrasing is never a real
# supplier name. Checked in addition to CATEGORY_WORD.
TITLE_ROW = re.compile(
    r"(purchase orders?|payments?)\b.{0,30}(over|greater than)\s*€?\s*20[,.]?000"
    r"|payments?\s+(and|or)\s+purchase orders?"
    # gov.ie department banners phrase the threshold as "Payments for/of €20,000 or above/over"
    # (DFAT, Health, Education) — the literal €20,000 in the banner otherwise leaks as a fake
    # supplier row with amount 20,000. "or above/over/more" only ever appears in a heading.
    r"|payments?\s+(for|of)\s+€?\s*20[,.]?000"
    r"|€?\s*20[,.]?000\s+or\s+(above|over|more)",
    re.I,
)


# ============================================================================ extract
def _confidence(good: int) -> str:
    return "high" if good > 20 else ("medium" if good > 3 else "low")


# Wiring spec per bespoke reading-order reader. Every one of these branches differs ONLY in:
# which read_* parses the bytes, which record keys feed description/po_number/paid_flag, where
# the page number comes from, and whether a PER-ROW payment date overrides the file-URL period
# ("iso" = YYYY-MM-DD, "dmy" = DD/MM/YYYY with URL fallback on a malformed cell). That wiring is
# data, not code: adding a publisher layout = its read_* function (with a unit test) + one entry
# here, not another elif branch. Readers are named (not referenced) so they resolve at call time
# — same late binding the old elif chain had, and what lets tests monkeypatch them.
_RO_SPECS: dict[str, dict] = {
    "reading_order": {
        "fn": "read_reading_order",
        "desc": "desc",
        "po": "ref",
        "paid": None,
        "page": "page",
        "date": "iso",
    },
    "reading_order_courts": {"fn": "read_courts", "desc": "desc", "po": "ref", "paid": "paid"},
    "reading_order_revenue": {"fn": "read_revenue", "desc": "desc", "po": "ref", "paid": "paid"},
    "reading_order_culture": {"fn": "read_culture", "desc": "desc", "po": None, "paid": None},
    "reading_order_dper": {"fn": "read_dper", "desc": "desc", "po": "ref", "paid": "paid"},
    "reading_order_tailte": {"fn": "read_tailte", "desc": "desc", "po": "ref", "paid": "paid"},
    "reading_order_housing": {"fn": "read_housing", "desc": "desc", "po": None, "paid": None, "date": "dmy"},
    "reading_order_fallback": {"fn": "read_pdf_reading_order_fallback", "desc": "desc", "po": "ref", "paid": None},
    "reading_order_defence": {"fn": "read_defence", "desc": "category", "po": "ref", "paid": None},
}


def _emit_generic_pdf(base, b, max_pages) -> tuple[list[dict], str, bool, dict | None]:
    """Generic PDF chain: word-geometry read -> amount-anchored reading-order fallback ->
    unparsed. Returns (rows, confidence, caveat_detected, unparsed_stat_or_None)."""
    info = read_pdf(b, max_pages)
    caveat_detected = bool(CAVEAT_RE.search(info["page0"]) or CAVEAT_RE.search(info["header_label"]))
    geom_ok = info["digital"] and info["cols"] and "amount" in info["roles"]
    rows_out: list[dict] = []
    if geom_ok:
        sup_i = info["roles"].get("supplier")
        amt_i = info["roles"]["amount"]
        desc_i, po_i, paid_i = (info["roles"].get(k) for k in ("description", "po", "paid"))
        for srn, (page, rec) in enumerate(info["rows"]):
            amt = to_eur(rec[amt_i]) if amt_i < len(rec) else None
            if amt is None:
                continue
            sup = clean_supplier(rec[sup_i]) if sup_i is not None and sup_i < len(rec) else None
            desc = rec[desc_i] if desc_i is not None and desc_i < len(rec) else None
            # Drop total/category/title-masquerade rows. The page banner ("... Payments greater
            # than €20,000") splits across cells — "greater than" into the description, the
            # "€20,000" into the amount column — so no single cell holds the whole phrase. Test
            # TITLE_ROW against the JOINED row (bucket order re-adjoins "greater than … 20,000").
            rowtext = " ".join(str(x) for x in rec if x)
            if (sup and CATEGORY_WORD.search(sup)) or TITLE_ROW.search(rowtext):
                continue
            rows_out.append(
                base(
                    srn,
                    page,
                    sup,
                    amt,
                    desc,
                    clean_supplier(rec[po_i]) if po_i is not None and po_i < len(rec) else None,
                    rec[paid_i] if paid_i is not None and paid_i < len(rec) else None,
                )
            )
    if not rows_out:
        # The word-geometry reader found no usable rows (no column grid / amount header wrapped).
        # Try the amount-anchored reading-order fallback. This runs ONLY here, so a publisher
        # whose files already parse via geometry is never touched (no regression).
        rows_out = [
            base(srn, 1, clean_supplier(r["supplier"]), r["amount"], r["desc"], r["ref"], None)
            for srn, r in enumerate(read_pdf_reading_order_fallback(b, max_pages))
        ]
    if not rows_out:
        return (
            [],
            "low",
            caveat_detected,
            {
                "status": "unparsed",
                "reason": "scanned/no-header/no-amount",
                "rows": 0,
                "confidence": "low",
                "pages": info.get("pages"),
            },
        )
    return rows_out, _confidence(len(rows_out)), caveat_detected, None


def _emit_tabular(base, b, fmt) -> tuple[list[dict], str, bool, dict | None]:
    """xlsx/xls/csv chain. Returns (rows, confidence, caveat_detected, unparsed_stat_or_None)."""
    reader = {"xlsx": read_xlsx, "xls": read_xls, "csv": read_csv}[fmt]
    header, rows, full = reader(b)
    caveat_detected = bool(CAVEAT_RE.search(full) or any(CAVEAT_RE.search(h or "") for h in header))
    roles = detect_roles_tab(header, rows)
    sup_i, amt_i = roles["supplier"], roles["amount"]
    if amt_i is None:
        return (
            [],
            "low",
            caveat_detected,
            {"status": "unparsed", "reason": "no-amount-col", "rows": 0, "confidence": "low"},
        )
    desc_i, po_i, paid_i = roles["description"], roles["po"], roles["paid"]
    rows_out: list[dict] = []
    for srn, r in enumerate(rows):
        amt = to_eur(r[amt_i]) if amt_i < len(r) else None
        if amt is None:
            continue
        sup = clean_supplier(r[sup_i]) if sup_i is not None and sup_i < len(r) else None
        if sup and CATEGORY_WORD.search(sup):
            continue
        rows_out.append(
            base(
                srn,
                None,
                sup,
                amt,
                r[desc_i] if desc_i is not None and desc_i < len(r) else None,
                clean_supplier(r[po_i]) if po_i is not None and po_i < len(r) else None,
                r[paid_i] if paid_i is not None and paid_i < len(r) else None,
            )
        )
    return rows_out, _confidence(len(rows_out)), caveat_detected, None


def _repair_and_flag(rows_out: list[dict], conf: str, caveat_detected: bool) -> None:
    """Post-processing applied to every emitted row, in place."""
    for r in rows_out:
        r["extraction_status"] = "extracted"
        r["extraction_confidence"] = conf
        r["caveat_text_detected"] = caveat_detected
        # Blank-supplier repair: a mis-mapped column can leave supplier_raw empty while the
        # company name sits in po_number ("AN POST", "AIRNAV IRELAND"). Promote it back IF it
        # looks like a multi-word name and carries no big number (which would mean it is a
        # category-total line, e.g. ESB Networks "Meter Reading Services 3,823,410").
        sup = (r.get("supplier_raw") or "").strip()
        po = (r.get("po_number") or "").strip()
        if not sup and po and NAME_LIKE.search(po) and not NUMERIC_NOISE.search(po):
            r["supplier_raw"] = po
            r["po_number"] = None
            sup = po
        # Anything still missing a supplier is NOT a clean supplier-level row (category totals,
        # blank cells) — downgrade so it is filterable and never ranked as a real supplier.
        if not sup:
            r["extraction_confidence"] = "low"
            r["caveat_text_detected"] = True


def emit_rows(cf, file_url, b, fmt, max_pages) -> tuple[list[dict], dict]:
    """Parse one file -> gold-schema row dicts + a small per-file stat block."""
    fhash = hashlib.sha256(b).hexdigest()[:16]
    period, year, quarter = period_from_url(file_url)
    rows_out: list[dict] = []
    caveat_detected = False
    conf = "low"

    def base(srn, page, supplier, amount, desc, po, paid, period=period, year=year, quarter=quarter):
        return {
            "publisher_id": cf["id"],
            "publisher_name": cf["name"],
            "publisher_type": cf["ptype"],
            "sector": cf["sector"],
            "source_landing_url": cf["listing_url"],
            "source_file_url": file_url,
            "source_file_hash": fhash,
            "period": period,
            "year": year,
            "quarter": quarter,
            "supplier_raw": supplier,
            "amount_eur": amount,
            "amount_semantics": cf["amount_semantics"],
            "description": desc,
            "po_number": po,
            "paid_flag": paid,
            "source_row_number": srn,
            "source_page_number": page,
            "parser_name": f"public_body_{fmt}",
            "parser_version": PARSER_VERSION,
            "source_caveat": cf["caveat"] or None,
        }

    reader_key = cf.get("reader")
    if fmt == "pdf" and reader_key in _RO_SPECS:
        # Bespoke reading-order readers — one wiring spec each (_RO_SPECS above documents the
        # per-publisher quirks that used to live in nine near-identical elif branches).
        spec = _RO_SPECS[reader_key]
        for srn, r in enumerate(globals()[spec["fn"]](b, max_pages)):
            per, yr, q = period, year, quarter
            if spec.get("date") == "iso":
                yr = int(r["date"][:4])
                q = (int(r["date"][5:7]) - 1) // 3 + 1
                per = f"{yr}-Q{q}"
            elif spec.get("date") == "dmy":
                dd = r["date"]
                try:
                    yr = int(dd[6:10])
                    q = (int(dd[3:5]) - 1) // 3 + 1
                    per = f"{yr}-Q{q}"
                except (ValueError, IndexError):
                    per, yr, q = period, year, quarter
            rows_out.append(
                base(
                    srn,
                    r[spec["page"]] if spec.get("page") else 1,
                    clean_supplier(r["supplier"]),
                    r["amount"],
                    r[spec["desc"]],
                    r[spec["po"]] if spec["po"] else None,
                    r[spec["paid"]] if spec["paid"] else None,
                    period=per,
                    year=yr,
                    quarter=q,
                )
            )
        conf = _confidence(len(rows_out))

    elif fmt == "pdf":
        rows_out, conf, caveat_detected, unparsed = _emit_generic_pdf(base, b, max_pages)
        if unparsed:
            return [], unparsed

    else:  # xlsx / xls / csv
        rows_out, conf, caveat_detected, unparsed = _emit_tabular(base, b, fmt)
        if unparsed:
            return [], unparsed

    _repair_and_flag(rows_out, conf, caveat_detected)
    return rows_out, {"status": "ok" if rows_out else "empty", "rows": len(rows_out), "confidence": conf}


# Within-source-file duplicate signature: a row identical to another in EVERY extracted field
# (same file + supplier + amount + description + PO + page + paid-flag + period) is an
# indistinguishable repeat the word-row clusterer emitted more than once (a table row captured
# twice, a header re-clustered), and summing both double-counts. A row that differs in ANY field
# — notably `description` — is a DISTINCT payment and MUST be kept (e.g. Courts had 9 lines that
# share a mis-parsed amount + truncated name but carry 9 different descriptions). DQ audit 2026-06-05.
DEDUP_SIG = [
    "source_file_hash",
    "supplier_raw",
    "amount_eur",
    "description",
    "po_number",
    "source_page_number",
    "paid_flag",
    "period",
]


def dedup_source_repeats(df: pl.DataFrame) -> tuple[pl.DataFrame, int]:
    """Drop indistinguishable within-file PDF parser repeats (see DEDUP_SIG). Keeps the
    first occurrence; returns (deduped_df, n_dropped).

    PDF-ONLY (fixed 2026-06-21). The PDF word-row clusterer can emit the same visual row
    more than once, which this is here to catch. The tabular readers (xlsx/xls/csv) iterate
    each source cell EXACTLY ONCE and so cannot manufacture a duplicate — every identical
    tabular row is a genuinely distinct published payment (e.g. CHI publishes 6 separate
    €45,398.38 rent invoices in one quarter, no invoice-ref column to tell them apart). The
    old blanket dedup collapsed those, understating CHI's quarter by €3.74m / 15% vs its own
    published "Total" row (which reconciles to the UN-deduped sum to 0.15%). So dedup PDF
    rows only and pass tabular rows through untouched. Errs toward UNDER-deduping even on
    PDFs: any differing field (notably `description`) preserves the row."""
    if df.is_empty():
        return df, 0
    keys = [c for c in DEDUP_SIG if c in df.columns]
    n = df.height
    if "parser_name" not in df.columns:  # defensive: behave as before if provenance absent
        out = df.unique(subset=keys, keep="first", maintain_order=True)
        return out, n - out.height
    # occ = 0-based rank within each identical-key group, in row order; >0 means a later
    # repeat. Drop only PDF repeats; tabular rows are never dropped.
    df = df.with_row_index("_ri")
    occ = pl.int_range(pl.len()).over(keys)
    is_pdf_repeat = pl.col("parser_name").str.ends_with("pdf") & (occ > 0)
    out = df.filter(~is_pdf_repeat).drop("_ri")
    return out, n - out.height


# Generic business/industry words that, standing ALONE as the whole normalised supplier name, do
# NOT identify a specific company — they are the remnant after a distinctive leading word was
# truncated at source (published "Construction Ltd" / "Aircraft Ltd" / "Ireland Energy Ltd" ->
# norm "CONSTRUCTION"/"AIRCRAFT"/"ENERGY"), or a too-generic published name ("Infrastructure DAC").
# Distinct from real one-word firms whose distinctive token survives (SODEXO, FUJITSU, ADSTON,
# ACCENTURE). Single-token match only, so "NBI INFRASTRUCTURE" / "DSV LOGISTICS" are NOT flagged.
GENERIC_SUPPLIER_NAME = frozenset(
    {
        "infrastructure",
        "energy",
        "construction",
        "aircraft",
        "shipping",
        "media",
        "technology",
        "partnership",
        "bundle",
        "group",
        "holdings",
        "services",
        "solutions",
        "systems",
        "engineering",
        "logistics",
        "properties",
        "developments",
        "consulting",
        "consultants",
        "management",
        "international",
        "contractors",
        # legal-form / geographic remnants left after the distinctive lead word was truncated
        # (e.g. "Deloitte LLP" -> "LLP", "[Brand] Electric Ltd" -> "ELECTRIC", "X UK Ltd" -> "UK")
        "llp",
        "electric",
        "europe",
        "uk",
        "ireland",
    }
)


def canonicalise_supplier_raw(df: pl.DataFrame) -> pl.DataFrame:
    """Evidence-based merge of known split entities BEFORE normalisation (no name fabrication —
    uses only strings already published in this data + the po_number signal). NBI: the National
    Broadband Plan contractor is published both as 'Infrastructure DAC' (po_number 'NBI', Dept
    Climate) and 'NBI Infrastructure DAC' (Dept Culture) — one legal entity. Rewrite the po=NBI
    'Infrastructure DAC' form to 'NBI Infrastructure DAC' so it merges with the NBI-prefixed rows
    and normalises to the identifiable 'NBI INFRASTRUCTURE' instead of the generic 'INFRASTRUCTURE'.
    DQ audit 2026-06-05 (A2)."""
    if df.is_empty() or "supplier_raw" not in df.columns or "po_number" not in df.columns:
        return df
    # Defensive dtype cast: a body whose parse yielded an all-null supplier column comes through as
    # dtype Null (not Utf8), and the str ops below then raise "expected String type, got: null".
    # Casting makes the merge a no-op on such rows instead of crashing the whole run.
    df = df.with_columns(pl.col("supplier_raw").cast(pl.Utf8))
    is_nbi = (pl.col("po_number").cast(pl.Utf8).str.to_uppercase().str.strip_chars() == "NBI") & (
        pl.col("supplier_raw").str.contains(r"(?i)\binfrastructure dac\b")
    )
    return df.with_columns(
        pl.when(is_nbi).then(pl.lit("NBI Infrastructure DAC")).otherwise(pl.col("supplier_raw")).alias("supplier_raw")
    )


def flag_unidentifiable_suppliers(df: pl.DataFrame) -> pl.DataFrame:
    """Downgrade extraction_confidence to 'low' where the normalised supplier name is empty
    (truncated to just a legal suffix, e.g. 'LTD'/'IRELAND LTD' -> '') or is a single generic
    business word (truncation remnant, GENERIC_SUPPLIER_NAME). Such rows have a real amount —
    they STAY summable (value_safe_to_sum untouched) — but no usable supplier identity, so the
    low-confidence flag lets a supplier ranking filter them out. Real one-word firms keep their
    distinctive token and are unaffected. DQ audit 2026-06-05 (A2/A4)."""
    if df.is_empty() or "supplier_normalised" not in df.columns:
        return df
    norm = pl.col("supplier_normalised")
    unidentifiable = (
        norm.is_null()
        | (norm.str.strip_chars() == "")
        | norm.str.to_lowercase().str.strip_chars().is_in(list(GENERIC_SUPPLIER_NAME))
    )
    conf = pl.col("extraction_confidence") if "extraction_confidence" in df.columns else pl.lit("high")
    return df.with_columns(pl.when(unidentifiable).then(pl.lit("low")).otherwise(conf).alias("extraction_confidence"))


def classify_and_flag(df: pl.DataFrame) -> pl.DataFrame:
    """supplier_normalised + supplier_class + privacy_status; quarantine DEFERRED."""
    if df.is_empty():
        return df
    df = (
        df.with_columns(
            name_norm_expr("supplier_raw").alias("supplier_normalised"),
            pl.col("supplier_raw")
            .map_elements(lambda s: bool(PUBLIC_BODY.search(s or "")), return_dtype=pl.Boolean)
            .alias("_pub"),
            pl.col("supplier_raw")
            .map_elements(lambda s: bool(COMPANY_SUFFIX.search(s or "")), return_dtype=pl.Boolean)
            .alias("_co"),
            pl.col("supplier_raw")
            .map_elements(lambda s: bool(FOREIGN_FORM.search(s or "")), return_dtype=pl.Boolean)
            .alias("_for"),
        )
        .with_columns(
            pl.when(pl.col("_pub"))
            .then(pl.lit("public_body"))
            .when(pl.col("_co"))
            .then(pl.lit("company"))
            .when(pl.col("_for"))
            .then(pl.lit("foreign_company"))
            .when(pl.col("supplier_raw").is_null() | (pl.col("supplier_raw").str.strip_chars() == ""))
            .then(pl.lit("unknown"))
            .otherwise(pl.lit("sole_trader_or_individual"))
            .alias("supplier_class"),
        )
        .with_columns(
            # privacy_status flags likely-personal rows (sole traders / individuals).
            pl.when(pl.col("supplier_class") == "sole_trader_or_individual")
            .then(pl.lit("review_personal_data"))
            .otherwise(pl.lit("ok"))
            .alias("privacy_status"),
            # QUARANTINE APPLIED: a likely-personal supplier is never displayable. Rows are RETAINED
            # for analysis/coverage (nothing dropped) — only the display flag is gated, so a
            # downstream UI / promotion must filter on public_display.
            (pl.col("supplier_class") != "sole_trader_or_individual").alias("public_display"),
            # po_committed / payment_actual are summable; contract_award_value is caution-only.
            # EXCLUDE public_body suppliers: a payment whose recipient is itself a public body is an
            # intergovernmental TRANSFER / grant (e.g. TII -> county-council road grants = €2.5bn /
            # 32% of this fact), NOT private procurement. Summing them inflates "procurement spend"
            # and triple-counts the same euro (TII grant -> council -> contractor in la_payments_fact
            # -> the contractor's eTenders/TED award). They are RETAINED (public_display stays True)
            # but never summed. DQ audit 2026-06-05; supplier_class is derived in the block above.
            (
                pl.col("amount_semantics").is_in(["po_committed", "payment_actual"])
                & pl.col("amount_eur").is_not_null()
                & (pl.col("amount_eur") > 0)
                # Belt-and-braces against an order/PO number read as an amount (the recurring
                # €30bn/€400m bug on reading-order PDFs, e.g. dept_justice's "109245 …" lines): no
                # single public-body PO/payment line is ≥ €100m, so such a value is a parse error,
                # never summable. Mirrors the LA extractor's post-guard. The row is RETAINED (low
                # confidence) for audit but excluded from any total.
                & (pl.col("amount_eur") < 100_000_000)
                # A row with NO identifiable supplier is never summable spend: it is either a
                # category/quarterly SUBTOTAL (e.g. dept_social_protection PDFs emit 4 blank-supplier
                # 'Sum:' rows/year worth €428m that would DOUBLE-COUNT the per-supplier rows) or a
                # parse gap. CATEGORY_WORD misses these because the total label sits in the
                # description, not the blank supplier cell. Rows kept (low-conf) for audit, never
                # totalled. GENERIC_SUPPLIER_NAME single-word firms keep a non-empty normalised name
                # and stay summable — only a TRULY empty supplier_normalised is excluded here.
                & pl.col("supplier_normalised").is_not_null()
                & (pl.col("supplier_normalised").str.strip_chars() != "")
                & (pl.col("supplier_class") != "public_body")
            ).alias("value_safe_to_sum"),
        )
        .drop(["_pub", "_co", "_for"])
    )
    return df
