"""Consolidate the per-publisher payment-grain facts into one gold fact.

The semistate/public-body lane produced several silver facts (lifted out of data/sandbox/
2026-06-12), all sharing an IDENTICAL 28-column schema and NO publisher overlap:
  public_payments_fact (28 publishers) + hse_tusla + nta + nphdb + seai  →  one gold fact.

The 31 local authorities' Purchase-Orders/Payments-over-€20k fact (silver
la_payments_fact, extractors/procurement_la_payments_extract.py) is ALSO folded in here, via a
dedicated conformer (it was already built on the canonical value_kind + realisation_tier
taxonomy). Councils enter the gold fact as publisher_type='local_authority' and surface
through v_procurement_payments automatically.

⚠️ TRIPLE-COUNT TRAP (do not let a consumer sum the whole fact blindly): TII "Road Grant"
rows in public_payments_fact are central→council TRANSFERS (supplier is a council,
supplier_class='public_body'); the LA fact then records that same money flowing council→
contractor. Summing both double-counts it. The guard is at the consuming view/page: exclude
supplier_class='public_body' from spend totals (those rows are transfers/councils-as-payee,
not procurement). The LA contractors are companies, so this fold does not worsen the trap — it
just makes both legs visible in one fact.

This is the Stage-D consolidation (see doc/PROCUREMENT_MASTER.md §6). It does four things, all
mechanical — no re-parsing of source documents:
  1. concat the conformed facts (asserting schema identity first);
  2. add ``vat_status`` so totals are never silently summed across different VAT bases
     (HSE/Tusla publish VAT-inclusive; the rest are not confirmed → ``unknown``);
  3. map the legacy ``amount_semantics`` enum onto the canonical 2-axis taxonomy
     (``value_kind`` + ``realisation_tier``) the rest of procurement uses;
  4. attach the CRO company match (same matcher as eTenders/TED — join the already-normalised
     supplier name to data/silver/cro/companies.parquet).

PRIVACY (owner decision 2026-06-06, see PROCUREMENT_MASTER.md §6): suppliers are NAMED,
including sole traders / individuals, because the source documents are official published
PO/payments-over-€20k lists (Circular 07/2012 / FOI) — re-surfacing name+amount+description is
not a new disclosure. We carry the original ``supplier_class`` / ``privacy_status`` columns for
transparency but DO NOT suppress rows. The facts hold no address/PII beyond what is published.

VALUE IS NOT INTERCHANGEABLE: ``po_committed`` (ordered) and ``payment_actual`` (paid) are
different lifecycle tiers — never summed together, and only ``value_safe_to_sum`` rows sum even
within a tier. The views and page enforce one tier per section.

Writes data/gold/parquet/procurement_payments_fact.parquet (+ a coverage JSON). Gold parquets
are gitignore-negated already (!data/gold/parquet/*.parquet), so the output is tracked.
"""

from __future__ import annotations

import contextlib
import csv
import json
import re
import sys
import unicodedata
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "extractors"))
from _publisher_regime import regime_for  # noqa: E402

from extractors._paid_flag_clean import clean_paid_flag  # noqa: E402
from services import parse_qa  # noqa: E402
from services.data_contracts import guard_payment_fact, reconciliation_violations  # noqa: E402
from services.deflator import value_plausible_expr  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402
from shared.name_norm import name_norm_expr  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

SILVER = ROOT / "data/silver/parquet"
CRO = ROOT / "data/silver/cro/companies.parquet"
OUT = ROOT / "data/gold/parquet/procurement_payments_fact.parquet"
OUT_COV = ROOT / "data/_meta/procurement_payments_fact_coverage.json"
# Row floor for the consolidated gold fact (423,989 rows 2026-06-28 after disclosed_bq_po new-bodies
# Tranche 1+2 + HSE history). Consolidate unions the upstream silver facts; if one came in truncated
# (its own floor was bypassed, or a non-floored source emptied), gold should not be rebuilt smaller.
# ~30% headroom; force a deliberate small rebuild with DAIL_SKIP_ROW_FLOOR=1. Raise again when a
# future tranche/lane lands (plan §11g).
MIN_FACT_ROWS = 295_000
# Hand-curated supplier-class overrides (firms/foreign/semi-states the regex+CRO can't resolve).
# Only sum-neutral classes (company/foreign_company); transfer bodies live in a separate review CSV.
CLASS_OVERRIDES = ROOT / "data/_meta/procurement_supplier_class_overrides.csv"

# The 31 local authorities' PO/Payments-over-€20k fact (silver, canonical taxonomy already) —
# folded in via _load_la_fact() rather than SOURCE_FACTS (different layer + native value_kind).
LA_FACT = SILVER / "la_payments_fact.parquet"

# The per-publisher facts to fold in. All share the 28-column schema; none overlap.
SOURCE_FACTS = [
    "public_payments_fact.parquet",
    "hse_tusla_payments_fact.parquet",
    "nta_payments_fact.parquet",
    "nphdb_payments_fact.parquet",
    "seai_payments_fact.parquet",
    # Bespoke reading-order parse of the 3 single-column dept PDFs the generic reader can't split
    # (DFAT payment / Justice + Transport PO), built by
    # extractors/procurement_dept_readingorder_parser.py. Identical 29-col schema, no
    # publisher overlap with the above. Adds ~€2.35bn sum-safe (Justice €1.47bn, Transport €486m,
    # DFAT €394m) — incl. BearingPoint/Accenture/Capita consultancy + asylum-accommodation providers.
    "dept_readingorder_payments_fact.parquet",
    # DISCLOSED national PO/payments-over-€20k BigQuery extract — GENUINELY-NEW bodies lane
    # (extractors/disclosed_bq_po_newbodies_extract.py, registry data/_meta/procurement_disclosed_bodies.csv).
    # Tranche 1 = 8 county councils (6 already in the LA SCHEMA_MAP but with a broken live harvest →
    # 0 gold rows; Tipperary+Louth verified) + An Garda + EPA + Louth&Meath ETB, ALL po_committed,
    # regime source-authoritative. Disjoint from every other lane (asserted in the extractor AND in
    # main() below). NOTE this is SEPARATE from the HSE-history sidecar (disclosed_bq_po_payments_fact),
    # which folds into hse_tusla above so ie_hse stays single-source.
    "disclosed_bq_po_newbodies_fact.parquet",
]

# Publishers known to publish VAT-INCLUSIVE figures (mixing bases would corrupt any
# cross-publisher total). Everything else is left 'unknown' rather than assumed exclusive —
# honest, and the "never sum across differing vat_status" rule then holds by default.
VAT_INCLUSIVE_PUBLISHERS = {
    "Health Service Executive",
    "Tusla – Child and Family Agency",
}

# amount_semantics (legacy single enum) → canonical 2-axis taxonomy.
SEMANTICS_TO_KIND = {
    "payment_actual": ("payment_actual", "SPENT"),
    "po_committed": ("po_committed", "COMMITTED"),
}


# Leading reference tokens that bled into the supplier column from certain council parsers — a
# published row index, a date, a PO/reference number, an Excel sci-notation of a big number, or a
# "####" mask — e.g. "36 Ward Bros Plant Hire", "03-607 O'Shaughnessy and Associates",
# "2.4E+08 349849 Patrick Mc Caffrey & Sons Ltd". They fragment one firm across dozens of distinct
# normalised keys and break the CRO match. ⚠️ APPLIED ONLY IN THIS PAYMENTS LANE, never in shared
# name_norm: eTenders / TED leading-number names are real brands ("3M", "53 Degrees Design",
# "247meeting") that are either FUSED or never carry a row index, so a blanket strip would corrupt
# them — but no such space-separated number-brand exists in the council payment data (verified).
_LEAD_REF_RE = re.compile(
    r"^(?:\s*(?:"
    r"\d{1,2}[/.\-]\d{1,2}[/.\-]\d{2,4}"  # date (02/08/2023)
    r"|\d+(?:[.,]\d+)?[eE][+\-]?\d+"  # Excel sci-notation (2.4E+08)
    r"|#+"  # masked digits (####)
    r"|\d{1,6}(?:[-/]\d+)*"  # row index / dash-joined ref (36, 03-607)
    r"|[A-Za-z]{3,4}\d{3}"  # Pobal scheme code (CMO005, CER002, CBN001) prefixing the real vendor
    r"|\d{3}[A-Za-z]{2}(?:-\d+)?"  # ETB accounting code (LOETB '020OF', '143AP', '020IT-213'); 3
    # digits exactly so a real 2-digit-prefixed name ('24HR CARE SERVICES') is never stripped
    r")\s+)+",
    re.I,
)


def _strip_leading_ref(name: str | None) -> str | None:
    """Remove a leading run of bled-in reference tokens (see ``_LEAD_REF_RE``). Conservative: the
    strip is taken only when a real alphabetic name (≥2 letters) remains — otherwise the value was
    a pure-number / footnote junk row, left untouched for the existing junk filters."""
    if not name:
        return name
    m = _LEAD_REF_RE.match(name)
    if not m:
        return name
    rest = name[m.end() :]
    return rest if re.search(r"[A-Za-z]{2,}", rest) else name


# Evidence-based merges of ONE legal entity that a publisher splits across several published name
# forms (same philosophy as the NBI rule in procurement_public_body_extract.py: no name fabrication —
# only collapses strings already in the data, guarded by the paying body). Each rule = (publisher
# substring, name regex, canonical name). Add a row when a high-value entity is found fragmented.
# AIRBUS: the Dept of Defence publishes the C295 maritime-patrol-aircraft supplier (Ireland's
# largest-ever DF equipment project, ~€235m) as "AIRBUS DEFENCE & SPACE SAU SPAIN", "DEFENCE & SPACE
# SAU SPAIN" AND "& SPACE SAU SPAIN" — 3 keys for one firm. All share "SPACE SAU"; the "&" drops in
# normalisation and the leading words vary. Collapse to the registered form (ends in the SAU legal
# form, so the foreign-form classifier then recognises it). SAS (the French Airbus entity) is left
# separate — a different legal person, already classed foreign_company.
_ENTITY_MERGES = [
    ("Defence", r"(?i)\bspace\s*&?\s*sau\b", "Airbus Defence and Space SAU"),
    # Ernst & Young: published as "Ernst & Young Business Advisory", "Ernst and Young Business
    # Advisory" AND the orphaned tail "& Young"/"and Young" (the "Ernst &" cut at the column edge).
    # Body-agnostic (EY is paid by many depts). The orphan is matched only as the WHOLE name so a
    # firm legitimately containing "young" mid-string is untouched.
    ("", r"(?i)ernst.{0,8}young|^\s*(?:and|&)\s+young\s*$", "Ernst and Young"),
]


def _canonicalise_split_entities(df: pl.DataFrame) -> pl.DataFrame:
    """Collapse the curated split-entity name variants (``_ENTITY_MERGES``) BEFORE normalisation, so
    one firm stops fragmenting across keys. Guarded by the paying body to avoid an over-broad merge."""
    if df.is_empty() or "publisher_name" not in df.columns:
        return df
    expr = pl.col("supplier_raw")
    for body, rx, canon in _ENTITY_MERGES:
        body_ok = pl.lit(True) if body == "" else pl.col("publisher_name").str.contains(body)
        expr = pl.when(body_ok & pl.col("supplier_raw").str.contains(rx)).then(pl.lit(canon)).otherwise(expr)
    return df.with_columns(expr.alias("supplier_raw"))


def _clean_supplier_names(df: pl.DataFrame) -> pl.DataFrame:
    """Strip the bled-in leading reference tokens from the published supplier name and recompute the
    normalised join key, so one firm stops fragmenting across many keys (and so the cleaned key now
    also matches the same firm's eTenders/CRO form). Runs before the CRO match + reclassification so
    both benefit from the de-fragmented key."""
    if df.is_empty():
        return df
    before = df["supplier_normalised"].n_unique()
    df = df.with_columns(
        pl.col("supplier_raw").map_elements(_strip_leading_ref, return_dtype=pl.Utf8).alias("supplier_raw")
    ).with_columns(name_norm_expr("supplier_raw").alias("supplier_normalised"))
    after = df["supplier_normalised"].n_unique()
    if after < before:
        print(f"  cleaned leading row-index/ref prefixes: {before - after:,} fewer distinct supplier keys")
    return df


# Per-source (publisher set, rows, summed €) captured at LOAD time, so the consolidation
# can AUDIT (reconcile) its own output before publishing — see _reconcile() in main().
# {source_label: {"publishers": frozenset, "rows": int, "eur": float}}
_SOURCE_STATS: dict[str, dict] = {}


def _drop_unattributable(df: pl.DataFrame, label: str) -> pl.DataFrame:
    """Drop rows with NO supplier, NO description AND NO po_number.

    Such rows carry zero identifying information — they are period/section TOTAL or page-furniture
    lines the parsers emitted as amount-only rows (DQ audit garble scan 2026-06: ie_opw €155.78m,
    ie_prisons €74.0m annual totals; dept_social_protection/dept_health/ie_ntma quarterly totals;
    501 rows / €857.8m in all). They were already value_safe_to_sum=False, so dropping them changes
    NO summable total — it only removes un-attributable junk that otherwise shows as a huge
    "payment to (no payee)" when browsing a publisher. Applied BEFORE _capture_stats so the
    source-fact reconciliation baseline is computed post-drop (stays exact). Logged per source.
    """

    def _blank(c: str) -> pl.Expr:
        return pl.col(c).is_null() | (pl.col(c).cast(pl.Utf8).str.strip_chars() == "")

    # A description that is ONLY the bled amount ('€ 2,532,553.23', nothing else) carries no info —
    # treat it as blank here so the row is dropped at load (before the stats baseline), rather than
    # surviving until _strip_bled_amount blanks it post-baseline and leaks an all-blank row to gold.
    _lead = (
        pl.col("description")
        .str.extract(r"^[€$£]?\s*([0-9][0-9,]*\.?[0-9]{0,2})", 1)
        .str.replace_all(",", "")
        .cast(pl.Float64, strict=False)
    )
    _resid = (
        pl.col("description").cast(pl.Utf8).str.replace(r"^[€$£]?\s*[0-9][0-9,]*\.?[0-9]{0,2}\s*", "").str.strip_chars()
    )
    _desc_is_just_amount = _lead.is_not_null() & ((_lead - pl.col("amount_eur")).abs() < 1) & (_resid == "")
    desc_blank = _blank("description") | _desc_is_just_amount

    mask = _blank("supplier_raw") & desc_blank & _blank("po_number")
    n = df.filter(mask).height
    if n:
        eur = float(df.filter(mask)["amount_eur"].sum() or 0.0)
        print(
            f"    dropped {n} unattributable rows from {label} (€{eur / 1e6:.1f}m, blank supplier+desc+po, non-summable)"
        )
        df = df.filter(~mask)
    return df


def _strip_bled_amount(df: pl.DataFrame) -> pl.DataFrame:
    """Remove a leading currency+amount that bled into ``description`` — but ONLY when that number
    equals ``amount_eur`` (so it is provably the amount, not a real spec like '70% Bitumen Emulsion'
    or a code prefix). Several publishers' layouts duplicate the amount into the description column
    (DQ audit garble scan 2026-06: dept_education '€80,000,000.00 Third Level Building…', ie_la_meath,
    dept_defence — 5,788 rows). amount_eur is unchanged; this only cleans the display text (and so
    feeds a cleaner spend_category). Runs before _derive_spend_category.
    """
    if "description" not in df.columns:
        return df
    lead = pl.col("description").str.extract(r"^[€$£]?\s*([0-9][0-9,]*\.?[0-9]{0,2})", 1)
    lead_num = lead.str.replace_all(",", "").cast(pl.Float64, strict=False)
    safe = lead_num.is_not_null() & ((lead_num - pl.col("amount_eur")).abs() < 1)
    n = int(df.select(safe.sum()).item() or 0)
    if n:
        df = df.with_columns(
            pl.when(safe)
            .then(pl.col("description").str.replace(r"^[€$£]?\s*[0-9][0-9,]*\.?[0-9]{0,2}\s*", ""))
            .otherwise(pl.col("description"))
            .alias("description")
        )
        print(f"  stripped bled amount from {n:,} descriptions (amount preserved; cleaner category text)")
    return df


def _capture_stats(label: str, df: pl.DataFrame) -> None:
    _SOURCE_STATS[label] = {
        "publishers": frozenset(df["publisher_id"].unique().to_list()),
        "rows": df.height,
        "eur": float(df["amount_eur"].sum() or 0.0),
    }


def _load_facts() -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    base_cols: set[str] | None = None
    for fname in SOURCE_FACTS:
        path = SILVER / fname
        if not path.exists():
            print(f"  WARN missing fact, skipped: {fname}")
            continue
        df = pl.read_parquet(path)
        # Fold the disclosed-BigQuery HSE history INTO the hse/tusla source (not a separate
        # SOURCE_FACTS entry) so ie_hse stays single-source: the reconciliation audit keys on each
        # source's DISJOINT publisher set, so the same publisher across two sources would
        # double-count (actual > expected → halt). The disclosed HSE periods are verified disjoint
        # from the PDF parse (2017-Q3..2020-Q2 + 2025-Q4 + 2026-Q1, absent from hse_tusla), so this
        # is a pure period-backfill of the body we already publish. See doc/DISCLOSED_PO_INTEGRATION_PLAN.md.
        if fname == "hse_tusla_payments_fact.parquet":
            _sidecar = SILVER / "disclosed_bq_po_payments_fact.parquet"
            if _sidecar.exists():
                _extra = pl.read_parquet(_sidecar)
                if set(_extra.columns) == set(df.columns):
                    _extra = _extra.select(df.columns).cast(dict(df.schema))  # all-null cols read back as Null type
                    df = pl.concat([df, _extra], how="vertical")
                    print(f"    + folded disclosed HSE history into hse_tusla: {_extra.height:,} rows")
                else:
                    print("    WARN disclosed HSE sidecar schema mismatch, NOT folded")
            elif OUT.exists():
                # Carry-forward (same philosophy as the LA listing-rot guard): the raw drop + its
                # gitignored silver are absent (e.g. a cloud run), so regenerate-from-source is
                # impossible — but the disclosed HSE history is immutable and already in gold. Keep
                # it instead of silently dropping it on the rebuild.
                _prior = pl.read_parquet(OUT).filter(pl.col("parser_name") == "disclosed_bq_po")
                if _prior.height and set(df.columns) <= set(_prior.columns):
                    _prior = _prior.select(df.columns).cast(dict(df.schema))
                    df = pl.concat([df, _prior], how="vertical")
                    print(
                        f"    ! carry-forward disclosed HSE history (sidecar absent): kept {_prior.height:,} gold rows"
                    )
        if base_cols is None:
            base_cols = set(df.columns)
        elif set(df.columns) != base_cols:
            raise SystemExit(f"schema drift in {fname}: +{set(df.columns) - base_cols} -{base_cols - set(df.columns)}")
        df = _drop_unattributable(df, fname)
        frames.append(df)
        _capture_stats(fname, df)
        print(f"  + {fname:38} {df.height:>7,} rows")
    if not frames:
        raise SystemExit("no payment facts found under data/silver/parquet/")
    return pl.concat(frames, how="vertical")


def _canon_la_publisher_names(df: pl.DataFrame) -> pl.DataFrame:
    """Force every local_authority publisher_name onto the CANONICAL council spelling.

    THE BUG THIS FIXES (2026-07-14). Two lanes feed councils into gold with DIFFERENT spellings:
      * la_payments_fact          → 23 councils, SHORT ("Dublin City", "Carlow")
      * disclosed_bq_po_newbodies →  8 councils, FORMAL ("Dublin City Council", "Carlow County
        Council") — and these are precisely the 8 the LA lane could never harvest
        (Carlow/Cavan/Roscommon = Playwright, Kerry = scanned; Dublin City/DLR/Louth/Tipperary
        publish no PO list). So they are complementary coverage, not duplicates — but the
        spelling split silently ORPHANED them: 69,715 rows (41% of all LA payments, incl. Dublin
        City's 40,431) failed to join `constituency_la_crosswalk` AND the AFS accounts fact,
        whose join key is `payments.publisher_name == afs.council`. Dublin City — the largest
        council — was therefore missing its audited-accounts lane on the council dossier entirely.

    The canonical spelling is the crosswalk's `local_authority` (also used by la_afs_divisions,
    la_budget_divisions, v_la_chief_executives, the LPT fact and the councillor facts). Applied
    ONCE here, at the last choke point before the gold write, so no lane can reintroduce a
    variant. Fails loudly on an unmappable council rather than silently orphaning it again.
    """
    if "publisher_type" not in df.columns:
        return df
    xwalk = ROOT / "data" / "_meta" / "constituency_la_crosswalk.csv"
    canon = sorted({r["local_authority"] for r in csv.DictReader(xwalk.open(encoding="utf-8"))})

    def _fold(s: str) -> str:
        s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
        s = re.sub(r"\b(county|city|and|&|council|co)\b", " ", s)
        return re.sub(r"[^a-z]+", "", s)

    by_fold = {_fold(c): c for c in canon}
    present = (
        df.filter(pl.col("publisher_type") == "local_authority")["publisher_name"].unique().to_list()
    )
    mapping = {p: by_fold[_fold(p)] for p in present if p and _fold(p) in by_fold and by_fold[_fold(p)] != p}
    unmapped = [p for p in present if p and _fold(p) not in by_fold]
    if unmapped:
        raise SystemExit(f"local_authority publishers not in the canonical 31: {sorted(unmapped)}")
    if mapping:
        print(f"  canonicalised {len(mapping)} council name(s) → crosswalk spelling: {sorted(mapping)}")
        df = df.with_columns(
            pl.when(pl.col("publisher_type") == "local_authority")
            .then(pl.col("publisher_name").replace(mapping))
            .otherwise(pl.col("publisher_name"))
            .alias("publisher_name")
        )
    return df


def _load_la_fact(base: pl.DataFrame) -> pl.DataFrame | None:
    """Conform the local-authority fact to the base 28-col schema so it can
    concat. The LA fact was built on the canonical taxonomy (value_kind ∈ {po_committed,
    payment_actual}); that vocabulary is identical to the base's legacy ``amount_semantics``, so
    the map is lossless and _conform then re-derives value_kind/realisation_tier consistently.

    Privacy: the LA fact carries its own quarantine vocab (public/quarantined); we remap it to
    the consolidated fact's (ok/review_personal_data) and keep the columns as transparency
    metadata. No rows are suppressed — the gold view names suppliers either way (the established
    owner decision), and the LA source is the council's own published over-€20k list.
    """
    if not LA_FACT.exists():
        print("  WARN local-authority fact absent — councils not folded in")
        return None
    la = pl.read_parquet(LA_FACT)
    n, n_la = la.height, la["publisher_name"].n_unique()
    la = la.with_columns(
        pl.col("value_kind").alias("amount_semantics"),  # same vocab; _conform re-derives the 2 axes
        pl.lit("extracted").alias("extraction_status"),
        pl.lit("high").alias("extraction_confidence"),  # reconcile-gated parse, no OCR
        pl.lit(False).alias("caveat_text_detected"),  # LA caveat is config-level (source_caveat), not doc-detected
        pl.col("privacy_status")
        .replace({"quarantined": "review_personal_data", "public": "ok"})
        .alias("privacy_status"),
    )
    missing = set(base.columns) - set(la.columns)
    if missing:
        raise SystemExit(f"LA fact cannot conform — missing base columns: {sorted(missing)}")
    la = la.select(base.columns).cast(dict(base.schema))
    la = _drop_unattributable(la, "la_payments_fact.parquet")
    # Capture BEFORE the carry-forward append, keyed by the silver LA publisher set — the
    # carried rows belong to a council ABSENT from silver, so reconciling gold by this set is
    # exact and needs no carry-forward fudge (the carried rows are verbatim copies anyway).
    _capture_stats("la_payments_fact.parquet", la)
    print(f"  + la_payments_fact.parquet (silver)    {n:>7,} rows  [{n_la} local authorities]")
    # Listing-rot guard: a council whose site newly blocks the harvester (bot-wall, moved
    # listing) must not vanish from gold — its published over-€20k disclosures are immutable
    # history (Waterford/Wicklow grew JS challenges 2026-06; Wicklow has no bronze cache to
    # replay). Carry such a council's rows forward from the existing gold fact, loudly.
    if OUT.exists():
        gold = pl.read_parquet(OUT)
        # A council already provided by ANOTHER lane THIS run is NOT "gone": the
        # disclosed_bq_po_newbodies lane owns the 6 LA-config councils whose live harvest is broken
        # (Dublin City, DLR, Cavan, Kerry, Roscommon, Carlow) + Tipperary/Louth, and those rows are
        # already in `base`. Without this exclusion the LA carry-forward would resurrect them from
        # the prior gold AND the disclosed lane would fold them again = double-count (the reconcile
        # then trips on the +rows drift on the SECOND rebuild). A genuine LA-lane listing-rot council
        # is in neither the LA silver NOR base, so it is still carried forward as before.
        owned_elsewhere = set(base["publisher_id"].unique())
        gone = (
            set(gold.filter(pl.col("publisher_type") == "local_authority")["publisher_id"].unique())
            - set(la["publisher_id"].unique())
            - owned_elsewhere
        )
        if gone:
            carried = (
                gold.filter(pl.col("publisher_id").is_in(sorted(gone))).select(base.columns).cast(dict(base.schema))
            )
            print(
                f"  ! listing-rot carry-forward: {sorted(gone)} absent from silver — kept {carried.height:,} gold rows"
            )
            la = pl.concat([la, carried], how="vertical")
    return la


# PAGE-FURNITURE rows: a publisher's page heading ("Purchase Orders over €20,000", "Notice on
# publication of Purchase Orders…") misparsed as a SUPPLIER row carrying the literal threshold as
# its amount — a fake payment that inflates the total. Excluded from value_safe_to_sum. Deliberately
# NARROW: it does NOT catch pure-number names or anonymised/aggregate labels ("IT Service Provider",
# "Sundry Supplier") — those are REAL spend to a real (un-named) vendor, and de-summing them would
# UNDERSTATE the total (the opposite error). They stay summable; they are merely un-rankable.
# Matched on the lowercased, punctuation-stripped supplier_normalised. NOT a bare "quarter"
# ("Abbey Quarter Development" is a real place); the header rows always carry "20 000"/"over 20"/
# "purchase order" too, so they are still caught.
_JUNK_SUPPLIER_RE = (
    r"(?i)\b20 ?000\b|\bover 20\b|purchase order|payments (over|greater)"
    r"|publication purchase|notice on publication|council herewith publishes|in terms circular"
)


def _conform(df: pl.DataFrame) -> pl.DataFrame:
    # value_kind + realisation_tier from amount_semantics (canonical 2-axis taxonomy)
    kind = pl.col("amount_semantics").replace_strict({k: v[0] for k, v in SEMANTICS_TO_KIND.items()}, default="unknown")
    tier = pl.col("amount_semantics").replace_strict({k: v[1] for k, v in SEMANTICS_TO_KIND.items()}, default="UNKNOWN")
    vat = (
        pl.when(pl.col("publisher_name").is_in(list(VAT_INCLUSIVE_PUBLISHERS)))
        .then(pl.lit("incl_vat"))
        .otherwise(pl.lit("unknown"))
    )
    # Sum-safe invariant enforced at the fold (defense-in-depth — the last common chokepoint; never
    # trust a source fact's flag blindly). A row is summable only if it was flagged safe AND:
    #   • supplier_class != public_body — a public_body RECIPIENT is an intergovernmental transfer /
    #     council-as-payee (TII Road Grants central→council; LA payments to Irish Water, ETBs, other
    #     councils), NOT procurement spend; summing it double-counts the council→contractor leg.
    #   • supplier_normalised is non-blank — a row whose supplier normalised to empty (e.g.
    #     "& COMPANY", "(IRELAND) LTD", or a category/subtotal line) is never identifiable spend.
    #     Catches pre-guard rows a source parser left flagged safe (e.g. 53 stale LA rows / €8.7m,
    #     DQ 2026-06-13). NO €-cap here on purpose: NTA BusConnects (€140.6m) and the children's-
    #     hospital payments (€107.6m) legitimately exceed €100m; the per-source extractors own that
    #     cap for the bodies where a ≥€100m line can only be a parse error.
    #   • supplier is not a JUNK / aggregate placeholder — a cell that is just a number (an amount
    #     leaked into the supplier column), a page-header/threshold fragment ("… over €20,000 -
    #     Quarter"), or an un-named bucket ("Sundry/External/Various Supplier") is not identifiable
    #     spend. These are RETAINED + hidden but excluded from totals (DQ 2026-06-13b, ~€49m).
    # Rows stay in the fact, visible; they are merely excluded from spend totals.
    safe = (
        pl.col("value_safe_to_sum")
        & (pl.col("supplier_class") != "public_body")
        & pl.col("supplier_normalised").is_not_null()
        & (pl.col("supplier_normalised").str.strip_chars() != "")
        & ~pl.col("supplier_normalised").fill_null("").str.contains(_JUNK_SUPPLIER_RE)
    )
    # Privacy-flag invariant (re-derived, never trusted): public_display must be False for
    # any likely natural person. The base extractor enforces this at write time, but bespoke
    # sandbox parsers have drifted (nta/nphdb/seai reading_order parsers set
    # privacy_status='review_personal_data' yet left public_display=True — 830 such rows
    # reached gold before 2026-06-11, and the base view's public_display gate made them
    # visible). The fold is the last common chokepoint, so the rule lives here too.
    display = (
        pl.col("public_display")
        & (pl.col("supplier_class") != "sole_trader_or_individual")
        & (pl.col("privacy_status") != "review_personal_data")
    )
    return df.with_columns(
        kind.alias("value_kind"),
        tier.alias("realisation_tier"),
        vat.alias("vat_status"),
        safe.alias("value_safe_to_sum"),
        display.alias("public_display"),
    )


def _attach_regime(df: pl.DataFrame) -> pl.DataFrame:
    """Attach the publisher's DISCLOSURE REGIME (see extractors/_publisher_regime.py): the legal
    basis, threshold, threshold-VAT and procurement legal-class. Keyed by (publisher_id,
    publisher_type), so it backfills the gold fact WITHOUT re-running any source extractor. This
    is what lets the UI render each body's real basis/threshold instead of one hard-coded label —
    e.g. CHI €25k incl-VAT, ESB Networks as a utility/contracting-entity outside the €20k scheme."""
    keys = df.select("publisher_id", "publisher_type").unique()
    rows = [
        {
            "publisher_id": k["publisher_id"],
            "publisher_type": k["publisher_type"],
            **regime_for(k["publisher_id"], k["publisher_type"]),
        }
        for k in keys.to_dicts()
    ]
    reg = pl.DataFrame(rows).with_columns(pl.col("disclosure_threshold_eur").cast(pl.Int64))
    out = df.join(reg, on=["publisher_id", "publisher_type"], how="left")
    n_basis = out["disclosure_basis"].n_unique()
    n_thr = out["disclosure_threshold_eur"].n_unique()
    print(f"  attached disclosure regime: {n_basis} distinct bases, {n_thr} distinct thresholds")
    return out


def _attach_cro(df: pl.DataFrame) -> pl.DataFrame:
    if not CRO.exists():
        print("  WARN CRO register absent — payments will carry no company match")
        return df.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("cro_company_num"),
            pl.lit(None, dtype=pl.Utf8).alias("cro_company_status"),
        )
    cro = (
        pl.read_parquet(CRO)
        .select(["name_norm", "company_num", "company_status"])
        .filter(pl.col("name_norm").str.len_chars() >= 4)
        .unique(subset=["name_norm"])
    )
    # Match EVERY row on the normalised name (not only regex-classed companies). An exact
    # name_norm match to a CRO registration is a hard identifier the name-suffix regex lacks —
    # _upgrade_class_from_cro uses it to reclassify suffix-less companies (e.g. "Duggan Bros",
    # "Byrne Wallace Solicitors") the regex mis-binned as sole traders. The match is carried for
    # any row; the reclassification + privacy re-derivation happen in the next step.
    return df.join(cro, left_on="supplier_normalised", right_on="name_norm", how="left").rename(
        {"company_num": "cro_company_num", "company_status": "cro_company_status"}
    )


# A suffix-less company name is mis-binned as a sole trader by the name-suffix regex in the
# source parsers (which only catch "Ltd"/"Limited"/… and a few keywords, and differ between the
# folded facts). The consolidation is the last common chokepoint, so a UNIFORM reclassification
# lives here, on two signals that cannot be a lone private individual:
#   1. CRO exact match — the normalised name equals a registered company's name_norm (a hard
#      identifier the regex lacks). Length-floored to skip 4-char parsing fragments ("& BOYD"
#      -> "BOYD"); a genuine individual would have to share a normalised name with a registered
#      company to flip, which is rare and, even then, the over-€20k lists already publish the name.
#   2. Firm-indicator word — a legal form, a plurality word, or a business-activity STEM that a
#      lone private individual's name never carries (Consulting Engineers / Architects / Software /
#      Medical / Insurance / Asset Management / Distributors …). Mirrors the source COMPANY_SUFFIX
#      vocabulary, applied uniformly across every folded fact. STEMS match inflections (leading
#      \b only, no trailing \b) so "engineerS"/"consultING"/"technologY" all hit — the original
#      whole-word-only list ("contractors", "developments") missed the singular/other inflections
#      and left ~€5.8bn of obvious firms (Arup Consulting Engineers, RPS, Version 1 Software,
#      Alliance Medical, …) quarantined as sole traders. Expanded 2026-06-13.
# value_safe_to_sum is unaffected (it never depended on sole-trader class: both sole_trader and
# company are summable — this only flips the privacy/display flag, never a spend total).
_CRO_UPGRADE_MIN_LEN = 5
# Whole-word legal/plurality forms (need both boundaries so short tokens don't over-match inside
# a surname) | business-activity stems (leading boundary only → inflection-safe).
_FIRM_WORDS = (
    "ltd|limited|dac|plc|clg|llp|teo|teoranta|gmbh|inc|llc|srl|sarl|bros|brothers|son|sons|group|"
    "cuideachta|co|jv|ppp|ulc"  # co = truncated "& Co"; jv = joint venture; ppp = PPP vehicle;
    # ulc = Irish Unlimited Company (Harrington Concrete & Quarries ULC); son (whole-word) = "& Son"
    # — NOT a stem, so it can't match "Sonia"/"Sonny" inside a forename
)
_FIRM_STEMS = (
    "consult|engineer|engine|architect|surveyor|solicitor|solrs|barrister|accountant|advis|"
    "contract|construct|develop|enterprise|industr|technolog|system|software|servic|solution|"
    "logistic|distribut|manufactur|pharma|biotech|diagnostic|laborator|healthcare|medical|"
    "insuranc|assuranc|management|communicat|telecom|propert|holding|internation|institut|"
    "foundation|partner|associat|incorporat|corporat|recruit|"
    # truncation-tolerant: source column-width cuts a legal/activity word mid-string
    # ("...LIMITE[D]", "...BUILDER[S]", "...ENGINE[ERING]"). "limit" covers limited/limite/limit;
    # "build" covers builders/building; "ventur" covers (joint) venture(s); "solrs" = solicitors.
    "limit|build|ventur"
)
# Foreign legal forms — anchored at the END (or before a trailing country word, since defence/EU
# suppliers publish as "… SAU SPAIN", "… B.V. NETHERLANDS"), because the abbreviations collide with
# English words ("as"/"sa"/"ab") mid-name. supplier_normalised has punctuation stripped, so
# "B.V."→"b v", "S.A."→"s a", "SP.J"→"sp j". AS=Norwegian Aksjeselskap, BV/NV=Dutch, OY=Finnish,
# SAU/SA/SL=Spanish, GMBH=German, SARL=French.
_FOREIGN_COUNTRY = (
    r"spain|france|germany|deutschland|deutchland|netherlands|italy|belgium|norway|sweden|denmark|"
    r"finland|portugal|austria|poland|czech|switzerland|luxembourg"
)
_FOREIGN_FORM_RE = (
    r"(?:\b(?:as|asa|oy|oyj|ab|bv|nv|sau|sarl|srl|spa|sl|gmbh|aps|sa|b v|n v|s a|s l|a s|sp j))"
    r"(?:\s+(?:" + _FOREIGN_COUNTRY + r"))?\s*$"  # "a s" = Danish A/S (Bavarian Nordic A/S)
)
_ORG_FORM_RE = r"(?i)(?:\b(?:" + _FIRM_WORDS + r")\b|\b(?:" + _FIRM_STEMS + r")|" + _FOREIGN_FORM_RE + r")"


def _reclassify_missed_companies(df: pl.DataFrame) -> pl.DataFrame:
    """Reclassify sole_trader_or_individual rows that are demonstrably firms (exact CRO match, or
    an organisation-form word in the name) to ``company``, re-deriving the privacy flags so the
    upgraded rows become displayable. Conservative by design: only signals that cannot denote a
    lone private individual flip a row."""
    if "cro_company_num" not in df.columns:
        return df
    is_sole = pl.col("supplier_class") == "sole_trader_or_individual"
    cro_match = pl.col("cro_company_num").is_not_null() & (
        pl.col("supplier_normalised").str.len_chars() >= _CRO_UPGRADE_MIN_LEN
    )
    org_form = pl.col("supplier_normalised").fill_null("").str.contains(_ORG_FORM_RE)
    upgrade = is_sole & (cro_match | org_form)
    n_cro = df.filter(is_sole & cro_match).height
    n_org = df.filter(is_sole & org_form & ~cro_match).height
    n_sup = df.filter(upgrade)["supplier_normalised"].n_unique()
    if n_cro or n_org:
        print(
            f"  reclassified {df.filter(upgrade).height:,} rows ({n_sup:,} suppliers) "
            f"sole_trader_or_individual -> company [{n_cro:,} CRO-match, {n_org:,} org-form word]"
        )
    # Final class after the upgrade; the CRO number is only ever carried on a company-class row
    # (invariant: a CRO match implies company). A below-floor sole-trader that coincidentally hit a
    # short company name is NOT upgraded, so its speculative match is dropped rather than left as a
    # contradictory "sole trader with a company number".
    new_class = pl.when(upgrade).then(pl.lit("company")).otherwise(pl.col("supplier_class"))
    is_company = new_class == "company"
    return df.with_columns(
        new_class.alias("supplier_class"),
        pl.when(upgrade).then(pl.lit("ok")).otherwise(pl.col("privacy_status")).alias("privacy_status"),
        pl.when(upgrade).then(pl.lit(True)).otherwise(pl.col("public_display")).alias("public_display"),
        pl.when(is_company).then(pl.col("cro_company_num")).otherwise(None).alias("cro_company_num"),
        pl.when(is_company).then(pl.col("cro_company_status")).otherwise(None).alias("cro_company_status"),
    )


# ── Sole-trader contractors (owner decision 2026-06-13e) ─────────────────────────────────────────
# A named individual the State pays >€20k for GOODS/SERVICES is a sole-trader BUSINESS, published by
# the State under the over-€20k transparency regime — hiding them is a transparency gap (a Ltd
# builder's council contracts are visible but a sole-trader builder's identical ones are not). So a
# still-quarantined supplier is surfaced as a displayable ``sole_trader`` IFF (supplier-level):
#   • at least one payment carries a COMMERCIAL purpose (construction / maintenance / professional /
#     trade services / materials / supply), AND
#   • NO payment touches a PRIVATE matter — land/CPO/property purchase, rent to a private landlord,
#     or a personal grant/care/allowance. Any single private-category row keeps the WHOLE supplier
#     hidden (a person who is both a contractor AND sold land under CPO must not be exposed).
# Source-grounded + no-inference: the gate reads the publisher's OWN published purpose text only.
# Uncategorised-only suppliers stay hidden (can't confirm commercial). value_safe_to_sum unchanged.
_PRIVATE_CAT_RE = (
    r"(?i)\bland\b|\bcpo\b|compulsory|dwelling|\bpropert|house purchase|site purchase|land purchase|"
    r"acquisition|roadwidening|\brent\b|leasehold|operating lease|"
    r"\bgrant|aftercare|res care|residential care|\ballowance|refund|compensation|section 58|"
    r"lodging|fostering|bursary|scholarship|mileage|expenses claim|croi conaithe|top up|"
    r"counsel|own solicitor"  # the State paying a PRIVATE party's own legal costs (CPO/claims)
)
_COMMERCIAL_CAT_RE = (
    r"(?i)construct|maintenance|\bcontract|\bworks\b|materials|professional fee|\bservices?\b|supply|"
    r"roof|electric|plumb|\bheat|clean|civil|road|bitumen|bitmac|tarmac|plant|fitout|refurb|survey|"
    r"architect|engineer|consult|medical|clinical|nursing|\bgp\b|ict|equipment|hardware|software|"
    r"fencing|painting|sandblast|trade services|haulage|catering|\bsecurity|landscap|forestry"
)


def _surface_sole_trader_contractors(df: pl.DataFrame) -> pl.DataFrame:
    """Reclassify still-quarantined commercial contractors to the displayable ``sole_trader`` class
    (see block above). Supplier-level gate: surface only if some payment is commercial AND none is
    private. Runs after spend_category is derived; never touches company/public_body/id_code rows."""
    if df.is_empty() or "spend_category" not in df.columns:
        return df
    cat = pl.coalesce([pl.col("spend_category"), pl.col("description")]).fill_null("")
    is_sole = pl.col("supplier_class") == "sole_trader_or_individual"
    flags = (
        df.with_columns(
            (is_sole & cat.str.contains(_PRIVATE_CAT_RE)).alias("_priv"),
            (is_sole & cat.str.contains(_COMMERCIAL_CAT_RE)).alias("_comm"),
        )
        .group_by("supplier_normalised")
        .agg(pl.col("_priv").any().alias("_any_priv"), pl.col("_comm").any().alias("_any_comm"))
    )
    surface_keys = flags.filter(pl.col("_any_comm") & ~pl.col("_any_priv"))["supplier_normalised"]
    surface = is_sole & pl.col("supplier_normalised").is_in(surface_keys)
    n = df.filter(surface).height
    if n:
        nsup = df.filter(surface)["supplier_normalised"].n_unique()
        val = df.filter(surface)["amount_eur"].sum()
        print(
            f"  surfaced {n:,} rows ({nsup:,} suppliers, €{val / 1e6:.1f}m) sole_trader_or_individual -> sole_trader (commercial contractors)"
        )
    return df.with_columns(
        pl.when(surface).then(pl.lit("sole_trader")).otherwise(pl.col("supplier_class")).alias("supplier_class"),
        pl.when(surface).then(pl.lit("ok")).otherwise(pl.col("privacy_status")).alias("privacy_status"),
        pl.when(surface).then(pl.lit(True)).otherwise(pl.col("public_display")).alias("public_display"),
    )


# Anonymised payee CODE (OPW pseudonymises some contractors as "JOH260ZZ"/"DUG001ZZ"): a hard ID,
# never a natural person's name. Classed id_code → hidden (anonymised on purpose) but kept summable
# (it IS a real payment). Mirrors the existing id_code class in procurement_la_payments_extract.py.
_ID_CODE_RE = r"(?i)^[a-z]{2,4}\d{2,}[a-z]{0,3}$"


def _classify_id_codes(df: pl.DataFrame) -> pl.DataFrame:
    """Reclassify still-quarantined rows whose supplier is an anonymised id-code to ``id_code``
    (distinct from a natural person), hidden but summable. Runs AFTER the firm reclassifier so a
    code never reaches it; never touches an already-identified company."""
    is_sole = pl.col("supplier_class") == "sole_trader_or_individual"
    is_code = pl.col("supplier_normalised").fill_null("").str.contains(_ID_CODE_RE)
    hit = is_sole & is_code
    n = df.filter(hit).height
    if n:
        print(
            f"  classified {n:,} rows ({df.filter(hit)['supplier_normalised'].n_unique():,} codes) as id_code (anonymised payee)"
        )
    return df.with_columns(
        pl.when(hit).then(pl.lit("id_code")).otherwise(pl.col("supplier_class")).alias("supplier_class"),
        pl.when(hit).then(pl.lit("ok")).otherwise(pl.col("privacy_status")).alias("privacy_status"),
        # id_code stays hidden: an anonymised code is not a useful public identity.
        pl.when(hit).then(pl.lit(False)).otherwise(pl.col("public_display")).alias("public_display"),
    )


def _apply_class_overrides(df: pl.DataFrame) -> pl.DataFrame:
    """Apply the hand-curated supplier-class overrides (CLASS_OVERRIDES): firms / foreign companies /
    commercial semi-states the regex+CRO can't resolve but a human has verified. All override classes
    are summable, so this is SUM-NEUTRAL — it only flips display. A wrong/typo'd key silently no-ops
    (fail-safe). Skipped cleanly if the file is absent."""
    if not CLASS_OVERRIDES.exists():
        return df
    # supplier_normalised is upper-case; the CSV keys are lower-case — join on a case-folded key.
    # truncate_ragged_lines: the free-text `basis` column may contain commas; only the first two
    # columns (key, class) are used, so extra split fields are harmless to drop.
    ov = (
        pl.read_csv(CLASS_OVERRIDES, comment_prefix="#", truncate_ragged_lines=True)
        .select(
            pl.col("supplier_normalised").str.strip_chars().str.to_lowercase().alias("_ovkey"),
            pl.col("override_class").str.strip_chars(),
        )
        .filter(pl.col("_ovkey").str.len_chars() > 0)
        .unique(subset=["_ovkey"])
    )
    df = df.with_columns(pl.col("supplier_normalised").str.to_lowercase().alias("_ovkey")).join(
        ov, on="_ovkey", how="left"
    )
    hit = pl.col("override_class").is_not_null()
    n = df.filter(hit).height
    if n:
        print(
            f"  applied {ov.height:,} curated overrides -> {n:,} rows ({df.filter(hit)['supplier_normalised'].n_unique():,} suppliers) reclassified"
        )
    out = df.with_columns(
        pl.when(hit).then(pl.col("override_class")).otherwise(pl.col("supplier_class")).alias("supplier_class"),
        pl.when(hit).then(pl.lit("ok")).otherwise(pl.col("privacy_status")).alias("privacy_status"),
        pl.when(hit).then(pl.lit(True)).otherwise(pl.col("public_display")).alias("public_display"),
    ).drop(["override_class", "_ovkey"])
    return out


# --------------------------------------------------------------------------- spend_category
# A source-grounded spend category: the publisher's OWN published purpose text (the `description`
# field), canonicalised ONLY for truncation + casing — never re-grouped into an invented taxonomy
# (owner decision 2026-06-13: "department's exact words"). This keeps the category verifiable
# (it IS the published label) and inference-free: "IP Accommodation", "School Building Projects",
# "Passport Booklets" are the department's words, merely de-noised. Canonicalisation does only:
#   1. drop a leaked LEADING amount ("€80,000,000.00 Third Level Building…" — Education prefixes the
#      amount into the description column);
#   2. strip a TRAILING run of dangling truncation tails (punctuation + bare connectors a mid-phrase
#      parser cut left hanging: "Ukraine Accommodation and/or" → "Ukraine Accommodation"); NEVER a
#      content word (services/support/maintenance/accommodation are kept);
#   3. smart-case: keep short ALL-CAPS / mixed-caps acronyms verbatim (IP, ICT, IM&T, IRCG, I.T.),
#      title-case ordinary words, keep connectors lowercase — so "IT software"/"IT Software" merge.
# Rows whose description is null/blank get spend_category = NULL (surfaced as "Uncategorised").
# Leading noise to drop: a bare/amount-bearing € sign and any run of number-groups a council parser
# bled into the description column ("€80,000,000.00 Third Level…", "€ Construction Costs", "0 0 0").
_CAT_LEAD_MONEY = re.compile(r"^\s*(?:€\s*)?(?:\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?\s*)*")
_CAT_TAIL = re.compile(r"(?:[\s,;:./&-]+|\b(?:and/or|and|or)\b)+$", re.I)
_CAT_SMALL = {"and", "or", "of", "the", "for", "to", "a", "an", "in", "on", "with", "and/or"}


def _cat_is_acronym(w: str) -> bool:
    letters = re.sub(r"[^A-Za-z]", "", w)
    return bool((w.upper() == w and letters and len(letters) <= 4) or re.search(r"[a-z][A-Z]", w))


def _cat_case_word(w: str, first: bool) -> str:
    lw = w.lower()
    if lw in _CAT_SMALL:
        return lw if not first else lw[:1].upper() + lw[1:]
    if _cat_is_acronym(w):
        return w
    return lw[:1].upper() + lw[1:]


def canon_spend_category(s: str | None) -> str | None:
    """Canonicalise a published description into a source-grounded spend category (see block above).
    Pure function — unit-tested. Returns None for empty/blank input."""
    if not s:
        return None
    s = re.sub(r"\s+", " ", str(s)).strip().strip("\"'")
    s = _CAT_LEAD_MONEY.sub("", s).strip()
    prev = None
    while prev != s:
        prev = s
        s = _CAT_TAIL.sub("", s).strip()
    # A residue with no letters (e.g. "0 0 0", a lone "€", a stray code) is not a purpose label.
    if not re.search(r"[A-Za-z]", s):
        return None
    return " ".join(_cat_case_word(w, i == 0) for i, w in enumerate(s.split()))


def _derive_spend_category(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty() or "description" not in df.columns:
        return df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("spend_category"))
    out = df.with_columns(
        pl.col("description").map_elements(canon_spend_category, return_dtype=pl.Utf8).alias("spend_category")
    )
    n_cat = out.filter(pl.col("spend_category").is_not_null())["spend_category"].n_unique()
    cov = round(100.0 * out["spend_category"].is_not_null().sum() / out.height, 1)
    print(
        f"  derived spend_category: {n_cat:,} distinct categories, {cov}% of rows covered (source: published description)"
    )
    return out


def main() -> None:
    print("Consolidating payment-grain facts → gold:")
    base = _load_facts()
    la = _load_la_fact(base)

    # CROSS-LANE DISJOINTNESS GUARD (plan §10): the reconciliation audit below keys on each source's
    # publisher set and so CANNOT detect a publisher_id that appears in TWO lanes (it would
    # double-count AND reconcile to itself — the documented silent trap). Prevent it structurally:
    # assert every publisher_id belongs to exactly one source fact. No-op on today's disjoint data;
    # fires only if e.g. a recovered LA harvest starts emitting a council the disclosed lane owns.
    _owner: dict[str, str] = {}
    _dups: dict[str, list[str]] = {}
    for _label, _stats in _SOURCE_STATS.items():
        for _pid in _stats["publishers"]:
            if _pid in _owner:
                _dups.setdefault(_pid, [_owner[_pid]]).append(_label)
            else:
                _owner[_pid] = _label
    if _dups:
        raise SystemExit(
            "CROSS-LANE DOUBLE-COUNT — publisher_id present in multiple source facts (would silently "
            f"double-count in gold): {_dups}. Resolve ownership (one lane per publisher_id) before rebuilding."
        )

    df = pl.concat([base, la], how="vertical") if la is not None else base
    df = _canonicalise_split_entities(df)
    df = _clean_supplier_names(df)
    # paid_flag column-misalignment repair (idempotent; see extractors/_paid_flag_clean.py).
    # Silver is already cleaned at source, so this is defence-in-depth — but it runs BEFORE
    # _derive_spend_category so any category text recovered from a leaked paid_flag also feeds
    # the published-description spend category. Schema/row/€ preserving (reconciliation-safe).
    df, _pf = clean_paid_flag(df)
    if _pf.get("n_leak"):
        print(
            f"  paid_flag repair: cleared {_pf['n_leak']:,} leaked values, "
            f"recovered {_pf.get('n_recovered', 0):,} category texts into description"
        )
    df = _conform(df)
    df = _attach_regime(df)
    df = _attach_cro(df)
    df = _reclassify_missed_companies(df)
    df = _classify_id_codes(df)
    df = _apply_class_overrides(df)
    df = _strip_bled_amount(df)  # clean amount-bled descriptions before category derivation
    df = _derive_spend_category(df)
    df = _surface_sole_trader_contractors(df)

    # AUDIT (write-audit-publish): this fold maps silver→gold WITHOUT re-parsing, so every
    # source's rows and € MUST survive exactly. Reconcile the output against the per-source
    # totals captured at load time, keyed by each source's (disjoint) publisher set. A non-zero
    # unexplained delta means a concat/dedup/join bug dropped or duplicated rows — the silent
    # partial-data failure the >50% row-count baseline can't see. Halt before publishing.
    actual = {
        label: (
            df.filter(pl.col("publisher_id").is_in(list(stats["publishers"]))).height,
            float(df.filter(pl.col("publisher_id").is_in(list(stats["publishers"])))["amount_eur"].sum() or 0.0),
        )
        for label, stats in _SOURCE_STATS.items()
    }
    expected = {label: (stats["rows"], stats["eur"]) for label, stats in _SOURCE_STATS.items()}
    recon = reconciliation_violations(expected, actual)
    if recon:
        raise SystemExit("RECONCILIATION FAILED — gold does not preserve the source facts:\n  " + "\n  ".join(recon))
    print(f"  reconciled {len(expected)} source facts: rows + € preserved exactly")

    # DATA CONTRACT (runtime drift gate, see services/data_contracts.py): this is the last
    # common chokepoint before gold, so it is where we refuse to ship drift. Any closed-vocab
    # classification column carrying an unrecognised value (a new amount_semantics the map left
    # as value_kind='unknown', a supplier_class the parsers never emit, …) HALTS the run; the
    # offending rows — plus the known paid_flag column-misalignment leakage — are written to
    # data/_meta/quarantine/ for investigation either way. It also re-asserts the cross-column
    # invariants (no summable public-body transfer, CRO⇒company, privacy …). hard=True so a
    # green run guarantees a classified, internally-consistent fact.
    guard_payment_fact(df, name="procurement_payments_fact", hard=True)

    # PRIVACY INVARIANT (runtime, -O-proof): mirrors procurement_public_body_extract.py —
    # refuse to write gold if any likely-person row is left displayable.
    leaked = df.filter(
        pl.col("public_display")
        & (
            (pl.col("supplier_class") == "sole_trader_or_individual")
            | (pl.col("privacy_status") == "review_personal_data")
        )
    )
    if leaked.height:
        raise SystemExit(
            f"privacy quarantine breached: {leaked.height} likely-person rows left "
            "public_display=True; refusing to write procurement_payments_fact"
        )

    # Magnitude-plausibility flag (parse-artefact guard) — additive, never alters amount_eur.
    # Lets a real-terms / sum view exclude sub-€100 noise before any deflator scales it.
    df = df.with_columns(value_plausible_expr("amount_eur").alias("value_plausible"))

    # PARSE-QUALITY GATE (services/parse_qa): catch a parser regression that collapses
    # several records into one cell — a whole PO table or OCR'd page dumped into a single
    # description (max/p99 ratio + an absolute-length backstop that survives p99 inflation).
    # tolerate covers the KNOWN, VERIFIED-GENUINE long-description residual, NOT collapsed cells:
    #  (a) dept_children (DCEDIY) reading-order PDFs (a heuristic truncation clips ~944 legit
    #      descriptions — see project_dept_children_asylum_spend_gap); and
    #  (b) the disclosed_bq_po_newbodies lane (2026-06-27): councils publish verbose single-PO
    #      specs (e.g. Dublin City street-lighting lantern specs, ≤231 chars) — confirmed genuine,
    #      and parse_qa.scan_frame() on that lane ALONE is clean, so it adds no collapsed cells; it
    #      only shifts the frame-wide distribution, raising the outlier count to ~86. tolerate=110
    #      keeps headroom while the absolute >2000-char backstop (n_huge, untouched here) still
    #      catches any true mass-collapse regardless of this budget.
    # The gate trips if the residual grows materially or a new column starts collapsing.
    for _r in parse_qa.scan_frame(df):
        print(f"  parse-qa residual: {_r}")
    parse_qa.assert_clean(df, tolerate=110)

    df = _canon_la_publisher_names(df)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    save_parquet(df, OUT, min_rows=MIN_FACT_ROWS)
    print(f"\nwrote {df.height:,} rows / {df['publisher_name'].n_unique()} publishers -> {OUT}")

    safe = df.filter(pl.col("value_safe_to_sum"))
    by_tier = (
        safe.group_by("realisation_tier")
        .agg(pl.col("amount_eur").sum().alias("safe_eur"), pl.len().alias("rows"))
        .sort("safe_eur", descending=True)
    )
    cov = {
        "generated_utc": datetime.now(UTC).isoformat(),
        "layer": "gold",
        "source": "extractors/procurement_payments_consolidate.py",
        "n_rows": df.height,
        "n_publishers": int(df["publisher_name"].n_unique()),
        "n_suppliers": int(df["supplier_normalised"].n_unique()),
        "cro_matched_pct": round(100.0 * df["cro_company_num"].is_not_null().sum() / df.height, 1),
        "rows_by_publisher_type": dict(df["publisher_type"].value_counts(sort=True).iter_rows()),
        "n_local_authorities": int(
            df.filter(pl.col("publisher_type") == "local_authority")["publisher_name"].n_unique()
        ),
        "safe_eur_by_tier": {r["realisation_tier"]: round(r["safe_eur"], 2) for r in by_tier.to_dicts()},
        "vat_status_counts": dict(df["vat_status"].value_counts().iter_rows()),
        "disclosure_basis_counts": dict(df["disclosure_basis"].value_counts(sort=True).iter_rows()),
        "disclosure_threshold_counts": {
            str(k): v for k, v in df["disclosure_threshold_eur"].value_counts(sort=True).iter_rows()
        },
        "body_class_counts": dict(df["body_procurement_class"].value_counts(sort=True).iter_rows()),
        "regime_note": "Publishers fall under DIFFERENT disclosure regimes/thresholds (carried per row in "
        "disclosure_basis / disclosure_threshold_eur / threshold_vat / body_procurement_class). Most publish "
        "over €20,000 under the FOI Act 2014 s.8 model scheme (origin Circular FIN 07/12); CHI publishes over "
        "€25,000 incl-VAT; utilities (ESB/EirGrid/Uisce Éireann) are contracting ENTITIES outside that scheme. "
        "Do NOT describe the corpus as a single '€20,000 / Circular 07/2012' regime.",
        "privacy_note": "Suppliers named per each body's own published PO/payments disclosures "
        "(see disclosure_basis); no address/PII beyond the published figure.",
        "value_note": "po_committed (ordered) and payment_actual (paid) are different lifecycle "
        "tiers — never summed together; only value_safe_to_sum rows sum, and never across vat_status.",
        "triple_count_note": "Includes both central→council transfers (TII Road Grants, "
        "supplier_class='public_body') and council→contractor LA payments. To avoid double-"
        "counting the same money across those two legs, exclude supplier_class='public_body' "
        "from spend totals at the consuming view/page.",
    }
    OUT_COV.write_text(json.dumps(cov, indent=2), encoding="utf-8")
    print(f"wrote coverage -> {OUT_COV}")
    print("\nsafe €/tier:", cov["safe_eur_by_tier"])


if __name__ == "__main__":
    main()
