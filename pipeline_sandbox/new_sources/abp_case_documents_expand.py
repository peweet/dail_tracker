"""ACP case-document index EXPANSION — a pure re-parse of ALREADY-CACHED bronze HTML.

    uv run --no-sync python -m pipeline_sandbox.new_sources.abp_case_documents_expand --dry-run
    uv run --no-sync python -m pipeline_sandbox.new_sources.abp_case_documents_expand

``--no-sync`` is the documented form ON PURPOSE, and this is the script an operator runs first.
``uv run --locked --extra pipeline`` syncs the venv to lockfile + exactly the extras named, and
winocr is declared only under the ``ocr`` extra (pyproject.toml:75-79) — so that form UNINSTALLS
winocr, and the next ``abp_doc_text_extract`` run takes its no-OCR branch, prints
'[ocr] DISABLED: winocr unavailable' and caches near-empty text rows (MEMORY.md
feedback_uv_sync_prunes_the_ocr_stack). If you do sync, sync BOTH extras in BOTH scripts so
neither run prunes the other's engine:
    script 1 (this file)  uv run --locked --extra pipeline --extra ocr python -m pipeline_sandbox.new_sources.abp_case_documents_expand
    script 2 (text layer) uv run --locked --extra pipeline --extra ocr python -m pipeline_sandbox.new_sources.abp_doc_text_extract
``--extra ocr`` also pulls ocrmypdf, which is NOT installed in this venv today, so either synced
form needs a network install before it will run. Pre-flight before anything that will OCR:
    uv run --no-sync python -c "import winocr, fitz; from PIL import Image; print('ok')"

WHY: ``abp_case_documents.py`` measured the BASE RATE from a deliberately category-balanced
52-case sample, and that measurement is finished. What is missing is the INDEX: 236 cases carry
``has_tree=true`` in ``abp_case_document_census.parquet`` and only 52 have been parsed into
``abp_case_documents.parquet`` (5,261 document rows). Every one of the 1,462 case pages fetched
by that probe is still on disk under ``bronze/abp_case_documents/case_<id>.html`` with its
``.meta.json`` sibling, so expanding the index costs ZERO network requests — it is one regex
pass over local bytes.

CACHE-ONLY BY CONSTRUCTION, three independent guards, because the cost of getting this wrong is
236+ unwanted requests against a live public site:
  1. ``_common.fetch`` and ``abp_case_documents.fetch`` are rebound to a raiser, as is
     ``_common._SESSION.get``. There is no code path left that can open a socket.
  2. A case whose ``case_<id>.html`` or ``case_<id>.meta.json`` is missing is recorded as a SKIP
     and never passed to ``get_case_html()`` at all.
  3. Every returned meta must carry ``from_cache=True``. Enforced with an explicit ``raise``
     (``CacheOnlyViolation``) and never an ``assert``: ``python -O`` strips asserts, which would
     silently drop this guard and leave only two — the same reason abp_doc_text_extract.py:166
     defines ``PrivacyInvariantError`` instead of asserting.

THE PATH TRAP this script exists downstream of: ``_common.py:20`` hardcodes
``ROOT = c:/tmp/dail_new_sources``, which holds no bronze and no silver — the live tree is
``data/_sandbox/dail_new_sources/``. Merely importing ``_common`` re-creates those two empty
directories (``_common.py:23-24``), so an unpatched run does not fail loudly; it silently misses
every cache lookup. Both modules' path globals are rebound below, before the first cache read.
``abp_case_documents.py:46`` does ``from _common import BRONZE, SILVER`` — a VALUE binding — so
patching ``_common`` alone is not enough and ``acd.BRONZE`` must be rebound too.

ISOLATION: reads bronze HTML + three sandbox parquets, writes three NEW ``*_expanded`` parquets
under the sandbox silver. It never touches ``abp_case_documents.parquet``,
``abp_case_documents_coverage.parquet`` or ``abp_case_decision_docs.parquet``, so the sampled
measurement stays reproducible and ``abp_case_documents.report()`` keeps the Horvitz-Thompson
columns it divides at ``:348``. Nothing here promotes anything. Polars only.
"""
from __future__ import annotations

# isort: off
# MUST stay first: caps the BLAS thread count before polars/numpy load. Ordering is the
# contract — once numpy is imported the cap is a no-op. See services/runtime_env.py.
import services.runtime_env  # noqa: F401
# isort: on

import argparse
import sys
from pathlib import Path

import polars as pl

from pipeline_sandbox.new_sources import _common
from pipeline_sandbox.new_sources import abp_case_documents as acd
from services.parquet_io import save_parquet

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SANDBOX = PROJECT_ROOT / "data" / "_sandbox" / "dail_new_sources"


# ---------------------------------------------------------------- path + network guards


class CacheOnlyViolation(RuntimeError):
    """The cache-only guarantee was about to break. Hard gate, never an assert (-O strips those)."""


def _no_network(*_a, **_kw):  # noqa: ANN002, ANN003
    """Hard stop. Reached only if a cache lookup regressed — crash instead of hitting pleanala.ie."""
    raise CacheOnlyViolation(
        "abp_case_documents_expand is a CACHE-ONLY re-parse: no network request may be issued. "
        "A call reaching here means a bronze cache lookup failed silently."
    )


def repoint_and_seal() -> None:
    """Point both sandbox modules at the REAL tree, then seal the network.

    Order matters: the rebinding must happen before the first ``get_case_html`` call, and both
    modules must be rebound because ``abp_case_documents`` imported BRONZE/SILVER by value.
    ``FRAME_PARQUET`` and ``MISSES`` were computed from the stale SILVER at import time
    (``abp_case_documents.py:53,56``), so they are recomputed here too.
    """
    _common.ROOT = SANDBOX
    _common.BRONZE = SANDBOX / "bronze"
    _common.SILVER = SANDBOX / "silver"
    acd.BRONZE = _common.BRONZE
    acd.SILVER = _common.SILVER
    acd.FRAME_PARQUET = acd.SILVER / "abp_inspector_reports.parquet"
    acd.MISSES = acd.SILVER / "abp_case_documents_misses.txt"
    # Three seals. fetch is imported by name into abp_case_documents, so both names are rebound,
    # and the shared session is sealed as well in case anything reaches past both.
    acd.fetch = _no_network
    _common.fetch = _no_network
    _common._SESSION.get = _no_network  # noqa: SLF001 — deliberate: the last socket in the module


# ---------------------------------------------------------------- outputs

SOURCE = "abp_case_documents_expanded"
COVERAGE_SOURCE = "abp_case_documents_coverage_expanded"
DECISION_SOURCE = "abp_case_decision_docs_expanded"
CENSUS = "abp_case_document_census.parquet"
BASE_DOCS = "abp_case_documents.parquet"

# The row floor is DERIVED from the census (see expected_doc_rows), not hardcoded, and this is the
# fraction of the census's own prediction a healthy run must clear. 0.90 = a 10% margin for drift
# between what the census counted and what parse_case_page finds. Measured on the 43 cases that are
# both already extracted AND has_tree in the census: the parser produced 4,999 document rows where
# the census predicted 4,879 (+2.5%), so the census under-counts slightly and the margin only has to
# absorb noise in the generous direction. At census scope today the derived floor is
# 32,237 * 0.90 = 29,013 rows; the hardcoded 20,000 it replaces would have accepted a 38% shortfall.
MIN_ROWS_FRACTION = 0.90

# The 22 columns of the live abp_case_documents.parquet, IN ORDER, so the expanded file is a
# drop-in superset rather than a differently-shaped sibling. 10 come from parse_case_page's
# _docs dicts, 4 are per-case enrichment, 8 are provenance.
DOC_COLUMNS = [
    "abp_case",
    "path_case_id",
    "doc_category",
    "doc_subcategory",
    "path_depth",
    "rel_path",
    "filename",
    "file_ext",
    "full_url",
    "is_scoping",
    "category",
    "case_year",
    "case_type",
    "planning_authority",
    "source_url",
    "source_document_hash",
    "fetched_at",
    "source_last_modified",
    "source_published_date",
    "extraction_method",
    "confidence",
    "privacy_tier",
]
# All-null in every live parquet (pleanala sends no Last-Modified). Measured with
# pl.read_parquet_schema this session: abp_case_documents.parquet stores BOTH of these as dtype
# Null — so pinning them to String here is a deliberate DIVERGENCE from the base file, not a match
# to it. It is worth it because a Null column has no type for a later vintage that does carry a
# Last-Modified value to land in. The price: pl.concat([base, expanded]) raises SchemaError under
# the default `vertical`/`diagonal` strategy and needs `vertical_relaxed`/`diagonal_relaxed`.
# dtype_divergence_note() measures and prints this at run time so a consumer sees it, not only here.
_NULL_STRING_COLUMNS = ("source_last_modified", "source_published_date")

ENRICHMENT_COLUMNS = ["abp_case", "planning_authority", "category", "case_year", "abp_decision_raw"]


# ---------------------------------------------------------------- case selection


def cached_case_ids() -> set[str]:
    """Every case id with BOTH bronze files present. The html alone is not enough — get_case_html
    only takes the cache branch when the .meta.json exists too (abp_case_documents.py:295)."""
    d = acd.BRONZE / acd.SOURCE
    if not d.exists():
        return set()
    ids = set()
    for p in d.glob("case_*.html"):
        cid = p.name[len("case_") : -len(".html")]
        if (d / f"case_{cid}.meta.json").exists():
            ids.add(cid)
    return ids


def select_cases(scope: str) -> tuple[list[str], dict[str, int]]:
    """Return (ordered case ids, counts).

    scope='census' (default): every ``has_tree`` case in the census, UNION the cases already in
    the 52-case extract. The union is not cosmetic — 9 of the 52 extracted cases carry no
    ``has_tree`` row (measured: 7 sit in the census with ``has_tree=false``, 2 are absent from it
    altogether), so a plain "expand to the 236" scope would silently DROP them and the expanded
    file would not be a superset of the file it replaces.

    scope='cached': every cached page. Costs the same zero network and avoids the census's
    measured blind spot — case 304951 is ``has_tree=false`` in the census yet its cached page
    carries EIAR-NIS PDFs at a FLAT 4-segment path, which parse_case_page handles at :233-234.
    The census has no producer script in the repo, so it cannot be regenerated or audited.
    """
    have = cached_case_ids()
    counts = {"cached_pages": len(have)}

    census_ids: set[str] = set()
    census_path = acd.SILVER / CENSUS
    if census_path.exists():
        census = pl.read_parquet(census_path, columns=["abp_case", "has_tree"])
        census_ids = set(census.filter(pl.col("has_tree"))["abp_case"].to_list())
    counts["census_has_tree"] = len(census_ids)

    extracted_ids: set[str] = set()
    base_path = acd.SILVER / BASE_DOCS
    if base_path.exists():
        extracted_ids = set(pl.read_parquet(base_path, columns=["abp_case"])["abp_case"].unique().to_list())
    counts["already_extracted"] = len(extracted_ids)

    wanted = have if scope == "cached" else (census_ids | extracted_ids)
    counts["selected"] = len(wanted)
    counts["selected_without_cache"] = len(wanted - have)
    return sorted(wanted), counts


def expected_doc_rows(case_ids: list[str]) -> int:
    """Documents the CENSUS predicts for the selected cases that actually have a cached page.

    ``n_eiar`` is the census's own per-case count of EIAR-NIS documents, so summing it over the
    selection gives a floor that tracks the source instead of a literal that goes stale on the next
    vintage. Two deliberate biases, both DOWNWARD, so a derived floor cannot trip a healthy run:
      * a selected case the census does not cover contributes 0 — the 9 extracted cases with no
        ``has_tree`` row hold 262 document rows in the live extract and are simply not predicted;
      * a selected case with no cached page contributes 0, because build_rows skips it. Cache
        presence is a DISK fact read before any parsing, so this is not circular: the expectation
        falls only when bronze is genuinely absent, never because a parse failed.
    """
    census_path = acd.SILVER / CENSUS
    if not census_path.exists():
        return 0
    eligible = set(case_ids) & cached_case_ids()
    if not eligible:
        return 0
    census = pl.read_parquet(census_path, columns=["abp_case", "n_eiar", "has_tree"])
    predicted = census.filter(pl.col("has_tree") & pl.col("abp_case").is_in(eligible))["n_eiar"].sum()
    return int(predicted or 0)


def enrichment_map() -> dict[str, dict]:
    """abp_case -> the five frame fields the original extractor took from its sample row.

    The inspector-report frame is the same source ``build_sample`` reads; ``abp_case`` is unique
    there, so a dict lookup replaces the whole sampling path. Cases outside the frame default to
    None on all four enrichment columns rather than being dropped.
    """
    if not acd.FRAME_PARQUET.exists():
        print(f"[warn] frame parquet absent ({acd.FRAME_PARQUET.name}) — enrichment columns will be null")
        return {}
    frame = pl.read_parquet(acd.FRAME_PARQUET, columns=ENRICHMENT_COLUMNS)
    return {r["abp_case"]: r for r in frame.iter_rows(named=True)}


# ---------------------------------------------------------------- row building


def build_rows(case_ids: list[str], enrich: dict[str, dict]) -> tuple[list[dict], list[dict], list[dict], dict[str, int]]:
    """Re-parse each cached page. Returns (doc_rows, decision_rows, coverage_rows, counts).

    Provenance is rebuilt EXACTLY as abp_case_documents.py:531-540 does it, from the cached
    ``.meta.json``. ``fetched_at`` therefore carries the ORIGINAL fetch timestamp, which is the
    correct provenance for a re-parse — a re-parse does not re-fetch, so stamping it with now()
    would be a false claim about when the bytes were obtained.
    """
    doc_rows: list[dict] = []
    dec_rows: list[dict] = []
    cov_rows: list[dict] = []
    counts = {"parsed": 0, "skipped_no_cache": 0, "with_tree": 0, "path_case_id_mismatch": 0}

    d = acd.BRONZE / acd.SOURCE
    for i, cid in enumerate(case_ids, 1):
        hp, mp = d / f"case_{cid}.html", d / f"case_{cid}.meta.json"
        if not (hp.exists() and mp.exists()):
            # Guard 2: never hand an uncached id to get_case_html — that is the branch that fetches.
            counts["skipped_no_cache"] += 1
            continue
        payload, meta = acd.get_case_html(cid, refresh=False)
        # Guard 3: the cache branch is the ONLY acceptable outcome for this script. A raise, not an
        # assert — under `python -O` an assert is stripped and this guard would vanish silently.
        if not meta.get("from_cache"):
            raise CacheOnlyViolation(
                f"case {cid}: get_case_html returned meta with from_cache={meta.get('from_cache')!r} — "
                "the network path was reached in a cache-only re-parse. Refusing to continue."
            )
        if payload is None:  # unreachable on the cache branch; kept so a regression is loud
            counts["skipped_no_cache"] += 1
            continue

        parsed = acd.parse_case_page(cid, payload)
        docs = parsed.pop("_docs")
        dec = parsed.pop("_decision")
        counts["parsed"] += 1
        counts["with_tree"] += bool(parsed["has_eiar_tree"])

        r = enrich.get(cid, {})
        prov = {
            "source_url": meta["source_url"],
            "source_document_hash": meta["source_document_hash"],
            "fetched_at": meta["fetched_at"],
            "source_last_modified": meta.get("source_last_modified"),
            "source_published_date": None,
            "extraction_method": "html_regex_case_page",
            "confidence": "high",  # the manifest is server-rendered markup, not inference
            "privacy_tier": "public",
        }
        for doc in docs:
            if doc["path_case_id"] != cid:
                # A cross-linked document: the URL path names a DIFFERENT case. The row is still
                # KEPT under abp_case=cid — parse_case_page stamps it that way
                # (abp_case_documents.py:237) and the base extract does the same, so dropping it
                # here would make the expanded file diverge from the file it is meant to supersede.
                # The mismatch is surfaced as a count only; path_case_id stays on the row, so a
                # consumer that cares can re-attribute on it.
                counts["path_case_id_mismatch"] += 1
            doc_rows.append(
                doc
                | {
                    "category": r.get("category"),
                    "case_year": r.get("case_year"),
                    "case_type": parsed.get("case_type"),
                    "planning_authority": r.get("planning_authority"),
                }
                | prov
            )
        dec_rows.extend(x | prov for x in dec)

        cov_rows.append(
            {
                "abp_case": cid,
                "planning_authority": r.get("planning_authority"),
                "category": r.get("category"),
                "case_year": r.get("case_year"),
                "abp_decision_raw": r.get("abp_decision_raw"),
                "http_status": meta.get("http_status"),
                "source_url": meta["source_url"],
                "source_document_hash": meta["source_document_hash"],
                "fetched_at": meta["fetched_at"],
                "source_last_modified": meta.get("source_last_modified"),
                "source_published_date": None,
                "extraction_method": "html_regex_case_page",
                "privacy_tier": "public",
                "confidence": "high",
            }
            | parsed
        )
        if i % 250 == 0:
            print(f"  [{i}/{len(case_ids)}] parsed={counts['parsed']} docs={len(doc_rows)} skipped={counts['skipped_no_cache']}")

    return doc_rows, dec_rows, cov_rows, counts


def frame_from(rows: list[dict], columns: list[str] | None = None) -> pl.DataFrame:
    """List-of-dicts -> DataFrame with the null-typed provenance columns pinned to String.

    ``infer_schema_length=None`` scans every row: the default 100-row window is what turns a
    column that is null in the first cases and populated later into a silent parse error.
    """
    if not rows:
        return pl.DataFrame()
    df = pl.DataFrame(rows, infer_schema_length=None)
    casts = [pl.col(c).cast(pl.String).alias(c) for c in _NULL_STRING_COLUMNS if c in df.columns]
    if "case_year" in df.columns:
        casts.append(pl.col("case_year").cast(pl.Int64).alias("case_year"))
    if "path_depth" in df.columns:
        casts.append(pl.col("path_depth").cast(pl.Int64).alias("path_depth"))
    if casts:
        df = df.with_columns(casts)
    return df.select(columns) if columns else df


# ---------------------------------------------------------------- acceptance checks


def dtype_divergence_note(docs_df: pl.DataFrame) -> None:
    """Print every column whose dtype differs from the live ``abp_case_documents.parquet``.

    Measured against the file on disk, not asserted from a comment. This is the one place a
    consumer finds out before they try ``pl.concat([base, expanded])``, which raises SchemaError
    under the default strategy whenever a dtype pair disagrees (verified: polars 1.41.2 rejects
    Null vs String under both ``vertical`` and ``diagonal``).
    """
    base_path = acd.SILVER / BASE_DOCS
    if docs_df.is_empty() or not base_path.exists():
        return
    base_schema = pl.read_parquet_schema(base_path)
    diverged = [
        (c, base_schema[c], docs_df.schema[c])
        for c in docs_df.columns
        if c in base_schema and base_schema[c] != docs_df.schema[c]
    ]
    if not diverged:
        print(f"[dtypes] expanded frame matches {BASE_DOCS} column for column — a plain concat is safe")
        return
    pairs = "; ".join(f"{c}: base={b}, expanded={n}" for c, b, n in diverged)
    print(f"[dtypes] {len(diverged)} column(s) diverge from {BASE_DOCS} — {pairs}")
    print(
        "[dtypes] therefore pl.concat([base, expanded]) MUST pass how='vertical_relaxed' "
        "(or how='diagonal_relaxed' if the column sets also differ); the default raises SchemaError"
    )


def idempotence_check(docs_df: pl.DataFrame) -> None:
    """A pure re-parse of unchanged bytes MUST reproduce the existing rows exactly.

    Any per-case drift against the live 52-case extract means the cache or the parser moved, and
    the expanded file must not be promoted until that is explained. Counts only — no rows dumped.
    """
    base_path = acd.SILVER / BASE_DOCS
    if not base_path.exists() or docs_df.is_empty():
        print("[check] idempotence: base extract absent — skipped")
        return
    base = pl.read_parquet(base_path, columns=["abp_case"]).group_by("abp_case").len().rename({"len": "n_base"})
    new = docs_df.group_by("abp_case").len().rename({"len": "n_new"})
    joined = base.join(new, on="abp_case", how="left").with_columns(pl.col("n_new").fill_null(0))
    same = joined.filter(pl.col("n_base") == pl.col("n_new")).height
    print(
        f"[check] idempotence: {same}/{joined.height} original cases reproduce their row count exactly "
        f"(base rows={int(joined['n_base'].sum())}, re-parsed rows for those cases={int(joined['n_new'].sum())})"
    )


def resolve_floor(args: argparse.Namespace, case_ids: list[str]) -> int | None:
    """The row floor save_parquet will enforce on the document parquet, printed with its reason.

    Precedence: --limit disables the floor (a short frame is the point of that flag), an explicit
    --min-rows wins next, otherwise the floor is MIN_ROWS_FRACTION of what the census predicts.
    """
    if args.limit:
        print("[floor] disabled: --limit is set, so a short frame is the intended outcome")
        return None
    if args.min_rows is not None:
        print(f"[floor] {args.min_rows} rows (explicit --min-rows, overriding the census-derived floor)")
        return args.min_rows
    expected = expected_doc_rows(case_ids)
    if not expected:
        print(f"[floor] disabled: {CENSUS} predicts no documents for these cases — nothing to derive a floor from")
        return None
    floor = int(expected * MIN_ROWS_FRACTION)
    print(
        f"[floor] {floor} rows = {MIN_ROWS_FRACTION:.0%} of the {expected} documents {CENSUS} predicts "
        f"for the selected cases that have a cached page"
    )
    return floor


# ---------------------------------------------------------------- main


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Expand the ACP case-document index from cached bronze HTML. ZERO network."
    )
    ap.add_argument(
        "--scope",
        choices=("census", "cached"),
        default="census",
        help="census (default): census has_tree cases UNION the cases already extracted. "
        "cached: every cached page (superset; avoids the census's flat-tree blind spot).",
    )
    ap.add_argument("--limit", type=int, default=0, help="stop after N cases (0 = no limit, the default)")
    ap.add_argument("--dry-run", action="store_true", help="parse and report counts, write nothing")
    ap.add_argument(
        "--min-rows",
        type=int,
        default=None,
        help="explicit row floor for the document parquet. Default: DERIVED from the census — "
        f"{MIN_ROWS_FRACTION:.0%} of the documents it predicts for the selected, cached cases. "
        "Ignored under --limit, where a short frame is the intended outcome rather than a "
        "truncated harvest.",
    )
    args = ap.parse_args()

    repoint_and_seal()
    if not (acd.BRONZE / acd.SOURCE).exists():
        print(f"[fatal] bronze cache not found at {acd.BRONZE / acd.SOURCE} — nothing to re-parse")
        sys.exit(1)

    case_ids, sel = select_cases(args.scope)
    print(
        f"[scope] {args.scope}: cached_pages={sel['cached_pages']} census_has_tree={sel['census_has_tree']} "
        f"already_extracted={sel['already_extracted']} selected={sel['selected']} "
        f"(without a cached page: {sel['selected_without_cache']})"
    )
    if args.limit:
        case_ids = case_ids[: args.limit]
        print(f"[scope] --limit {args.limit}: {len(case_ids)} cases this run")

    enrich = enrichment_map()
    doc_rows, dec_rows, cov_rows, counts = build_rows(case_ids, enrich)

    docs_df = frame_from(doc_rows, DOC_COLUMNS)
    dec_df = frame_from(dec_rows)
    cov_df = frame_from(cov_rows)

    print(
        f"[parse] cases parsed={counts['parsed']}  skipped (no cached page)={counts['skipped_no_cache']}  "
        f"cases with an EIAR-NIS tree={counts['with_tree']}"
    )
    print(
        f"[rows] documents={docs_df.height} over {docs_df['abp_case'].n_unique() if docs_df.height else 0} cases  "
        f"decision_docs={dec_df.height}  coverage={cov_df.height}"
    )
    if docs_df.height:
        print(
            f"[rows] is_scoping=true: {int(docs_df['is_scoping'].sum())}  "
            f"file_ext=pdf: {docs_df.filter(pl.col('file_ext') == 'pdf').height}  "
            f"path_case_id != abp_case: {counts['path_case_id_mismatch']}"
        )
    idempotence_check(docs_df)
    dtype_divergence_note(docs_df)

    floor = resolve_floor(args, case_ids)

    if args.dry_run:
        print("[dry-run] nothing written")
        return
    if docs_df.is_empty():
        print("[write] no document rows — refusing to write an empty frame")
        return

    p1 = save_parquet(docs_df, acd.SILVER / f"{SOURCE}.parquet", min_rows=floor)
    print(f"[silver] documents {docs_df.height} rows -> {p1} (row floor={floor})")
    if not cov_df.is_empty():
        # Deliberately a NEW name and WITHOUT the stratum_* columns: this frame has no sampling
        # design, so it must never be fed to abp_case_documents.report(), which divides
        # stratum_frame_n / stratum_sample_n at :348 for the Horvitz-Thompson estimate.
        p2 = save_parquet(cov_df, acd.SILVER / f"{COVERAGE_SOURCE}.parquet")
        print(f"[silver] coverage (case grain, zeros included) {cov_df.height} rows -> {p2}")
    if not dec_df.is_empty():
        p3 = save_parquet(dec_df, acd.SILVER / f"{DECISION_SOURCE}.parquet")
        print(f"[silver] decision docs {dec_df.height} rows -> {p3}")
    print("[note] the original abp_case_documents*.parquet files are untouched — diff before promoting")
    print(
        f"[note] concatenating them is NOT plain: source_last_modified/source_published_date are dtype "
        f"Null in {BASE_DOCS} and String here, so pl.concat([base, expanded]) needs "
        f"how='vertical_relaxed' — or how='diagonal_relaxed' where the column sets also differ, as "
        f"they do for the coverage frames (no stratum_* columns here). The default strategy raises."
    )


if __name__ == "__main__":
    main()
