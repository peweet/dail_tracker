"""ACP (An Coimisiún Pleanála) CASE-PAGE DOCUMENT MANIFEST — coverage probe.

THE QUESTION THIS ANSWERS: which ACP cases actually carry a
``publicaccess/EIAR-NIS/...`` document tree — the applicant's full EIA document
set (EIAR chapters + appendices, NIS, photomontages, planning pack)? One case
having it proves nothing. This probe measures the BASE RATE across a stratified
sample, broken down by case category and year, so "would this help?" can be
answered with a number instead of an anecdote.

WHY IT IS CHEAP: the entire manifest is server-rendered into the case page HTML
at https://www.pleanala.ie/en-ie/case/<caseid> (~30-45 KB). One GET enumerates
every document path. No JS, no API. The case page ALSO carries:
  - the decision-doc links already known to be deterministic
    (cases/reports/<grp>/r<cid>.pdf, orders/d<cid>.pdf, directions/s<cid>.pdf,
    bmr/b<cid>.pdf),
  - explicit ``EIAR`` / ``NIS`` Yes/No fields,
  - the sentence "The application is subject to an EIA procedure.",
  - case type and development description.

RELATION TO WHAT EXISTS: ``abp_inspector_reports.py`` already ingests the
INSPECTOR's report (a different path, ``cases/reports/``) and its ``has_eia``
column is only a keyword regex over the inspector's own prose — NOT the case's
real EIA-procedure status. This probe reads the status the register itself
states, and enumerates the applicant-side documents that the inspector report
does not contain.

SAMPLING: drawn from the EXISTING silver
``c:/tmp/dail_new_sources/silver/abp_inspector_reports.parquet`` (13,720 cases)
— no ArcGIS re-query. Stratified by (category, case_year) with a FIXED seed.
Rare categories are deliberately OVER-sampled (floor per category), so the raw
sample fraction is NOT the population base rate: the run prints BOTH the raw
sample rate and the stratum-weighted population estimate. Read the weighted one.

ISOLATION: sandbox only. Reads one existing silver parquet, writes only under
c:/tmp/dail_new_sources/. Nothing touches data/gold or pipeline.py. Polars only.

Licence: ACP case register is CC-BY (same chain as the promoted outcomes
extractor). The linked documents are the applicant's public EIA submission,
published by ACP for public inspection. Confirm re-use terms in
doc/source_licensing.md before any promotion.
"""
from __future__ import annotations

import argparse
import json
import random
import re
from pathlib import Path
from urllib.parse import quote, unquote

import polars as pl
import requests

from pipeline_sandbox.new_sources import _common
from pipeline_sandbox.new_sources._common import BRONZE, SILVER, cache_raw, fetch, sha256_bytes, write_silver

# The task brief specifies 0.5s. _common.fetch reads this global at call time.
_common.POLITE_DELAY_S = 0.5

SOURCE = "abp_case_documents"
COVERAGE_SOURCE = "abp_case_document_coverage"
FRAME_PARQUET = Path("c:/tmp/dail_new_sources/silver/abp_inspector_reports.parquet")
CASE_URL = "https://www.pleanala.ie/en-ie/case/{cid}"
HOST = "https://www.pleanala.ie"
BRONZE_DIR = BRONZE / SOURCE
MISSES = SILVER / f"{SOURCE}_misses.tsv"
SEED = 20260817  # fixed: the sample must be reproducible

# --- parsers -----------------------------------------------------------------
# hrefs are raw (un-encoded) paths with literal spaces. TWO shapes exist, and only
# the first is the structured tree the probe is measuring:
#   1. /publicaccess/EIAR-NIS/315933/Environmental/EIAR Appendices/Appendix 10-1 - Carbon Loss Calculation.pdf
#      -> tree='EIAR-NIS', deterministic <case>/<category>/<subcategory?>/<file>
#   2. /publicaccess/302885 - Inspector's Report/Appendix 4 ECIA.pdf
#      -> tree='other', a FREE-TEXT folder name with no taxonomy at all
# Matching only shape 1 would have silently reported shape-2 cases as "no documents".
_PUBLICACCESS = re.compile(r'href="(/publicaccess/[^"]+)"')
_LEADING_CASE = re.compile(r"^(\d{6})")
_DECISION_DOC = re.compile(
    r'href="(/anbordpleanala/media/abp/cases/(reports|orders|directions|bmr)/\d{3}/[^"]+)"'
)
# The case detail block is a run of label/value pairs in sibling divs.
_PAIR = re.compile(r'case-sub">\s*(.*?)\s*</p>.*?case-summary">\s*(.*?)\s*</p>', re.S)
_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")
# The EIA-procedure sentence. Captured as a whole sentence so a NEGATED variant
# ("is not subject to") is visible in the data rather than silently read as True.
_EIA_SENTENCE = re.compile(r"[^.<>]{0,120}subject to an EIA procedure[^.<>]{0,60}\.", re.I)
_SCOPING = re.compile(r"scoping", re.I)


def _clean(s: str) -> str:
    return _WS.sub(" ", _TAG.sub(" ", s)).strip()


def _abs_url(href: str) -> str:
    """Absolute URL with the path percent-encoded (hrefs carry literal spaces)."""
    return HOST + quote(href, safe="/-_.~()&,'")


def parse_documents(cid: str, html: str) -> list[dict]:
    """Every /publicaccess/ document on the page, split into its path parts.

    Within the EIAR-NIS tree, depth VARIES: 'Environmental' documents sit under a
    subcategory ('EIAR Chapters', 'Photomontages', ...); 'Planning' documents sit
    directly under the category with NO subcategory. Both shapes are recorded,
    with doc_subcategory null for the flat shape — do not assume a fixed depth.
    """
    out, seen = [], set()
    for href in _PUBLICACCESS.findall(html):
        if href in seen:
            continue
        seen.add(href)
        parts = unquote(href).strip("/").split("/")
        if len(parts) < 3:
            continue
        if parts[1] == "EIAR-NIS" and len(parts) >= 5:
            tree, path_case_id, category, rest = "EIAR-NIS", parts[2], parts[3], parts[4:]
        else:
            m = _LEADING_CASE.match(parts[1])
            tree, path_case_id, category, rest = "other", (m.group(1) if m else None), parts[1], parts[2:]
        filename = rest[-1]
        subcategory = "/".join(rest[:-1]) or None
        out.append(
            {
                "abp_case": cid,
                "tree": tree,
                "path_case_id": path_case_id,
                "doc_category": category,
                "doc_subcategory": subcategory,
                "filename": filename,
                "file_ext": filename.rsplit(".", 1)[-1].lower() if "." in filename else None,
                "path_depth": len(rest),
                "is_scoping_doc": bool(_SCOPING.search(filename)),
                "full_url": _abs_url(href),
                "href_path": href,
            }
        )
    return out


def parse_decision_docs(html: str) -> list[dict]:
    """The four deterministic decision-doc trees, as actually linked on the page."""
    out, seen = [], set()
    for href, kind in _DECISION_DOC.findall(html):
        if href in seen:
            continue
        seen.add(href)
        out.append({"doc_kind": kind, "full_url": HOST + quote(href, safe="/-_.~()&,'"), "href_path": href})
    return out


def parse_case_fields(html: str) -> dict:
    """Label/value pairs from the case detail block (Description, Case type,
    Decision, Date signed, EIAR, NIS)."""
    fields = {}
    for k, v in _PAIR.findall(html):
        key = _clean(k)
        if key and key not in fields:
            fields[key] = _clean(v)
    sent = _EIA_SENTENCE.search(_clean(html))
    return {
        "page_description": (fields.get("Description") or None),
        "page_case_type": fields.get("Case type"),
        "page_decision": fields.get("Decision"),
        "page_date_signed": fields.get("Date signed"),
        "page_eiar_flag": fields.get("EIAR"),
        "page_nis_flag": fields.get("NIS"),
        "eia_procedure_sentence": _clean(sent.group(0)) if sent else None,
        "states_eia_procedure": bool(sent) and " not subject" not in sent.group(0).lower(),
        "detail_field_labels": "|".join(sorted(fields)),
    }


# --- sampling ----------------------------------------------------------------
def build_sample(n: int, seed: int, min_per_category: int) -> pl.DataFrame:
    """Stratified sample over (category, case_year), fixed seed.

    Allocation is NOT proportional: each category gets a floor of
    ``min(stratum_size, min_per_category)`` so the rare, EIA-heavy categories
    (SID, LAP SID, Substitute Consent) are actually observed. The remainder is
    allocated proportionally. Each row carries ``stratum_weight`` =
    population_size / sampled_size so the population base rate can be recovered.
    """
    frame = pl.read_parquet(
        FRAME_PARQUET, columns=["abp_case", "category", "case_year", "planning_authority", "abp_decision_raw"]
    ).unique(subset=["abp_case"], keep="first")
    total = frame.height
    cats = frame.group_by("category").len().sort("len", descending=True)

    alloc: dict[str, int] = {}
    for cat, size in cats.iter_rows():
        alloc[cat] = min(size, min_per_category)
    used = sum(alloc.values())
    remaining = max(0, n - used)
    # Proportional top-up on the population that the floor has not already taken.
    spare = {c: max(0, s - alloc[c]) for c, s in cats.iter_rows()}
    spare_total = sum(spare.values())
    if spare_total and remaining:
        for cat, sp in sorted(spare.items(), key=lambda kv: -kv[1]):
            add = int(round(remaining * sp / spare_total))
            alloc[cat] += min(add, sp)
    # Trim/expand to hit n exactly, largest category absorbing the difference.
    biggest = cats.row(0)[0]
    diff = n - sum(alloc.values())
    alloc[biggest] = max(0, alloc[biggest] + diff)

    rng = random.Random(seed)
    picked: list[dict] = []
    for cat, k in alloc.items():
        sub = frame.filter(pl.col("category") == cat)
        if k >= sub.height:
            chosen = sub
        else:
            # Spread across years within the category, then random inside each year.
            years = sub.group_by("case_year").len().sort("len", descending=True)
            per_year, left = {}, k
            for yr, size in years.iter_rows():
                per_year[yr] = min(size, max(1, int(k * size / sub.height)))
            # fix rounding
            over = sum(per_year.values()) - k
            for yr in sorted(per_year, key=lambda y: -per_year[y]):
                while over > 0 and per_year[yr] > 1:
                    per_year[yr] -= 1
                    over -= 1
            left = k - sum(per_year.values())
            for yr in sorted(per_year, key=lambda y: -per_year[y]):
                cap = years.filter(pl.col("case_year").is_null() if yr is None else pl.col("case_year") == yr).row(0)[1]
                take = min(left, cap - per_year[yr])
                per_year[yr] += take
                left -= take
                if left <= 0:
                    break
            frames = []
            for yr, kk in per_year.items():
                if kk <= 0:
                    continue
                pool = sub.filter(pl.col("case_year").is_null() if yr is None else pl.col("case_year") == yr)
                ids = pool["abp_case"].to_list()
                rng.shuffle(ids)
                frames.append(pool.filter(pl.col("abp_case").is_in(ids[:kk])))
            chosen = pl.concat(frames) if frames else sub.head(0)
        pop = sub.height
        got = chosen.height
        for r in chosen.iter_rows(named=True):
            picked.append(
                {
                    **r,
                    "stratum_category": cat,
                    "stratum_population": pop,
                    "stratum_sampled": got,
                    "stratum_weight": pop / got if got else 0.0,
                }
            )
    df = pl.DataFrame(picked).sort("abp_case")
    print(f"[sample] frame={total} cases  drawn={df.height}  seed={seed}  floor={min_per_category}/category")
    small = [c for c, k in alloc.items() if k >= dict(cats.iter_rows())[c]]
    if small:
        print(f"[sample] TAKEN IN FULL (stratum smaller than allocation): {', '.join(sorted(small))}")
    return df


# --- fetch / resume ----------------------------------------------------------
def _bronze_paths(cid: str) -> tuple[Path, Path]:
    return BRONZE_DIR / f"case_{cid}.html", BRONZE_DIR / f"case_{cid}.meta.json"


def _load_misses() -> dict[str, str]:
    if not MISSES.exists():
        return {}
    out = {}
    for line in MISSES.read_text(encoding="utf-8").splitlines():
        if "\t" in line:
            cid, status = line.split("\t", 1)
            out[cid] = status
    return out


def _save_misses(m: dict[str, str]) -> None:
    MISSES.parent.mkdir(parents=True, exist_ok=True)
    MISSES.write_text("\n".join(f"{k}\t{v}" for k, v in sorted(m.items())), encoding="utf-8")


def get_case_html(cid: str) -> tuple[str | None, dict]:
    """Cached-first GET of a case page. Returns (html|None, meta).

    meta['http_status'] distinguishes a 404 (case page does not exist) from a
    200 that simply carries no document tree — the two are DIFFERENT findings
    and must never be collapsed.
    """
    html_p, meta_p = _bronze_paths(cid)
    if html_p.exists() and meta_p.exists():
        meta = json.loads(meta_p.read_text(encoding="utf-8"))
        meta["from_cache"] = True
        return html_p.read_text(encoding="utf-8", errors="ignore"), meta
    url = CASE_URL.format(cid=cid)
    try:
        html, meta = fetch(url)
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else -1
        return None, {"source_url": url, "http_status": status, "fetched_at": _common.now_iso(), "from_cache": False}
    except Exception as e:  # noqa: BLE001 — network/DNS; recorded, not fatal
        return None, {"source_url": url, "http_status": -1, "error": str(e)[:200],
                      "fetched_at": _common.now_iso(), "from_cache": False}
    body = html.encode("utf-8", errors="ignore")
    cache_raw(SOURCE, f"case_{cid}.html", body)
    meta = {
        "source_url": meta["source_url"],
        "http_status": meta["status"],
        "source_document_hash": sha256_bytes(body),
        "source_last_modified": meta.get("source_last_modified"),
        "fetched_at": meta["fetched_at"],
        "bytes": len(body),
        "from_cache": False,
    }
    meta_p.write_text(json.dumps(meta), encoding="utf-8")
    return html, meta


PROV_METHOD = "html_href_regex"


def main() -> None:
    ap = argparse.ArgumentParser(description="ACP case-page document-tree coverage probe (sandbox).")
    ap.add_argument("--sample-size", type=int, default=200, help="stratified sample size (default 200)")
    ap.add_argument("--limit", type=int, default=0, help="cap on cases actually fetched this run (0 = all sampled)")
    ap.add_argument("--seed", type=int, default=SEED, help="fixed sampling seed (reproducibility)")
    ap.add_argument("--min-per-category", type=int, default=12, help="floor per category stratum")
    args = ap.parse_args()

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    sample = build_sample(args.sample_size, args.seed, args.min_per_category)
    todo = sample.head(args.limit) if args.limit else sample
    misses = _load_misses()
    print(f"[queue] {todo.height} cases to process (known non-200 from previous runs: {len(misses)})")

    cov_rows: list[dict] = []
    doc_rows: list[dict] = []
    cached = fetched = 0
    for i, r in enumerate(todo.iter_rows(named=True), 1):
        cid = r["abp_case"]
        html, meta = get_case_html(cid)
        if meta.get("from_cache"):
            cached += 1
        else:
            fetched += 1
        base = {
            "abp_case": cid,
            "frame_category": r["category"],
            "frame_case_year": r["case_year"],
            "planning_authority": r["planning_authority"],
            "stratum_population": r["stratum_population"],
            "stratum_sampled": r["stratum_sampled"],
            "stratum_weight": r["stratum_weight"],
            "case_url": meta.get("source_url"),
            "http_status": meta.get("http_status"),
            "source_url": meta.get("source_url"),
            "source_document_hash": meta.get("source_document_hash"),
            "source_last_modified": meta.get("source_last_modified"),
            "source_published_date": None,
            "fetched_at": meta.get("fetched_at"),
            "extraction_method": PROV_METHOD,
            "privacy_tier": "public",
        }
        if html is None:
            misses[cid] = str(meta.get("http_status"))
            cov_rows.append({**base, "page_ok": False, "n_eiar_nis_docs": None, "n_decision_docs": None,
                             "has_eiar_nis_tree": None, "confidence": "none", "page_bytes": None})
            continue
        alldocs = parse_documents(cid, html)
        docs = [d for d in alldocs if d["tree"] == "EIAR-NIS"]
        other = [d for d in alldocs if d["tree"] != "EIAR-NIS"]
        decs = parse_decision_docs(html)
        fields = parse_case_fields(html)
        for d in alldocs:
            doc_rows.append({
                **d,
                "frame_category": r["category"],
                "frame_case_year": r["case_year"],
                "page_case_type": fields["page_case_type"],
                "source_url": base["source_url"],
                "source_document_hash": base["source_document_hash"],
                "source_last_modified": base["source_last_modified"],
                "source_published_date": None,
                "fetched_at": base["fetched_at"],
                "extraction_method": PROV_METHOD,
                "confidence": "high",  # a literal href on the case page — no inference
                "privacy_tier": "public",
            })
        subcats = sorted({d["doc_subcategory"] or d["doc_category"] for d in docs})
        cov_rows.append({
            **base,
            **fields,
            "page_ok": True,
            "page_bytes": len(html.encode("utf-8", errors="ignore")),
            "n_eiar_nis_docs": len(docs),
            "has_eiar_nis_tree": bool(docs),
            "n_other_publicaccess_docs": len(other),
            "has_other_publicaccess": bool(other),
            "other_publicaccess_folders": "|".join(sorted({d["doc_category"] for d in other})) if other else None,
            "n_doc_categories": len({d["doc_category"] for d in docs}),
            "doc_subcategories": "|".join(subcats) if subcats else None,
            "n_scoping_docs": sum(1 for d in alldocs if d["is_scoping_doc"]),
            "n_decision_docs": len(decs),
            "decision_doc_kinds": "|".join(sorted({d["doc_kind"] for d in decs})) if decs else None,
            "has_inspector_report": any(d["doc_kind"] == "reports" for d in decs),
            "has_order": any(d["doc_kind"] == "orders" for d in decs),
            "has_direction": any(d["doc_kind"] == "directions" for d in decs),
            "has_bmr": any(d["doc_kind"] == "bmr" for d in decs),
            "confidence": "high",
        })
        if i % 25 == 0:
            print(f"  [{i}/{todo.height}] cached={cached} fetched={fetched} "
                  f"with_tree={sum(1 for c in cov_rows if c.get('has_eiar_nis_tree'))}")

    _save_misses(misses)
    cov = pl.DataFrame(cov_rows, infer_schema_length=None)
    write_silver(COVERAGE_SOURCE, cov)
    if doc_rows:
        write_silver(SOURCE, pl.DataFrame(doc_rows, infer_schema_length=None))
    print(f"[silver] coverage rows={cov.height}  document rows={len(doc_rows)}  (cached={cached} fetched={fetched})")
    report(cov, pl.DataFrame(doc_rows, infer_schema_length=None) if doc_rows else None)


# --- measurement -------------------------------------------------------------
def report(cov: pl.DataFrame, docs: pl.DataFrame | None) -> None:
    pl.Config.set_tbl_rows(60)
    pl.Config.set_fmt_str_lengths(60)
    ok = cov.filter(pl.col("page_ok"))
    n404 = cov.filter(~pl.col("page_ok")).height
    with_tree = ok.filter(pl.col("has_eiar_nis_tree"))
    print("\n================ COVERAGE ================")
    print(f"sampled={cov.height}  page_200={ok.height}  page_non200={n404}")
    print(f"RAW sample rate with an EIAR-NIS tree: {with_tree.height}/{ok.height} "
          f"({100 * with_tree.height / max(1, ok.height):.1f}%)  <- NOT the population rate (rare cats over-sampled)")

    # Stratum-weighted population estimate: sum(weight * hit) / sum(weight).
    wt = ok.select(
        (pl.col("stratum_weight") * pl.col("has_eiar_nis_tree").cast(pl.Float64)).sum().alias("num"),
        pl.col("stratum_weight").sum().alias("den"),
    ).row(0)
    print(f"WEIGHTED population estimate: {100 * wt[0] / wt[1]:.2f}% of the 13,720-case frame carry a tree")

    oth = ok.filter(pl.col("has_other_publicaccess").fill_null(False))
    any_pa = ok.filter(pl.col("has_eiar_nis_tree") | pl.col("has_other_publicaccess").fill_null(False))
    wt2 = ok.select(
        (pl.col("stratum_weight") * (pl.col("has_eiar_nis_tree") | pl.col("has_other_publicaccess").fill_null(False))
         .cast(pl.Float64)).sum().alias("num"),
        pl.col("stratum_weight").sum().alias("den"),
    ).row(0)
    print(f"OTHER (free-text folder) /publicaccess tree: {oth.height}/{ok.height} raw")
    print(f"ANY /publicaccess documents: {any_pa.height}/{ok.height} raw; "
          f"WEIGHTED {100 * wt2[0] / wt2[1]:.2f}% of the frame")

    print("\n--- by CATEGORY (raw, within stratum) ---")
    print(ok.group_by("frame_category").agg(
        pl.len().alias("sampled"),
        pl.col("has_eiar_nis_tree").sum().alias("with_tree"),
        (100 * pl.col("has_eiar_nis_tree").mean()).round(1).alias("pct"),
        pl.col("stratum_population").first().alias("pop"),
    ).sort("pct", descending=True))

    print("\n--- by YEAR (raw) ---")
    print(ok.group_by("frame_case_year").agg(
        pl.len().alias("sampled"),
        pl.col("has_eiar_nis_tree").sum().alias("with_tree"),
        (100 * pl.col("has_eiar_nis_tree").mean()).round(1).alias("pct"),
    ).sort("frame_case_year"))

    print("\n--- by PAGE case type (the register's own type string) ---")
    print(ok.group_by("page_case_type").agg(
        pl.len().alias("sampled"),
        pl.col("has_eiar_nis_tree").sum().alias("with_tree"),
    ).sort("with_tree", descending=True).head(20))

    print("\n--- EIAR / NIS flags vs the tree ---")
    print(ok.group_by(["page_eiar_flag", "page_nis_flag"]).agg(
        pl.len().alias("cases"),
        pl.col("has_eiar_nis_tree").sum().alias("with_tree"),
        pl.col("n_eiar_nis_docs").sum().alias("docs"),
    ).sort("cases", descending=True))
    print(f"states 'subject to an EIA procedure': {ok['states_eia_procedure'].sum()}/{ok.height}")

    if with_tree.height:
        s = with_tree["n_eiar_nis_docs"]
        print(f"\ndocs per case WITH a tree: n={with_tree.height} median={s.median()} "
              f"mean={s.mean():.1f} min={s.min()} max={s.max()} total={s.sum()}")

    print("\n--- decision-doc trees (linked on the page) ---")
    print(ok.select(
        pl.col("has_inspector_report").sum().alias("reports"),
        pl.col("has_order").sum().alias("orders"),
        pl.col("has_direction").sum().alias("directions"),
        pl.col("has_bmr").sum().alias("bmr"),
        pl.len().alias("of_pages"),
    ))

    if docs is not None and not docs.is_empty():
        print("\n--- TAXONOMY: tree / doc_category / doc_subcategory frequencies ---")
        print(docs.group_by(["tree", "doc_category", "doc_subcategory"]).agg(
            pl.len().alias("docs"), pl.col("abp_case").n_unique().alias("cases")
        ).sort("docs", descending=True).head(45))
        print("\n--- EIAR-NIS subcategory naming stability (cases carrying each label) ---")
        e = docs.filter(pl.col("tree") == "EIAR-NIS")
        print(e.group_by(["doc_category", "doc_subcategory"]).agg(
            pl.col("abp_case").n_unique().alias("cases"), pl.len().alias("docs")
        ).sort("cases", descending=True).head(30))
        print("\n--- SCOPING documents: filename variants ---")
        sc = docs.filter(pl.col("is_scoping_doc"))
        print(f"scoping docs={sc.height} across {sc['abp_case'].n_unique()} cases")
        if sc.height:
            print(sc.group_by("filename").agg(pl.len().alias("n")).sort("n", descending=True).head(25))
        print("\n--- file extensions ---")
        print(docs.group_by("file_ext").agg(pl.len().alias("n")).sort("n", descending=True).head(10))

    # Corpus projection: weighted per-case document expectation x frame size.
    per_case = ok.select(
        (pl.col("stratum_weight") * pl.col("n_eiar_nis_docs").fill_null(0)).sum().alias("num"),
        pl.col("stratum_weight").sum().alias("den"),
    ).row(0)
    print(f"\n[projection] weighted mean docs/case = {per_case[0] / per_case[1]:.2f} "
          f"-> ~{per_case[0] / per_case[1] * 13720:,.0f} document rows if the whole 13,720-case frame were indexed")


if __name__ == "__main__":
    main()
