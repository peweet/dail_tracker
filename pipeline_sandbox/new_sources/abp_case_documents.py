"""ACP (An Coimisiún Pleanála) CASE-DOCUMENT COVERAGE PROBE — sandbox measurement, not an ingest.

THE QUESTION: pleanala.ie serves the APPLICANT'S full EIA document set at
    https://www.pleanala.ie/publicaccess/EIAR-NIS/<caseid>/<Category>/<Subcategory>/<name>.pdf
and the whole manifest is server-rendered into the case page at
    https://www.pleanala.ie/en-ie/case/<caseid>
so ONE GET enumerates every document. Verified on case 315933 (Sheskin South wind farm).

One case having a tree proves nothing. This probe measures the BASE RATE: what fraction of ACP
cases actually carry an EIAR-NIS tree, broken down by category and year, so the applicability
question ("is this concentrated in SID/direct energy applications rather than ordinary appeals?")
is answered with a number instead of an anecdote.

FRAME: the existing sandbox parquet ``silver/abp_inspector_reports.parquet`` (13,720 cases,
2020-2026, cases whose inspector-report PDF was successfully fetched). NO ArcGIS re-query.
Stratified by (category, case_year) with a FIXED seed; small strata taken whole. Each sampled
case carries its Horvitz-Thompson weight (frame stratum size / sampled stratum size) so a
frame-level base rate can be estimated from a deliberately category-balanced sample.

WHAT IS RECORDED: a row for EVERY sampled case — including the zeros. A 404 (case page gone)
is recorded distinctly from a 200-with-no-tree; the negative IS the measurement. Also captured:
the decision-doc links present on the page (reports / orders / directions / bmr), the page's own
EIAR and NIS flags, case type, decision, date signed and development description.

ISOLATION: sandbox only. Reads one sandbox parquet, writes only under c:/tmp/dail_new_sources/.
Nothing touches data/gold, pipeline.py, or any promotion path. Polars only.

Licence: ACP case register is CC-BY (as used by the already-promoted outcomes chain). The
applicant document set is published by ACP under the EIA public-participation regime; confirm
re-use terms in doc/source_licensing.md before ANY promotion.
"""
from __future__ import annotations

import argparse
import html
import json
import random
import re
from collections import Counter
from urllib.parse import quote

import polars as pl
import requests

from pipeline_sandbox.new_sources import _common
from pipeline_sandbox.new_sources._common import BRONZE, SILVER, cache_raw, fetch, now_iso, sha256_bytes

# The probe is a measurement pass over a live public site — be slower than the house default.
_common.POLITE_DELAY_S = 0.5

SOURCE = "abp_case_documents"
COVERAGE_SOURCE = "abp_case_documents_coverage"
FRAME_PARQUET = SILVER / "abp_inspector_reports.parquet"
CASE_URL = "https://www.pleanala.ie/en-ie/case/{cid}"
HOST = "https://www.pleanala.ie"
MISSES = SILVER / "abp_case_documents_misses.txt"  # case ids whose case page 404s

# EVERY /publicaccess/ document href. Anchored on the enclosing double quote, NOT on a character
# class: an earlier version excluded "'" and silently TRUNCATED paths like
# ".../1.0 King's Island FRS Cover Letter/..." — which then failed the filename test and dropped a
# whole case's tree. Delimit on the quote and nothing else.
_PUBACCESS_HREF = re.compile(r'href="(/publicaccess/[^"]+)"', re.I)
# Observed EIAR-NIS path depths run 4..9 segments:
#   /publicaccess/EIAR-NIS/<cid>/<file>                          FLAT (no folders at all)
#   /publicaccess/EIAR-NIS/<cid>/<category>/<file>
#   /publicaccess/EIAR-NIS/<cid>/<category>/<subcategory>/.../<file>
# so category/subcategory are OPTIONAL, and anything deeper is kept in `rel_path`.
_EIARNIS_SEG = "eiar-nis"
# Board decision documents on the same host, all deterministic on the 3-digit case group.
_DECISION_HREF = re.compile(
    r"/?(?:anbordpleanala/)?media/abp/cases/(reports|orders|directions|bmr)/(\d{3})/([^\"'<>]+\.pdf)", re.I
)
# Server-rendered key/value block: <p class="case-sub">LABEL</p> ... <p class="case-summary">VALUE</p>
_FIELD = re.compile(
    r'case-sub">\s*([^<]{1,40}?)\s*</p>.{0,200}?class="case-summary">\s*(.{0,600}?)\s*</p>', re.S
)
_EIA_PROCEDURE = re.compile(r"subject to an EIA procedure", re.I)
_TAG = re.compile(r"<[^>]+>")

_WANTED_FIELDS = {
    "description": "Description",
    "case_type": "Case type",
    "page_decision": "Decision",
    "date_signed": "Date signed",
    "eiar_flag": "EIAR",
    "nis_flag": "NIS",
}
# Scoping-document filename variants. Kept deliberately loose: the probe REPORTS the variants it
# sees rather than assuming "Appendix 2-1 - Scoping Responses.pdf" is the canonical name.
_SCOPING = re.compile(r"scoping", re.I)


# ---------------------------------------------------------------- sampling frame


def build_sample(seed: int, size: int) -> tuple[pl.DataFrame, dict[tuple[str, int | None], int]]:
    """Stratified (category, case_year) sample from the inspector-report frame.

    Allocation is deliberately NOT proportional: 89% of the frame is 'Appeals', so a proportional
    draw would leave ~2 SID cases and could not answer the concentration question at all. Instead
    every category with enough rows gets a floor of MIN_PER_CATEGORY, small categories are taken
    whole, and the remainder goes to Appeals. Each row carries its stratum weight so a frame-level
    rate can still be estimated (Horvitz-Thompson) — see report().
    """
    frame = pl.read_parquet(FRAME_PARQUET).select(
        ["abp_case", "planning_authority", "category", "case_year", "abp_decision_raw"]
    )
    counts = {c: n for c, n in frame.group_by("category").len().iter_rows()}
    min_per_cat = 20

    alloc: dict[str, int] = {}
    for cat, n in counts.items():
        alloc[cat] = min(n, min_per_cat)
    spare = size - sum(alloc.values())
    if spare > 0:  # give the slack to the dominant category
        big = max(counts, key=lambda c: counts[c])
        alloc[big] = min(counts[big], alloc[big] + spare)

    rng = random.Random(seed)
    picked: list[dict] = []
    stratum_sizes: dict[tuple[str, int | None], int] = {}
    for cat, want in sorted(alloc.items()):
        sub = frame.filter(pl.col("category") == cat)
        years = sorted(
            ((y, n) for y, n in sub.group_by("case_year").len().iter_rows()),
            key=lambda t: (t[0] is None, t[0]),
        )
        total = sum(n for _, n in years)
        # largest-remainder allocation across years, capped by each year's size
        raw = {y: want * n / total for y, n in years}
        take = {y: min(int(v), dict(years)[y]) for y, v in raw.items()}
        while sum(take.values()) < want:
            rem = sorted(
                ((y, raw[y] - take[y]) for y, n in years if take[y] < dict(years)[y]),
                key=lambda t: -t[1],
            )
            if not rem:
                break
            take[rem[0][0]] += 1
        for y, n_take in take.items():
            if n_take <= 0:
                continue
            rows = sub.filter(
                pl.col("case_year").is_null() if y is None else pl.col("case_year") == y
            ).to_dicts()
            rows.sort(key=lambda r: r["abp_case"])  # deterministic order before the seeded shuffle
            rng.shuffle(rows)
            chosen = rows[:n_take]
            stratum_sizes[(cat, y)] = len(rows)
            for r in chosen:
                r["_stratum_frame_n"] = len(rows)
                r["_stratum_sample_n"] = len(chosen)
                r["_stratum_whole"] = len(chosen) == len(rows)
            picked.extend(chosen)
    # Interleave the strata so --limit N is a spread across categories/years, not the first
    # category alphabetically. Same seeded rng, so the order is still reproducible.
    rng.shuffle(picked)
    return pl.DataFrame(picked), stratum_sizes


def boost_category(sample: pl.DataFrame, category: str, n_extra: int, seed: int) -> pl.DataFrame:
    """Add n_extra cases of one category, DISJOINT from the existing sample, and recompute the
    stratum sample sizes so the Horvitz-Thompson weights stay correct.

    Needed because 'Appeals' is 89% of the ACP register: a 0/49 result there is consistent with
    anything up to ~7% by Clopper-Pearson, and 7% of 23,386 appeals is a 1,600-case swing in the
    corpus projection. A bigger Appeals stratum buys a much tighter upper bound.
    """
    frame = pl.read_parquet(FRAME_PARQUET).select(
        ["abp_case", "planning_authority", "category", "case_year", "abp_decision_raw"]
    )
    have = set(sample["abp_case"].to_list())
    pool = frame.filter((pl.col("category") == category) & ~pl.col("abp_case").is_in(list(have)))
    rows = pool.to_dicts()
    rows.sort(key=lambda r: r["abp_case"])
    random.Random(seed + 1).shuffle(rows)
    extra = rows[:n_extra]
    for r in extra:
        r["_stratum_frame_n"] = 0  # placeholder; recomputed below
        r["_stratum_sample_n"] = 0
        r["_stratum_whole"] = False
    out = pl.concat([sample, pl.DataFrame(extra)], how="diagonal_relaxed")
    # Recompute per-(category, year) sample sizes; frame sizes come from the frame itself.
    fsize = {
        (c, y): n for c, y, n in frame.group_by(["category", "case_year"]).len().iter_rows()
    }
    ssize = {
        (c, y): n for c, y, n in out.group_by(["category", "case_year"]).len().iter_rows()
    }
    return out.with_columns(
        pl.struct(["category", "case_year"])
        .map_elements(lambda s: fsize.get((s["category"], s["case_year"]), 0), return_dtype=pl.Int64)
        .alias("_stratum_frame_n"),
        pl.struct(["category", "case_year"])
        .map_elements(lambda s: ssize.get((s["category"], s["case_year"]), 0), return_dtype=pl.Int64)
        .alias("_stratum_sample_n"),
    ).with_columns((pl.col("_stratum_frame_n") == pl.col("_stratum_sample_n")).alias("_stratum_whole"))


# ---------------------------------------------------------------- parsing


def parse_case_page(cid: str, text: str) -> dict:
    """Everything the probe needs from one server-rendered case page."""
    fields: dict[str, str] = {}
    for label, value in _FIELD.findall(text):
        lab = html.unescape(_TAG.sub("", label)).strip()
        val = re.sub(r"\s+", " ", html.unescape(_TAG.sub("", value))).strip()
        fields.setdefault(lab, val)

    docs: list[dict] = []
    other_fams: Counter = Counter()
    seen: set[str] = set()
    for href in _PUBACCESS_HREF.findall(text):
        path = html.unescape(href).strip()
        if path in seen:
            continue
        seen.add(path)
        seg = [s for s in path.split("/") if s]
        if len(seg) < 3 or "." not in seg[-1]:
            continue  # not a file link
        if seg[1].lower() != _EIARNIS_SEG:
            # A DIFFERENT publicaccess tree (Case Documentation / Submissions / FurtherInformation /
            # Observation / referrals / Responses / SubstituteConsent, plus per-case one-off folders).
            # Counted, because "no EIAR-NIS tree" is NOT the same as "no documents".
            fam = seg[1]
            if re.match(r"^\d{4,6}", fam) or " - " in fam:
                fam = "<case-specific folder>"
            other_fams[fam] += 1
            continue
        filename = seg[-1]
        doc_cid = seg[2]
        category = seg[3] if len(seg) >= 5 else None
        subcategory = seg[4] if len(seg) >= 6 else None
        docs.append(
            {
                "abp_case": cid,
                "path_case_id": doc_cid,
                "doc_category": category,
                "doc_subcategory": subcategory,
                "path_depth": len(seg),
                "rel_path": "/".join(seg[3:-1]) or None,
                "filename": filename,
                "file_ext": filename.rsplit(".", 1)[-1].lower(),
                "full_url": HOST + quote(path),
                "is_scoping": bool(_SCOPING.search(filename)),
            }
        )

    decision: list[dict] = []
    dseen: set[tuple[str, str]] = set()
    for kind, grp, name in _DECISION_HREF.findall(text):
        key = (kind.lower(), name)
        if key in dseen:
            continue
        dseen.add(key)
        decision.append(
            {
                "abp_case": cid,
                "doc_kind": kind.lower(),
                "filename": name,
                "full_url": f"{HOST}/anbordpleanala/media/abp/cases/{kind.lower()}/{grp}/{quote(name)}",
            }
        )

    out = {
        "eia_procedure_stated": bool(_EIA_PROCEDURE.search(text)),
        "doc_count": len(docs),
        "has_eiar_tree": len(docs) > 0,
        "scoping_doc_count": sum(1 for d in docs if d["is_scoping"]),
        "decision_doc_kinds": ",".join(sorted({d["doc_kind"] for d in decision})) or None,
        "decision_doc_count": len(decision),
        # The wider applicant-document surface OUTSIDE the EIAR-NIS tree.
        "other_doc_count": int(sum(other_fams.values())),
        "other_doc_families": ",".join(sorted(other_fams)) or None,
        "has_any_documents": bool(docs) or bool(other_fams),
    }
    for key, label in _WANTED_FIELDS.items():
        out[key] = fields.get(label)
    return out | {"_docs": docs, "_decision": decision}


# ---------------------------------------------------------------- fetch / cache


def _bronze_paths(cid: str):
    d = BRONZE / SOURCE
    return d / f"case_{cid}.html", d / f"case_{cid}.meta.json"


def get_case_html(cid: str, refresh: bool) -> tuple[str | None, dict]:
    """Return (html, meta). Cached HTML is reused unless --refresh. meta['http_status'] is 404
    when the case page is gone — recorded, never silently skipped."""
    hp, mp = _bronze_paths(cid)
    if hp.exists() and mp.exists() and not refresh:
        meta = json.loads(mp.read_text(encoding="utf-8"))
        meta["from_cache"] = True
        return hp.read_text(encoding="utf-8", errors="ignore"), meta
    url = CASE_URL.format(cid=cid)
    try:
        payload, meta = fetch(url)
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else 0
        return None, {
            "source_url": url,
            "http_status": status,
            "source_document_hash": None,
            "fetched_at": now_iso(),
            "bytes": 0,
            "from_cache": False,
        }
    except requests.RequestException as e:  # noqa: BLE001 — transient network, recorded as an error row
        return None, {
            "source_url": url,
            "http_status": -1,
            "error": type(e).__name__,
            "source_document_hash": None,
            "fetched_at": now_iso(),
            "bytes": 0,
            "from_cache": False,
        }
    raw = payload.encode("utf-8", errors="ignore")
    cache_raw(SOURCE, f"case_{cid}.html", raw)
    meta = {
        "source_url": meta["source_url"],
        "http_status": meta["status"],
        "source_document_hash": sha256_bytes(raw),
        "fetched_at": meta["fetched_at"],
        "source_last_modified": meta.get("source_last_modified"),
        "bytes": len(raw),
        "from_cache": False,
    }
    _bronze_paths(cid)[1].write_text(json.dumps(meta), encoding="utf-8")
    return payload, meta


# ---------------------------------------------------------------- reporting


def report(cov: pl.DataFrame, docs: pl.DataFrame) -> None:
    ok = cov.filter(pl.col("http_status") == 200)
    hit = ok.filter(pl.col("has_eiar_tree"))
    print("\n================ COVERAGE ================")
    print(f"[sample] {cov.height} cases fetched  |  200={ok.height}  404={cov.filter(pl.col('http_status') == 404).height}  error={cov.filter(pl.col('http_status') < 0).height}")
    print(f"[base rate] cases WITH an EIAR-NIS tree: {hit.height}/{ok.height} = {100 * hit.height / max(ok.height, 1):.1f}% (sample, category-balanced)")

    # Horvitz-Thompson: reweight the balanced sample back to the frame.
    w = ok.with_columns((pl.col("stratum_frame_n") / pl.col("stratum_sample_n")).alias("w"))
    num = w.filter(pl.col("has_eiar_tree"))["w"].sum()
    den = w["w"].sum()
    print(f"[base rate] frame-weighted estimate (Horvitz-Thompson over 13,720-case frame): {100 * num / den:.2f}%")

    print("\n-- by category --")
    by_cat = (
        ok.group_by("category")
        .agg(
            pl.len().alias("n"),
            pl.col("has_eiar_tree").sum().alias("with_tree"),
            pl.col("doc_count").sum().alias("docs"),
        )
        .with_columns((100 * pl.col("with_tree") / pl.col("n")).round(1).alias("pct"))
        .sort("pct", descending=True)
    )
    with pl.Config(tbl_rows=30):
        print(by_cat)

    print("\n-- by year --")
    by_yr = (
        ok.group_by("case_year")
        .agg(pl.len().alias("n"), pl.col("has_eiar_tree").sum().alias("with_tree"))
        .with_columns((100 * pl.col("with_tree") / pl.col("n")).round(1).alias("pct"))
        .sort("case_year")
    )
    with pl.Config(tbl_rows=30):
        print(by_yr)

    print("\n-- page EIAR flag vs actual tree (is the flag a cheap predictor?) --")
    with pl.Config(tbl_rows=20):
        print(
            ok.group_by(["eiar_flag", "has_eiar_tree"]).len().sort("len", descending=True)
        )
    print("\n-- page NIS flag vs actual tree --")
    with pl.Config(tbl_rows=20):
        print(ok.group_by(["nis_flag", "has_eiar_tree"]).len().sort("len", descending=True))

    if hit.height:
        d = hit["doc_count"]
        print(f"\n[docs/case | cases WITH a tree] n={hit.height} median={d.median():.0f} mean={d.mean():.1f} min={d.min()} max={d.max()}")

    if docs.height:
        print(f"\n-- taxonomy: {docs.height} document rows --")
        with pl.Config(tbl_rows=40, fmt_str_lengths=60):
            print(
                docs.group_by(["doc_category", "doc_subcategory"])
                .agg(pl.len().alias("docs"), pl.col("abp_case").n_unique().alias("cases"))
                .sort("docs", descending=True)
            )
        print("\n-- scoping-style filenames --")
        sc = docs.filter(pl.col("is_scoping"))
        print(f"[scoping] {sc['abp_case'].n_unique()} of {docs['abp_case'].n_unique()} tree-bearing cases carry >=1 scoping-named doc ({sc.height} docs)")
        with pl.Config(tbl_rows=25, fmt_str_lengths=90):
            print(sc.group_by("filename").len().sort("len", descending=True).head(25))

    print("\n-- OTHER publicaccess trees (EIAR-NIS is not the only document surface) --")
    anyd = ok.filter(pl.col("has_any_documents"))
    other_only = ok.filter(~pl.col("has_eiar_tree") & (pl.col("other_doc_count") > 0))
    print(f"[surface] cases with ANY publicaccess document: {anyd.height}/{ok.height} = {100 * anyd.height / max(ok.height, 1):.1f}%")
    print(f"[surface] cases with documents but NO EIAR-NIS tree: {other_only.height}")
    fam_counter: Counter = Counter()
    for s in ok["other_doc_families"].drop_nulls().to_list():
        for f in s.split(","):
            fam_counter[f] += 1
    for k, v in fam_counter.most_common(15):
        print(f"    {v:4d} cases  /publicaccess/{k}")
    with pl.Config(tbl_rows=20):
        print(other_only.group_by("category").len().sort("len", descending=True))

    print("\n-- decision-doc availability (reports/orders/directions/bmr) --")
    with pl.Config(tbl_rows=20):
        print(ok.group_by("decision_doc_kinds").len().sort("len", descending=True))

    print("\n-- case type of tree-bearing cases (the concentration question) --")
    with pl.Config(tbl_rows=25, fmt_str_lengths=60):
        print(hit.group_by("case_type").len().sort("len", descending=True))

    # Corpus-size projection over the SAMPLE FRAME only (13,720 report-bearing cases 2020-2026).
    frame_total = int(ok["stratum_frame_n"].unique().sum()) if ok.height else 0
    if hit.height:
        w_hit = w.filter(pl.col("has_eiar_tree"))
        est_cases = float(num)
        est_docs = float((w_hit["w"] * w_hit["doc_count"]).sum())
        print(f"\n[projection] frame strata covered = {frame_total} cases")
        print(f"[projection] estimated tree-bearing cases in frame: {est_cases:,.0f}")
        print(f"[projection] estimated document ROWS if the whole frame were indexed: {est_docs:,.0f}")


# ---------------------------------------------------------------- main


def main() -> None:
    ap = argparse.ArgumentParser(description="ACP case-document coverage probe (measurement, not an ingest)")
    ap.add_argument("--sample-size", type=int, default=200, help="stratified sample size (default 200)")
    ap.add_argument("--seed", type=int, default=20260817, help="fixed seed — the run is reproducible")
    ap.add_argument("--limit", type=int, default=0, help="stop after N cases (0 = whole sample)")
    ap.add_argument("--refresh", action="store_true", help="re-fetch even if bronze HTML is cached")
    ap.add_argument(
        "--boost",
        default="",
        help="tighten one category's CI, e.g. 'Appeals=200'. Appeals is 89%% of the register, so a "
        "0/49 result there leaves a wide upper bound on the whole corpus estimate. Boost rows are "
        "drawn with the SAME seed, disjoint from the main sample, and their stratum weights are "
        "recomputed so the Horvitz-Thompson estimate stays unbiased.",
    )
    ap.add_argument("--report-only", action="store_true", help="re-report from existing silver, no network")
    args = ap.parse_args()

    if args.report_only:
        cov = pl.read_parquet(SILVER / f"{COVERAGE_SOURCE}.parquet")
        docs = pl.read_parquet(SILVER / f"{SOURCE}.parquet")
        report(cov, docs)
        return

    sample, strata = build_sample(args.seed, args.sample_size)
    if args.boost:
        cat, n_extra = args.boost.rsplit("=", 1)
        sample = boost_category(sample, cat, int(n_extra), args.seed)
        print(f"[boost] +{n_extra} extra '{cat}' cases; stratum weights recomputed")
    print(f"[sample] {sample.height} cases, seed={args.seed}, strata={len(strata)}")
    whole = sample.filter(pl.col("_stratum_whole"))
    print(f"[sample] {whole.height} rows come from strata taken WHOLE (stratum smaller than its allocation)")
    with pl.Config(tbl_rows=20):
        print(sample.group_by("category").len().sort("len", descending=True))

    todo = sample.to_dicts()
    if args.limit:
        todo = todo[: args.limit]

    cov_rows: list[dict] = []
    doc_rows: list[dict] = []
    dec_rows: list[dict] = []
    misses: set[str] = set()
    cached = 0
    for i, r in enumerate(todo, 1):
        cid = r["abp_case"]
        payload, meta = get_case_html(cid, args.refresh)
        cached += bool(meta.get("from_cache"))
        base = {
            "abp_case": cid,
            "planning_authority": r["planning_authority"],
            "category": r["category"],
            "case_year": r["case_year"],
            "abp_decision_raw": r["abp_decision_raw"],
            "stratum_frame_n": r["_stratum_frame_n"],
            "stratum_sample_n": r["_stratum_sample_n"],
            "stratum_whole": r["_stratum_whole"],
            "http_status": meta["http_status"],
            "source_url": meta["source_url"],
            "source_document_hash": meta["source_document_hash"],
            "fetched_at": meta["fetched_at"],
            "source_last_modified": meta.get("source_last_modified"),
            "source_published_date": None,
            "extraction_method": "html_regex_case_page",
            "privacy_tier": "public",
        }
        if payload is None:
            # A 404 (or transient error) is a DISTINCT outcome from a 200 with no tree. Recorded.
            if meta["http_status"] == 404:
                misses.add(cid)
            cov_rows.append(
                base
                | {
                    "has_eiar_tree": None,
                    "doc_count": 0,
                    "scoping_doc_count": 0,
                    "decision_doc_count": 0,
                    "decision_doc_kinds": None,
                    "eia_procedure_stated": None,
                    "description": None,
                    "case_type": None,
                    "page_decision": None,
                    "date_signed": None,
                    "eiar_flag": None,
                    "nis_flag": None,
                    "confidence": "none",
                }
            )
            continue
        parsed = parse_case_page(cid, payload)
        docs = parsed.pop("_docs")
        dec = parsed.pop("_decision")
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
        for d in docs:
            doc_rows.append(
                d
                | {
                    "category": r["category"],
                    "case_year": r["case_year"],
                    "case_type": parsed.get("case_type"),
                    "planning_authority": r["planning_authority"],
                }
                | prov
            )
        dec_rows.extend(d | prov for d in dec)
        cov_rows.append(base | parsed | {"confidence": "high"})
        if i % 25 == 0:
            print(f"  [{i}/{len(todo)}] trees so far={sum(1 for c in cov_rows if c.get('has_eiar_tree'))} docs={len(doc_rows)} cached={cached}")

    if misses:
        MISSES.write_text("\n".join(sorted(misses)), encoding="utf-8")

    cov = pl.DataFrame(cov_rows)
    docs_df = pl.DataFrame(doc_rows) if doc_rows else pl.DataFrame()
    p1 = _common.write_silver(COVERAGE_SOURCE, cov)
    print(f"\n[silver] coverage (case grain, ZEROS INCLUDED) {cov.height} rows -> {p1}")
    if not docs_df.is_empty():
        p2 = _common.write_silver(SOURCE, docs_df)
        print(f"[silver] documents (document grain) {docs_df.height} rows -> {p2}")
    if dec_rows:
        p3 = _common.write_silver("abp_case_decision_docs", pl.DataFrame(dec_rows))
        print(f"[silver] decision docs {len(dec_rows)} rows -> {p3}")
    print(f"[cache] {cached}/{len(todo)} served from bronze (resumable)")

    report(cov, docs_df)

    # Naming-drift check: is the subcategory vocabulary stable across cases, or does it wander?
    if not docs_df.is_empty():
        vocab = Counter(
            (d or "") for d in docs_df["doc_subcategory"].to_list()
        )
        print(f"\n[naming] {len(vocab)} distinct subcategory strings observed across {docs_df['abp_case'].n_unique()} tree-bearing cases")
        for k, v in vocab.most_common(30):
            print(f"    {v:5d}  {k!r}")


if __name__ == "__main__":
    main()
