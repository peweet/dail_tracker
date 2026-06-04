"""PHASE-2 PROBE (PRE-ETL): fetch each public-body landing page and report what's there.

Reads the seed list from procurement_publishers_seed.py and, for every row with a
landing_url, REFINES the registry from real HTTP evidence — exactly the LA harvester
pattern (procurement_la_seed.py), generalised from "PO-over-20k" to public-body
PO/payments pages and given a deeper one-hop crawl because gov.ie *collection* pages link
to publication pages that hold the actual files.

It deliberately does the cheap discovery work only (plan §3.2):
  - fetch landing page (requests, curl fallback for council-style TLS quirks)
  - extract pdf/xlsx/xls/csv links that look procurement-ish
  - if none on the landing page, one-hop crawl same-host nav links and look there
  - download ONE sample per publisher and classify: PDF digital(fitz)/scanned(OCR),
    XLSX header row, CSV shape
  - NO full corpus download, NO OCR, NO parsing of every file

Writes a coverage report with the plan §6/Phase-2 keys to:
  c:/tmp/procurement_publishers/procurement_publishers_probe.json

Run:
  ./.venv/Scripts/python.exe pipeline_sandbox/probe_procurement_publishers.py
  ./.venv/Scripts/python.exe pipeline_sandbox/probe_procurement_publishers.py --dry-run
  ./.venv/Scripts/python.exe pipeline_sandbox/probe_procurement_publishers.py --only ie_opw,ie_teagasc
"""

from __future__ import annotations

import argparse
import contextlib
import json
import re
import subprocess
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "extractors"))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from procurement_publishers_seed import SEEDS  # noqa: E402

H = {"User-Agent": "Mozilla/5.0 (dail-tracker research probe)"}
TMP = Path("c:/tmp/procurement_publishers")
OUT = TMP / "procurement_publishers_probe.json"

DATA_EXT = (".pdf", ".xlsx", ".xls", ".csv")
# link/url must look procurement-ish to avoid harvesting nav junk
PO_HINT = re.compile(
    r"purchase|p\.?o\.?s?\b|20[,]?0?00|20k|25[,]?0?00|payment|supplier|procure|"
    r"quarter|q[1-4]|spend|expenditure|over.?20|over.?25",
    re.I,
)
# an ACTUAL period data file (vs a policy/guidance doc) — used to pick the best sample
DATA_FILE_RE = re.compile(r"q[1-4]\b|qtr|quarter|20[12]\d|h[12]\b|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec", re.I)
# exclude policy / guidance / annual-report-ish docs from the sample pick
POLICY_RE = re.compile(
    r"guide|guidelin|\bplan\b|policy|circular|strategy|manual|terms|fin.?07|"
    r"prompt.?payment|\bcontract\b|appendix|procedure|annual.?report|statement",
    re.I,
)
HREF_RE = re.compile(r"""href\s*=\s*["']([^"']+)["']""", re.I)
# nav links worth a one-hop crawl when the landing page lists no files directly
NAV_HINT = re.compile(
    r"purchase|procure|over.?20|over.?25|20k|payment|quarter|qtr|finance|"
    r"publication|spend|supplier|expenditure|disclosure|fees-and-charges",
    re.I,
)
# privacy tripwire: surfaces when sampled rows / link text hint at individuals
PERSON_HINT = re.compile(r"\bsole.?trader\b|\bindividual\b|\bgrant(ee|-aid)\b|\bbursar|\bstipend\b", re.I)


def hr(t: str) -> None:
    print(f"\n{'=' * 74}\n{t}\n{'=' * 74}")


def _curl(url: str) -> bytes | None:
    """Fallback fetch via curl — some gov/council hosts fail Python's TLS stack but
    answer curl fine (not a server block). -k tolerates their cert quirks."""
    try:
        p = subprocess.run(
            ["curl", "-sS", "-k", "-L", "--max-time", "40", "-A", H["User-Agent"], url],
            capture_output=True, timeout=60,
        )
        return p.stdout if p.returncode == 0 and p.stdout else None
    except Exception:
        return None


def fetch_bytes(url: str) -> bytes | None:
    try:
        r = requests.get(url, headers=H, timeout=40)
        r.raise_for_status()
        return r.content
    except Exception:
        return _curl(url)


def fetch_text(url: str) -> str | None:
    b = fetch_bytes(url)
    return b.decode("utf-8", "ignore") if b else None


def extract_data_links(html: str, base: str) -> dict[str, list[str]]:
    hits: dict[str, list[str]] = {}
    for href in HREF_RE.findall(html):
        low = href.lower().split("?")[0]
        ext = next((e for e in DATA_EXT if low.endswith(e)), None)
        if ext and PO_HINT.search(href):
            hits.setdefault(ext, []).append(urljoin(base, href))
    return hits


def pick_sample(hits: dict[str, list[str]]) -> str | None:
    def best(urls: list[str]) -> str | None:
        good = [u for u in urls if DATA_FILE_RE.search(u) and not POLICY_RE.search(u)]
        return (good or urls)[0] if urls else None
    for e in (".xlsx", ".csv", ".xls", ".pdf"):  # prefer tabular, else pdf
        if hits.get(e):
            return best(hits[e])
    return None


def harvest_links(landing: str, crawl_cap: int = 10) -> dict:
    """Harvest data links from the landing page; if none, one-hop crawl same-host nav
    links (gov.ie collection -> publication pages). Returns ok/formats/sample/via/error."""
    html = fetch_text(landing)
    if html is None:
        return {"ok": False, "error": "fetch failed (requests+curl)", "formats": {},
                "sample": None, "via": None, "person_hint": False}
    person = bool(PERSON_HINT.search(html))
    hits = extract_data_links(html, landing)
    via = "landing"
    if not hits:
        host = urlparse(landing).netloc
        subs, seen = [], set()
        for href in HREF_RE.findall(html):
            full = urljoin(landing, href)
            low = full.lower().split("?")[0]
            if urlparse(full).netloc != host or full == landing:
                continue
            if any(low.endswith(e) for e in DATA_EXT):
                continue
            if NAV_HINT.search(href) and full not in seen:
                seen.add(full)
                subs.append(full)
        for s in subs[:crawl_cap]:
            sub_html = fetch_text(s)
            if not sub_html:
                continue
            if PERSON_HINT.search(sub_html):
                person = True
            for e, v in extract_data_links(sub_html, s).items():
                hits.setdefault(e, []).extend(v)
            if hits and via == "landing":
                via = f"crawl:{s.rsplit('/', 1)[-1][:30] or urlparse(s).path[:30]}"
    return {"ok": True, "error": None, "formats": {e: len(v) for e, v in hits.items()},
            "sample": pick_sample(hits), "via": via, "person_hint": person}


def classify(url: str) -> dict:
    """Download one file and report format + digital/scanned + header/first cells.
    Returns {format, detail, supplier_col_guess, amount_col_guess, strange}."""
    ext = next((e for e in DATA_EXT if url.lower().split("?")[0].endswith(e)), "")
    TMP.mkdir(parents=True, exist_ok=True)
    b = fetch_bytes(url)
    if not b:
        return {"format": "UNKNOWN", "detail": "download ERR (requests+curl)"}
    dest = TMP / (re.sub(r"[^A-Za-z0-9._-]", "_", url.rsplit("/", 1)[-1])[:60] or "sample")
    if not dest.suffix:
        dest = dest.with_suffix(ext or ".bin")
    dest.write_bytes(b)
    try:
        if ext == ".pdf":
            import fitz
            d = fitz.open(dest)
            npages = d.page_count
            txt = d[0].get_text("text")
            d.close()
            digital = len(txt.strip()) > 200
            first = " ".join(txt.split()[:12])
            return {"format": "PDF_DIGITAL" if digital else "PDF_SCANNED",
                    "detail": f"{npages}pp | {first[:80]}",
                    "supplier_col_guess": bool(re.search(r"supplier|payee|vendor|name", txt, re.I)),
                    "amount_col_guess": bool(re.search(r"amount|total|euro|€|value|paid", txt, re.I)),
                    "strange": {"scanned": not digital}}
        if ext in (".xlsx", ".xls"):
            import openpyxl
            ws = openpyxl.load_workbook(dest, read_only=True, data_only=True).active
            rows = [r for r in ws.iter_rows(values_only=True)][:12]
            hdr = max(rows[:6], default=(),
                      key=lambda r: sum(isinstance(c, str) and c.strip() != "" for c in (r or ())))
            hdr_s = " ".join(str(c) for c in hdr if c is not None)
            return {"format": "XLSX", "detail": f"header≈ {tuple(str(c)[:20] for c in hdr if c is not None)}",
                    "supplier_col_guess": bool(re.search(r"supplier|payee|vendor|name", hdr_s, re.I)),
                    "amount_col_guess": bool(re.search(r"amount|total|euro|€|value|gross|paid", hdr_s, re.I)),
                    "strange": {}}
        if ext == ".csv":
            import io

            import polars as pl
            df = pl.read_csv(io.BytesIO(b), infer_schema_length=0, truncate_ragged_lines=True,
                             ignore_errors=True, encoding="utf8-lossy")
            cols = " ".join(df.columns)
            return {"format": "CSV", "detail": f"{df.height}rows cols={df.columns[:8]}",
                    "supplier_col_guess": bool(re.search(r"supplier|payee|vendor|name", cols, re.I)),
                    "amount_col_guess": bool(re.search(r"amount|total|euro|value|gross|paid", cols, re.I)),
                    "strange": {}}
    except Exception as e:
        return {"format": "UNKNOWN", "detail": f"parse ERR {e!r}"[:90]}
    return {"format": "UNKNOWN", "detail": "unknown ext"}


def infer_status(line: dict) -> str:
    """Map probe evidence -> a source_status enum value (conservative)."""
    if not line.get("landing_url"):
        return "NOT_FOUND"
    if line.get("seed_status") == "FOI_CLUE_ONLY":
        return "FOI_CLUE_ONLY"
    if not line.get("ok"):
        return "NEEDS_MANUAL_CHECK"
    if not line.get("formats"):
        return "NEEDS_MANUAL_CHECK"  # page reachable but no data files surfaced (JS/subpage)
    c = line.get("sample_classify") or {}
    fmt = c.get("format", "UNKNOWN")
    has_supplier = c.get("supplier_col_guess")
    # tabular/digital + a supplier-looking column == promising supplier-level lead
    if fmt in ("XLSX", "CSV", "PDF_DIGITAL") and has_supplier:
        return "CONFIRMED_SUPPLIER_LEVEL"  # promising; still must be parser-verified later
    if fmt in ("XLSX", "CSV", "PDF_DIGITAL"):
        return "NEEDS_MANUAL_CHECK"  # has files but supplier column not obvious
    if fmt == "PDF_SCANNED":
        return "NEEDS_MANUAL_CHECK"  # scanned -> OCR territory, defer
    return "NEEDS_MANUAL_CHECK"


def run_probe(only: set[str] | None, dry_run: bool) -> list[dict]:
    report: list[dict] = []
    for s in SEEDS:
        if only and s["publisher_id"] not in only:
            continue
        line = {
            "publisher_id": s["publisher_id"], "publisher_name": s["publisher_name"],
            "publisher_type": s["publisher_type"], "sector": s["sector"],
            "landing_url": s["landing_url"], "seed_status": s["source_status"],
            "privacy_risk": s["privacy_risk"],
            "ok": None, "error": None, "formats": {}, "sample": None, "via": None,
            "person_hint": False, "sample_classify": None, "inferred_status": None,
        }
        if dry_run or not s["landing_url"]:
            line["inferred_status"] = infer_status(line)
            report.append(line)
            tag = "DRY" if dry_run else "SKIP"
            print(f"[{tag}] {s['publisher_id']:<22} {s['publisher_name']}")
            continue

        h = harvest_links(s["landing_url"])
        line.update({k: h[k] for k in ("ok", "error", "formats", "sample", "via", "person_hint")})
        tag = "OK " if h["ok"] else "ERR"
        print(f"\n[{tag}] {s['publisher_id']:<22} {s['publisher_name']}")
        print(f"      {s['landing_url']}")
        if not h["ok"]:
            print(f"      fetch failed: {h['error']}")
        elif not h["formats"]:
            print("      no PO/payment data links found (JS-rendered? deeper sub-page? FOI-only?)")
        else:
            print(f"      data links: {h['formats']}  [via {h['via']}]")
            if h["sample"]:
                c = classify(h["sample"])
                line["sample_classify"] = c
                print(f"      sample: {h['sample'].rsplit('/', 1)[-1][:55]}")
                print(f"        -> {c.get('format')}: {c.get('detail')}")
                if c.get("supplier_col_guess"):
                    print("        -> supplier-looking column detected")
        if h.get("person_hint"):
            print("      ! privacy: page text hints at individuals/sole-traders/grantees")
        line["inferred_status"] = infer_status(line)
        report.append(line)
    return report


def build_coverage(report: list[dict]) -> dict:
    """Coverage report with the plan §6/Phase-2 keys."""
    inferred = Counter(r["inferred_status"] for r in report)
    fmt_seen = Counter()       # data-file links seen, by extension
    sampled_fmt = Counter()    # what the ONE sampled file per publisher classified as
    files_seen = 0
    for r in report:
        for ext, n in (r.get("formats") or {}).items():
            fmt_seen[ext] += n
            files_seen += n
        c = r.get("sample_classify") or {}
        if c.get("format") and c["format"] != "UNKNOWN":
            sampled_fmt[c["format"]] += 1
    failures = [{"publisher_id": r["publisher_id"], "error": r["error"]}
                for r in report if r["ok"] is False]
    privacy = [r["publisher_id"] for r in report
               if r.get("person_hint") or r.get("privacy_risk") in ("high", "medium")]
    # next publishers: reachable + has files + supplier column looks present
    recommended = [r["publisher_id"] for r in report
                   if (r.get("sample_classify") or {}).get("supplier_col_guess")]
    return {
        "publishers_total": len(report),
        "publishers_confirmed_supplier_level": inferred.get("CONFIRMED_SUPPLIER_LEVEL", 0),
        "publishers_confirmed_category_level": inferred.get("CONFIRMED_CATEGORY_LEVEL", 0),
        "publishers_annual_report_only": inferred.get("ANNUAL_REPORT_ONLY", 0),
        "publishers_foi_aie_confirmed_exists": inferred.get("FOI_AIE_CONFIRMED_EXISTS", 0)
        + inferred.get("FOI_CLUE_ONLY", 0),
        "publishers_needs_manual_check": inferred.get("NEEDS_MANUAL_CHECK", 0),
        "publishers_not_found": inferred.get("NOT_FOUND", 0),
        "files_seen": files_seen,
        "formats_seen": dict(fmt_seen),
        "sampled_formats": dict(sampled_fmt),
        "top_failures": failures,
        "privacy_warnings": privacy,
        "recommended_next_publishers": recommended,
        "inferred_status_breakdown": dict(inferred),
        "schema_version": 1,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "caveat": "PRE-ETL probe. 'confirmed_supplier_level' here means a tabular/digital file with a "
                  "supplier-looking column was sampled — it is a promising lead, NOT a verified parse. "
                  "No value is spend until the source says so; no source is supplier-level until parsed.",
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Probe public-body procurement publishers.")
    ap.add_argument("--dry-run", action="store_true",
                    help="no network; emit skeleton report from the seed only")
    ap.add_argument("--only", default="", help="comma-separated publisher_ids to probe")
    args = ap.parse_args()
    only = {x.strip() for x in args.only.split(",") if x.strip()} or None

    hr("PUBLIC-BODY PROCUREMENT PUBLISHER PROBE" + (" (DRY RUN)" if args.dry_run else ""))
    report = run_probe(only, args.dry_run)
    coverage = build_coverage(report)

    TMP.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps({"coverage": coverage, "publishers": report}, indent=2),
                   encoding="utf-8")

    hr("COVERAGE SUMMARY")
    for k in ("publishers_total", "publishers_confirmed_supplier_level",
              "publishers_needs_manual_check", "publishers_foi_aie_confirmed_exists",
              "publishers_not_found", "files_seen"):
        print(f"  {k:<38}: {coverage[k]}")
    print(f"  formats_seen                          : {coverage['formats_seen']}")
    print(f"  recommended_next_publishers           : {coverage['recommended_next_publishers']}")
    if coverage["top_failures"]:
        print(f"  fetch failures                        : {[f['publisher_id'] for f in coverage['top_failures']]}")
    print(f"\nwrote {OUT}")
    print("PRE-ETL. Next: sample-extraction reports for the recommended supplier-level leads.")


if __name__ == "__main__":
    main()
