"""INGEST: LGAS statutory audit reports — the independent auditor's opinion + findings on
every council's AFS, per (council, year), centrally published on gov.ie since 2012.

The Local Government Audit Service audits each council's Annual Financial Statement and
publishes a short templated report (~4-8pp, born-digital): audit opinion → findings
(numbered sections) → Chief Executive's responses. This is the accountability layer the AFS
NUMBERS lack — and it pairs 1:1 with `la_afs_divisions` ([[project_la_afs_fact]]) on
(council, year).

DISCOVERY: gov.ie renders its collection lists client-side, so per-year collection pages are
empty HTML — but every per-council publication page is in the gov.ie SITEMAPS. We sweep
sitemap-en*.xml for `…/publications/<council>…audit-report…<year>/`, fetch each publication
page for its assets.gov.ie PDF, and cache PDFs in bronze. ~435 reports, 31 councils × 2012→.

NO-INFERENCE RULE: the fact stores VERBATIM text (the audit-opinion paragraph, the section
headings) and presence booleans anchored to literal headings ("Emphasis of Matter"). It never
classifies a report as good/bad — the reader sees what the auditor wrote.

gov.ie WAF: browser-UA needed; refresh is local-box only (Cloud fetches 403).

Run:
  ./.venv/Scripts/python.exe extractors/lgas_audit_reports_extract.py            # all
  ./.venv/Scripts/python.exe extractors/lgas_audit_reports_extract.py --min-year 2020
"""

from __future__ import annotations

import argparse
import contextlib
import json
import re
import sys
import time
import unicodedata
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote
from urllib.request import Request, urlopen

import fitz  # PyMuPDF
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import config  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402

CACHE = config.BRONZE_PDF_DIR / "lgas"
OUT_PARQUET = config.SILVER_PARQUET_DIR / "la_lgas_audit_reports.parquet"
OUT_COV = ROOT / "data/_meta/lgas_audit_coverage.json"
SITEMAP_INDEX = "https://www.gov.ie/sitemap.xml"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

CANON_31 = {
    "Carlow", "Cavan", "Clare", "Cork City", "Cork County", "Donegal", "Dublin City",
    "Dun Laoghaire-Rathdown", "Fingal", "Galway City", "Galway County", "Kerry", "Kildare",
    "Kilkenny", "Laois", "Leitrim", "Limerick", "Longford", "Louth", "Mayo", "Meath",
    "Monaghan", "Offaly", "Roscommon", "Sligo", "South Dublin", "Tipperary", "Waterford",
    "Westmeath", "Wexford", "Wicklow",
}
# compact (no spaces/hyphens, accent-folded, lowercase) → canonical; longest key first so
# 'corkcity' can never be shadowed by a bare 'cork'
_COMPACT = sorted(
    ((re.sub(r"[^a-z]", "", unicodedata.normalize("NFKD", c).encode("ascii", "ignore").decode().lower()), c)
     for c in CANON_31),
    key=lambda kv: -len(kv[0]),
)


def fetch(url: str, retries: int = 3) -> bytes:
    time.sleep(0.8)  # EVERY request — gov.ie locks out bursts with 405s mid-run (162 of 393
    # failed on the first sweep when only PDF downloads were throttled)
    for attempt in range(retries + 1):
        try:
            return urlopen(Request(url, headers={"User-Agent": UA}), timeout=90).read()
        except Exception:  # noqa: BLE001 — rate-limited; back off harder each time
            if attempt == retries:
                raise
            time.sleep(10 * (attempt + 1))
    return b""


def council_from_slug(slug: str) -> str | None:
    compact = re.sub(r"[^a-z]", "", unicodedata.normalize("NFKD", unquote(slug)).encode("ascii", "ignore").decode().lower())
    for key, canon in _COMPACT:
        if compact.startswith(key):
            # 'corkcountycouncil…' must map to Cork County, not Cork City: startswith on the
            # longest-first compact keys handles it because 'corkcounty' sorts before 'corkcity'
            # alphabetical ties don't arise (keys differ), and 'southdublin' beats nothing else.
            return canon
    # county-only slugs like 'carlow-county-council-…' reduce to 'carlowcountycouncil' — the
    # bare-county canon key 'carlow' IS a prefix of that, so the loop above already matched.
    return None


def discover() -> list[dict]:
    """Sweep gov.ie sitemaps for per-council audit-report publication pages."""
    idx = fetch(SITEMAP_INDEX).decode("utf-8", errors="replace")
    maps = re.findall(r"<loc>(https://[^<]+\.xml)</loc>", idx)
    out: dict[tuple[str, int], dict] = {}
    for sm in maps:
        with contextlib.suppress(Exception):
            body = fetch(sm).decode("utf-8", errors="replace")
            for url in re.findall(
                r"<loc>(https://www\.gov\.ie/en/department-of-housing-local-government-and-heritage/"
                r"publications/[^<]*audit-report[^<]*)</loc>",
                body,
            ):
                slug = url.rstrip("/").rsplit("/", 1)[-1]
                if "overview" in slug or "value-for-money" in slug:
                    continue
                ym = re.search(r"(20[12]\d)", slug)
                council = council_from_slug(slug)
                if ym and council:
                    out[(council, int(ym.group(1)))] = {"council": council, "year": int(ym.group(1)), "page": url}
    return sorted(out.values(), key=lambda r: (r["council"], r["year"]))


def pdf_url_from_page(page_url: str) -> str | None:
    html = fetch(page_url).decode("utf-8", errors="replace")
    # publication pages link their document(s) on assets.gov.ie (old /NNNNN/uuid.pdf and new
    # /static/documents/ forms both occur); take the first PDF link
    m = re.search(r'href="(https://assets\.gov\.ie/[^"]+?\.pdf)"', html)
    return m.group(1) if m else None


_OPINION_HEAD = re.compile(r"(?:^|\n)\s*(?:\d+\.?\s*)?Audit Opinion\s*\n", re.I)
_SECTION_HEAD = re.compile(r"(?:^|\n)\s*(\d{1,2})\.?\s+([A-Z][A-Za-z’' /&\-]{3,60})\s*\n")


def parse_report(p: Path) -> dict:
    doc = fitz.open(p)
    text = "\n".join(doc[i].get_text("text") for i in range(doc.page_count))
    npages = doc.page_count
    doc.close()
    headings = [f"{n}. {h.strip()}" for n, h in _SECTION_HEAD.findall(text)]
    op = ""
    m = _OPINION_HEAD.search(text)
    if m:  # older editions carry an 'Audit Opinion' SECTION — take it verbatim
        rest = text[m.end():]
        nxt = _SECTION_HEAD.search(rest)
        op = rest[: nxt.start() if nxt else 1800].strip()[:1800]
    if not op:
        # recent editions state the opinion in a sentence instead ('My audit opinion, which is
        # unmodified, is stated on page 7 of the AFS') — capture that sentence verbatim
        flat = re.sub(r"\s+", " ", text)
        sm = re.search(r"[^.]{0,160}\baudit opinion\b[^.]{0,240}\.", flat, re.I)
        op = sm.group(0).strip() if sm else ""
    return {
        "pages": npages,
        "audit_opinion_text": op,
        "has_emphasis_of_matter": bool(re.search(r"Emphasis of Matter", text, re.I)),
        "has_ce_response": bool(re.search(r"Chief Executive.{0,3}s Response", text, re.I)),
        "section_headings": " | ".join(headings[:25]),
        "chars": len(text),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-year", type=int, default=2012)
    args = ap.parse_args()

    print("discovering publication pages via sitemaps…")
    pubs = [r for r in discover() if r["year"] >= args.min_year]
    print(f"  {len(pubs)} reports across {len({r['council'] for r in pubs})} councils")

    rows, cov_fail = [], []
    for r in pubs:
        dest = CACHE / r["council"].replace(" ", "_").lower() / f"{r['year']}.pdf"
        try:
            if not (dest.exists() and dest.stat().st_size > 20_000):
                pdf_url = pdf_url_from_page(r["page"])
                if not pdf_url:
                    cov_fail.append({**r, "status": "no-pdf-link"})
                    continue
                body = fetch(pdf_url)
                if body[:4] != b"%PDF":
                    cov_fail.append({**r, "status": "not-pdf"})
                    continue
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(body)
                time.sleep(0.4)  # assets.gov.ie politeness — bursts get rate-limited
            parsed = parse_report(dest)
            if parsed["chars"] < 500:  # scanned/empty guard — record, never emit garbage
                cov_fail.append({**r, "status": f"low-text({parsed['chars']}ch)"})
                continue
            rows.append({**{k: r[k] for k in ("council", "year")}, "report_page_url": r["page"], **parsed})
        except Exception as exc:  # noqa: BLE001 — one bad report must not kill a 400-file run
            cov_fail.append({**r, "status": f"error: {str(exc)[:120]}"})
    if not rows:
        print("no rows extracted — refusing to write")
        return

    df = pl.DataFrame(rows).drop("chars").sort(["council", "year"])
    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    save_parquet(df, OUT_PARQUET)
    n_op = df.filter(pl.col("audit_opinion_text").str.len_chars() > 50).height
    print(f"  rows: {df.height} | councils: {df['council'].n_unique()} | years {df['year'].min()}–{df['year'].max()}")
    print(f"  opinion text extracted: {n_op}/{df.height} | emphasis-of-matter: {df['has_emphasis_of_matter'].sum()}")
    OUT_COV.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
                "grain": "one row per (council, year) statutory audit report; verbatim opinion + headings",
                "caveat": "Verbatim extracts from the LGAS auditor's published report — no classification "
                "or scoring is derived; presence flags are anchored to literal headings.",
                "rows": df.height,
                "councils": sorted(df["council"].unique()),
                "years": [int(df["year"].min()), int(df["year"].max())],
                "failures": cov_fail,
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    print(f"  wrote {OUT_PARQUET}\n        {OUT_COV}  ({len(cov_fail)} failures recorded)")


if __name__ == "__main__":
    main()
