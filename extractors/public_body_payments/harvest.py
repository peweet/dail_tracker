"""Fetch/harvest loop for the public-body payments extractor package (split
out of extractors/procurement_public_body_extract.py, doc/REFACTORING_CANDIDATES.md
C7, pure move-function -- no logic changes).

Owns ALL of this extractor's mutable module-level state: REPORT (the shared
FetchReport instance main.py records failures/breaker-trips/zero-harvests
into) and LAST_ERR (the dict fetch_bytes populates on failure, read straight
back by main.py after a failed fetch_to_bronze). Neither name is ever
REBOUND after this module's import -- only mutated in place (REPORT via its
own methods, LAST_ERR via .clear()/.update()) -- so a plain
``from .harvest import REPORT, LAST_ERR`` in main.py stays in sync; no
attribute-access-through-the-module workaround is needed here.
"""

from __future__ import annotations

import re
import subprocess
import time
from pathlib import Path
from urllib.parse import quote, unquote, urljoin, urlparse

import requests

from services.fetch_report import FetchReport, classify_exception

from .config import DATA_EXT
from .readers import period_from_url

ROOT = Path(__file__).resolve().parents[2]

H = {"User-Agent": "Mozilla/5.0 (dail-tracker research probe)"}
TMP = Path("c:/tmp/procurement_publishers")
BRONZE = ROOT / "data/bronze/pdfs/public_body_procurement"
REPORT = FetchReport("public_body")
LAST_ERR: dict = {}  # set by fetch_bytes on failure, read by the download loop

# ----------------------------------------------------------------------------- regexes
HREF_RE = re.compile(r"""href\s*=\s*["']([^"']+)["']""", re.I)
# exclude policy/guidance/privacy/contract docs when harvesting period data files
POLICY_RE = re.compile(
    r"guide|guidelin|\bplan\b|policy|circular|strategy|manual|terms|fin.?07|privacy|"
    r"prompt.?payment|appendix|procedure|annual.?report|statement|setup|form|charter|scheme",
    re.I,
)
DATA_FILE_RE = re.compile(r"q[1-4]\b|qtr|quarter|20[12]\d|h[12]\b|over.?20|over.?25|payment|purchase|\bpo[s]?\b", re.I)
NAV_HINT = re.compile(
    r"purchase|procure|over.?20|over.?25|20k|payment|quarter|qtr|finance|"
    r"publication|spend|supplier|expenditure|disclosure|financial",
    re.I,
)


# ============================================================================ fetch
def _curl(url: str) -> bytes | None:
    try:
        p = subprocess.run(
            ["curl", "-sS", "-k", "-L", "--max-time", "90", "-A", H["User-Agent"], url],
            capture_output=True,
            timeout=120,
        )
        return p.stdout if p.returncode == 0 and p.stdout else None
    except Exception:
        return None


def fetch_bytes(url: str) -> bytes | None:
    # some publishers emit hrefs with raw spaces — requests/curl reject them as malformed;
    # '%' stays in the safe set so already-encoded hrefs don't double-encode.
    url = quote(url, safe="!#$%&'()*+,/:;=?@[]~")
    LAST_ERR.clear()
    try:
        r = requests.get(url, headers=H, timeout=90, allow_redirects=True)
        r.raise_for_status()
        return r.content
    except Exception as e:
        ec, status = classify_exception(e)
        LAST_ERR.update({"error_class": ec, "http_status": status})
        b = _curl(url)
        if b:
            LAST_ERR.clear()
        return b


def fetch_text(url: str) -> str | None:
    b = fetch_bytes(url)
    return b.decode("utf-8", "ignore") if b else None


def fetch_to_bronze(pub_id: str, url: str, ext: str, refetch: bool = False) -> tuple[bytes | None, bool]:
    """Self-fetch a source file to bronze/pdfs/public_body_procurement/<id>/ and reuse the
    cached copy on re-runs — quarterly disclosures are immutable, so steady-state runs only
    download newly published files (same shape as the LA extractor). Returns
    ``(bytes, fresh_download)``. The DNN ``?sfvrsn=`` version param drops out of the cache
    key deliberately: same filename = same historical document."""
    dest = (
        BRONZE
        / pub_id
        / (re.sub(r"[^A-Za-z0-9._-]", "_", unquote(url.split("?")[0].rsplit("/", 1)[-1]))[:80] or "file")
    )
    if not dest.suffix:
        dest = dest.with_suffix(ext if ext in DATA_EXT else ".pdf")
    if not refetch and dest.exists() and dest.stat().st_size > 1500:
        return dest.read_bytes(), False
    time.sleep(1.0)  # politeness: only on a real network fetch, never on a cache hit
    b = fetch_bytes(url)
    if b and len(b) > 1500:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b)
    return b, True


# ============================================================================ harvest
def harvest_files(cf: dict, crawl_cap: int = 12) -> list[str]:
    """Collect ALL period data-file links for a publisher: landing page + one-hop crawl,
    minus policy/guidance docs. Union with direct_files. Honours an optional include re."""
    found: list[str] = list(cf["direct_files"])
    html = fetch_text(cf["listing_url"])
    if html:

        def scan(page_html: str, base: str) -> list[str]:
            out = []
            for href in HREF_RE.findall(page_html):
                low = href.lower().split("?")[0]
                if not any(low.endswith(e) for e in DATA_EXT):
                    continue
                if POLICY_RE.search(href):
                    continue
                if not DATA_FILE_RE.search(href):
                    continue
                if cf["include"] and not cf["include"].search(href):
                    continue
                if cf["exclude"] and cf["exclude"].search(href):
                    continue
                out.append(urljoin(base, href))
            return out

        hits = scan(html, cf["listing_url"])
        if not hits:  # one-hop crawl same-host nav links
            host = urlparse(cf["listing_url"]).netloc
            subs, seen = [], set()
            for href in HREF_RE.findall(html):
                full = urljoin(cf["listing_url"], href)
                low = full.lower().split("?")[0]
                if urlparse(full).netloc != host or full == cf["listing_url"]:
                    continue
                if any(low.endswith(e) for e in DATA_EXT):
                    continue
                if NAV_HINT.search(href) and full not in seen:
                    seen.add(full)
                    subs.append(full)
            for s in subs[:crawl_cap]:
                sub_html = fetch_text(s)
                if sub_html:
                    hits.extend(scan(sub_html, s))
        found.extend(hits)
    # Dedup by basename STEM (extension stripped), not basename: the same quarterly
    # report is sometimes published in two formats (e.g. dept_climate Q1-2026 as both
    # .xlsx AND .pdf) which a with-extension key let through -> double-counted rows.
    # On a stem collision prefer the cleaner tabular format (xlsx/csv > xls > pdf).
    # (Also still collapses the same file served via two hosts, e.g. TII.)
    fmt_pref = {".xlsx": 0, ".csv": 1, ".xls": 2, ".pdf": 3}

    def stem_ext(u: str) -> tuple[str, str]:
        base = u.rsplit("/", 1)[-1].split("?")[0].lower()
        for e in DATA_EXT:
            if base.endswith(e):
                return base[: -len(e)], e
        return base, ""

    best: dict[str, str] = {}
    order: list[str] = []
    for u in found:
        s, e = stem_ext(u)
        if s not in best:
            best[s] = u
            order.append(s)
        elif fmt_pref.get(e, 9) < fmt_pref.get(stem_ext(best[s])[1], 9):
            best[s] = u  # keep the more reliable format for the same report
    result = [best[s] for s in order]

    # Cross-format same-period dedup: some publishers post the SAME quarter as BOTH a tabular file
    # (csv/xlsx) AND a pdf under DIFFERENT filenames (e.g. dept_agriculture Q4-2025 as
    # "Q4_2025_Purchase_Orders_over_20k.pdf" + "Payments_to_Suppliers…_Q4_2025.csv"). Different
    # stems slip past the stem-dedup above and the quarter is counted twice. When a period carries
    # BOTH a tabular and a pdf, drop the pdf (the tabular has cleaner columns). Crucially this only
    # fires across DIFFERENT formats — same-format repeats in one period are untouched, so NTMA's
    # 6 per-business-unit pdfs/quarter and any genuinely split quarter survive.
    def _is_tab(u: str) -> bool:
        return stem_ext(u)[1] in (".csv", ".xlsx", ".xls")

    period_fmts: dict[str | None, set[bool]] = {}
    for u in result:
        per = period_from_url(u)[0]
        if per:
            period_fmts.setdefault(per, set()).add(_is_tab(u))
    drop_pdf_periods = {p for p, fmts in period_fmts.items() if True in fmts and False in fmts}
    return [u for u in result if _is_tab(u) or period_from_url(u)[0] not in drop_pdf_periods]
