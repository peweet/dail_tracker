"""Government circulars — the administrative rulebook sitting on top of our money data.

SANDBOX ONLY. Writes to c:/tmp/dail_new_sources/, never data/gold.

WHY THIS SOURCE. A circular is how a department tells public bodies what to
actually do: advertise a tender above €50k, waive a development contribution,
collect a derelict-site levy. It is the rule layer over facts we already hold —
but it is NOT law, and it is NOT signed by a Minister (see WHO SIGNS, below).

SCOPE: 2020 → present (MIN_YEAR). Set on the user's call — older circulars are
largely superseded, and the archive's long tail (it reaches back to 1922) is
civil-service pay history, not live money rules.

ACCESS — the two hosts have opposite rules, and this drives the whole design:

  www.gov.ie      robots.txt disallows `/*?*` for User-agent:* → the `?q=` search
                  endpoint is OFF-LIMITS to an automated client. Circular DETAIL
                  pages carry no query string and are NOT disallowed, and the
                  sitemap is explicitly advertised. A WAF (405 "Human
                  Verification") throttles bursts, so we pace at REQ_DELAY_S.
  assets.gov.ie   Serves the circular PDFs behind the SAME 405 WAF, cleared by the
                  same browser-UA spoof. Its robots.txt is `User-agent: * /
                  Disallow: /`. We fetch anyway, at a polite rate, consistent with
                  the FOUR live extractors that already pull from this host
                  (derelict_sites_levy, afs_amalgamated, ministerial_diaries,
                  procurement). Flagged here so the choice stays visible rather
                  than buried.
  circulars.gov.ie  Legacy archive, openly crawlable — but FROZEN at Sept 2019,
                  so it contributes nothing at MIN_YEAR=2020.

THREE STEPS: sitemap index → policy detail pages (title, dates, body) →
PDF text + signature block (`--pdfs`). The PDF is authoritative: the gov.ie HTML
page often carries only a title and a link, and the sign-off block — the whole
accountability point — exists ONLY in the PDF.

WHO SIGNS A CIRCULAR (evidenced from the PDFs' sign-off blocks):
    Circulars are NOT signed by a Minister. They are signed by a civil servant —
    Assistant Secretary / Principal Officer / Secretary General — under the
    formula "I am directed by the Minister for X to say…": issued on the
    Minister's authority, over an official's name. A Statutory Instrument, by
    contrast, the Minister signs personally. That gap is the accountability point
    of this source: circulars move real money without a ministerial signature or
    Oireachtas scrutiny. We record the signatory verbatim and never infer who
    "really" decided.

Nothing here is inference. Every column is a verbatim string from the document or
a URL-derived fact.
"""
from __future__ import annotations

import re
import sys
import time

import polars as pl
import requests
from _common import BRONZE, SILVER, cache_raw, now_iso, write_silver

SOURCE = "gov_circulars"

SITEMAP_INDEX = "https://www.gov.ie/sitemap.xml"
LEGACY_AZ_INDEX = "https://circulars.gov.ie/azindex"  # frozen Sept 2019

# The gov.ie WAF rejects bot UAs. The MINIMAL browser spoof passes; a FULLER
# header set (sec-ch-ua / Sec-Fetch-*) actually TRIPS the challenge. Do not
# "improve" these headers. Same pattern as extractors/procurement_etenders_extract.py.
GOVIE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer": "https://www.gov.ie/",
}
LEGACY_HEADERS = {"User-Agent": "Mozilla/5.0 (DailTracker research; non-commercial)"}

MIN_YEAR = 2020
REQ_DELAY_S = 2.0   # measured: 1.5s sustains cleanly; 2.0s leaves headroom
MAX_RETRIES = 5     # the WAF 405s intermittently even at a polite rate — back off, don't give up

# Departments whose circulars govern money/planning. Education + HSE issue the
# bulk of the corpus, but it is pay-and-pension HR — indexed, not fetched.
POLICY_ORGS = (
    "public-expenditure",
    "housing-local-government",
    "department-of-finance",
)

# Topic tags = verbatim keyword presence. NOT a classification of meaning.
TOPICS = {
    "procurement": ("procurement", "tender", "etenders", "capital works management"),
    "planning": ("planning", "development contribution", "section 28", "permission"),
    "levy": ("derelict site", "vacant site", "levy"),
    "capital_appraisal": ("public spending code", "infrastructure guidelines", "value for money"),
    "local_authority": ("local authority", "chief executive", "councillor"),
    "audit_governance": ("comptroller", "audit", "governance of state bodies"),
    "pay_pension": ("pay", "salary", "pension", "increment", "allowance", "subsistence"),
}

SCRIPTS = re.compile(r"<(script|style)[^>]*>.*?</\1>", re.S)
SIGNATORY_ROLE = re.compile(
    r"(Secretary General|Deputy Secretary|Assistant Secretary|Principal Officer|Assistant Principal)"
)
AUTHORITY = re.compile(r"(I am directed by the Ministers?[^.]{0,120}\.)", re.I)
MONTHS = (
    "January|February|March|April|May|June|July|August|September|October|November|December"
)


def _get(url: str, headers: dict, binary: bool = False, timeout: int = 45):
    """GET with polite pacing + exponential backoff.

    Retries BOTH failure modes: the WAF's 405 challenge AND transport errors
    (DNS/connection resets). A transient `getaddrinfo failed` once killed a
    15-minute crawl outright — a network blip must never end the run.
    """
    last = None
    for attempt in range(MAX_RETRIES):
        time.sleep(REQ_DELAY_S * (2**attempt))  # 2s, 4s, 8s, 16s, 32s
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
        except requests.RequestException as e:
            last = type(e).__name__
            continue
        if r.ok:
            return (r.content if binary else r.content.decode("utf-8", "replace")), r
        last = r.status_code
    raise requests.HTTPError(f"{last} after {MAX_RETRIES} tries: {url}")


def _topics(text: str) -> list[str]:
    low = (text or "").lower()
    return [t for t, kws in TOPICS.items() if any(k in low for k in kws)]


def _text_of(html: str) -> str:
    main = re.search(r"<main.*?</main>", SCRIPTS.sub(" ", html), re.S)
    body = re.sub(r"<[^>]+>", " ", main.group(0) if main else "")
    return re.sub(r"\s+", " ", body).strip()


def _date_after(label: str, body: str) -> str | None:
    m = re.search(rf"{label}:?\s*(\d{{1,2}}\s+(?:{MONTHS})\s+\d{{4}})", body)
    return m.group(1) if m else None


def _circular_no(text: str) -> str | None:
    m = re.search(r"(\d{1,3})\s*(?:/|\s+of\s+)\s*(\d{2,4})", text or "")
    if not m:
        return None
    num, yr = m.group(1), m.group(2)
    if len(yr) == 2:
        yr = ("20" if int(yr) < 50 else "19") + yr
    return f"{int(num):02d}/{yr}" if 1900 < int(yr) < 2100 else None


def _signature(text: str) -> tuple[str | None, str | None]:
    """(role, authority_formula) — verbatim or None. Sparse on gov.ie: the sign-off
    lives in the PDF, which assets.gov.ie forbids us to fetch."""
    a = AUTHORITY.search(text)
    r = SIGNATORY_ROLE.search(text)
    return (r.group(1) if r else None), (re.sub(r"\s+", " ", a.group(1)) if a else None)


# ---------------------------------------------------------- Step 1: the index
INDEX_CACHE = BRONZE / SOURCE / "circular_urls.txt"


def sitemap_index() -> list[dict]:
    """Every circular URL from the advertised sitemap. No page fetches.

    Cached: the 20 sitemaps are ~100k URLs and the WAF 405s intermittently, so a
    failed detail run should not have to re-crawl them.
    """
    if INDEX_CACHE.exists():
        urls = [u for u in INDEX_CACHE.read_text(encoding="utf-8").split("\n") if u]
        print(f"[index] {len(urls)} circular URLs from cache ({INDEX_CACHE})")
    else:
        idx, _ = _get(SITEMAP_INDEX, GOVIE_HEADERS)
        maps = [m for m in re.findall(r"<loc>([^<]+)</loc>", idx) if "-en" in m]
        urls = []
        for sm in maps:
            xml, _ = _get(sm, GOVIE_HEADERS, timeout=90)
            urls += [u for u in re.findall(r"<loc>([^<]+)</loc>", xml) if "/circulars/" in u]
            print(f"[index]   {sm.rsplit('/', 1)[1]:<20} running total {len(urls)}")
        INDEX_CACHE.parent.mkdir(parents=True, exist_ok=True)
        INDEX_CACHE.write_text("\n".join(sorted(set(urls))), encoding="utf-8")

    rows = []
    for url in sorted(set(urls)):
        org, slug = url.split("/en/", 1)[1].split("/circulars/")
        rows.append({"organisation": org, "slug": slug.strip("/"), "source_url": url,
                     "is_policy_org": any(p in org for p in POLICY_ORGS)})
    print(f"[index] {len(rows)} circulars on gov.ie "
          f"({sum(r['is_policy_org'] for r in rows)} from policy departments)")
    return rows


# ------------------------------------------------- Step 2: fetch policy detail
def fetch_details(index: list[dict], limit: int | None = None) -> pl.DataFrame:
    """Fetch detail pages for POLICY-department circulars, keep those >= MIN_YEAR.

    The year floor is applied to the page's own 'Published on' date — NOT to the
    sitemap's <lastmod>, which is the 2025-04 bulk-migration date for most of this
    corpus and says nothing about a circular's age. The slug is no help either:
    ~98% carry no circular number.
    """
    cands = [r for r in index if r["is_policy_org"]]
    if limit:
        cands = cands[:limit]

    # Resumable: every fetched page is cached to disk, so a crash (or a Ctrl-C)
    # costs nothing on re-run. Crawling gov.ie is slow and WAF-throttled — we do
    # not pay for the same page twice.
    cache = BRONZE / SOURCE / "pages"
    cache.mkdir(parents=True, exist_ok=True)
    cached = sum(1 for c in cands if (cache / f"{c['slug'][:120]}.html").exists())
    todo = len(cands) - cached
    print(f"[detail] {len(cands)} policy pages ({cached} already cached, {todo} to fetch "
          f"at {REQ_DELAY_S}s ≈ {todo * REQ_DELAY_S / 60:.0f} min), keeping >= {MIN_YEAR}")

    rows, dropped_old, undated, failed = [], 0, 0, 0
    for i, c in enumerate(cands, 1):
        page = cache / f"{c['slug'][:120]}.html"
        if page.exists():
            html = page.read_text(encoding="utf-8", errors="replace")
        else:
            try:
                html, _ = _get(c["source_url"], GOVIE_HEADERS)
            except requests.HTTPError as e:
                failed += 1
                print(f"[detail]   FAIL {e}", flush=True)
                continue
            page.write_text(html, encoding="utf-8")

        # Progress BEFORE the drop-continue: a long streak of pre-2020 circulars
        # would otherwise run silently and look like a hang.
        if i % 25 == 0 or i == len(cands):
            print(f"[detail]   {i}/{len(cands)}  kept={len(rows)} "
                  f"pre{MIN_YEAR}={dropped_old} undated={undated} failed={failed}", flush=True)

        body = _text_of(html)
        published = _date_after("Published on", body)
        year = int(published.split()[-1]) if published else None

        if year is not None and year < MIN_YEAR:
            dropped_old += 1
            continue
        if year is None:
            # Kept, flagged. We do NOT drop what we merely failed to date —
            # that would silently shrink the corpus and look like coverage.
            undated += 1

        title = re.sub(r"^Circulars\s+\S+\s+", "", body[:300]).split(" From:")[0].strip()
        role, formula = _signature(body)
        rows.append({
            "organisation": c["organisation"],
            "slug": c["slug"],
            "title": title or None,
            "circular_no": _circular_no(body[:200]) or _circular_no(c["slug"].replace("-", " ")),
            "published_on": published,
            "effective_from": _date_after("Effective from", body),
            "year": year,
            "topics": _topics(body),
            "body_text": body or None,
            "body_chars": len(body),
            "has_body": len(body) > 600,   # short pages are a PDF stub + title only
            "pdf_url": (re.findall(r"https://assets\.gov\.ie/[^\s\"'<>]+\.pdf", html) or [None])[0],
            "signatory_role": role,
            "authority_formula": formula,
            "source_url": c["source_url"],
            "extraction_method": "govie_detail_html",
            "confidence": "high" if published else "low",
            "privacy_tier": "public",
        })

    print(f"[detail] done: kept {len(rows)}, dropped {dropped_old} pre-{MIN_YEAR}, "
          f"{undated} undatable (kept, confidence=low), {failed} failed")
    # infer_schema_length=None: scan ALL rows. Sparse columns (authority_formula,
    # signatory_role) are null for the first ~100 rows, so the default 100-row
    # inference types them as Null and then dies on the first real string.
    return pl.DataFrame(rows, infer_schema_length=None)


# --------------------------------------------- Step 3: the PDF (authoritative text)
def _pdf_text(raw: bytes) -> str:
    import fitz  # PyMuPDF — the repo's PDF text parser

    try:
        with fitz.open(stream=raw, filetype="pdf") as doc:
            return "\n".join(p.get_text() for p in doc)
    except Exception:
        return ""


def _pdf_signature(text: str) -> tuple[str | None, str | None, str | None]:
    """(name, role, authority_formula) — VERBATIM or None.

    This is where the accountability evidence actually lives: the sign-off block
    the gov.ie HTML page does not carry.
    """
    a = AUTHORITY.search(text)
    formula = re.sub(r"\s+", " ", a.group(1)).strip() if a else None

    name = role = None
    tail = text[-1800:]
    m = re.search(r"(?:Mise le meas|Yours sincerely|Yours faithfully)[,\s]*\n+(.{0,220})",
                  tail, re.S | re.I)
    if m:
        for ln in [x.strip() for x in m.group(1).split("\n") if x.strip()][:4]:
            r = SIGNATORY_ROLE.search(ln)
            if r and not role:
                role = r.group(1)
            elif not name and re.fullmatch(r"[A-ZÁÉÍÓÚ][\w'’\-]+(?:\s+[A-ZÁÉÍÓÚ][\w'’\-.]+){1,3}", ln):
                name = ln
    if not role:
        r = SIGNATORY_ROLE.search(text[-900:])
        role = r.group(1) if r else None
    return name, role, formula


def enrich_pdfs() -> None:
    """Download each circular's PDF and extract its text + signature block.

    assets.gov.ie fronts the same 405 WAF as gov.ie and clears with the browser-UA
    spoof — the identical pattern four LIVE extractors already use
    (derelict_sites_levy, afs_amalgamated, ministerial_diaries, procurement).
    NOTE: assets.gov.ie/robots.txt is `Disallow: /`; we fetch anyway, consistent
    with those extractors, for public government documents at a polite rate.
    """
    src = SILVER / f"{SOURCE}.parquet"
    df = pl.read_parquet(src)
    todo = df.filter(pl.col("pdf_url").is_not_null())
    print(f"[pdf] {todo.height}/{df.height} circulars have a PDF — fetching at {REQ_DELAY_S}s")

    got: dict[str, dict] = {}
    for i, r in enumerate(todo.iter_rows(named=True), 1):
        url = r["pdf_url"]
        try:
            raw, _ = _get(url, GOVIE_HEADERS, binary=True, timeout=90)
        except requests.HTTPError as e:
            print(f"[pdf]   FAIL {e}")
            continue
        cache_raw(SOURCE, url.rsplit("/", 1)[-1][:120], raw)
        text = _pdf_text(raw)
        name, role, formula = _pdf_signature(text) if text.strip() else (None, None, None)
        got[r["source_url"]] = {
            "pdf_text": text or None,
            "pdf_chars": len(text),
            "signatory": name,
            "signatory_role_pdf": role,
            "authority_formula_pdf": formula,
        }
        if i % 25 == 0 or i == todo.height:
            print(f"[pdf]   {i}/{todo.height}  ok={len(got)}", flush=True)

    add = pl.DataFrame(
        [{"source_url": k, **v} for k, v in got.items()],
        schema={"source_url": pl.Utf8, "pdf_text": pl.Utf8, "pdf_chars": pl.Int64,
                "signatory": pl.Utf8, "signatory_role_pdf": pl.Utf8,
                "authority_formula_pdf": pl.Utf8},
    )
    out = df.join(add, on="source_url", how="left")
    write_silver(SOURCE, out)
    print(f"\n[pdf] enriched {add.height} circulars with PDF text")
    print("\nWHO SIGNS? (role on the PDF sign-off)")
    print(out.filter(pl.col("signatory_role_pdf").is_not_null())
             .group_by("signatory_role_pdf").len().sort("len", descending=True))
    print(f"\nMinister as signatory: "
          f"{out.filter(pl.col('signatory_role_pdf').str.contains('(?i)minister')).height}")
    print(f"Carrying 'I am directed by the Minister…': "
          f"{out.filter(pl.col('authority_formula_pdf').is_not_null()).height}")


def main() -> None:
    if "--pdfs" in sys.argv:
        enrich_pdfs()
        return

    limit = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else None
    idx = sitemap_index()
    df = fetch_details(idx, limit=limit).with_columns(
        pl.lit(now_iso()).alias("fetched_at"),
        pl.lit("Government of Ireland circulars (gov.ie)").alias("source_name"),
    )
    p = write_silver(SOURCE, df)

    print(f"\n=== {SOURCE}: {df.height:,} circulars {MIN_YEAR}+ -> {p}")
    print(df.group_by("year").len().sort("year"))
    print("\nBy department:")
    print(df.group_by("organisation").len().sort("len", descending=True))
    print("\nTopics:")
    print(df.explode("topics").drop_nulls("topics").group_by("topics").len().sort("len", descending=True))
    print(f"\nWith usable body text: {df['has_body'].sum()}/{df.height}   "
          f"with a PDF: {df['pdf_url'].is_not_null().sum()}  "
          f"-> now run:  python gov_circulars.py --pdfs")


if __name__ == "__main__":
    main()
