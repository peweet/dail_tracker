"""Extract + verify every embedded hyperlink and legal citation in the C&AG
IPAS chapters (2024 ch.10 + 2015 ch.6). SANDBOX ONLY.

- URLs: PDF link annotations + regex over the text layer, deduped, HTTP-checked
  (browser UA, record status + final URL; dead/blocked flagged, never silently dropped).
- Legal citations: Statutory Instruments, Acts, EU Regulations — SIs cross-referenced
  to the built `statutory_instruments` gold parquet (si_id, si_title, eisb_url) so the
  chapter's "binding law" ties into the existing SI surface.
"""
from __future__ import annotations

import re
import polars as pl
import requests

import fitz
from _common import BRONZE, SILVER, now_iso

SI_GOLD = "c:/Users/pglyn/PycharmProjects/dail_extractor/data/gold/parquet/statutory_instruments.parquet"
PDF_DIR = BRONZE / "cag_reports" / "pdf"
CHAPTERS = {
    2024: "10-management-of-international-protection-accommodation-contracts-copy.pdf",
    2015: "2015-annual-report-chapter-6-procurement-and-management-of-contracts-for-direct-provision.pdf",
}
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

URL_RX = re.compile(r"https?://[^\s)>\]]+")
SI_RX = re.compile(r"S\.?I\.?\s*(?:No\.?\s*)?(\d{1,4})\s*(?:/|of)\s*(\d{4})", re.I)
EU_RX = re.compile(r"Regulation\s*\(EU\)\s*(\d{4})/(\d+)")
ACT_RX = re.compile(r"([A-Z][A-Za-z&]+(?:\s+(?:and\s+)?[A-Z][A-Za-z&]+){0,5})\s+Acts?\s+(\d{4})(?:\s+to\s+(\d{4}))?")


def gather():
    urls, sis, eus, acts = {}, set(), set(), set()
    for yr, fn in CHAPTERS.items():
        doc = fitz.open(PDF_DIR / fn)
        text = "".join(p.get_text("text") for p in doc)
        for pno, page in enumerate(doc, 1):
            for lk in page.get_links():
                if lk.get("uri"):
                    urls.setdefault(lk["uri"].rstrip(".,"), (yr, pno, "annotation"))
        for m in URL_RX.finditer(text):
            u = m.group().rstrip(".,")
            urls.setdefault(u, (yr, None, "text"))
        for m in SI_RX.finditer(text):
            sis.add((int(m.group(1)), int(m.group(2))))
        for m in EU_RX.finditer(text):
            eus.add((int(m.group(1)), int(m.group(2))))
        for m in ACT_RX.finditer(text):
            name = re.sub(r"\s+", " ", m.group(1)).strip()
            if len(name) > 4 and name.lower() not in ("the", "these", "such"):
                acts.add((f"{name} Act{'s' if m.group(3) else ''} {m.group(2)}"
                          + (f" to {m.group(3)}" if m.group(3) else "")))
    return urls, sis, eus, acts


def check(url):
    try:
        r = requests.get(url, headers=UA, timeout=20, allow_redirects=True, stream=True)
        return r.status_code, r.url
    except Exception as e:
        return None, type(e).__name__


def main():
    urls, sis, eus, acts = gather()

    # --- URLs ---
    url_rows = []
    for u, (yr, pno, how) in sorted(urls.items()):
        code, final = check(u)
        url_rows.append({"url": u, "found_in_year": yr, "page": pno, "how": how,
                         "http_status": code, "resolved_url": final,
                         "alive": bool(code and code < 400)})
        print(f"[{code}] {u}")
    if url_rows:
        pl.DataFrame(url_rows).write_parquet(SILVER / "cag_ipas_links.parquet",
                                             compression="zstd", statistics=True)

    # --- Legal citations, SIs cross-referenced ---
    si_lf = pl.read_parquet(SI_GOLD, columns=["si_id", "si_year", "si_number",
                                              "si_title", "eisb_url", "iris_source_pdf"])
    law_rows = []
    for (num, yr) in sorted(sis):
        hit = si_lf.filter((pl.col("si_number") == num) & (pl.col("si_year") == yr))
        matched = hit.height > 0
        row = hit.row(0, named=True) if matched else {}
        law_rows.append({"citation": f"SI {num}/{yr}", "kind": "statutory_instrument",
                         "in_our_si_data": matched, "si_id": row.get("si_id"),
                         "si_title": row.get("si_title"), "eisb_url": row.get("eisb_url"),
                         "irishstatutebook_url": f"https://www.irishstatutebook.ie/eli/{yr}/si/{num}/made/en/print"})
    for (yr, num) in sorted(eus):
        law_rows.append({"citation": f"Regulation (EU) {yr}/{num}", "kind": "eu_regulation",
                         "in_our_si_data": False, "si_id": None, "si_title": None,
                         "eisb_url": None,
                         "irishstatutebook_url": f"https://eur-lex.europa.eu/eli/reg/{yr}/{num}/oj"})
    for a in sorted(acts):
        law_rows.append({"citation": a, "kind": "act", "in_our_si_data": False,
                         "si_id": None, "si_title": None, "eisb_url": None,
                         "irishstatutebook_url": None})

    law = pl.DataFrame(law_rows)
    law.write_parquet(SILVER / "cag_ipas_legal_citations.parquet",
                      compression="zstd", statistics=True)
    prov = {"source": "RoAPS 2024 ch.10 + 2015 ch.6", "derived_at": now_iso()}
    (SILVER / "_eyeball").mkdir(exist_ok=True)
    pl.DataFrame(url_rows).write_csv(SILVER / "_eyeball" / "cag_ipas_links.csv")
    law.write_csv(SILVER / "_eyeball" / "cag_ipas_legal_citations.csv")

    print(f"\nURLs: {len(url_rows)} ({sum(r['alive'] for r in url_rows)} alive)")
    print(f"SIs cited: {len(sis)} ({law.filter((pl.col('kind')=='statutory_instrument') & pl.col('in_our_si_data')).height} matched in our SI data)")
    with pl.Config(tbl_rows=40, fmt_str_lengths=60, tbl_width_chars=160):
        print(law.filter(pl.col("kind") == "statutory_instrument"))
        print(law.filter(pl.col("kind") != "statutory_instrument").select("citation", "kind"))


if __name__ == "__main__":
    main()
