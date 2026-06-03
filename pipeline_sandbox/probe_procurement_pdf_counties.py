"""PROBE (throwaway): OFF-CATALOG multi-county PDF scan — does the "digital, no OCR"
pattern (Kildare, Galway City) hold across MORE county councils, or does a smaller /
older council finally publish a SCANNED image that reopens OCR risk?

Complements probe_procurement_pdf_galway.py (Galway) and probe_procurement_dublin_la.py
(Dublin region). Seeds here are concrete PO-over-20k PDF URLs harvested via web search
from each council's OWN site (none of these are on data.gov.ie):

  Cork City · Cork County · Mayo · Clare · Donegal

For each: fetch -> digital-or-scanned? -> fitz word-geometry + largest-x-gap column
split (handles per-council column ORDER differences) -> supplier/€ rows -> CRO 1:1.

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/probe_procurement_pdf_counties.py
Reads CRO silver; downloads sampled PDFs to c:/tmp; writes no repo data.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import fitz  # PyMuPDF
import polars as pl
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from cro_normalise import name_norm_expr  # noqa: E402

CRO = ROOT / "data/silver/cro/companies.parquet"
TMP = Path("c:/tmp/procurement_pdf")
H = {"User-Agent": "Mozilla/5.0 (dail-tracker research probe)"}

MONEY_RE = re.compile(r"(?:€|EUR)?\s?\d{1,3}(?:,\d{3})+(?:\.\d{2})?|\d+\.\d{2}")
NUM_RE = re.compile(r"\d[\d,]*(?:\.\d+)?")

# concrete off-catalog PO-over-20k files, harvested via web search (June 2026)
SEEDS: dict[str, list[str]] = {
    # Cork CITY moved to JS-listed Umbraco /media/{guid}/ paths (old media-folder
    # URLs now 404); current PO files aren't guessable without a browser render.
    "Cork City Council": [
        "https://www.corkcity.ie/en/media-folder/finance/q1-2021-pos-over-20k.pdf",
    ],
    # Cork COUNTY: live Drupal sites/default/files URLs (re-found 2026-06-03; the
    # 2019 deep-link had 404'd). Search pre-extracted "Supplier Name Total ..." text.
    "Cork County Council": [
        "https://www.corkcoco.ie/sites/default/files/2025-08/2025-q2-purchase-orders-in-excess-of-eu20000.pdf",
        "https://www.corkcoco.ie/sites/default/files/2025-05/2025-q1-purchase-orders-in-excess-of-eu20000-pdf.pdf",
        "https://www.corkcoco.ie/sites/default/files/2024-11/2024-q3-purchase-orders-in-excess-of-eu20000-pdf.pdf",
    ],
    "Mayo County Council": [
        "https://www.mayo.ie/getattachment/6a915b4f-ab78-4b69-8ba6-d07569222e03/attachment.aspx",
        "https://www.mayo.ie/getattachment/00705716-9095-410a-a719-9305e0a37f4f/attachment.aspx",
        "https://www.mayo.ie/getattachment/8a28d549-64a7-4016-98ad-7d31fd3ccfa3/attachment.aspx",
    ],
    "Clare County Council": [
        "https://www.clarecoco.ie/sites/default/files/2025-08/purchase-orders-over-20-000-in-the-2nd-quarter-of-2025-58204.pdf",
        "https://www.clarecoco.ie/services/business/procurement/pos-over-20k/2016/clare-county-council-q4-2016-25875.pdf",
    ],
    "Donegal County Council": [
        "https://www.donegalcoco.ie/media/e5lhtnog/2026.pdf",
        "https://www.donegalcoco.ie/media/h0flvm3b/2025.pdf",
        "https://www.donegalcoco.ie/media/b2aopuh2/2024.pdf",
    ],
}


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def to_eur(token: str) -> float:
    m = NUM_RE.search(token)
    if not m:
        return 0.0
    try:
        return float(m.group().replace(",", ""))
    except ValueError:
        return 0.0


def fetch(url: str, idx: int) -> Path | None:
    name = re.sub(r"[^A-Za-z0-9._-]", "_", url.rsplit("/", 1)[-1])[:70]
    # .aspx attachment endpoints (Mayo) don't end in .pdf — force a stable name
    if not name.lower().endswith(".pdf"):
        name = f"county_{idx}_{name}.pdf"
    dest = TMP / name
    if dest.exists() and dest.stat().st_size > 2000:
        return dest
    for u in ([url, url.replace("https://", "http://")] if "https://" in url else [url]):
        try:
            r = requests.get(u, headers=H, timeout=90, allow_redirects=True)
            if r.content[:4] == b"%PDF":
                TMP.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(r.content)
                return dest
            print(f"    not a PDF (got {r.content[:12]!r}, ctype={r.headers.get('content-type','?')})")
            return None
        except Exception as e:
            print(f"    {u[:34]}… ERR {type(e).__name__}")
    return None


def cluster_word_rows(page, ytol: float = 3.0) -> list[list]:
    words = page.get_text("words")
    words.sort(key=lambda w: (round(w[1] / ytol), w[0]))
    rows, cur, cur_y = [], [], None
    for w in words:
        y = w[1]
        if cur_y is None or abs(y - cur_y) <= ytol:
            cur.append(w)
            cur_y = y if cur_y is None else cur_y
        else:
            rows.append(cur)
            cur, cur_y = [w], y
    if cur:
        rows.append(cur)
    return rows


def split_row(words: list) -> dict | None:
    """Layout-agnostic: drop the money token, split remaining words at their largest
    horizontal gap (left=supplier, right=category). Order-independent."""
    ws = sorted(words, key=lambda w: w[0])
    money_idx = [i for i, w in enumerate(ws) if MONEY_RE.search(w[4])]
    if not money_idx:
        return None
    amt_i = max(money_idx, key=lambda i: ws[i][0])
    eur = to_eur(ws[amt_i][4])
    rest = [w for i, w in enumerate(ws) if i != amt_i and not MONEY_RE.search(w[4])]
    if not rest or eur < 1000:
        return None
    if len(rest) >= 2:
        gap, cut = max((rest[i + 1][0] - rest[i][2], i) for i in range(len(rest) - 1))
        if gap < 12:
            cut = len(rest) - 1
    else:
        cut = 0
    supplier = " ".join(w[4] for w in rest[: cut + 1]).strip(" -:|")
    category = " ".join(w[4] for w in rest[cut + 1:]).strip(" -:|")
    # per-council quirk: Mayo/Donegal prepend a PO number (and sometimes a supplier
    # ID) to the row -> strip any leading run of pure-digit tokens before the name,
    # else CRO matching fails AND distinct-supplier counts explode (each PO# unique).
    supplier = re.sub(r"^(?:\d{3,}\s+){1,2}", "", supplier).strip(" -:|")
    if len(supplier) < 3:
        return None
    return {"supplier": supplier, "eur": eur, "category": category}


def parse_pdf(path: Path) -> dict:
    doc = fitz.open(path)
    npages = doc.page_count
    chars = 0
    rows: list[dict] = []
    sample: list[str] = []
    for i in range(npages):
        page = doc[i]
        chars += len(page.get_text("text").strip())
        for wrow in cluster_word_rows(page):
            rec = split_row(wrow)
            if rec is None:
                continue
            rows.append(rec)
            if len(sample) < 4 and i < 2:
                sample.append(f"{rec['supplier'][:36]:<36} | {rec['eur']:>12,.2f} | {rec['category'][:22]}")
    doc.close()
    return {"pages": npages, "chars": chars, "digital": chars > 200, "rows": rows, "sample": sample}


def main() -> None:
    cro = pl.read_parquet(CRO).select(["name_norm", "company_num"])
    summary = []
    grand = 0

    for ci, (council, urls) in enumerate(SEEDS.items()):
        hr(council)
        c_rows: list[dict] = []
        dig = scan = got = 0
        for ui, url in enumerate(urls):
            print(f"\n• {url.rsplit('/', 1)[-1][:60]}")
            p = fetch(url, ci * 10 + ui)
            if not p:
                continue
            got += 1
            info = parse_pdf(p)
            dig += info["digital"]
            scan += not info["digital"]
            kind = "DIGITAL" if info["digital"] else "SCANNED (needs OCR)"
            print(f"  pages={info['pages']}  text_chars={info['chars']:,}  rows={len(info['rows'])}  -> {kind}")
            for s in info["sample"]:
                print(f"      {s}")
            c_rows += info["rows"]

        if got == 0:
            summary.append((council, "BLOCKED", 0, 0, 0.0, 0.0))
            continue
        if not c_rows:
            summary.append((council, f"{dig}D/{scan}S", 0, 0, 0.0, 0.0))
            continue

        cdf = pl.DataFrame(c_rows)
        total = cdf["eur"].sum()
        sup = (cdf.select("supplier")
               .with_columns(name_norm_expr("supplier").alias("nn"))
               .filter(pl.col("nn").str.len_chars() >= 4)
               .unique(subset=["nn"]))
        m = sup.join(cro, left_on="nn", right_on="name_norm", how="left")
        hit = m.filter(pl.col("company_num").is_not_null()).select("nn").n_unique()
        rate = hit / max(1, sup.height)
        print(f"\n  PARSED: {cdf.height:,} PO rows  €{total / 1e6:.1f}m  "
              f"distinct suppliers {sup.height:,}  CRO 1:1 {hit} ({rate:.0%})")
        grand += cdf.height
        summary.append((council, f"{dig}D/{scan}S", cdf.height, sup.height, total / 1e6, rate))

    hr("CROSS-COUNTY SUMMARY")
    print(f"{'council':<26}{'docs':<9}{'rows':>7}{'suppliers':>11}{'€m':>9}{'CRO':>7}")
    for c, d, nr, ns, em, rate in summary:
        print(f"{c:<26}{d:<9}{nr:>7,}{ns:>11,}{em:>8.1f}{rate:>7.0%}")

    hr("VERDICT")
    scanned = [c for c, d, *_ in summary if "/" in d and d.split("/")[1].rstrip("S").isdigit()
               and int(d.split("/")[1].rstrip("S")) > 0]
    blocked = [c for c, d, *_ in summary if d == "BLOCKED"]
    digital_ok = [c for c, d, *_ in summary if "/" in d and d.startswith(tuple("123456789"))
                  and (not d.split("/")[1].rstrip("S").isdigit() or int(d.split("/")[1].rstrip("S")) == 0)]
    print(f"councils probed: {len(summary)}  |  total PO rows lifted: {grand:,}")
    print(f"  digital (fitz, no OCR): {digital_ok}")
    if scanned:
        print(f"  *** SCANNED (needs PaddleOCR): {scanned}  <- OCR risk is REAL off-catalog")
    else:
        print("  scanned: NONE -> the 'all digital' pattern now holds across "
              f"{len(digital_ok) + 2} councils (these + Kildare + Galway City).")
    if blocked:
        print(f"  blocked (host refused fetch): {blocked}  -> needs browser/other IP, status UNKNOWN")
    print("\nTakeaway: each council = bespoke host + column order, but extraction is fitz +")
    print("largest-x-gap + CRO matcher. Build = per-council seed list; OCR only if one scans.")


if __name__ == "__main__":
    main()
