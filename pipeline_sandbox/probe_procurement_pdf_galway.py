"""PROBE (throwaway): OFF-CATALOG council PDF scan — Galway City + County Council.

The CKAN census (probe_procurement_pdf_discovery.py) showed data.gov.ie only
catalogues PDFs from ONE council (Kildare). The real PO-over-20k PDF universe lives
on each council's OWN website, never registered centrally. This probe tests that
hypothesis on TWO new bodies (Galway City + County) whose quarterly-PO PDF URLs were
discovered via web search:

  Q1: are these councils' PDFs digital (fitz, no OCR) or scanned (PaddleOCR)?
      -> Kildare being digital does NOT guarantee a different council is.
  Q2: does the fitz word-geometry row reader (SIPO trick) lift supplier+€ rows?
  Q3: supplier -> CRO exact-name feasibility, same matcher as eTenders.

Seeds are the quarterly PDFs found on the two councils' finance pages (June 2026).

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/probe_procurement_pdf_galway.py
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

from shared.name_norm import name_norm_expr  # noqa: E402

CRO = ROOT / "data/silver/cro/companies.parquet"
TMP = Path("c:/tmp/procurement_pdf")
H = {"User-Agent": "dail-tracker research probe"}

MONEY_RE = re.compile(r"(?:€|EUR)?\s?\d{1,3}(?:,\d{3})+(?:\.\d{2})?|\d+\.\d{2}")
NUM_RE = re.compile(r"\d[\d,]*(?:\.\d+)?")


def to_eur(token: str) -> float:
    """Parse the first numeric run in a money token, tolerating €/� prefixes and a
    stray trailing period (e.g. '20,000.00.')."""
    m = NUM_RE.search(token)
    if not m:
        return 0.0
    try:
        return float(m.group().replace(",", ""))
    except ValueError:
        return 0.0
COMPANY_SUFFIX_RE = re.compile(
    r"\b(limited|ltd|dac|plc|clg|uc|llp|teoranta|teo|unlimited|company|holdings|group)\b", re.I
)

# discovered off-catalog quarterly PO-over-20k PDFs (web search, June 2026)
SEEDS: dict[str, list[str]] = {
    "Galway County Council": [
        # bare galwaycoco.ie has no DNS record; the files live on the www host
        "https://www.galwaycoco.ie/sites/default/files/2026-01/Quarter%201%202025%20%28ENG%29.pdf",
        "https://www.galwaycoco.ie/sites/default/files/2026-01/Quarter%202%202025%20%28ENG%29.pdf",
        "https://www.galwaycoco.ie/sites/default/files/2025-06/Quater%201%202024.pdf",
        "https://www.galwaycoco.ie/sites/default/files/2025-06/Quater%203%202024.pdf",
    ],
    "Galway City Council": [
        "https://www.galwaycity.ie/sites/default/files/2025-05/Qtr%201%202025%20_Purchase%20Orders%20Over%20%E2%82%AC20k.pdf",
        "https://www.galwaycity.ie/sites/default/files/2025-08/Qtr%202%202025%20Purchase%20Order%20over%20%E2%82%AC20k.pdf",
        "https://www.galwaycity.ie/sites/default/files/2025-10/Qtr%203%202025%20Purchase%20Order%20over%20%E2%82%AC20k.pdf",
        "https://www.galwaycity.ie/sites/default/files/2026-05/Qtr%201%202026_Purchase%20Orders%20over%20%E2%82%AC20k_0.pdf",
    ],
}


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def fetch(url: str) -> Path | None:
    name = re.sub(r"[^A-Za-z0-9._-]", "_", url.rsplit("/", 1)[-1])[:90]
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    dest = TMP / name
    if dest.exists() and dest.stat().st_size > 2000:
        return dest
    # some council servers (galwaycoco.ie) drop a strict TLS1.3 handshake with an
    # SSL EOF; retry the bare host and an http fallback before giving up.
    candidates = [url]
    if "www.galwaycoco.ie" in url:
        candidates.append(url.replace("www.galwaycoco.ie", "galwaycoco.ie"))
        candidates.append(url.replace("https://", "http://"))
    b = None
    for u in candidates:
        try:
            r = requests.get(u, headers=H, timeout=90, allow_redirects=True)
            if r.content[:4] == b"%PDF":
                b = r.content
                break
        except Exception as e:
            print(f"    try {u[:38]}… ERR {type(e).__name__}")
    if b is None:
        return None
    TMP.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(b)
    return dest


def cluster_word_rows(page, ytol: float = 3.0) -> list[list]:
    """Group word boxes into rows, KEEPING the (x0,y0,x1,y1,text) tuples so we can
    split columns geometrically (council PO layouts differ in column ORDER)."""
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
    """Layout-agnostic column split. Drop the money token (the amount), then split
    the remaining words at their LARGEST horizontal gap: left = supplier, right =
    category. Works whether the € sits last (Galway) or mid-row (Kildare) because
    we remove it before measuring gaps."""
    ws = sorted(words, key=lambda w: w[0])
    money_idx = [i for i, w in enumerate(ws) if MONEY_RE.fullmatch(w[4].replace("€", "").replace("�", "").strip(" .")) or MONEY_RE.search(w[4])]
    if not money_idx:
        return None
    # the amount = the money token furthest right (PO listings put the € last/right)
    amt_i = max(money_idx, key=lambda i: ws[i][0])
    eur = to_eur(ws[amt_i][4])
    rest = [w for i, w in enumerate(ws) if i != amt_i and not MONEY_RE.search(w[4])]
    if len(rest) < 1 or eur < 1000:
        return None
    # largest x-gap between consecutive words = the supplier|category boundary
    if len(rest) >= 2:
        gaps = [(rest[i + 1][0] - rest[i][2], i) for i in range(len(rest) - 1)]
        gap, cut = max(gaps)
        if gap < 12:  # one-column row (no real category gap) -> all supplier
            cut = len(rest) - 1
    else:
        cut = 0
    supplier = " ".join(w[4] for w in rest[: cut + 1]).strip(" -:|")
    category = " ".join(w[4] for w in rest[cut + 1:]).strip(" -:|")
    if len(supplier) < 3:
        return None
    return {"supplier": supplier, "eur": eur, "category": category}


def parse_pdf(path: Path) -> dict:
    """Classify digital/scanned + lift supplier/€ rows via word-geometry columns."""
    doc = fitz.open(path)
    npages = doc.page_count
    text_chars = 0
    rows_out: list[dict] = []
    sample_lines: list[str] = []
    for i in range(npages):
        page = doc[i]
        txt = page.get_text("text")
        text_chars += len(txt.strip())
        for wrow in cluster_word_rows(page):
            rec = split_row(wrow)
            if rec is None:
                continue
            rows_out.append({**rec, "category": rec["category"]})
            if len(sample_lines) < 5 and i < 2:
                sample_lines.append(
                    f"{rec['supplier'][:38]:<38} | {rec['eur']:>12,.2f} | {rec['category'][:24]}"
                )
    doc.close()
    return {
        "pages": npages,
        "text_chars": text_chars,
        "digital": text_chars > 200,
        "rows": rows_out,
        "sample": sample_lines,
    }


def main() -> None:
    cro = pl.read_parquet(CRO).select(["name_norm", "company_num"])
    grand: list[dict] = []
    summary: list[tuple[str, str, int, int, float]] = []

    for council, urls in SEEDS.items():
        hr(council)
        c_rows: list[dict] = []
        digital_docs = scanned_docs = 0
        for url in urls:
            print(f"\n• {url.rsplit('/', 1)[-1][:70]}")
            p = fetch(url)
            if not p:
                continue
            info = parse_pdf(p)
            kind = "DIGITAL" if info["digital"] else "SCANNED (needs OCR)"
            digital_docs += info["digital"]
            scanned_docs += not info["digital"]
            print(f"  pages={info['pages']}  text_chars={info['text_chars']:,}  "
                  f"rows={len(info['rows'])}  -> {kind}")
            for s in info["sample"]:
                print(f"      {s}")
            c_rows += info["rows"]

        if not c_rows:
            # distinguish a host that BLOCKED us (0 docs fetched) from a scanned doc
            label = "BLOCKED" if (digital_docs + scanned_docs) == 0 else "no rows"
            summary.append((council, label, digital_docs, scanned_docs, 0.0))
            continue

        cdf = pl.DataFrame(c_rows)
        total = cdf["eur"].sum()
        sup = (
            cdf.select("supplier")
            .with_columns(name_norm_expr("supplier").alias("nn"))
            .filter(pl.col("nn").str.len_chars() >= 4)
            .unique(subset=["nn"])
        )
        m = sup.join(cro, left_on="nn", right_on="name_norm", how="left")
        hit = m.filter(pl.col("company_num").is_not_null()).select("nn").n_unique()
        print(f"\n  PARSED: {cdf.height:,} PO rows  €{total / 1e6:.1f}m  "
              f"distinct suppliers {sup.height:,}  CRO 1:1 {hit} ({hit / max(1, sup.height):.0%})")
        print("  top payees:",
              cdf.group_by("supplier").agg(pl.col("eur").sum().alias("e"))
              .sort("e", descending=True).head(4).select("supplier").to_series().to_list())
        grand += c_rows
        summary.append((council, f"{digital_docs}D/{scanned_docs}S", cdf.height,
                        sup.height, hit / max(1, sup.height)))

    hr("CROSS-COUNCIL SUMMARY")
    print(f"{'council':<26}{'docs':<10}{'rows':>8}{'suppliers':>11}{'CRO 1:1':>9}")
    for council, docs, nrows, nsup, rate in summary:
        print(f"{council:<26}{docs:<10}{nrows:>8,}{nsup:>11,}{rate:>8.0%}")

    hr("VERDICT (off-catalog Galway scan)")
    # docs labels look like "4D/0S"; a scanned doc => the S-count is > 0
    any_scanned = any(
        d.endswith("S") and d.split("/")[1].rstrip("S").isdigit() and int(d.split("/")[1].rstrip("S")) > 0
        for _, d, *_ in summary if "/" in d
    )
    blocked = [c for c, d, *_ in summary if d == "BLOCKED"]
    print(f"total PO rows lifted from off-catalog councils: {len(grand):,}")
    print("These PDFs are NOT on data.gov.ie — pure off-catalog, found via the councils'")
    print("own finance pages. Confirms the real PO-over-20k universe is per-council web, not CKAN.")
    print("Extraction =", "MIXED (some scanned -> OCR)" if any_scanned else
          "fitz word-geometry + largest-x-gap column split, NO OCR (digital text layers).")
    if blocked:
        print(f"\nNOTE: {', '.join(blocked)} could NOT be fetched — host drops every TLS")
        print("connection (requests SSL-EOF, curl http=000, even no-verify/legacy-reneg).")
        print("That's a WAF/egress block, NOT a scanned-vs-digital signal: digital-vs-scanned")
        print("for that council stays UNKNOWN until fetched via a browser (Playwright) or other IP.")
    print("Build path: a small per-council seed list (finance page -> quarterly PDFs) +")
    print("the shared fitz row reader + the validated CRO matcher; OCR only if a council scans.")


if __name__ == "__main__":
    main()
