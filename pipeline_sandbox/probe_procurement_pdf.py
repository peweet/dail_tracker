"""PROBE (throwaway): the PDF-format slice of the actual-spend procurement layer.

The CSV/XLSX spend datasets are handled by probe_procurement_spend.py. But the
'Purchase Orders over 20,000' / 'Procurement Related Payments over 20,000'
families also publish a minority of resources as **PDF** — those are invisible to
the CSV probe and to any tabular pipeline. This probe answers the prior question
before any build: are those procurement PDFs *digital* (a real text layer fitz can
lift straight into a table) or *scanned images* (needing the PaddleOCR engine that
the SIPO work just validated)? And once extracted, does the supplier+amount signal
survive well enough to join to CRO / sum real spend?

Strategy per sampled PDF:
  1. fitz get_text("text")  -> char count = is there a digital text layer at all?
  2. fitz get_text("words") -> y-cluster into rows, look for €-amount + name signal
  3. if the page is image-only (near-zero text), render @300dpi and note that OCR
     (probe_sipo_ocr_paddle.py scaffold) is the required path — do NOT OCR the
     whole corpus here, just classify and measure the OCR debt.

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/probe_procurement_pdf.py
Reads CRO silver; downloads sampled PDFs to c:/tmp; writes no repo data.
"""

from __future__ import annotations

import re
import sys
from collections import Counter
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

# a money figure: €1,234.56 / 1234.56 / 12,345 (with or without symbol)
MONEY_RE = re.compile(r"(?:€|EUR)?\s?\d{1,3}(?:,\d{3})+(?:\.\d{2})?|\d+\.\d{2}")
COMPANY_SUFFIX_RE = re.compile(
    r"\b(limited|ltd|dac|plc|clg|uc|llp|teoranta|teo|unlimited|company|holdings|group)\b", re.I
)


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def search(q: str, rows: int = 200) -> list[dict]:
    r = requests.get(
        "https://data.gov.ie/api/3/action/package_search",
        params={"q": q, "rows": rows},
        headers=H,
        timeout=60,
    )
    return r.json()["result"]["results"]


def pdf_resources(pkgs: list[dict], want: str) -> list[tuple[str, str]]:
    """(dataset_title, url) for every PDF resource in matching datasets."""
    out = []
    for d in pkgs:
        if want.lower() not in d["title"].lower():
            continue
        for x in d.get("resources", []):
            u = x.get("url", "") or ""
            fmt = (x.get("format", "") or u.rsplit(".", 1)[-1]).lower()
            if u and (fmt == "pdf" or u.lower().endswith(".pdf")):
                out.append((d["title"][:60], u))
    return out


def _publisher(pkg: dict) -> str:
    """Best-effort publishing body for a package (org title > author > pkg title)."""
    org = (pkg.get("organization") or {}).get("title") or ""
    return (org or pkg.get("author") or pkg.get("title") or "unknown").strip()[:55]


def _fmt(res: dict) -> str:
    u = (res.get("url") or "").lower()
    f = (res.get("format") or "").strip().lower()
    if not f and "." in u:
        f = u.rsplit(".", 1)[-1]
    # collapse the tabular family for the headline count
    if f in {"xls", "xlsx", "xlsm"}:
        return "xlsx"
    return f or "unknown"


# Genuine procurement-spend datasets only. The CKAN free-text query "purchase
# orders over 20" returns 100+ GEOSPATIAL false positives (EPA "Groundwater
# Pressures", Marine Institute seabed surveys) that merely contain the words
# "over"/"20" — so a TITLE filter is mandatory or the census is meaningless.
SPEND_TITLE_RE = re.compile(
    r"purchase order|procurement related payment|payments over|purchases over|po'?s over",
    re.I,
)


def run_census(po: list[dict], pay: list[dict]) -> None:
    """How many spend publishers ship PDF vs CSV/XLSX — sizes the normalisation job."""
    seen: dict[str, dict] = {}
    for d in po + pay:
        seen[d["id"]] = d
    spend = [d for d in seen.values() if SPEND_TITLE_RE.search(d.get("title", ""))]

    hr("FORMAT CENSUS — genuine spend publishers (sizes the normalisation project)")
    print(f"raw free-text union: {len(seen)} datasets (mostly geospatial noise);")
    print(f"title-confirmed procurement-spend datasets: {len(spend)}")
    if not spend:
        print("none matched the spend-title filter.")
        return

    rows = [(_publisher(d), _fmt(x)) for d in spend for x in d.get("resources", []) if x.get("url")]
    print(f"total resources    : {len(rows):,}")
    print("resource formats   :", dict(Counter(f for _, f in rows).most_common()))

    by_pub: dict[str, Counter] = {}
    for pub, f in rows:
        by_pub.setdefault(pub, Counter())[f] += 1
    TAB = {"csv", "xlsx"}
    pdf_only = [p for p, c in by_pub.items() if set(c) and set(c) <= {"pdf"}]
    has_tab = [p for p, c in by_pub.items() if set(c) & TAB]
    has_pdf = [p for p, c in by_pub.items() if "pdf" in c]
    print(f"\ndistinct publishers          : {len(by_pub):,}")
    print(f"  publish ANY CSV/XLSX (easy): {len(has_tab):,}")
    print(f"  publish ANY PDF            : {len(has_pdf):,}")
    print(f"  *** PDF-ONLY (the debt)    : {len(pdf_only):,}  -> {sorted(pdf_only)}")
    print("\nper-publisher format breakdown:")
    for p, c in sorted(by_pub.items(), key=lambda kv: -sum(kv[1].values())):
        print(f"  {p:<48} {dict(c)}")


def fetch(url: str) -> Path | None:
    name = re.sub(r"[^A-Za-z0-9._-]", "_", url.rsplit("/", 1)[-1])[:80] or "doc.pdf"
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    dest = TMP / name
    if dest.exists() and dest.stat().st_size > 2000:
        return dest
    try:
        b = requests.get(url, headers=H, timeout=90).content
    except Exception as e:
        print(f"    download ERR {e!r}")
        return None
    if not b[:4] == b"%PDF":
        print("    not a PDF (server returned HTML/other)")
        return None
    TMP.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(b)
    return dest


def cluster_rows(page, ytol: float = 3.0) -> list[str]:
    """Group word boxes into reading-order rows (the SIPO geometry trick)."""
    words = page.get_text("words")  # (x0,y0,x1,y1, word, block, line, wordno)
    words.sort(key=lambda w: (round(w[1] / ytol), w[0]))
    rows, cur, cur_y = [], [], None
    for w in words:
        y = w[1]
        if cur_y is None or abs(y - cur_y) <= ytol:
            cur.append(w[4])
            cur_y = y if cur_y is None else cur_y
        else:
            rows.append(" ".join(cur))
            cur, cur_y = [w[4]], y
    if cur:
        rows.append(" ".join(cur))
    return rows


def classify_pdf(path: Path) -> dict:
    doc = fitz.open(path)
    npages = doc.page_count
    text_chars = 0
    money_hits = 0
    sample_rows: list[str] = []
    suppliers: list[str] = []
    for i in range(min(npages, 6)):
        page = doc[i]
        txt = page.get_text("text")
        text_chars += len(txt.strip())
        money_hits += len(MONEY_RE.findall(txt))
        if i < 2:
            rows = cluster_rows(page)
            for r in rows:
                if MONEY_RE.search(r) and len(r) > 12:
                    sample_rows.append(r)
            suppliers += [r for r in rows if COMPANY_SUFFIX_RE.search(r)]
    doc.close()
    digital = text_chars > 200  # any real text layer at all
    return {
        "pages": npages,
        "text_chars": text_chars,
        "money_hits": money_hits,
        "digital": digital,
        "sample_rows": sample_rows[:6],
        "suppliers": suppliers[:6],
    }


def main() -> None:
    po = search("Purchase Orders over 20")
    pay = search("Procurement Related Payments over 20,000")
    run_census(po, pay)

    hr("DISCOVER PDF-FORMAT PROCUREMENT RESOURCES")
    res = pdf_resources(po, "Purchase Orders over") + pdf_resources(pay, "Procurement Related Payments")
    print(f"PDF resources found across both families: {len(res)}")
    if not res:
        print("No PDF-format resources surfaced via CKAN. (Most bodies publish CSV/XLSX.)")
        print("=> the PDF debt may be near-zero; the CSV/XLSX probe likely covers the spend layer.")
        return
    pubs = Counter(t for t, _ in res)
    print("publishers with PDF resources (top):")
    for t, n in pubs.most_common(10):
        print(f"  {n:>2}x  {t}")

    sample = res[:8]
    hr(f"CLASSIFY A SAMPLE OF {len(sample)} PDFs (digital text vs scanned image)")
    classified = []
    for title, url in sample:
        print(f"\n• {title}\n  {url[:90]}")
        p = fetch(url)
        if not p:
            continue
        info = classify_pdf(p)
        kind = "DIGITAL text-layer" if info["digital"] else "SCANNED image (needs OCR)"
        print(f"  pages={info['pages']}  text_chars={info['text_chars']:,}  "
              f"money_tokens={info['money_hits']}  -> {kind}")
        for r in info["sample_rows"][:4]:
            print(f"      row: {r[:88]}")
        classified.append((title, info))

    if not classified:
        print("nothing downloaded; CKAN urls may be dead links.")
        return

    digital = [c for c in classified if c[1]["digital"]]
    scanned = [c for c in classified if not c[1]["digital"]]
    hr("EXTRACTABILITY VERDICT")
    print(f"sampled PDFs        : {len(classified)}")
    print(f"  digital text-layer: {len(digital)}  (fitz extracts directly, no OCR)")
    print(f"  scanned image     : {len(scanned)}  (needs PaddleOCR engine)")

    # supplier->CRO feasibility: split each row on the FIRST money token into
    # (supplier, amount, category) — the obvious 3-column layout these POs use.
    all_rows = [r for _, info in digital for r in info["sample_rows"]] + [
        r for _, info in digital for r in info["suppliers"]
    ]
    parsed = []
    for r in all_rows:
        m = MONEY_RE.search(r)
        if not m:
            continue
        supplier = r[: m.start()].strip(" -:\t")
        eur = float(re.sub(r"[^0-9.]", "", m.group()) or 0)
        category = r[m.end():].strip(" -:\t")
        if len(supplier) >= 3 and eur > 0:
            parsed.append({"supplier": supplier, "eur": eur, "category": category})
    if parsed:
        hr("COLUMN SPLIT + CRO FEASIBILITY (split row on the € token)")
        pdf_rows = pl.DataFrame(parsed)
        print(f"rows parsed into supplier/amount/category: {pdf_rows.height}")
        print(pdf_rows.head(6))
        sdf = (
            pdf_rows.select("supplier")
            .with_columns(name_norm_expr("supplier").alias("nn"))
            .filter(pl.col("nn").str.len_chars() >= 4)
            .unique(subset=["nn"])
        )
        cro = pl.read_parquet(CRO).select(["name_norm", "company_num"])
        m = sdf.join(cro, left_on="nn", right_on="name_norm", how="left")
        hit = m.filter(pl.col("company_num").is_not_null()).select("nn").n_unique()
        print(f"\ndistinct suppliers: {sdf.height}   CRO exact-name 1:1 hits: {hit} "
              f"({hit / max(1, sdf.height):.0%})")
        print("  (exact name_norm only, on a tiny 2-page-per-doc sample; the validated")
        print("   suffix-repair + fuzzy matcher from the eTenders work lifts this further.)")

    hr("VERDICT")
    if scanned and not digital:
        print("procurement PDFs are SCANNED -> OCR-gated; reuse the PaddleOCR scaffold.")
    elif digital and not scanned:
        print("procurement PDFs are DIGITAL -> fitz word-geometry extracts them with NO OCR;")
        print("normalisation (per-publisher column layout) is the real cost, not extraction.")
    else:
        print("MIXED: some digital, some scanned -> fitz-first, OCR-fallback per document.")
    print("Either way the PDF slice is a long tail vs the CSV/XLSX bulk; size it before building.")


if __name__ == "__main__":
    main()
