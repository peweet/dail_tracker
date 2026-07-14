"""Fetch the HIQA IPAS monitoring report (2024) + the Comprehensive Accommodation
Strategy, cache to bronze, extract text. SANDBOX ONLY."""
import fitz
import requests
from _common import BRONZE, sha256_bytes

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Referer": "https://www.gov.ie/"}
DOCS = {
    "hiqa_ipas_monitoring_2024.pdf":
        "https://www.hiqa.ie/sites/default/files/2025-03/Monitoring-of-International-Protection-Accommodation-Service-centres-in-2024.pdf",
    "comprehensive_accommodation_strategy.pdf":
        "https://assets.gov.ie/static/documents/comprehensive-accommodation-strategy-for-international-protection-applicants.pdf",
}
d = BRONZE / "ipas_context"
d.mkdir(parents=True, exist_ok=True)
tdir = d / "text"
tdir.mkdir(exist_ok=True)

for name, url in DOCS.items():
    p = d / name
    if not p.exists():
        r = requests.get(url, headers=UA, timeout=90)
        r.raise_for_status()
        p.write_bytes(r.content)
        print(f"fetched {name}: {len(r.content)/1024:.0f} KB sha={sha256_bytes(r.content)[:12]}")
    doc = fitz.open(p)
    pages = [f"\n=== PAGE {i+1} ===\n" + pg.get_text("text") for i, pg in enumerate(doc)]
    txt = "".join(pages)
    (tdir / (name[:-4] + ".txt")).write_text(txt, encoding="utf-8")
    nch = len(txt.replace(" ", "").replace("\n", ""))
    print(f"{name}: {doc.page_count} pages, {nch} chars "
          f"({'TEXT OK' if nch > 300*doc.page_count else 'LOW TEXT - check OCR'})")
    toc = doc.get_toc()
    if toc:
        print("  TOC:")
        for lvl, title, pno in toc[:40]:
            print(f"   {'  '*(lvl-1)}p{pno:>3} {title[:80]}")
