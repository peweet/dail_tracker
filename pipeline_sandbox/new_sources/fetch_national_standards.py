"""Fetch the National Standards for accommodation offered to people in the protection
process (2021), cache to bronze with SHA-256, dump the text layer.

assets.gov.ie sits behind a WAF that 403s a non-browser UA — the browser-UA + Referer
pattern used by fetch_hiqa_and_strategy.py / fetch_ipas_weekly_stats.py is required.
Polite pace >= 2s between attempts. SANDBOX ONLY.
"""
from __future__ import annotations

import sys
import time

import fitz
import requests

from _common import BRONZE, sha256_bytes

URL = "https://assets.gov.ie/static/documents/national-standards.pdf"
UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer": "https://www.gov.ie/",
    "Accept": "application/pdf,*/*",
    "Accept-Language": "en-IE,en;q=0.9",
}
D = BRONZE / "ipas_context"
TDIR = D / "text"
NAME = "national_standards.pdf"


def main() -> None:
    D.mkdir(parents=True, exist_ok=True)
    TDIR.mkdir(exist_ok=True)
    p = D / NAME
    if not p.exists():
        last = None
        for attempt in range(1, 4):
            try:
                r = requests.get(URL, headers=UA, timeout=90)
                print(f"attempt {attempt}: HTTP {r.status_code} "
                      f"{r.headers.get('content-type')} {len(r.content)} bytes")
                r.raise_for_status()
                if not r.content.startswith(b"%PDF"):
                    raise ValueError(f"not a PDF (first bytes {r.content[:16]!r}) - WAF interstitial?")
                p.write_bytes(r.content)
                break
            except Exception as e:  # noqa: BLE001
                last = e
                print(f"  attempt {attempt} failed: {e}")
                time.sleep(2.5)
        else:
            print(f"BLOCKED after 3 polite attempts: {last}")
            sys.exit(2)

    raw = p.read_bytes()
    sha = sha256_bytes(raw)
    doc = fitz.open(p)
    pages = [f"\n=== PAGE {i + 1} ===\n" + pg.get_text("text") for i, pg in enumerate(doc)]
    txt = "".join(pages)
    (TDIR / "national_standards.txt").write_text(txt, encoding="utf-8")
    nch = len(txt.replace(" ", "").replace("\n", ""))
    print(f"{NAME}: {doc.page_count} pages, {len(raw) / 1024:.0f} KB, sha256={sha}")
    print(f"text: {nch} chars ({'TEXT OK' if nch > 300 * doc.page_count else 'LOW TEXT - check OCR'})")
    toc = doc.get_toc()
    if toc:
        print("TOC:")
        for lvl, title, pno in toc[:60]:
            print(f"  {'  ' * (lvl - 1)}p{pno:>3} {title[:90]}")


if __name__ == "__main__":
    main()
