"""THROWAWAY PROBE: two-pass PDF parse of judiciary source PDFs.
Pass 1 = camelot (stream; no ghostscript so lattice unavailable) -> tables.
Pass 2 = fitz/PyMuPDF -> text + native table detection.
Run with the isolated env: C:/tmp/pdfprobe/Scripts/python.exe pipeline_sandbox/probe_judiciary_pdf.py
Not pipeline code.
"""

import sys
import warnings
warnings.filterwarnings("ignore")
import fitz  # PyMuPDF
import camelot

PDF = sys.argv[1] if len(sys.argv) > 1 else r"C:\tmp\jc_annual_2024.pdf"
KEYWORDS = ["complaint", "conduct", "received", "dismissed", "upheld", "admissib",
            "review", "panel", "court", "summary", "outcome", "referred"]

# --- scout: find candidate statistics pages with fitz ---
doc = fitz.open(PDF)
print(f"PDF: {PDF}  |  pages: {doc.page_count}")
cand = []
for i in range(doc.page_count):
    txt = doc.load_page(i).get_text().lower()
    score = sum(txt.count(k) for k in KEYWORDS)
    digits = sum(c.isdigit() for c in txt)
    if score >= 4 and digits >= 15:  # keyword-dense + numbery => likely a stats table
        cand.append((i + 1, score, digits))
cand.sort(key=lambda x: -(x[1] + x[2] / 20))
top_pages = [p for p, _, _ in cand[:8]]
print(f"candidate stat pages (1-indexed): {top_pages}")
print()

# ============ PASS 1 — CAMELOT (stream) ============
print("=" * 60)
print("PASS 1 — CAMELOT (flavor=stream)")
print("=" * 60)
pages_arg = ",".join(str(p) for p in top_pages) if top_pages else "all"
try:
    tables = camelot.read_pdf(PDF, flavor="stream", pages=pages_arg)
    print(f"tables found: {tables.n}")
    for t in tables:
        rep = t.parsing_report
        print(f"\n  [table {rep['order']}] page {rep['page']}  shape={t.df.shape}  "
              f"accuracy={rep['accuracy']:.0f}  whitespace={rep['whitespace']:.0f}")
        # show the table if it looks like real tabular content (>=3 cols or >=4 rows)
        if t.df.shape[1] >= 2 and t.df.shape[0] >= 3:
            preview = t.df.head(8).to_string(max_colwidth=24)
            print("\n".join("      " + ln for ln in preview.splitlines()))
except Exception as e:
    print("camelot stream FAILED:", repr(e))

# ============ PASS 2 — FITZ (text + native table finder) ============
print("\n" + "=" * 60)
print("PASS 2 — FITZ (PyMuPDF text + find_tables)")
print("=" * 60)
for p in top_pages[:5]:
    page = doc.load_page(p - 1)
    print(f"\n--- page {p} ---")
    # native table detection (PyMuPDF >=1.23)
    try:
        tf = page.find_tables()
        print(f"  find_tables(): {len(tf.tables)} table(s)")
        for ti, tab in enumerate(tf.tables):
            rows = tab.extract()
            print(f"    table {ti}: {len(rows)} rows x {len(rows[0]) if rows else 0} cols")
            for r in rows[:6]:
                cells = " | ".join((c or "").strip()[:22] for c in r)
                print(f"      {cells}")
    except Exception as e:
        print("  find_tables failed:", repr(e))
    # a slice of raw text so we can see what aggregate figures exist
    txt = page.get_text().strip()
    snippet = "\n".join(ln for ln in txt.splitlines() if any(k in ln.lower() for k in KEYWORDS))[:600]
    if snippet:
        print("  keyword lines:")
        print("\n".join("    " + ln for ln in snippet.splitlines()[:8]))

doc.close()
