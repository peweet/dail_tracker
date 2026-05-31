"""Throwaway probe: try fitz.find_tables() on the 5 source PDFs.

DELETE after running. Output goes to stdout for assessment.
"""
import fitz
from pathlib import Path

PDFS = [
    "SSHA_2024_Report.pdf",
    "SSHA_2025_FINAL.pdf",
    "PBO_OngoingNeed2024.pdf",
    "NOAC_LA_PerfInd_2024.pdf",
    "HAP_PerfInd_2024.pdf",
]

base = Path(__file__).parent


def show_table(t, label):
    print(f"\n  -- {label}: rows={t.row_count}, cols={t.col_count}")
    try:
        data = t.extract()
        for r in data[:6]:
            cells = [(str(c) if c is not None else "")[:22] for c in r]
            print("    | ".join(cells))
        if len(data) > 6:
            print(f"    ... +{len(data) - 6} more rows")
        return data
    except Exception as e:
        print(f"    extract error: {e}")
        return None


for pdf_name in PDFS:
    p = base / pdf_name
    if not p.exists():
        print(f"\nMISSING: {pdf_name}")
        continue
    print(f"\n{'=' * 60}\n{pdf_name}\n{'=' * 60}")
    doc = fitz.open(str(p))
    print(f"Pages: {doc.page_count}")

    total_tables = 0
    table_pages = []
    samples_shown = 0
    target_samples = 3

    for pi, page in enumerate(doc):
        tabs = page.find_tables()
        n = len(tabs.tables)
        if n > 0:
            total_tables += n
            table_pages.append((pi + 1, n))
            if samples_shown < target_samples:
                for t in tabs.tables:
                    if samples_shown >= target_samples:
                        break
                    show_table(t, f"p{pi + 1}")
                    samples_shown += 1

    print(f"\nSummary: {total_tables} tables on {len(table_pages)} pages")
    if 0 < len(table_pages) <= 12:
        print(f"Pages with tables: {table_pages}")

    # Targeted probes for known per-LA tables
    if pdf_name == "NOAC_LA_PerfInd_2024.pdf":
        print("\n  >> targeted scan for housing section (PDF pages 21-52)")
        for pi in range(20, 52):
            page = doc[pi]
            tabs = page.find_tables()
            if tabs.tables:
                t = tabs.tables[0]
                if t.row_count >= 5 and t.col_count >= 3:
                    show_table(t, f"NOAC housing p{pi + 1} (first table)")
                    break

    if pdf_name == "PBO_OngoingNeed2024.pdf":
        print("\n  >> targeted scan for §4 (PDF pages 8-13)")
        for pi in range(7, 13):
            page = doc[pi]
            tabs = page.find_tables()
            if tabs.tables:
                for t in tabs.tables:
                    if t.row_count >= 5:
                        show_table(t, f"PBO §4 p{pi + 1}")
                        break

    if pdf_name == "SSHA_2024_Report.pdf":
        print("\n  >> targeted scan for Appendix A1.5 (Main need by LA)")
        for pi in range(40, 52):
            page = doc[pi]
            tabs = page.find_tables()
            if tabs.tables:
                t = tabs.tables[0]
                if t.row_count >= 10:
                    data = show_table(t, f"SSHA appendix p{pi + 1}")
                    if data:
                        print(f"    [full row count: {len(data)}]")
                    break

    doc.close()

print("\n\n===== DONE =====")
