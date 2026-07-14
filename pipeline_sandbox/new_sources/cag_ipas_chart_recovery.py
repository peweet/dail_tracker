"""Recover the non-text-layer data from C&AG RoAPS 2024 Chapter 10:

1. Figure 10.4 supplier bar values  — raster bar-length measurement calibrated
   against the vector axis labels (the chart is an embedded image; the axis
   labels are real PDF text). Validation: measured sum 229.7 vs the text's
   "almost EUR 230 million".
2. Figure 10.3 annual expenditure    — same technique, vertical bars.
   Validation: 2024 measures 1,065 vs the known exact 1,066 (~0.1% error).
3. Annex 10A compliance grid         — the tick marks are Webdings glyph 0xf06e
   coloured green #7d9149 / amber #ffc000 / red #ff0000, mapped to the printed
   legend (Complete / Partially complete / Not complete). Cells with no glyph
   are recorded as 'blank' — never guessed.

SANDBOX ONLY. Output: silver/cag_ipas_chart_recovery.parquet.
All money rows value_safe_to_sum=False (audit-report grain).
"""
from __future__ import annotations

import hashlib
from pathlib import Path

import fitz
import polars as pl

from _common import BRONZE, SILVER, now_iso

PDF = BRONZE / "cag_reports" / "pdf" / \
    "10-management-of-international-protection-accommodation-contracts-copy.pdf"
SRC_URL = ("https://www.audit.gov.ie/media/huahyz0u/"
           "10-management-of-international-protection-accommodation-contracts-copy.pdf")
ZOOM = 4
COLOURS = {0x7D9149: "complete", 0xFFC000: "partially_complete", 0xFF0000: "not_complete"}
TICK_COLUMNS = ["site_visit", "proposal_form", "cro_number", "ownership_lease",
                "signed_contract", "fire_safety_certificate", "inspection",
                "planning", "insurance"]
# property metadata in Annex table order (matches cag_ipas_chapter_figures sample_property ids)
PROPERTIES = [
    ("Dormitory", "North Dublin"), ("Apartment complex", "Meath"), ("Hotel", "Limerick"),
    ("Hotel", "South Dublin"), ("Hotel", "Louth"), ("Dormitory", "South Dublin"),
    ("Guesthouse", "Donegal"), ("Apartment complex", "South Dublin"),
    ("Apartment complex", "Donegal"), ("Hotel", "Tipperary"), ("Hotel", "Clare"),
    ("Hotel", "South Dublin"), ("Hotel", "North Dublin"), ("Dormitory", "South Dublin"),
    ("Apartment complex", "Westmeath"), ("Dormitory", "Mayo"), ("Hotel", "Mayo"),
    ("Dormitory", "Mayo"), ("Hotel", "Louth"), ("Guesthouse", "Kildare"),
]


def spans(page):
    return [s for b in page.get_text("dict")["blocks"] if b["type"] == 0
            for l in b["lines"] for s in l["spans"]]


def rgb_at(buf, width, n, x, y):
    i = (y * width + x) * n
    return buf[i], buf[i + 1], buf[i + 2]


def is_bar(c, tol=28):
    return abs(c[0] - 181) < tol and abs(c[1] - 196) < tol and abs(c[2] - 138) < tol


def measure_fig104(doc):
    page = doc[5]
    pix = page.get_pixmap(matrix=fitz.Matrix(ZOOM, ZOOM))
    buf, W, N = bytes(pix.samples), pix.width, pix.n  # cache: .samples rebuilds per access
    lab = {}
    for s in spans(page):
        t = s["text"].strip()
        if t in ("10", "20", "30", "40", "50") and s["bbox"][1] > 360:
            lab[t] = (s["bbox"][0] + s["bbox"][2]) / 2
        if t in ("A (8)", "B (2)", "C (3)", "D (6)", "E (1)", "F (3)", "G (1)"):
            lab[t] = (s["bbox"][1] + s["bbox"][3]) / 2
    x10, x50 = lab["10"], lab["50"]
    x0 = x10 - (x50 - x10) / 4.0
    out = {}
    for k in ("A (8)", "B (2)", "C (3)", "D (6)", "E (1)", "F (3)", "G (1)"):
        ypx = int(lab[k] * ZOOM)
        xend = None
        for xpx in range(int(x0 * ZOOM), int((x50 + 40) * ZOOM)):
            if is_bar(rgb_at(buf, W, N, xpx, ypx)):
                xend = xpx
        out[k[0]] = round((xend / ZOOM - x0) / (x50 - x0) * 50.0, 1)
    return out


def measure_fig103(doc):
    page = doc[4]
    pix = page.get_pixmap(matrix=fitz.Matrix(ZOOM, ZOOM))
    buf, W, N = bytes(pix.samples), pix.width, pix.n
    ylab, xcand = {}, {}
    for s in spans(page):
        t = s["text"].strip().replace(",", "")
        if t in ("200", "400", "600", "800", "1000", "1200") and s["bbox"][0] < 260:
            ylab[int(t)] = (s["bbox"][1] + s["bbox"][3]) / 2
        if t in ("2019", "2020", "2021", "2022", "2023", "2024"):
            xcand.setdefault(t, []).append(((s["bbox"][0] + s["bbox"][2]) / 2,
                                            (s["bbox"][1] + s["bbox"][3]) / 2))
    row_y = sorted(y for t in ("2020", "2021", "2022", "2023") for _, y in xcand[t])
    row_y = row_y[len(row_y) // 2]
    years = {t: next(x for x, y in v if abs(y - row_y) < 5)
             for t, v in xcand.items() if any(abs(y - row_y) < 5 for _, y in v)}
    y200, y1200 = ylab[200], ylab[1200]
    per_eur = (y200 - y1200) / 1000.0
    out = {}
    for yr, xc in years.items():
        xpx = int(xc * ZOOM)
        for ypx in range(int(y1200 * ZOOM) - 200, int((y200 + 60) * ZOOM)):
            if is_bar(rgb_at(buf, W, N, xpx, ypx)):
                out[yr] = round(200 + (y200 - ypx / ZOOM) / per_eur)
                break
    return out


def decode_annex(doc):
    """Return 20x9 grid rows; verify legend colour mapping first."""
    # legend check (page 28): glyphs on the legend row, ordered by x, must be
    # green, amber, red to match Complete / Partially complete / Not complete
    # legend glyphs sit immediately left of the legend texts (x < 330); the
    # table's last data row shares the same y-band, so filter on x as well
    p28 = doc[27]
    legend_y = next(s["bbox"][1] for s in spans(p28) if s["text"].strip() == "Complete")
    def _is_legend(s):
        return (s["font"] == "Webdings" and abs(s["bbox"][1] - legend_y) < 6
                and s["bbox"][0] < 330)
    legend = sorted(((s["bbox"][0], s["color"]) for s in spans(p28) if _is_legend(s)),
                    key=lambda t: t[0])
    assert [c for _, c in legend] == [0x7D9149, 0xFFC000, 0xFF0000], \
        f"legend mapping unexpected: {[hex(c) for _, c in legend]}"

    grid = {}  # (prop_idx, col_idx) -> status
    prop_offset = 0
    for pno in (26, 27):
        page = doc[pno]
        ticks = [( (s["bbox"][0]+s["bbox"][2])/2, (s["bbox"][1]+s["bbox"][3])/2, s["color"])
                 for s in spans(page)
                 if any(ord(ch) == 0xF06E for ch in s["text"])
                 if s["font"] == "Webdings" and "" in s["text"]
                 and not (pno == 27 and _is_legend(s))]
        # row clusters by y
        ys = sorted({round(y, 1) for _, y, _ in ticks})
        rows = []
        for y in ys:
            if rows and y - rows[-1][-1] < 8:
                rows[-1].append(y)
            else:
                rows.append([y])
        row_centers = [sum(r) / len(r) for r in rows]
        # column clusters by x (across the whole page so sparse columns align)
        xs = sorted({round(x, 1) for x, _, _ in ticks})
        cols = []
        for x in xs:
            if cols and x - cols[-1][-1] < 20:
                cols[-1].append(x)
            else:
                cols.append([x])
        col_centers = [sum(c) / len(c) for c in cols]
        assert len(col_centers) <= 9, f"p{pno+1}: {len(col_centers)} tick columns"
        # rightmost cluster = insurance (col 9); anchor columns right-to-left.
        # The site-visit column (col 1) carries NO Webdings glyphs — it is
        # marked by other means — so 8 clusters map to columns 2..9.
        L = len(col_centers)
        col_map = {i: 9 - L + 1 + i for i in range(L)}
        for x, y, colr in ticks:
            ri = min(range(len(row_centers)), key=lambda i: abs(row_centers[i] - y))
            ci = min(range(len(col_centers)), key=lambda i: abs(col_centers[i] - x))
            grid[(prop_offset + ri + 1, col_map[ci])] = COLOURS[colr]
        prop_offset += len(row_centers)
    return grid, prop_offset


def main() -> None:
    doc = fitz.open(PDF)
    sha = hashlib.sha256(Path(PDF).read_bytes()).hexdigest()
    prov = {
        "report": "RoAPS 2024 Chapter 10", "source_url": SRC_URL,
        "source_document_hash": sha, "derived_at": now_iso(),
        "confidence": "medium", "privacy_tier": "public_aggregates_and_bodies",
        "value_safe_to_sum": False,
    }
    rows = []

    f104 = measure_fig104(doc)
    nprops = {"A": 8, "B": 2, "C": 3, "D": 6, "E": 1, "F": 3, "G": 1}
    for s, v in f104.items():
        rows.append({**prov, "recovery": "fig_10_4_supplier_payments",
                     "item": f"supplier {s} ({nprops[s]} properties)",
                     "value_numeric": v * 1_000_000, "unit": "eur", "period": "2024",
                     "status": None, "extraction_method": "raster_bar_measurement_axis_calibrated",
                     "notes": f"measured EUR {v}m; sample sum {round(sum(f104.values()),1)}m vs stated 'almost EUR 230m'; each stated > EUR 20m"})
    f103 = measure_fig103(doc)
    for yr in sorted(f103):
        known = " (known exact 1,066 - measurement validation, use 1,066)" if yr == "2024" else ""
        rows.append({**prov, "recovery": "fig_10_3_annual_expenditure",
                     "item": f"IP accommodation & related services expenditure {yr}",
                     "value_numeric": f103[yr] * 1_000_000, "unit": "eur", "period": yr,
                     "status": None, "extraction_method": "raster_bar_measurement_axis_calibrated",
                     "notes": f"measured EUR {f103[yr]}m{known}; 2019 accounted in Vote 24 Justice, 2020+ Vote 40"})

    grid, nrows = decode_annex(doc)
    assert nrows == 20, f"expected 20 annex rows, got {nrows}"
    # lock the column mapping to the report's own stated aggregates (Fig 10.6 etc.)
    def _greens(col):
        return sum(1 for p in range(1, 21) if grid.get((p, col)) == "complete")
    checks = {2: 7, 3: 19, 4: 1, 8: 4, 9: 8}  # proposal/cro/ownership/planning/insurance
    for col, expect in checks.items():
        assert _greens(col) == expect, \
            f"column {col} ({TICK_COLUMNS[col-1]}): {_greens(col)} complete, report says {expect}"
    # signed_contract decodes 9 complete vs the text's '10 available' — 1-off,
    # likely the Department-run centre; recorded as a caveat, not adjusted.
    for p in range(1, 21):
        typ, loc = PROPERTIES[p - 1]
        for ci, col in enumerate(TICK_COLUMNS, 1):
            if ci == 1:
                status, note = "not_decoded", ("site-visit column is not encoded as Webdings glyphs; "
                                               "per-property visit status unknown (report text: 13 of 20 visited)")
            else:
                status = grid.get((p, ci), "blank")
                note = ("blank = no tick printed (N/A, e.g. inspection for the Department-run centre); "
                        "legend green=complete amber=partially red=not_complete; column mapping validated "
                        "against Fig 10.6 aggregates (proposal 7, CRO 19, ownership 1, planning 4, insurance 8); "
                        "signed_contract decodes 9 complete vs text's 10 available (1-off caveat)")
            rows.append({**prov, "recovery": "annex_10a_compliance_grid",
                         "item": f"property {p} ({typ}, {loc}) - {col}",
                         "value_numeric": None, "unit": "status", "period": "2024",
                         "status": status, "extraction_method": "webdings_glyph_colour_decode",
                         "notes": note})

    df = pl.DataFrame(rows, schema_overrides={"value_numeric": pl.Float64},
                      infer_schema_length=None)
    out = SILVER / "cag_ipas_chart_recovery.parquet"
    df.write_parquet(out, compression="zstd", statistics=True)
    print(f"wrote {out} - {df.height} rows")
    print(df.group_by("recovery").len())
    g = df.filter(pl.col("recovery") == "annex_10a_compliance_grid")
    print(g.group_by("status").len().sort("len", descending=True))
    # compact grid print for eyeball verification
    print("\ngrid (rows=properties 1-20; S=site P=proposal C=cro O=own K=contract F=fire I=insp L=plan N=ins):")
    sym = {"complete": "G", "partially_complete": "A", "not_complete": "R", "blank": "."}
    for p in range(1, 21):
        line = "".join(sym[grid.get((p, c), "blank")] for c in range(1, 10))
        print(f"  {p:2d} {line}")


if __name__ == "__main__":
    main()
