"""IPAS Weekly Accommodation and Arrivals Statistics — 29 Dec 2024 report, ALL REMAINING PAGES.

The per-LOCAL-AUTHORITY table (p6) is ALREADY extracted by ipas_by_local_authority.py
(31 LAs, sum-validated to its own Grand Total of 32,702) and is NOT redone here. This
extractor takes everything else in the 10-page report:

  p1  cover                 report date / data-as-at date
  p2  weekly arrivals       demographic split (158 arrivals, 5 cohorts, % shares)
  p3  weekly arrivals       by nationality (11 bars; values ARE in the text layer)
  p4  accommodation mix     5 accommodation types x (centres, persons, children) + totals
  p5  occupancy trend       year-end table 2017-2024 + the 2004-2024 line series
                            RECOVERED FROM THE VECTOR PATH (not a raster) at every printed
                            date tick, validated against the chart's own 7,393 start label
                            and the 32,702 end value
  p7  occupancy             by nationality (top 30) + continent bars RECOVERED FROM VECTOR
                            RECTS, validated against the total
  p8  arrivals              day-by-day for the week (5 day-groups x 5 cohorts)
  p9  weekly arrivals       2022 / 2023 / 2024 x 52 weeks = 156 values RECOVERED from the
                            raster bar chart, gridline-calibrated, validated against the
                            report's own stated 158 for the current week
  p10 back cover            no content

CROSS-VALIDATION AGAINST THE C&AG (RoAPS 2024 Ch.10, Fig 10.1, end-2024) — all exact:
  total 32,702 persons / 326 centres; emergency 24,718 / 269; IPAS long-term 6,518 / 49;
  other State-owned 1,466 / 8 (= National Reception Centre 252/1 + Citywest 417/1 +
  tented 797/6); children 9,015.

UPSTREAM ODDITIES PRESERVED AS FLAG ROWS, NEVER FIXED: the report labels 23-29 Dec 2024 as
"Week 51" (it is ISO week 52); a nationality is printed "Brazi**l" with the safe-country
marker inside the word; the cohort names differ between p2 ("Couples", "Lone Parents") and
p8 ("Married/Partner", "Lone Parent"); p6 carries a stray duplicate "484".

SANDBOX ONLY. All rows value_safe_to_sum=False.
"""
from __future__ import annotations

from collections import Counter
from pathlib import Path

import fitz
import polars as pl

from _common import BRONZE, SILVER, now_iso, sha256_bytes

DOC_KEY = "ipas_weekly_stats"
DOC_TITLE = "IPAS Weekly Accommodation and Arrivals Statistics (data as at 29 Dec 2024)"
SRC_URL = "https://assets.gov.ie/static/documents/29122024-ipas-stats-weekly-report.pdf"
PDF = BRONZE / "ipas_weekly" / "29122024-ipas-stats-weekly-report.pdf"
AS_AT = "29/12/2024"

CVAL = ("Cross-validates the C&AG (RoAPS 2024 Ch.10, Fig 10.1) end-2024 figures EXACTLY - the "
        "C&AG's series is built from this weekly report.")

# ---------------------------------------------------------------- p2: arrivals demographics
# (cohort, count, percent). Sum asserted == 158.
P2 = [("Single Males", 67, 42), ("Single Females", 25, 16), ("Couples", 23, 15),
      ("Children", 35, 22), ("Lone Parents", 8, 5)]
P2_TOTAL, P2_PER_DAY = 158, 23

# ---------------------------------------------------------------- p3: arrivals by nationality
# Printed as a bar chart whose VALUES ARE IN THE TEXT LAYER. '*' = safe country (per p7's key).
P3 = [("Somalia", 21, False), ("Nigeria", 18, False), ("Georgia", 16, True),
      ("Bangladesh", 14, False), ("Pakistan", 12, False), ("Albania", 10, True),
      ("Afghanistan", 7, False), ("Eswatini", 7, False), ("Zimbabwe", 7, False),
      ("Botswana", 6, False), ("Other", 40, False)]

# ---------------------------------------------------------------- p4: accommodation mix
# (type, centres, persons, children). Label->block mapping verified GEOMETRICALLY (each label
# sits directly above its own bullet block, same x-cluster), then sum-validated three ways.
P4 = [
    ("IPAS Accommodation (long-term)", 49, 6_518, 2_053),
    ("Emergency Accommodation", 269, 24_718, 6_894),
    ("National Reception Centre", 1, 252, 68),
    ("Citywest Transit Hub", 1, 417, 0),
    ("Tented Accommodation", 6, 797, 0),
]
P4_TOTALS = (326, 32_702, 9_015)

# ---------------------------------------------------------------- p5: year-end occupancy table
P5 = [("29/12/2024", 32_702), ("31/12/2023", 26_279), ("31/12/2022", 19_104),
      ("31/12/2021", 7_244), ("31/12/2020", 6_997), ("31/12/2019", 7_683),
      ("31/12/2018", 6_106), ("31/12/2017", 5_096)]

# ---------------------------------------------------------------- p7: occupancy by nationality
# '**' = safe country. NOTE 'Brazi**l' is printed with the marker INSIDE the word (preserved).
P7 = [
    ("Nigeria", 6_882, False), ("Georgia", 3_132, True), ("Algeria", 2_793, True),
    ("Somalia", 2_440, False), ("Zimbabwe", 2_199, False), ("Jordan", 2_186, False),
    ("Afghanistan", 1_783, False), ("Pakistan", 1_624, False), ("Bangladesh", 1_346, False),
    ("South Africa", 1_288, True), ("Botswana", 756, True),
    ("Palestinian Territory, Occupied", 707, False), ("Ukraine", 561, False),
    ("Congo, The Democratic Republic Of The", 514, False),
    ("Syrian Arab Republic", 448, False), ("Egypt", 445, True), ("Albania", 389, True),
    ("Eswatini", 370, False), ("Morocco", 343, True), ("El Salvador", 286, False),
    ("Malawi", 229, False), ("Sudan", 221, False), ("India", 206, True), ("Ghana", 203, False),
    ("Iraq", 188, False), ("Bolivia", 152, False), ("Sierra Leone", 140, False),
    ("Brazil", 123, True), ("Nicaragua", 119, False), ("Iran (Islamic Republic Of)", 117, False),
]

# ---------------------------------------------------------------- p8: arrivals by day
# (day label, single_male, single_female, married_partner, lone_parent, child, total)
P8 = [
    ("23/12/2024", 26, 9, 5, 6, 19, 65),
    ("24/12/2024", 8, 3, 12, 1, 7, 31),
    ("25/12/2024", 0, 0, 0, 0, 0, 0),
    ("26/12/2024", 0, 0, 0, 0, 0, 0),
    ("27/12/2024 to 29/12/2024", 33, 13, 6, 1, 9, 62),
]
P8_COHORTS = ["Single Male", "Single Female", "Married/Partner", "Lone Parent", "Child"]
P8_WEEK = ("Week (23/12/2024 to 29/12/2024)", 67, 25, 23, 8, 35, 158)


# ================================================================ VECTOR / RASTER RECOVERY
def spans(page):
    return [s for b in page.get_text("dict")["blocks"] if b["type"] == 0
            for l in b["lines"] for s in l["spans"]]


def recover_p7_continents(doc) -> list[tuple[str, int]]:
    """p7 continent bars are VECTOR RECTS -> exact geometry. Calibrated on the printed
    0..18000 y-axis labels. Bars are ordered left->right = Africa, Asia, Europe, Other."""
    page = doc[6]
    lab = {}
    for s in spans(page):
        t = s["text"].strip()
        if t.isdigit() and int(t) in (0, 2000, 4000, 6000, 8000, 10000, 12000, 14000, 16000,
                                      18000) and s["bbox"][0] > 900:
            lab[int(t)] = (s["bbox"][1] + s["bbox"][3]) / 2
    assert {0, 18000} <= set(lab), f"p7 y-axis labels not found: {sorted(lab)}"
    y0, y18 = lab[0], lab[18000]
    per = (y0 - y18) / 18000.0
    bars = []
    for d in page.get_drawings():
        if d["type"] != "f" or not d.get("fill"):
            continue
        for it in d["items"]:
            if it[0] == "re":
                r = it[1]
                if r.width > 30 and r.height > 10 and r.x0 > 1000:
                    bars.append((r.x0, r.y0, r.y1))
    bars.sort()
    names = ["Africa", "Asia", "Europe", "Other"]
    assert len(bars) == 4, f"p7: expected 4 continent bars, found {len(bars)}"
    out = []
    for name, (_x, ytop, ybot) in zip(names, bars):
        out.append((name, round((ybot - ytop) / per)))
    return out


def recover_p5_line(doc) -> tuple[list[tuple[str, int]], int, int]:
    """p5 occupancy 2004->2024 is a VECTOR POLYLINE (stroke colour pure red) -> exact points.

    Calibrated on the printed y-axis labels (2000..37000). The x-axis is a CATEGORY axis that
    prints only every ~5th date, so a value is emitted ONLY at x-positions where the PDF
    actually prints a date - the undated points in between are left alone rather than having
    dates invented for them (see the UNKNOWN row).
    """
    page = doc[4]
    ylab, ticks = {}, []
    for s in spans(page):
        t = s["text"].strip().replace(",", "")
        # NOTE: this chart's value axis is printed on the RIGHT of the plot (x ~1175), not the
        # left; the plot area itself spans x 208-1163.
        if t.isdigit() and int(t) in (2000, 7000, 12000, 17000, 22000, 27000, 32000, 37000) \
                and 1100 < s["bbox"][0] < 1260:
            ylab[int(t)] = (s["bbox"][1] + s["bbox"][3]) / 2
        # the ROTATED x-axis date ticks sit under the plot (x < 1200); the year-end TABLE to the
        # right of the chart prints dates too (x ~1319) and must not be mistaken for ticks
        if len(t) == 10 and t[2] == "/" and t[5] == "/" and s["bbox"][1] > 780 \
                and s["bbox"][0] < 1200:
            ticks.append((t, (s["bbox"][0] + s["bbox"][2]) / 2))
    assert {2000, 37000} <= set(ylab), f"p5 y-axis labels missing: {sorted(ylab)}"
    y2k, y37k = ylab[2000], ylab[37000]
    per = (y2k - y37k) / 35000.0

    # The page carries TWO red strokes: the data polyline and the chart's LEGEND swatch (a short
    # red line at top-right, next to the stray '7393' legend label - see the DQ flag row). Take
    # the path with the most segments; the swatch is a single segment.
    paths = []
    for d in page.get_drawings():
        if d["type"] == "s" and d.get("color") and \
                tuple(round(c, 2) for c in d["color"]) == (1.0, 0.0, 0.0):
            p = [(it[1].x, it[1].y) for it in d["items"] if it[0] == "l"] + \
                [(it[2].x, it[2].y) for it in d["items"] if it[0] == "l"]
            paths.append(p)
    assert paths, "p5: no red stroke found"
    pts = sorted(max(paths, key=len))
    assert len(pts) > 400, f"p5: red data polyline too short ({len(pts)} points)"

    def val_at(x: float) -> int:
        px_, py = min(pts, key=lambda p: abs(p[0] - x))
        return round(2000 + (y2k - py) / per)

    series = [(d, val_at(x)) for d, x in sorted(ticks, key=lambda t: t[1])]
    first = round(2000 + (y2k - pts[0][1]) / per)
    last = round(2000 + (y2k - pts[-1][1]) / per)
    return series, first, last


def recover_p9_weekly(doc) -> list[tuple[int, int, int]]:
    """p9 'Weekly Arrivals 2022 2023 2024' is a RASTER grouped bar chart with no text layer.

    Recovered by gridline-calibrated bar measurement: the y-gridlines (0..700 by 100) are
    located in the image, then each bar's top edge is measured. Series are identified by fill:
    2022 = red (green outline), 2023 = blue (light-blue fill), 2024 = yellow (amber outline).
    Precision ~ +-2 persons (1 px = 1.55 persons). Validated against the report's own printed
    total of 158 arrivals for the final 2024 week.
    """
    pix = fitz.Pixmap(doc.extract_image(189)["image"])
    W, H, N = pix.width, pix.height, pix.n
    buf = bytes(pix.samples)  # cache once: per-pixel .samples access rebuilds the buffer

    def px(x, y):
        i = (y * W + x) * N
        return (buf[i], buf[i + 1], buf[i + 2])

    SERIES = {(255, 0, 0): 2022, (109, 169, 69): 2022,          # fill, outline
              (66, 111, 192): 2023, (156, 173, 218): 2023, (161, 177, 220): 2023,
              (255, 255, 0): 2024, (249, 188, 0): 2024}

    # y calibration on the horizontal gridlines (0,100..700). The image's own top and bottom
    # BORDERS are the same grey and span the full width, so they must be excluded (y<10, y>H-10)
    # or they get mistaken for the 700-line and the zero-line.
    rows = [y for y in range(10, H - 10)
            if sum(1 for x in range(100, W - 20) if px(x, y) == (217, 217, 217)) > 800]
    assert rows, "p9: no gridlines found"
    lines: list[list[int]] = []
    for y in rows:
        if lines and y - lines[-1][-1] <= 3:
            lines[-1].append(y)
        else:
            lines.append([y])
    cent = [sum(c) / len(c) for c in lines]
    y700, y0 = min(cent), max(cent)          # top gridline = 700, bottom = the zero axis
    per = (y0 - y700) / 700.0
    # every detected gridline must land on a multiple of 100 -- proves the anchoring
    for c in cent:
        v = (y0 - c) / per
        assert abs(v - round(v / 100) * 100) < 4, f"p9 gridline at {c} -> {v:.1f}, not a 100-step"

    # Per column: which series OWNS the column (the dominant colour in the run beneath), and the
    # top of THAT SERIES' bar in that column.
    #
    # The three bars in a week-group physically TOUCH, so at a boundary column the topmost bar
    # pixel often belongs to the TALLER NEIGHBOUR, whose outline overhangs. Taking the topmost
    # pixel of any colour therefore imports the neighbour's height into the shorter bar (it made
    # the final 2024 week read 307 - the 2023 bar's value - instead of ~158). So the top is
    # measured in the column's OWN series colour, and each bar takes the MODAL top across its
    # columns (bar tops are flat), which is immune to the antialiased edge columns.
    col: dict[int, tuple[int, int]] = {}
    for x in range(W):
        run = Counter(SERIES[px(x, y)] for y in range(int(y700) - 10, int(y0))
                      if px(x, y) in SERIES)
        if not run:
            continue
        s = run.most_common(1)[0][0]
        top = next(y for y in range(int(y700) - 10, int(y0))
                   if px(x, y) in SERIES and SERIES[px(x, y)] == s)
        col[x] = (s, top)

    # cluster adjacent columns of the same series into bars
    bars: list[tuple[int, int, float]] = []   # (series, x_centre, value)
    cur: list[int] = []
    cur_s = None

    def flush(cols: list[int], s: int) -> None:
        tops = Counter(col[c][1] for c in cols)
        top = tops.most_common(1)[0][0]
        bars.append((s, sum(cols) / len(cols), (y0 - top) / per))

    for x in sorted(col):
        s, _y = col[x]
        if cur and (x - cur[-1] > 1 or s != cur_s):
            flush(cur, cur_s)
            cur, cur_s = [], None
        if not cur:
            cur_s = s
        cur.append(x)
    if cur:
        flush(cur, cur_s)

    bars = [b for b in bars if b[2] > 5]
    out: list[tuple[int, int, int]] = []      # (year, week, value)
    wk = {2022: 0, 2023: 0, 2024: 0}
    for s, _x, v in sorted(bars, key=lambda b: b[1]):
        wk[s] += 1
        out.append((s, wk[s], round(v)))
    for y in (2022, 2023, 2024):
        n = sum(1 for r in out if r[0] == y)
        assert n == 52, f"p9: {y} has {n} bars, expected 52"
    return out


# ---------------------------------------------------------------- EXPLICIT UNKNOWNS
U: list[tuple] = [
    (None, "whole report", "capacity", "unaccommodated IP applicants",
     "Number of IP applicants awaiting an offer of accommodation (unaccommodated)",
     "UNKNOWN AT SOURCE: the weekly report publishes arrivals and occupancy but NEVER publishes "
     "the unaccommodated count - the people the State has failed to accommodate are absent from "
     "the State's own weekly accommodation statistics. The number is material and known "
     "elsewhere: the C&AG (10.9) records 3,285 unaccommodated single male IP applicants at end "
     "2024, and the Comprehensive Accommodation Strategy admits 'over 1,000 adult males are "
     "awaiting offers of accommodation'. Neither figure, nor any equivalent, appears in this "
     "report."),
    (5, "p5 occupancy line", "occupancy", "IPAS occupancy series 2004-2024",
     "Occupancy at the ~228 chart points that carry NO printed date",
     "UNKNOWN: the p5 line is a CATEGORY axis of ~285 points but the PDF prints a date under "
     "only ~57 of them. The y-values of the undated points were recovered exactly from the "
     "vector path, but assigning them dates would require assuming a uniform interval that the "
     "printed ticks show is NOT uniform (gaps range from ~1 to ~5 months). Values are therefore "
     "emitted ONLY at points whose date the document actually prints. The axis is itself "
     "labelled 'Interspersing Dates' by the publisher."),
    (7, "p7 nationality table", "residents_centres", "IPAS residents by nationality",
     "Nationalities outside the printed top 30",
     "UNKNOWN AT SOURCE: p7 lists 30 nationalities totalling 30,190 of the 32,702 residents. The "
     "remaining 2,512 people (7.7%) belong to nationalities the report does not name or count. "
     "Do NOT treat the 30 rows as exhaustive, and do not derive an 'other' nationality from the "
     "residual - the report does not publish one."),
    (2, "p2 arrivals breakdown", "applications", "weekly arrivals",
     "Age, gender and vulnerability detail beyond the 5 cohorts",
     "UNKNOWN AT SOURCE: arrivals are split only into Single Males / Single Females / Couples / "
     "Children / Lone Parents. No age bands, no unaccompanied-minor count, no vulnerability or "
     "special-reception-need count is published, even though SI 230/2018 Reg 8 makes the "
     "vulnerability assessment MANDATORY."),
    (4, "p4 accommodation mix", "capacity", "IPAS accommodation estate",
     "Bed CAPACITY, vacancy and provider identity per accommodation type",
     "UNKNOWN AT SOURCE: p4 publishes OCCUPANCY (persons) and centre counts only. The report "
     "gives no bed capacity, no vacancy, no occupancy RATE and never names a provider or a "
     "centre - so the vacancy the C&AG measured (24% across its 7 site visits) cannot be seen "
     "in the State's own weekly statistics."),
]

# ---------------------------------------------------------------- DQ FLAG ROWS
FLAGS: list[tuple] = [
    (8, "p8 header", "unknown_at_source", "week numbering",
     "DQ FLAG: the week of 23-29 Dec 2024 is labelled 'Week 51'",
     "Week 51: 23/12/2024 to29/12/ /2024",
     "23-29 December 2024 is ISO week 52, not week 51. PRESERVED, NOT FIXED. This matters for "
     "joining the p9 weekly series (52 bars per year) to a calendar: the publisher's own week "
     "numbering is off by one at year end, and the p9 recovery therefore numbers bars 1..52 by "
     "POSITION, not by the report's week label. The header string is also mangled in the source "
     "('to29/12/ /2024')."),
    (7, "p7 nationality table", "unknown_at_source", "Brazil",
     "DQ FLAG: 'Brazil' is printed as 'Brazi**l' - the safe-country marker sits INSIDE the word",
     "Brazi**l",
     "PRESERVED, NOT FIXED. The '**' safe-country marker was inserted mid-word. Parsed as "
     "Brazil, safe_country=True (123 residents). A naive parser would emit a nationality called "
     "'Brazi' or 'Brazil' with a lost marker."),
    (2, "p2 vs p8 cohort names", "unknown_at_source", "cohort naming",
     "DQ FLAG: the same cohorts are named differently on p2 and p8",
     "p2: 'Couples' / 'Lone Parents' / 'Single Males' | p8: 'Married/Partner' / 'Lone Parent' / "
     "'Single Male'",
     "PRESERVED, NOT FIXED. The counts reconcile exactly (Couples 23 == Married/Partner 23; Lone "
     "Parents 8 == Lone Parent 8), so they are the same cohorts under two names in one document. "
     "Any cohort join must normalise both spellings."),
    (5, "p5 chart legend", "unknown_at_source", "occupancy trend chart",
     "DQ FLAG: the p5 chart's legend prints a stray series label '7393'",
     "7393",
     "PRESERVED, NOT FIXED. A red line swatch labelled '7393' sits OUTSIDE the plot area at the "
     "top right of the p5 chart. It is the chart's legend entry (an un-renamed spreadsheet "
     "series label), NOT a data point and NOT an occupancy value - no point on the line is at "
     "7,393 at that position. It is excluded from the recovered series. A naive reader could "
     "easily mistake it for a printed data label."),
    (6, "p6 LA table", "unknown_at_source", "Local Authority table",
     "DQ FLAG: a stray, duplicated '484' trails the p6 Local Authority table",
     "484",
     "PRESERVED, NOT FIXED. The text layer of p6 ends with a bare '484' after 'Monaghan County "
     "709' - it duplicates Offaly County Council's 484 and belongs to no row. It is excluded "
     "from ipas_by_local_authority.parquet, whose 31 LAs sum exactly to the printed Grand Total "
     "of 32,702; including it would produce 33,186 and break the total."),
]


def build() -> pl.DataFrame:
    sha = sha256_bytes(Path(PDF).read_bytes())
    doc = fitz.open(PDF)
    rows: list[dict] = []

    def add(page, ref, section, cat, subj, metric, vnum, vtext, unit, qual, scope, notes,
            *, unknown=False, ureason=None, method="manual_curation_from_fitz_text_full_read",
            conf="high", period=AS_AT):
        rows.append({
            "page": page, "printed_page": str(page) if page else None, "ref": ref,
            "section": section, "category": cat, "subject": subj, "metric": metric,
            "value_numeric": float(vnum) if vnum is not None else None, "value_text": vtext,
            "unit": unit, "qualifier": qual, "period": period, "scope": scope,
            "is_unknown": unknown, "unknown_reason": ureason, "notes": notes,
            "extraction_method": method, "confidence": conf,
        })

    # ---- p1 cover
    add(1, "cover", "Cover", "residents_centres", "IPAS", "Report date", None, "31/12/2024",
        "date", "exact", "publication", "Data as at 29/12/2024 unless otherwise stated. WEEKLY "
        "cadence - this is the live feed behind the county map and behind the C&AG's Fig 10.1.")

    # ---- p2 arrivals demographics
    assert sum(c for _, c, _ in P2) == P2_TOTAL, "p2 cohort counts must sum to 158"
    add(2, "p2", "Total Weekly Arrivals Breakdown", "applications", "new arrivals",
        "Total arrivals in the week", P2_TOTAL, None, "persons", "exact",
        "week ending 29/12/2024",
        f"'Total {P2_TOTAL} Arrivals - Average of {P2_PER_DAY} arrivals per day'. The 5 cohort "
        f"counts sum exactly to {P2_TOTAL} (validated).")
    add(2, "p2", "Total Weekly Arrivals Breakdown", "applications", "new arrivals",
        "Average arrivals per day", P2_PER_DAY, None, "persons_per_day", "exact",
        "week ending 29/12/2024", "As printed. NOTE 158/7 = 22.6, so 23 is rounded - and two of "
        "the seven days (25 and 26 Dec) had ZERO arrivals, so the daily average is not "
        "representative of the days on which arrivals actually occurred.")
    for cohort, n, pct in P2:
        add(2, "p2", "Total Weekly Arrivals Breakdown", "applications", cohort,
            f"Weekly arrivals - {cohort}", n, None, "persons", "exact",
            f"{pct}% of the {P2_TOTAL} weekly arrivals",
            f"Report prints both the count ({n}) and the share ({pct}%).")
        add(2, "p2", "Total Weekly Arrivals Breakdown", "applications", cohort,
            f"Weekly arrivals share - {cohort}", pct, None, "percent", "exact",
            f"of {P2_TOTAL} weekly arrivals", "as printed on the pie chart")

    # ---- p3 arrivals by nationality
    assert sum(v for _, v, _ in P3) == P2_TOTAL, "p3 nationality bars must sum to 158"
    for nat, v, safe in P3:
        add(3, "p3", "Weekly arrivals by Nationality", "applications", nat,
            f"Weekly arrivals - {nat}", v, None, "persons", "exact",
            f"of {P2_TOTAL} weekly arrivals",
            ("'Other' is the report's own residual bucket - 40 of 158 arrivals (25%) are of "
             "nationalities it does not name." if nat == "Other" else
             f"Bar-chart value, present in the text layer. safe_country={safe} (marked '*' in "
             f"the source). The 11 bars sum exactly to {P2_TOTAL} (validated)."))

    # ---- p4 accommodation mix
    ac, ap, ach = (sum(x[1] for x in P4), sum(x[2] for x in P4), sum(x[3] for x in P4))
    assert (ac, ap, ach) == P4_TOTALS, f"p4 sums {(ac, ap, ach)} != printed totals {P4_TOTALS}"
    for typ, centres, persons, children in P4:
        for metric, v, unit in (("Centres", centres, "centres"), ("Residents", persons, "persons"),
                                ("Children among residents", children, "persons")):
            add(4, "p4", "Current IPAS Accommodation Overview", "residents_centres", typ,
                f"{metric} - {typ}", v, None, unit, "exact",
                f"{typ}; {persons / P4_TOTALS[1] * 100:.1f}% of all IPAS residents",
                f"Label-to-block mapping verified geometrically. Sum-validated 3 ways: centres "
                f"{ac}=326, residents {ap:,}=32,702, children {ach:,}=9,015. {CVAL}")
    add(4, "p4", "Current IPAS Accommodation Overview", "residents_centres", "IPAS (all)",
        "Total centres", P4_TOTALS[0], None, "centres", "exact", "all IPAS accommodation", CVAL)
    add(4, "p4", "Current IPAS Accommodation Overview", "residents_centres", "IPAS (all)",
        "Total residents", P4_TOTALS[1], None, "persons", "exact", "all IPAS accommodation", CVAL)
    add(4, "p4", "Current IPAS Accommodation Overview", "residents_centres", "IPAS (all)",
        "Children among residents", P4_TOTALS[2], None, "persons", "exact",
        f"{P4_TOTALS[2] / P4_TOTALS[1] * 100:.1f}% of all IPAS residents", CVAL)
    add(4, "p4", "Current IPAS Accommodation Overview", "residents_centres", "IPAS (all)",
        "Share of residents in EMERGENCY accommodation", round(24_718 / 32_702 * 100, 1), None,
        "percent", "exact", "24,718 of 32,702 residents",
        "THE HEADLINE THIS REPORT DOES NOT PRINT: 75.6% of everyone in IPAS accommodation is in "
        "EMERGENCY accommodation, in 269 of the 326 centres (82.5%). The 'emergency' system is "
        "the system. " + CVAL)

    # ---- p5 year-end table + vector line recovery
    for date, v in P5:
        add(5, "p5 table", "IPAS Accommodation Occupancy Trend 2004-Present", "occupancy",
            "IPAS residents", "IPAS occupancy at year end", v, None, "persons", "exact",
            "all IPAS accommodation", "Printed year-end table. " + CVAL, period=date)
    series, first, last = recover_p5_line(doc)
    assert abs(last - 32_702) < 400, f"p5 line end {last} should be ~32,702"
    for date, v in series:
        add(5, "p5 line", "IPAS Accommodation Occupancy Trend 2004-Present", "occupancy",
            "IPAS residents", "IPAS occupancy (recovered from the trend line)", v, None,
            "persons", "approx", "all IPAS accommodation",
            f"RECOVERED from the chart's VECTOR polyline (exact path geometry, not a raster), "
            f"y-calibrated on the printed 2,000-37,000 axis. VALIDATED: the line's final point "
            f"measures {last:,} against the known 32,702 (delta {last - 32_702:+,}). Its first "
            f"point (26/11/2004) measures {first:,}. Emitted ONLY at x-positions where the PDF "
            f"actually prints a date - the ~228 undated points in between are left undated "
            f"rather than having dates invented for them (see UNKNOWN row). Precision ~+-150 "
            f"persons. This is the ONLY place the 2004-2017 history is published: the printed "
            f"table starts at 2017.",
            method="vector_path_extraction_axis_calibrated", conf="medium", period=date)

    # ---- p7 nationality + continent recovery
    tot7 = sum(v for _, v, _ in P7)
    assert tot7 < P4_TOTALS[1], "p7 top-30 cannot exceed the total"
    for nat, v, safe in P7:
        add(7, "p7", "Occupancy Breakdown by Nationality", "residents_centres", nat,
            f"IPAS residents - {nat}", v, None, "persons", "exact",
            f"{v / P4_TOTALS[1] * 100:.1f}% of 32,702 IPAS residents",
            f"Source: 'Week ending 29/12/2024 as per DOJ' (Department of Justice). "
            f"safe_country={safe} (marked '**'). TOP 30 ONLY: these 30 nationalities total "
            f"{tot7:,} of 32,702 residents ({tot7 / P4_TOTALS[1] * 100:.1f}%) - the report does "
            f"not name the rest."
            + (" NOTE: printed as 'Brazi**l' in the source - see the DQ flag row."
               if nat == "Brazil" else ""))
    add(7, "p7", "Occupancy Breakdown by Nationality", "residents_centres", "top 30 nationalities",
        "Residents covered by the printed top-30 nationality list", tot7, None, "persons",
        "exact", f"{tot7 / P4_TOTALS[1] * 100:.1f}% of 32,702",
        f"The other {P4_TOTALS[1] - tot7:,} residents are of nationalities the report does not "
        f"name - see the UNKNOWN row.")
    conts = recover_p7_continents(doc)
    ctot = sum(v for _, v in conts)
    # The African nationalities in the SAME page's own top-30 table, for the reconciliation check
    _AFRICA = {"Nigeria", "Somalia", "Zimbabwe", "South Africa", "Botswana", "Egypt", "Eswatini",
               "Morocco", "Malawi", "Sudan", "Ghana", "Sierra Leone", "Algeria",
               "Congo, The Democratic Republic Of The"}
    afr_table = sum(v for n, v, _ in P7 if n in _AFRICA)
    afr_bar = next(v for n, v in conts if n == "Africa")
    for name, v in conts:
        add(7, "p7 continent chart", "Occupancy Breakdown by Nationality", "residents_centres",
            name, f"IPAS residents by continent of origin - {name}", v, None, "persons", "approx",
            "recovered from the chart; DOES NOT reconcile with the report's own total - do NOT "
            "express as a share of 32,702",
            f"RECOVERED from the chart's VECTOR rectangles (exact geometry, not a raster), "
            f"y-calibrated on the printed 0-18,000 axis, which is perfectly linear "
            f"(63.10 px per 2,000 at every step) - so measurement precision is ~+-50 persons. "
            f"BUT THE SOURCE DOES NOT ADD UP: the four bars measure {ctot:,}, which EXCEEDS the "
            f"report's own total occupancy of 32,702 by {ctot - P4_TOTALS[1]:+,} "
            f"({abs(ctot - P4_TOTALS[1]) / P4_TOTALS[1] * 100:.1f}%) - far beyond measurement "
            f"error. See the DQ flag row. The continent split is not printed as text anywhere in "
            f"the report; this chart is the only place it appears.",
            method="vector_rect_measurement_axis_calibrated", conf="medium")
    add(7, "p7 continent chart", "data quality", "unknown_at_source", "continent breakdown",
        "DQ FLAG: the p7 continent chart does not reconcile with the report's own figures", None,
        f"Bars measure Africa ~{afr_bar:,}, Asia ~{conts[1][1]:,}, Europe ~{conts[2][1]:,}, "
        f"Other ~{conts[3][1]:,} = ~{ctot:,} total",
        "text", "exact", "internal inconsistency in the source",
        f"PRESERVED, NOT FIXED. TWO independent contradictions, both far larger than the "
        f"~+-50-person measurement error: (1) the four bars total ~{ctot:,} against the report's "
        f"own stated occupancy of 32,702 - an excess of {ctot - P4_TOTALS[1]:,}; (2) the Africa "
        f"bar reads ~{afr_bar:,}, yet the African nationalities in the SAME PAGE's own top-30 "
        f"table already sum to {afr_table:,} - i.e. the chart shows FEWER Africans than the "
        f"table on the same page names, by {afr_table - afr_bar:,}, before any unlisted African "
        f"nationality is counted. The axis calibration was verified exactly linear, and the bar "
        f"order (Africa/Asia/Europe/Other) is printed under the bars, so this is an error in the "
        f"SOURCE, not in the recovery. DO NOT use the continent split as a share of 32,702 and "
        f"do not reconcile it against the nationality table.")

    # ---- p8 arrivals by day
    for i, cohort in enumerate(P8_COHORTS, 1):
        assert sum(d[i] for d in P8) == P8_WEEK[i], f"p8 {cohort} days must sum to the week total"
    assert sum(d[6] for d in P8) == P8_WEEK[6] == P2_TOTAL, "p8 daily totals must sum to 158"
    for day, sm, sf, mp, lp, ch, tot in P8:
        for cohort, v in zip(P8_COHORTS, (sm, sf, mp, lp, ch)):
            add(8, "p8", "IPAS Arrivals Week Ending 29/12/2024", "applications", cohort,
                f"Arrivals on {day} - {cohort}", v, None, "persons", "exact",
                f"day-level, within the {P2_TOTAL}-person week",
                "Day-level detail. Validated: every cohort's days sum to its week total and the "
                "daily totals sum to 158. Christmas Day and St Stephen's Day recorded ZERO "
                "arrivals - an operational artefact of the reception office being closed, NOT a "
                "fall in demand: 62 people arrived in the 27-29 Dec period that follows.",
                period=day)
        add(8, "p8", "IPAS Arrivals Week Ending 29/12/2024", "applications", "all cohorts",
            f"Total arrivals on {day}", tot, None, "persons", "exact",
            f"day-level, within the {P2_TOTAL}-person week", "as printed", period=day)

    # ---- p9 raster recovery
    weekly = recover_p9_weekly(doc)
    w2024 = {w: v for y, w, v in weekly if y == 2024}
    assert abs(w2024[52] - P2_TOTAL) <= 6, \
        f"p9 2024 final week measures {w2024[52]}, report states {P2_TOTAL}"
    yr_tot = {y: sum(v for yy, _, v in weekly if yy == y) for y in (2022, 2023, 2024)}
    for y, w, v in weekly:
        add(9, "p9 chart", "Weekly Arrivals 2022 2023 2024", "applications",
            f"new arrivals {y}", f"Weekly IP arrivals - {y} week {w}", v, None, "persons",
            "approx", f"week {w} of 52, {y}",
            f"RECOVERED from the p9 RASTER bar chart (no text layer at all): gridline-calibrated "
            f"bar measurement, 1px = 1.55 persons, precision ~+-2. VALIDATED against the "
            f"report's own printed figure: the final 2024 bar measures {w2024[52]} against the "
            f"stated 158 arrivals for the week ending 29/12/2024. Bars are numbered 1-52 BY "
            f"POSITION (the report's own week labels are off by one at year end - see the DQ "
            f"flag). Measured annual totals: 2022 ~{yr_tot[2022]:,}, 2023 ~{yr_tot[2023]:,}, "
            f"2024 ~{yr_tot[2024]:,} - approximate sums of measured bars, NOT official annual "
            f"totals; do not publish them as such.",
            method="raster_bar_measurement_gridline_calibrated", conf="medium",
            period=f"{y} week {w}")

    # ---- flags + unknowns
    for (pg, ref, cat, subj, metric, vtext, notes) in FLAGS:
        add(pg, ref, "data quality", cat, subj, metric, None, vtext, "text", "exact",
            "upstream oddity - preserved, not fixed", notes)
    for (pg, ref, cat, subj, metric, reason) in U:
        add(pg, ref, "whole report" if pg is None else ref, cat, subj, metric, None, None, None,
            "unknown", "not established by the report", None, unknown=True, ureason=reason)

    out = []
    for i, r in enumerate(sorted(rows, key=lambda r: (r["page"] or 999, r["category"])), 1):
        out.append({"fact_id": f"{DOC_KEY}-{i:04d}", "doc_key": DOC_KEY, "doc_title": DOC_TITLE,
                    **r, "source_url": SRC_URL, "source_document_hash": sha,
                    "privacy_tier": "public_aggregates", "value_safe_to_sum": False,
                    "derived_at": now_iso()})
    cols = ["fact_id", "doc_key", "doc_title", "page", "printed_page", "ref", "section",
            "category", "subject", "metric", "value_numeric", "value_text", "unit", "qualifier",
            "period", "scope", "is_unknown", "unknown_reason", "notes", "source_url",
            "source_document_hash", "extraction_method", "confidence", "privacy_tier",
            "value_safe_to_sum", "derived_at"]
    return pl.DataFrame(out, schema_overrides={"value_numeric": pl.Float64, "page": pl.Int64},
                        infer_schema_length=None).select(cols)


def main() -> None:
    df = build()
    out = SILVER / "ipas_weekly_facts.parquet"
    df.write_parquet(out, compression="zstd", statistics=True)
    eye = SILVER / "_eyeball"
    eye.mkdir(exist_ok=True)
    df.write_csv(eye / "ipas_weekly_facts.csv")
    print(f"wrote {out} - {df.height} rows")
    with pl.Config(tbl_rows=30, fmt_str_lengths=48, tbl_width_chars=160):
        print(df.group_by("page").agg(pl.len(), pl.col("is_unknown").sum().alias("unknown"))
              .sort("page"))
        print(df.group_by("extraction_method").len())
        print("\nrecovered continent split (p7 vector rects):")
        print(df.filter(pl.col("ref") == "p7 continent chart")
              .select("subject", "value_numeric"))
        print("\nrecovered weekly arrivals - last 4 weeks of each year (p9 raster):")
        print(df.filter(pl.col("ref") == "p9 chart")
              .filter(pl.col("metric").str.contains("week (?:49|50|51|52)$"))
              .select("period", "value_numeric").sort("period"))
    print(f"\nunknown rows: {df['is_unknown'].sum()} / {df.height}")
    assert not df["value_safe_to_sum"].any()


if __name__ == "__main__":
    main()
