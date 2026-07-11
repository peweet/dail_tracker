"""
member_interests_supplements.py
-------------------------------
Parses SUPPLEMENTS to the Register of Members' Interests — the Section 29
statements laid before the Houses between annual registers. These are the late
filings and corrections (a member back-filling one or more past registration
periods in a single notice), a signal the annual registers never carry: the
annual register is a full restatement, so a correction is invisible there,
while the supplement documents the correction event itself.

Format (stable across 2005–2026 supplements): each statement is preceded by a
"Notice is given herewith … registration period(s) …" paragraph naming one or
MORE periods, then "Name of Member concerned: <honorific> First Last[, Const]",
then "Category of Registrable Interest(s) concerned:" with numbered CAPS
category headings ('3.  DIRECTORSHIPS:', '2.  SHARES etc.'). One PDF can hold
many statements (the 2013-01-15 Dáil supplement has dozens; the 2022-08-26 one
contains Robert Troy's ten-year back-fill).

Output: one row per (supplement file × member statement) →
    data/silver/parquet/member_interests_supplements.parquet (+ CSV sibling)
served by v_member_interests_supplements / v_member_interests_backfill
(sql_views/member/member_interests_supplements.sql).

Deliberately a SEPARATE table from the annual combined parquets: the register
is a full annual restatement and the supplement is an incremental correction —
different grains that must never be unioned or summed together.

Run:  python -m members.member_interests_supplements
"""

from __future__ import annotations

import pathlib
import re
import shutil
import subprocess
import tempfile

import fitz  # PyMuPDF
import polars as pl

from config import SILVER_DIR
from members.member_interests import (
    HISTORIC_SEANAD_PATH,
    HISTORIC_TD_PATH,
    INTEREST_CODE_MAP,
    INTERESTS_PDF_DIR,
    MASTER_SEANAD_PATH,
    MASTER_TD_PATH,
    PDF_PATHS,
    _MASTER_SELECT,
)
from services.parquet_io import save_parquet
from shared import normalise_join_key

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PARQUET_PATH = SILVER_DIR / "parquet" / "member_interests_supplements.parquet"
CSV_PATH = SILVER_DIR / "member_interests_supplements.csv"

# ~30 supplement files exist and the two multi-statement ones alone carry 30+
# statements; a run that parses fewer than this has lost the corpus, not found
# a smaller one.
MIN_ROWS = 30

ANCHOR_RE = re.compile(r"name of (the )?member concerned", re.I)
# The 2013-03-13 variant has no 'Name of Member concerned' line — the member
# rides on the category line instead ('Category of Registrable Interest(s)
# concerned:  Minister James Reilly (Dublin North)'). Used as fallback anchor
# only when a file has zero primary anchors.
FALLBACK_ANCHOR_RE = re.compile(r"category of registrable interest", re.I)
NOTICE_RE = re.compile(r"notice is (?:hereby\s+)?given", re.I)
YEAR_RE = re.compile(r"\b(?:19|20)\d{2}\b")
# Every statement cites 'ETHICS IN PUBLIC OFFICE ACTS, 1995 AND 2001' inside the
# notice paragraph — those Act years must not read as back-filled periods.
ACT_CITATION_RE = re.compile(r"(?i)(?:acts?|poibl[ií])[,\s]*1995(?:\s*(?:and|agus)\s*2001)?")
# The periods themselves are always announced as 'registration period(s) …'
# ending at 'in accordance with' — years are read from those spans only, so a
# signing date ('This 15th day of January, 2013') or an acquisition year in a
# neighbouring statement's body can never leak in.
PERIOD_SPAN_RE = re.compile(r"(?i)registration periods?\b(.{0,400}?)(?=in accordance|$)")
# Category headings inside a statement are CAPS ('3.  DIRECTORSHIPS:',
# '2.\t SHARES etc.'; OCR rots the dot to '-', '~' or ':' — '2 - SHARES etc.',
# '4~ LAND', '7: TRAVEL FACILITIES'). Matched against line.upper() and
# vocabulary-gated on the Ethics Act category lead-words so an address
# ('4 LAND…' cannot occur — body text is mixed-case) never fires.
CATEGORY_HEAD_RE = re.compile(
    r"^\s*([1-9])\s*[.,:\-~]?\s*(?:OCCUPATION|SHARE|DIRECTORSHIP|LAND\b|GIFT|PROPERT|TRAVEL|REMUNERAT|CONTRACT)"
)
HONORIFIC_RE = re.compile(
    r"^(?:an\s+)?(?:taoiseach|t[áa]naiste|minister of state|minister|senator|deputy|"
    r"dr\.?|mr\.?|mrs\.?|ms\.?|miss|prof\.?)\s+",
    re.I,
)
PAGE_NUMBER_RE = re.compile(r"^\s*\d+\s*$")

_PLAUSIBLE_YEARS = range(1995, 2040)


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def discover_supplements(pdf_dir: pathlib.Path = INTERESTS_PDF_DIR) -> list[pathlib.Path]:
    """Every Section 29 supplement in the bronze interests dir.

    Filename rule catches the ~30 'supplement-to-register…' files; the
    first-page probe additionally catches Section 29 statements published under
    a register-like filename (e.g. the 2025-06-18 'register…2022-2024' one-pager,
    which is a supplement in all but name). Annual registers already wired into
    PDF_PATHS are excluded so the two corpora can never overlap.
    """
    annual = {p.name for p in PDF_PATHS.values()}
    out: list[pathlib.Path] = []
    for p in sorted(pdf_dir.glob("*.pdf")):
        if p.name in annual:
            continue
        if "supplement-to-register" in p.name:
            out.append(p)
            continue
        if "register-of-member" in p.name:
            try:
                first_page = fitz.open(p)[0].get_text()
            except Exception:  # noqa: BLE001 — an unreadable PDF is just not a supplement
                continue
            if "SECTION 29" in first_page.upper():
                out.append(p)
    return out


def _file_meta(path: pathlib.Path) -> tuple[str, str]:
    """(house, publication date 'YYYY-MM-DD') from the standard filename."""
    house = "Seanad" if "seanad" in path.name.lower() else "Dáil"
    return house, path.name[:10]


# ---------------------------------------------------------------------------
# Parse
# ---------------------------------------------------------------------------


def extract_lines(path: pathlib.Path) -> list[str]:
    """All non-blank lines across pages, bare page numbers stripped.
    Empty list ⇢ a scanned supplement with no text layer (caller falls back to
    OCR, then skips loudly if that too yields nothing)."""
    doc = fitz.open(path)
    lines: list[str] = []
    for page in doc:
        for line in page.get_text(option="text").splitlines():
            if line.strip() and not PAGE_NUMBER_RE.match(line):
                lines.append(line)
    return lines


def _find_tesseract() -> str | None:
    """Tesseract exe if installed (PATH first, then the default Windows dir)."""
    exe = shutil.which("tesseract")
    if exe:
        return exe
    default = pathlib.Path(r"C:\Program Files\Tesseract-OCR\tesseract.exe")
    return str(default) if default.exists() else None


def ocr_lines(path: pathlib.Path) -> list[str]:
    """OCR fallback for the ~4 supplements that are pure scans (no text layer).

    Rasterises each page at 300 dpi and runs Tesseract (--psm 4, single-column).
    Probe-validated on the 2018-07-27 supplement: header, 'Notice is given…'
    period line, 'Name of Member concerned:' and category heads all come
    through cleanly enough for parse_statements; OCR-rotted NAMES may miss the
    roster join and are kept as 'unmatched' rows rather than dropped.
    Returns [] when Tesseract is not installed or produces nothing — the
    caller's loud-skip path then applies as before.
    """
    exe = _find_tesseract()
    if exe is None:
        return []
    doc = fitz.open(path)
    lines: list[str] = []
    with tempfile.TemporaryDirectory() as td:
        tmp = pathlib.Path(td)
        for i, page in enumerate(doc):
            png = tmp / f"p{i}.png"
            page.get_pixmap(dpi=300).save(png)
            out_base = tmp / f"p{i}"
            try:
                r = subprocess.run(
                    [exe, str(png), str(out_base), "--psm", "4", "-l", "eng"],
                    capture_output=True,
                    text=True,
                    timeout=120,
                )
            except (OSError, subprocess.TimeoutExpired):
                return []
            if r.returncode != 0:
                return []
            for line in (tmp / f"p{i}.txt").read_text(encoding="utf-8").splitlines():
                if line.strip() and not PAGE_NUMBER_RE.match(line):
                    lines.append(line)
    return lines


def clean_member_name(raw: str) -> str:
    """'Minister Robert Troy, Longford -' → 'Robert Troy';
    'Eamonn Maloney (Dublin South-West)' → 'Eamonn Maloney';
    'O'Keeffe, Batt  [Cork South Central]' → 'Batt O'Keeffe' (2005 format);
    'Kevin Humphreys TD' → 'Kevin Humphreys';
    'Sean 6 Fearghail' → 'Sean Ó Fearghail' (embedded-OCR rot of 'Ó');
    'Thomas P. Broughan' → 'Thomas Broughan' (middle initials are stripped on
    BOTH sides of the join — see _master_for — because the character-sorted
    join key breaks on the extra letter)."""
    name = re.sub(r"\s*[\(\[].*$", "", raw).strip()  # trailing '(Constituency…' / '[Constituency…'
    # Old format is 'SURNAME, First'; modern is 'First Last[, Constituency]'.
    # A single token on EACH side of the comma can only be surname-first —
    # 'First Last' before the comma is two tokens, and a spelled-out
    # constituency after a full name is reachable only via the 2+-token branch.
    m = re.fullmatch(r"([^,\s]+)\s*,\s*([^,\s]+)", name)
    name = f"{m.group(2)} {m.group(1)}" if m else name.split(",")[0].strip()
    for _ in range(3):  # stacked honorifics ('Minister of State Deputy …')
        stripped = HONORIFIC_RE.sub("", name)
        if stripped == name:
            break
        name = stripped
    name = re.sub(r"[,\s]+T\.?\s*D\.?\s*$", "", name)  # trailing 'TD' / 'T.D.'
    name = re.sub(r"\b6\b", "Ó", name)  # OCR rot: standalone 'Ó' read as '6'
    name = re.sub(r"\s[A-Z]\.\s*", " ", name)  # dotted middle initial
    return name.strip(" .-–:;")


def parse_statements(lines: list[str]) -> list[dict]:
    """Walk 'Name of Member concerned' anchors and build one dict per statement.

    years: taken from the statement's own 'Notice is given …' paragraph (the
    text between the previous statement and this anchor, from the last notice
    line onward) — NOT from the statement body, whose item detail can mention
    unrelated years ('date interest was acquired: 2015').
    categories: CAPS numbered headings in the forward window to the next anchor.
    """
    anchors = [i for i, line in enumerate(lines) if ANCHOR_RE.search(line)]
    if not anchors:  # 2013-03-13 variant: the name rides on the category line
        anchors = [i for i, line in enumerate(lines) if FALLBACK_ANCHOR_RE.search(line)]
    statements: list[dict] = []
    for k, i in enumerate(anchors):
        prev_end = anchors[k - 1] + 1 if k else 0
        next_start = anchors[k + 1] if k + 1 < len(anchors) else len(lines)

        # -- name: text after the anchor's colon, else the next line
        tail = lines[i].split(":", 1)[1] if ":" in lines[i] else ""
        if not re.search(r"[A-Za-z]", tail) and i + 1 < len(lines):
            tail = lines[i + 1]
        raw_name = tail.strip()
        name = clean_member_name(raw_name)

        # -- registration-period years from the notice paragraph before the anchor
        back = lines[prev_end:i]
        notice_at = max((j for j, line in enumerate(back) if NOTICE_RE.search(line)), default=None)
        zone = back[notice_at:] if notice_at is not None else back
        zone_text = ACT_CITATION_RE.sub("", " ".join(zone))
        period_spans = PERIOD_SPAN_RE.findall(zone_text)
        year_source = " ".join(period_spans) if period_spans else zone_text
        years = sorted({int(y) for y in YEAR_RE.findall(year_source) if int(y) in _PLAUSIBLE_YEARS})

        # -- declared categories in the statement body. Two layouts: digit and
        # label on one line ('3.  DIRECTORSHIPS:'), or a bare digit line whose
        # label starts the NEXT line ('4.' / 'Land (including property)').
        cats: set[str] = set()
        body = lines[i + 1 : next_start]
        for j, line in enumerate(body):
            up = line.upper()
            m = CATEGORY_HEAD_RE.match(up)
            if not m and re.fullmatch(r"\s*([1-9])\s*[.,]?\s*", line) and j + 1 < len(body):
                m = CATEGORY_HEAD_RE.match(re.sub(r"\s*([1-9])\s*[.,]?\s*", r"\1. ", line) + body[j + 1].upper())
            if m:
                cats.add(INTEREST_CODE_MAP.get(m.group(1), m.group(1)))

        statements.append(
            {
                "member_name_raw": raw_name,
                "member_name": name,
                "years_declared": ";".join(str(y) for y in years),
                "n_years": len(years),
                "categories": ";".join(sorted(cats)),
                "n_categories": len(cats),
            }
        )
    return statements


# ---------------------------------------------------------------------------
# Roster join
# ---------------------------------------------------------------------------


def _master_for(house: str) -> pl.DataFrame:
    """Current + historic roster for a house, keyed by the normalised name
    (same union + dedup the annual parser's join_master_list performs)."""
    master_path, historic_path = (
        (MASTER_TD_PATH, HISTORIC_TD_PATH) if house == "Dáil" else (MASTER_SEANAD_PATH, HISTORIC_SEANAD_PATH)
    )
    master = pl.read_csv(master_path).select(_MASTER_SELECT)
    if historic_path.exists():
        historic = pl.read_csv(historic_path).select(_MASTER_SELECT)
        master = pl.concat([master, historic], how="vertical_relaxed")
    master = master.with_columns(
        # Mirror clean_member_name's middle-initial strip ('Thomas P.' →
        # 'Thomas') — supplements never carry the initial, and one extra letter
        # breaks the character-sorted join key.
        pl.col("first_name").str.replace(r"\s+[A-Z]\.\s*$", "").alias("first_name")
    )
    master = master.with_columns(pl.concat_str(pl.col(["first_name", "last_name"])).alias("join_key"))
    master = normalise_join_key.normalise_df_td_name(master, "join_key")
    return master.unique(subset=["join_key"], keep="first", maintain_order=True).select(
        "join_key", "unique_member_code", "full_name", "party", "constituency_name"
    )


def join_roster(df: pl.DataFrame) -> pl.DataFrame:
    """Left-join statements to the per-house roster; unmatched statements are
    KEPT (registration_status='unmatched') — a supplement by an unrosterable
    declarer is still a real filing worth surfacing by name."""
    df = df.with_columns(pl.col("member_name").alias("join_key"))
    df = normalise_join_key.normalise_df_td_name(df, "join_key")
    parts = []
    for house in ("Dáil", "Seanad"):
        sub = df.filter(pl.col("house") == house)
        if sub.is_empty():
            continue
        parts.append(sub.join(_master_for(house), on="join_key", how="left"))
    out = pl.concat(parts) if parts else df
    return out.with_columns(
        pl.when(pl.col("unique_member_code").is_null())
        .then(pl.lit("unmatched"))
        .otherwise(pl.lit("matched"))
        .alias("registration_status")
    ).drop("join_key")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def build() -> pl.DataFrame:
    rows: list[dict] = []
    skipped_no_text: list[str] = []
    no_anchor: list[str] = []

    files = discover_supplements()
    print(f"=== Supplements parser: {len(files)} Section 29 files discovered ===")
    for path in files:
        house, pub_date = _file_meta(path)
        text_source = "embedded"
        lines = extract_lines(path)
        if not lines:
            lines = ocr_lines(path)
            text_source = "ocr"
            if lines:
                print(f"  OCR fallback ({len(lines)} lines): {path.name}")
        if not lines:
            skipped_no_text.append(path.name)
            print(f"  SKIP (no text layer, OCR unavailable/failed): {path.name}")
            continue
        statements = parse_statements(lines)
        if not statements:
            no_anchor.append(path.name)
            print(f"  WARN (0 'Name of Member concerned' anchors — format drift?): {path.name}")
            continue
        for s in statements:
            rows.append(
                {
                    "source_file": path.name,
                    "house": house,
                    "supplement_date": pub_date,
                    "text_source": text_source,
                    **s,
                }
            )
        print(f"  {path.name}: {len(statements)} statement(s)")

    df = pl.DataFrame(rows)
    df = join_roster(df)

    n_matched = df.filter(pl.col("registration_status") == "matched").height
    print(
        f"\n  statements: {df.height} | roster-matched: {n_matched} ({n_matched / max(df.height, 1):.0%})"
        f" | files skipped (scanned): {len(skipped_no_text)} | files w/o anchors: {len(no_anchor)}"
    )
    top = (
        df.filter(pl.col("n_years") > 0)
        .sort("n_years", descending=True)
        .select("member_name", "n_years", "years_declared", "source_file")
        .head(5)
    )
    print("  largest single-notice back-fills (the red-flag signal):")
    print(top)

    save_parquet(df, PARQUET_PATH, min_rows=MIN_ROWS)
    df.write_csv(CSV_PATH)
    print(f"Saved {PARQUET_PATH.name} ({df.height} rows) + {CSV_PATH.name}")
    return df


def main() -> None:
    build()


if __name__ == "__main__":
    main()
