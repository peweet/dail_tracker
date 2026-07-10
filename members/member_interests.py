"""
member_interest.py
------------------
Extracts, cleans, and structures the Register of Members' Interests from a PDF file.
This script processes the messy, unstructured PDF text, groups lines into member entries and their interests,
and outputs a normalized CSV and JSON. The main challenge is that the PDF contains a lot of superfluous data
(headers, footers, disclaimers, and line breaks in awkward places), so the approach is to aggressively cut away
the fluff and only keep the core data (member names and their interests). This is much easier and more robust
than trying to extract the relevant data directly from a very messy and inconsistent structure.

Regexes are used to:
- Identify category headers (e.g., "1. ", "2. ")
- Identify member name lines (e.g., "SMITH, John")
Cleaning steps:
- Remove empty lines, headers, and footers
- Group lines by member and interest category
- Normalize and split out names and interests for further analysis
The result is a structured dataset of members and their declared interests, suitable for downstream analysis..
"""

import json
import os
import pathlib
import re

import fitz  # PyMuPDF
import orjson
import polars as pl
import regex

from config import GOLD_DIR, INTERESTS_PDF_DIR, MEMBERS_DIR, SILVER_DIR
from services.parquet_io import save_parquet
from shared import normalise_join_key

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PDF_PATHS: dict[str, pathlib.Path] = {
    # SEANAD — historic. Registration periods verified from each PDF's title page
    # (pub year - 1 = declaration year; early periods ran Feb→Jan, labelled by
    # start year). No register was published for Seanad 1996, 1999 or 2004 —
    # genuine gaps in the source, not download misses.
    "1995_seanad": INTERESTS_PDF_DIR / "1996-04-24_register-of-members-interests-seanad-eireann_en.pdf",
    "1997_seanad": INTERESTS_PDF_DIR / "1998-04-24_register-of-members-interests-seanad-eireann_en.pdf",
    "1998_seanad": INTERESTS_PDF_DIR / "1999-05-19_register-of-members-interests-seanad-eireann_en.pdf",
    "2000_seanad": INTERESTS_PDF_DIR / "2001-05-11_register-of-members-interests-seanad-eireann_en.pdf",
    "2001_seanad": INTERESTS_PDF_DIR / "2002-07-11_register-of-members-interests-seanad-eireann_en.pdf",
    "2002_seanad": INTERESTS_PDF_DIR / "2003-06-24_register-of-members-interests-seanad-eireann_en.pdf",
    "2003_seanad": INTERESTS_PDF_DIR / "2004-05-21_register-of-members-interests-seanad-eireann_en.pdf",
    "2005_seanad": INTERESTS_PDF_DIR / "2006-03-31_register-of-members-interests-seanad-eireann_en.pdf",
    "2006_seanad": INTERESTS_PDF_DIR / "2007-04-02_register-of-members-interests-seanad-eireann_en.pdf",
    "2007_seanad": INTERESTS_PDF_DIR / "2008-04-18_register-of-members-interests-seanad-eireann_en.pdf",
    "2008_seanad": INTERESTS_PDF_DIR / "2009-03-26_register-of-members-interests-seanad-eireann_en.pdf",
    "2009_seanad": INTERESTS_PDF_DIR / "2010-03-26_register-of-members-interests-seanad-eireann_en.pdf",
    "2010_seanad": INTERESTS_PDF_DIR / "2011-04-06_register-of-members-interests-seanad-eireann_en.pdf",
    "2011_seanad": INTERESTS_PDF_DIR / "2012-04-03_register-of-members-interests-seanad-eireann_en.pdf",
    "2012_seanad": INTERESTS_PDF_DIR / "2013-02-20_register-of-members-interests-seanad-eireann_en.pdf",
    "2013_seanad": INTERESTS_PDF_DIR / "2014-03-21_register-of-members-interests-seanad-eireann_en.pdf",
    "2014_seanad": INTERESTS_PDF_DIR / "2015-03-20_register-of-members-interests-seanad-eireann_en.pdf",
    "2015_seanad": INTERESTS_PDF_DIR / "2016-03-16_register-of-members-interests-seanad-eireann_en.pdf",
    "2016_seanad": INTERESTS_PDF_DIR / "2017-03-14_register-of-members-interests-seanad-eireann_en.pdf",
    "2017_seanad": INTERESTS_PDF_DIR / "2018-03-09_register-of-members-interests-seanad-eireann_en.pdf",
    "2018_seanad": INTERESTS_PDF_DIR / "2019-03-01_register-of-members-interests-seanad-eireann_en.pdf",
    "2019_seanad": INTERESTS_PDF_DIR / "2020-02-28_register-of-members-interests-seanad-eireann_en.pdf",
    # Seanad — current convention
    "2020_seanad": INTERESTS_PDF_DIR / "2021-03-16_register-of-members-interests-seanad-eireann_en.pdf",
    "2021_seanad": INTERESTS_PDF_DIR / "2022-02-25_register-of-members-interests-seanad-eireann_en.pdf",
    "2022_seanad": INTERESTS_PDF_DIR / "2023-02-24_register-of-members-interests-seanad-eireann_en.pdf",
    "2023_seanad": INTERESTS_PDF_DIR / "2024-02-27_register-of-members-interests-seanad-eireann-2023_en.pdf",
    "2024_seanad": INTERESTS_PDF_DIR / "2025-02-27_register-of-member-s-interests-seanad-eireann-2024_en.pdf",
    "2025_seanad": INTERESTS_PDF_DIR / "2026-03-10_register-of-member-s-interests-seanad-eireann-2025_en.pdf",
    # DAIL — historic (born-digital, parse clean ~96%). The 2016 register
    # (published 2017-03-10) rots the category-1 marker '1.'→'l.' but is otherwise
    # clean text — repair_ocr_category_markers() recovers it (~98% roster match).
    # The 2012 register (published 2013-02-28) is a true scanned image: 0 lines of
    # extractable text. It is OCR'd (PaddleOCR, ~0.96 conf) to a reusable line
    # artifact and sourced via OCR_LINE_SOURCES below instead of extract_raw_lines;
    # the rest of the pipeline is identical. See
    # pipeline_sandbox/historic_members/ocr_2012_register.py.
    # DAIL — deep tail (1995–2010 declaration years; registers published 1996–2011,
    # pulled 2026-07 via the paginated publications index). All born-digital or
    # embedded-OCR text; per-year quality gate decides what ingests.
    "1995_dail": INTERESTS_PDF_DIR / "1996-04-04_register-of-members-interests-dail-eireann_en.pdf",
    "1996_dail": INTERESTS_PDF_DIR / "1997-04-18_register-of-members-interests-dail-eireann_en.pdf",
    "1997_dail": INTERESTS_PDF_DIR / "1998-04-22_register-of-members-interests-dail-eireann_en.pdf",
    "1998_dail": INTERESTS_PDF_DIR / "1999-05-12_register-of-members-interests-dail-eireann_en.pdf",
    "1999_dail": INTERESTS_PDF_DIR / "2000-06-26_register-of-members-interests-dail-eireann_en.pdf",
    "2000_dail": INTERESTS_PDF_DIR / "2001-05-11_register-of-members-interests-dail-eireann_en.pdf",
    "2001_dail": INTERESTS_PDF_DIR / "2002-07-10_register-of-members-interests-dail-eireann_en.pdf",
    "2002_dail": INTERESTS_PDF_DIR / "2003-05-09_register-of-members-interests-dail-eireann_en.pdf",
    "2003_dail": INTERESTS_PDF_DIR / "2004-05-21_register-of-members-interests-dail-eireann_en.pdf",
    "2004_dail": INTERESTS_PDF_DIR / "2005-05-04_register-of-members-interests-dail-eireann_en.pdf",
    "2005_dail": INTERESTS_PDF_DIR / "2006-03-31_register-of-members-interests-dail-eireann_en.pdf",
    "2006_dail": INTERESTS_PDF_DIR / "2007-03-30_register-of-members-interests-dail-eireann_en.pdf",
    "2007_dail": INTERESTS_PDF_DIR / "2008-03-11_register-of-members-interests-dail-eireann_en.pdf",
    "2008_dail": INTERESTS_PDF_DIR / "2009-03-12_register-of-members-interests-dail-eireann_en.pdf",
    "2009_dail": INTERESTS_PDF_DIR / "2010-03-12_register-of-members-interests-dail-eireann_en.pdf",
    "2010_dail": INTERESTS_PDF_DIR / "2011-03-31_register-of-members-interests-dail-eireann_en.pdf",
    "2011_dail": INTERESTS_PDF_DIR / "2012-03-30_register-of-members-interests-dail-eireann_en.pdf",
    "2012_dail": INTERESTS_PDF_DIR / "2013-02-28_register-of-members-interests-dail-eireann_en.pdf",
    "2013_dail": INTERESTS_PDF_DIR / "2014-03-25_register-of-members-interests-dail-eireann_en.pdf",
    "2014_dail": INTERESTS_PDF_DIR / "2015-03-11_register-of-members-interests-dail-eireann_en.pdf",
    "2015_dail": INTERESTS_PDF_DIR / "2016-03-01_register-of-members-interests-dail-eireann_en.pdf",
    "2016_dail": INTERESTS_PDF_DIR / "2017-03-10_register-of-members-interests-dail-eireann_en.pdf",
    "2017_dail": INTERESTS_PDF_DIR / "2018-02-14_register-of-members-interests-dail-eireann_en.pdf",
    "2018_dail": INTERESTS_PDF_DIR / "2019-02-13_register-of-members-interests-dail-eireann_en.pdf",
    "2019_dail": INTERESTS_PDF_DIR / "2020-03-03_register-of-members-interests-dail-eireann_en.pdf",
    # DAIL — current convention (published Feb of year+1)
    "2020_dail": INTERESTS_PDF_DIR / "2021-02-25_register-of-members-interests-dail-eireann_en.pdf",
    "2021_dail": INTERESTS_PDF_DIR / "2022-02-16_register-of-members-interests-dail-eireann_en.pdf",
    "2022_dail": INTERESTS_PDF_DIR / "2023-02-22_register-of-member-s-interests-dail-eireann-2022_en.pdf",
    "2023_dail": INTERESTS_PDF_DIR / "2024-02-21_register-of-member-s-interests-dail-eireann-2023_en.pdf",
    "2024_dail": INTERESTS_PDF_DIR / "2025-02-27_register-of-member-s-interests-dail-eireann-2024_en.pdf",
    "2025_dail": INTERESTS_PDF_DIR / "2026-02-25_register-of-member-s-interests-dail-eireann-2025_en.pdf",
}

# Years whose register is a scanned image with no text layer: lines come from a
# pre-built OCR artifact (a flat reading-order list[str], same shape as
# extract_raw_lines) instead of the PDF. Built by ocr_2012_register.py.
OCR_LINE_SOURCES: dict[str, pathlib.Path] = {
    "2012_dail": SILVER_DIR / "interests_ocr" / "2012_dail_lines.json",
}

CATEGORIES_PATTERN = re.compile(r"^\d+\.\s")
MEMBER_NAME_PATTERN = regex.compile(r"^\p{Lu}[\p{Lu}'\-]*(?:\s\p{Lu}[\p{Lu}'\-]*)*,\s")

INTEREST_CODE_MAP = {
    "1": "Occupations",
    "2": "Shares",
    "3": "Directorships",
    "4": "Land (including property)",
    "5": "Gifts",
    "6": "Property supplied or lent or a Service supplied",
    "7": "Travel Facilities",
    "8": "Remunerated Position",
    "9": "Contracts",
}

SPLIT_INTEREST_CODES = {"1", "2", "3", "4", "9"}

# Per-year parse-quality gate. A born-digital register matches the master roster
# at ~0.9+; a scanned/OCR'd one (digit→letter rot mangles names) collapses toward
# 0. Below this, the year is SKIPPED with a loud log rather than emitting garbage.
# Tune via probe_register_parse_quality.py before lowering.
QUALITY_MATCH_THRESHOLD = 0.5


def quality_match_rate(n_registered: int, n_parsed: int) -> float:
    """Share of parsed declarers that matched the master roster (0..1)."""
    return n_registered / max(n_parsed, 1)


def passes_quality_gate(n_registered: int, n_parsed: int) -> bool:
    """A year's parse is clean enough to ingest. Scanned/OCR'd registers mangle
    names and collapse the match rate toward 0 — they fail and are skipped."""
    return quality_match_rate(n_registered, n_parsed) >= QUALITY_MATCH_THRESHOLD

MASTER_TD_PATH = SILVER_DIR / "flattened_members.csv"
MASTER_SEANAD_PATH = SILVER_DIR / "flattened_seanad_members.csv"
MINISTER_PATH = GOLD_DIR / "enriched_td_attendance.csv"

# Former members (past terms, not in the current roster) — built by
# members/historic_members_build.py. Unioned into the master below so historic
# declarers stop being dropped. Optional: if absent, the join falls back to the
# current-roster-only behaviour exactly as before.
HISTORIC_TD_PATH = SILVER_DIR / "historic_members_dail.csv"
HISTORIC_SEANAD_PATH = SILVER_DIR / "historic_members_seanad.csv"

_MASTER_SELECT = [
    "unique_member_code",
    "first_name",
    "last_name",
    "constituency_name",
    "full_name",
    "party",
    "ministerial_office",
    "year_elected",
]


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------
PAGE_FOOTER_RE = re.compile(
    r"^\s*\d+\s*$"  # bare page numbers
    r"|IRIS OIFIGIÚIL"  # Irish Official Journal footer (both languages)
    r"|IRIS OIFIGI"  # truncated variant
    r"|DÁIL DEBATES"
)


def extract_raw_lines(pdf_path: pathlib.Path, header_skip: int = 8, footer_skip: int = 5) -> list[str]:
    """Open PDF, flatten all pages into a single list of non-blank lines, trim headers/footers.

    Requires: a valid PDF path; header_skip/footer_skip tuned per PDF layout (default 8/5).
    Produces: flat list[str] of raw text lines, footers and blank lines removed.

    Per-page footers (page numbers, 'IRIS OIFIGIÚIL' publication lines) are stripped
    before grouping. Without this, the footer text can match MEMBER_NAME_PATTERN
    (all-caps words followed by a comma) and incorrectly split a member's entry mid-way,
    causing categories that span a page break to be lost.
    """
    doc = fitz.open(pdf_path)
    print(f"Processing: {pdf_path.name} ({doc.page_count} pages)")
    text_boxes = []
    for page in doc:
        lines = page.get_text(option="text").splitlines(False)
        # Remove per-page footers: bare page numbers and IRIS OIFIGIÚIL lines
        lines = [line for line in lines if line.strip() and not PAGE_FOOTER_RE.search(line)]
        text_boxes.append(lines)

    flat = [item for sublist in text_boxes for item in sublist]
    return flat[header_skip : len(flat) - footer_skip]


def load_ocr_lines(artifact: pathlib.Path) -> list[str] | None:
    """Load the flat reading-order line list produced by OCR for a scanned year.

    Mirrors extract_raw_lines' output (footers already dropped during OCR
    reconstruction). Returns None if the artifact is absent so the caller can
    skip the year exactly as a missing PDF would be skipped.
    """
    if not artifact.exists():
        return None
    data = json.loads(artifact.read_text(encoding="utf-8"))
    lines = [ln for ln in data.get("lines", []) if ln.strip() and not PAGE_FOOTER_RE.search(ln)]
    print(f"Loaded OCR lines: {artifact.name} ({len(lines)} lines, mean conf {data.get('mean_conf')})")
    return lines


# ---------------------------------------------------------------------------
# Split embedded names
# ---------------------------------------------------------------------------

# Matches 2+ spaces preceding an all-caps name pattern mid-line, e.g.:
# "9. Contracts … Nil  O'DONOVAN, Denis"
#                    ^^^^^^^^^^^^^^^^^^^ — should become its own line
EMBEDDED_NAME_RE = regex.compile(r"(?<=\S)\s{2,}(?=\p{Lu}[\p{Lu}'\-]*(?:\s\p{Lu}[\p{Lu}'\-]*)*,\s)")


def split_embedded_names(lines: list[str]) -> list[str]:
    """Split lines where a member name is embedded mid-line after content.

    Requires: output of extract_raw_lines — flat list[str].
    Produces: same list[str] with any embedded name occurrences split onto their own line.

    PyMuPDF sometimes reads 'Nil  SMITH, John' as a single line because the
    name starts on the same text row as the previous category's trailing text.
    Splitting here means group_lines sees each name at the start of its own
    line, so the ^ anchor on MEMBER_NAME_PATTERN fires correctly.
    """
    result = []
    for line in lines:
        parts = EMBEDDED_NAME_RE.split(line)
        result.extend(p.strip() for p in parts if p.strip())
    return result


# Some older born-digital registers (e.g. the 2016 register, published 2017-03-10)
# rot the numeral '1' of the first category heading to a letter look-alike — the
# Occupations line reads 'l. Occupations' / 'I. Occupations' / 'i. Occupations'
# instead of '1. Occupations'. Because CATEGORIES_PATTERN only matches an ASCII
# digit, that line is NOT seen as a category boundary, so group_lines glues the
# entire Occupations block onto the member's NAME line — corrupting the name and
# collapsing the year's roster match-rate to 0 (the whole year then fails the
# quality gate). Repairing the leading marker to '1.' fixes the grouping AND the
# downstream interest_code (which is split off the same '.'). Only ever matches a
# rotted Occupations heading, so it is a no-op on registers that already read '1.'.
# The leading '1' rots to several look-alikes in the 2016 scan: l / I / i / ! / J.
# Gated on a following 'Occupation' so it can never fire on real body text. The
# stem 'Occupation' (not 'Occupations') covers BOTH the 2016 register's
# 'Occupations' heading and the 2012 register's 'Occupational Income' heading.
_OCR_CATEGORY1_ROT_RE = regex.compile(r"^[lIi!J]\.(\s+Occupation)")


def repair_ocr_category_markers(lines: list[str]) -> list[str]:
    """Repair OCR rot of the category-1 ('Occupations') marker '1.' → 'l.'/'I.'/'i.'.

    Requires: flat list[str] (post split_embedded_names).
    Produces: same list with any rotted Occupations heading normalised back to '1.'.
    """
    return [_OCR_CATEGORY1_ROT_RE.sub(r"1.\1", line) for line in lines]


# The 2003 register (published 2004-05-21) drops the space after the category
# marker's dot ('1.Occupational Income'), so CATEGORIES_PATTERN ('^\d+\.\s')
# misses every boundary and whole blocks glue onto the member's name line
# (the year collapsed to 4/164 roster match). Vocabulary-gated on the known
# category lead-words so it can never fire on body text like '3.5 acres' or a
# numbered address; a no-op on years that already read 'N. '.
_MISSING_CATEGORY_SPACE_RE = regex.compile(
    r"^([1-9])\.(?=Occupation|Share|Director|Land\b|Gift|Propert|Travel|Remunerat|Contract)"
)


def repair_missing_category_space(lines: list[str]) -> list[str]:
    """Insert the missing space in a category marker ('1.Occupations' → '1. Occupations').

    Requires: flat list[str] (post split_embedded_names).
    Produces: same list with space-less category markers normalised to 'N. '.
    """
    return [_MISSING_CATEGORY_SPACE_RE.sub(r"\1. ", line) for line in lines]


# In a scanned register (e.g. 2012) the '.' after a category number can OCR-rot to
# ',' or ';' (e.g. '9. Contracts' -> '9, Contracts'), so CATEGORIES_PATTERN (which
# needs 'N.') misses the boundary and the category glues onto the previous block.
# Gated on the known category lead-words (incl. common OCR misreads) so it can
# NEVER fire on body text like an address '9, Main Street'. Idempotent + a no-op
# on clean years (they already read 'N.').
_OCR_CATEGORY_NUM_ROT_RE = regex.compile(
    r"^([1-9])[,.;:]?\s+(?=Occupation|Share|Director|Land\b|Gift|Propert|Travel|"
    r"Remunerat|Renunerat|Contract|Cnr|Contraet)"
)


def repair_ocr_category_numbers(lines: list[str]) -> list[str]:
    """Normalise an OCR-rotted category-number marker ('9,'/'9;' → '9. ').

    Requires: flat list[str] (post split_embedded_names).
    Produces: same list with category-heading number markers restored to 'N. '.
    """
    return [_OCR_CATEGORY_NUM_ROT_RE.sub(r"\1. ", line) for line in lines]


# ---------------------------------------------------------------------------
# Group
# ---------------------------------------------------------------------------


def group_lines(
    lines: list[str],
    categories: re.Pattern,
    member_name: regex.Pattern,
) -> list[str]:
    """Concatenate continuation lines onto their parent block; return one string per block.

    Requires: output of split_embedded_names — flat list[str] with names on their own lines.
    Produces: list[str] where each element is one complete member-name or category block.
    """
    grouped = []
    current = ""
    for line in lines:
        if categories.match(line) or member_name.match(line):
            if current.strip():
                grouped.append(current.strip())
            current = line
        else:
            current = current + " " + line
    if current.strip():
        grouped.append(current.strip())
    return grouped


# ---------------------------------------------------------------------------
# Parse
# ---------------------------------------------------------------------------


def parse_members(grouped: list[str], member_name: regex.Pattern) -> list[dict]:
    """Walk grouped blocks and build [{name, interests}] — one dict per TD.

    Requires: output of group_lines — one string per block, names at block boundaries.
    Produces: list[dict] with keys 'name' (raw "LAST, First" string) and 'interests' (list[str] of category blocks).
    """
    members = []
    current_member = None
    for line in grouped:
        if member_name.match(line):
            if current_member:
                members.append(current_member)
            current_member = {"name": line, "interests": []}
        elif current_member is not None:
            current_member["interests"].append(line)
    if current_member:
        members.append(current_member)
    return members


def clean_interests(df: pl.DataFrame, year: int) -> pl.DataFrame:
    """Apply all Polars cleaning steps and return a cleaned DataFrame.

    Requires: DataFrame from pl.read_json — columns 'name' (str) and 'interests' (list[str]).
    Produces: one row per member per interest category with columns:
        name, constituency, last_name, first_name, interest_code, interest_category,
        interest_description_raw, interest_description_cleaned,
        join_key, is_landlord, is_property_owner, interest_flag.

    After splitting on ';' and exploding for SPLIT_INTEREST_CODES, a fragment filter
    drops supplementary notes (ownership %, status lines) that lack ':' and would
    otherwise appear as standalone rows. Non-split categories and 'No interests
    declared' rows are never dropped.
    """
    df = df.explode("interests")
    # The 1998–2003 registers put TWO spaces after the surname comma
    # ('AHERN,  Bertie  (Dublin Central)'), so the \s{2,} name/constituency split
    # below shears the line into THREE parts (surname / first name / constituency)
    # and the whole year joins at 0%. Collapse the post-comma gap to one space
    # first; clean years already read 'SURNAME, First' so this is a no-op there.
    df = df.with_columns(pl.col("name").str.replace_all(r",\s{2,}", ", "))
    # Some registers (e.g. 2016) put only ONE space before the trailing
    # '(Constituency)', so the \s{2,} split below would leave it stuck to the
    # first name (e.g. 'Gerry (Louth)') and break the roster join. Normalise a
    # single-space-before-trailing-paren up to two spaces first. No-op when it is
    # already two spaces; also repairs the odd same-shaped case in clean years.
    df = df.with_columns(pl.col("name").str.replace(r"\s+(\([^)]+\))\s*$", r"  ${1}"))
    df = df.with_columns(pl.col("name").str.split(by=r"\s{2,}", literal=False).alias("name_and_constituency"))
    df = df.with_columns(
        pl.col("name_and_constituency").list.get(0).alias("name"),
        pl.col("name_and_constituency").list.get(1, null_on_oob=True).alias("constituency"),
    ).drop("name_and_constituency")
    df = df.with_columns(
        pl.col("constituency").str.strip_chars("()"),
        pl.col("name").str.split(by=",", literal=True).alias("full_name"),
        pl.col("interests").str.splitn(by=".", n=2).alias("interests_code"),
    ).unnest("interests_code")
    df = df.rename({"field_0": "interest_code", "field_1": "interest_description_raw"})

    df = df.with_columns(
        pl.col("interests")
        .str.strip_chars_start('" ')
        .str.strip_chars_end('"')
        .str.replace_all(r"\xa0", " ")
        .str.replace_all(r"(Etc|including property|Property supplied |or lent  or a Service supplied  )", "")
        .str.replace_all(
            # 'Occupational Income' and 'Property and Service' are the pre-2012
            # category headings (Ethics Act 1995 wording) — longer alternatives
            # listed before their modern near-prefixes.
            r"(Occupational Income|Occupations|Shares|Directorships|Land|Gifts|Property supplied or lent or a Service supplied|Property and Service|Travel Facilities|Remunerated Position|Contracts)",
            "",
        )
        .str.replace_all(
            r'"Etc\b', ""
        )  # Remove 'Etc' when it appears at the start of the description, as it's a common trailing word that adds no meaning and can interfere with parsing (e.g. "Rental income from Smith Ltd etc" → "Rental income from Smith Ltd")
        .str.strip_chars_start()  # Remove leading spaces and quotation marks that can interfere with parsing and cause false positives in filters (e.g. ' "No interests declared' would not match 'No interests declared' without the leading quote)
        .str.replace_all(
            r"\s+", " "
        )  # Collapse multiple spaces into one, including those introduced by previous substitutions
        .str.replace_all(
            r"[.…]+", ""
        )  # Remove trailing ellipses that indicate continuation lines (now merged into the main line)
        .str.replace_all(
            r"^\s+", ""
        )  # Remove any remaining leading whitespace that could interfere with parsing and cause false positives in filters (e.g. ' No interests declared' would not match 'No interests declared' without the leading space)
        .str.replace_all(
            r"\b[1-9]\b", ""
        )  # Remove standalone numbers that are likely to be category codes or list numbers, as they add no meaning to the description and can interfere with parsing (e.g. "No interests declared 4" would not match "No interests declared" without the trailing number)
        .str.replace_all(
            r"\s*\(\)\s*", " "
        )  # Remove empty parentheses that are likely to be artifacts of formatting and add no meaning, while leaving any meaningful content within parentheses intact (e.g. "Rental income from Smith Ltd (my company) ()" would become "Rental income from Smith Ltd (my company)", preserving the meaningful parenthetical while removing the empty one)
        .str.replace_all(
            r" {2,}", " "
        )  # Collapse multiple spaces again in case previous substitutions introduced new ones
        .str.replace_all(
            r'["""]', ""
        )  # Remove any remaining quotation marks that could interfere with parsing and cause false positives in filters (e.g. 'No interests declared "etc"' would not match 'No interests declared etc' without the quotes)
        .str.replace_all(
            r"or lent\s*Nil?\s*or a Service supplied\s*Nil?", "Nil"
        )  # Standardize the common "Property supplied or lent or a Service supplied" category when it includes "Nil", as this phrase appears in various forms and can interfere with parsing and categorization (e.g. "Property supplied or lent or a Service supplied Nil" would become "Nil", while "Property supplied or lent or a Service supplied Rental income from Smith Ltd" would remain unchanged)
        .str.replace(
            r"(or lent or a Service supplied |or lent or a service supplied|or lent or a service supplied No interests declared|or lent or a service supplied No interests declared)",
            "No interests declared",
        )
        .str.replace_all(
            r" {2,}", " "
        )  # Collapse multiple spaces again in case previous substitutions introduced new ones
        .str.replace(
            r"^\(\)\s*", ""
        )  # Remove empty parentheses at the start of the description that are likely to be artifacts of formatting and add no meaning, while leaving any meaningful content within parentheses intact (e.g. " () Rental income from Smith Ltd" would become "Rental income from Smith Ltd", preserving the meaningful description while removing the empty parentheses)
        .str.replace(
            r'^"', ""
        )  # Remove a leading quote that can interfere with parsing and cause false positives in filters (e.g. '"No interests declared' would not match 'No interests declared' without the leading quote)
        .str.replace(
            r'^"', ""
        )  # Remove a second leading quote if present, as some entries have multiple leading quotes due to formatting issues (e.g. '""No interests declared' would not match 'No interests declared' without the leading quotes)
        .str.strip_chars_start(
            "-:;,. "
        )  # Remove leading punctuation and spaces that can interfere with parsing and cause false positives in filters (e.g. '- No interests declared' would not match 'No interests declared' without the leading dash and space)
        .str.strip_chars_start(
            "etc"
        )  # Remove 'etc' when it appears at the start of the description, as it's a common trailing word that adds no meaning and can interfere with parsing (e.g. "Etc No interests declared" would become "No interests declared")
        .str.replace(
            r'"$', ""
        )  # Remove a trailing quote that can interfere with parsing and cause false positives in filters (e.g. 'No interests declared"' would not match 'No interests declared' without the trailing quote)
        .str.replace_all(
            r"^(│Dr.|dr|dr.|prof|mr|mrs|ms|miss|bl)\s+", ""
        )  # Remove common honorifics and titles that can appear at the start of descriptions due to formatting issues and add no meaningful information about the interest itself, while leaving any meaningful content intact (e.g. "Dr. Rental income from Smith Ltd" would become "Rental income from Smith Ltd", while "Rental income from Dr. Smith Ltd" would remain unchanged)
        # Longer alternatives must come before shorter ones — "Níl aon rud" before "Níl",
        # otherwise "Níl" matches first and leaves "aon rud" as a suffix.
        .str.replace_all(
            r"No interests declared aon rud|etc Níl aon rud|Níl aon rud|Nil aon rud|Níl|Nil|Tada|Neamh-fheidhme|Neamh-infheidhme|Neamh infheidhme|Infheidhme",
            "No interests declared",
        )
        .str.replace(r"No interests declared\s+\d{2}$", "No interests declared")
        .str.replace(r"No interests declared\s+\d{3}$", "No interests declared")
        # Collapse accidental double-substitutions (e.g. "No interests declared No interests declared")
        .str.replace_all(r"(?:No interests declared\s*){2,}", "No interests declared")
        .str.replace(
            r"(lord:|lord)", "Landlord:"
        )  # Standardize 'lord' to 'Landlord' when it appears in the description, as this is a common shorthand for landlord status that can interfere with parsing and categorization (e.g. "Rental income from Smith Ltd lord" would become "Rental income from Smith Ltd Landlord", while "Rental income from Lord of the Manor" would remain unchanged)
        .str.strip_chars()
        .alias("interest_description_cleaned")
    )

    df = df.with_columns(
        pl.col("interest_code")
        .replace_strict(INTEREST_CODE_MAP, default=pl.col("interest_code"))
        .alias("interest_category")
    )

    df = df.with_columns(
        pl.col("full_name").list.get(0).alias("last_name"),
        pl.col("full_name").list.get(1).alias("first_name"),
    ).drop("full_name")

    df = df.with_columns(pl.concat_str(pl.col(["first_name", "last_name"])).alias("join_key")).filter(
        pl.col("interest_description_raw") != str(year),
        pl.col("interest_description_cleaned") != str(year),
        pl.col("interest_category") != str(year),
    )
    df = df.with_columns(
        pl.when(pl.col("interest_code").is_in(SPLIT_INTEREST_CODES))
        .then(pl.col("interest_description_cleaned").str.split(";"))
        .otherwise(pl.concat_list("interest_description_cleaned"))
        .alias("split_interests")
    ).explode("split_interests")

    df = df.with_columns(pl.col("split_interests").str.strip_chars().alias("interest_description_cleaned")).drop(
        "split_interests"
    )

    # --- Fragment filter (TEST) ---
    # In split categories (1,2,3,4,9), real entries follow 'Name, Address: description'.
    # Rows without ':' are supplementary notes (ownership %, status lines) that belong
    # to the preceding entry but ended up as their own row after the ';' split.
    # Non-split categories and 'No interests declared' rows are never dropped.
    # Fragment filter — only applied to Shares (2), Directorships (3), and Contracts (9).
    # These categories reliably follow 'Entity, Address: description' so entries without
    # ':' are supplementary notes (ownership %, status lines) rather than standalone entries.
    # Occupations (1) and Land (4) are excluded: occupations are plain text (no colon),
    # and land entries can be free-form descriptions without a clear entity:detail split.
    INTEREST_CODES = {"2", "3", "9"}
    is_colon_cat = pl.col("interest_code").is_in(INTEREST_CODES)
    has_colon = pl.col("interest_description_cleaned").str.contains(":")
    is_declared = pl.col("interest_description_cleaned").str.contains(r"(?i)no interests declared|nil")
    df = df.filter(~is_colon_cat | has_colon | is_declared)

    df = df.with_columns(
        pl.when(
            (
                ~pl.col("interest_description_cleaned").is_in(
                    ["ceased", "no rental income", "I own no land or residences"]
                )
            )
            & (
                pl.col("interest_description_cleaned").str.contains(
                    "let|rented|ARP|Léasóir|etc Lessor|Rental|rent received|Leasóir|Léasóir|lord|letting|renting|rental|HAP|RAS Scheme|lessor|Lessor|lord:"
                )
            )
        )
        .then(pl.lit("true"))
        .otherwise(pl.lit("false"))
        .alias("is_landlord")
    )
    df = df.with_columns(
        pl.col("interest_description_cleaned").str.replace(
            "or lent No interests declared or a Service supplied", "No interests declared"
        )
    )
    df = df.with_columns(
        pl.when(pl.col("interest_description_cleaned") != "No interests declared")
        .then(1)
        .otherwise(0)
        .alias("interest_flag")
    )
    df = df.with_columns(pl.concat_str(pl.col(["first_name", "last_name"])).alias("join_key"))
    df = df.with_columns(
        pl.when(
            (pl.col("interest_code") == "4") & (pl.col("is_landlord") == True)  # noqa: E712
            | ((pl.col("interest_code") == "4") & (pl.col("interest_description_cleaned") != "No interests declared"))
        )
        .then(pl.lit("TRUE"))
        .otherwise(pl.lit("FALSE"))
        .alias("is_property_owner")
    )

    df = df.with_columns(
        pl.when((pl.col("interest_code") == "1") & (pl.col("interest_description_cleaned") != "No interests declared"))
        .then(pl.lit(True))
        .otherwise(pl.lit(False))
        .alias("is_occupation")
    ).with_columns(
        pl.when((pl.col("interest_code") == "1") & (pl.col("interest_description_cleaned") != "No interests declared"))
        .then(pl.col("interest_description_cleaned"))
        .otherwise(pl.lit("N/A"))
        .alias("occupation_description")
    )

    return df


# ---------------------------------------------------------------------------
# Join
# ---------------------------------------------------------------------------
def join_master_list(
    df: pl.DataFrame,
    master_path: pathlib.Path,
    minister_path: pathlib.Path | None = None,
    historic_path: pathlib.Path | None = None,
) -> pl.DataFrame:
    """Join cleaned interests against the master member list and optional ministerial office lookup.

    When ``historic_path`` points at a former-members roster, it is unioned onto
    the current master before the join so declarers from past terms are retained
    rather than dropped. Returning members (same stable memberCode) are deduped on
    the normalised join_key with the current row kept, so no member is doubled.
    """
    df = normalise_join_key.normalise_df_td_name(df, "join_key")

    master = pl.read_csv(master_path).select(_MASTER_SELECT)
    if historic_path is not None and historic_path.exists():
        historic = pl.read_csv(historic_path).select(_MASTER_SELECT)
        master = pl.concat([master, historic], how="vertical_relaxed")
    master = master.with_columns(pl.concat_str(pl.col(["first_name", "last_name"])).alias("join_key"))
    master = normalise_join_key.normalise_df_td_name(master, "join_key")
    # Current rows were concatenated first → keep="first" lets a sitting member win
    # any name collision with a former member of the same normalised name.
    master = master.unique(subset=["join_key"], keep="first", maintain_order=True)

    result = (
        master.join(df, on="join_key", how="left")
        .with_columns(
            pl.when(pl.col("interest_code").is_null())
            .then(pl.lit("unregistered"))
            .otherwise(pl.lit("registered"))
            .alias("registration_status")
        )
        .with_columns(pl.col("unique_member_code").str.extract(r"\b\d{4}\b", 0).alias("year_elected"))
        .drop("join_key")
    )
    drop_cols = ["first_name_right", "interests", "name", "interest_description_raw", "last_name_right"]
    result = result.drop([c for c in drop_cols if c in result.columns])
    result = result.with_columns(pl.col("interest_flag").sum().over("unique_member_code").alias("interest_count")).drop(
        "interest_flag"
    )
    return result


# ---------------------------------------------------------------------------
# Combine
# ---------------------------------------------------------------------------


def combine_years(silver_dir: pathlib.Path, years: list[str], case: str) -> pl.DataFrame:
    """Read per-year CSVs, tag with year_declared, concatenate, filter, sort, and write combined."""
    frames = []
    for year_key in years:
        numeric_year = int(year_key.split("_")[0])
        path = silver_dir / f"{case}_member_interests_grouped_{year_key}.csv"
        # interest_code must be read as String in every year: a year whose codes
        # are all clean digits infers Int64 while one with an OCR-rotted code
        # infers String, and the vstack across years then fails on the mismatch.
        # infer_schema_length=None scans the whole file: in years where the first
        # ~100 rows are unregistered members (all-null flags), the default window
        # infers the boolean flag columns as String and the vstack fails the same
        # way ('is_landlord': String vs Boolean).
        frames.append(
            pl.read_csv(
                path,
                schema_overrides={"interest_code": pl.String},
                infer_schema_length=None,
            ).with_columns(year_declared=pl.lit(numeric_year))
        )
    combined = (
        pl.concat(frames)
        # Rogue rows whose "category code" is actually a calendar year (a date
        # line matched CATEGORIES_PATTERN). Any 19xx/20xx code is impossible —
        # real codes are 1–9 — so match the shape, not a hardcoded year list
        # (the old list started at 2019 and would miss 1990s-register artifacts).
        .filter(~pl.col("interest_code").cast(pl.String).str.contains(r"^(19|20)\d{2}$"))
        # Drop exact-duplicate rows. A member can't declare the identical thing
        # twice, so any full-row duplicate is a parse/OCR artifact (e.g. the 2012
        # scanned register reconstructed a 'No interests declared' line twice).
        # total_declarations = COUNT(*) downstream, so undeduped rows inflate the
        # declaration tally and the /interests leaderboard rank. Full-row dedup =
        # zero signal loss (DISTINCT property/share counts are untouched).
        .unique()
        .sort(["unique_member_code", "year_declared", "interest_code"])
    )
    out_path = silver_dir / f"{case}_member_interests_combined.csv"
    combined.write_csv(out_path)
    parquet_dir = silver_dir / "parquet"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = parquet_dir / f"{case}_member_interests_combined.parquet"
    save_parquet(combined, parquet_path)
    print(f"Saved combined: {out_path} + {parquet_path.name}")
    return combined


# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------


def save_output(df: pl.DataFrame, filename: str, silver_dir: pathlib.Path = SILVER_DIR) -> None:
    """Write a DataFrame to the silver directory."""
    silver_dir.mkdir(parents=True, exist_ok=True)
    path = silver_dir / filename
    df.write_csv(path)
    print(f"Saved {filename}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    print("=== Member interest pipeline starting ===")

    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    dail_years = []
    seanad_years = []

    for year_key, pdf_path in PDF_PATHS.items():
        print(f"\n--- Processing year {year_key} ---")
        numeric_year = int(year_key.split("_")[0])
        case = "DAIL" if "dail" in year_key else "SEANAD"

        # 1. Extract. Scanned years (no text layer) read from a pre-built OCR
        # artifact instead of the PDF; everything downstream is identical.
        is_ocr = year_key in OCR_LINE_SOURCES
        if is_ocr:
            lines = load_ocr_lines(OCR_LINE_SOURCES[year_key])
            if lines is None:
                print(
                    f"  SKIP {year_key}: OCR artifact missing ({OCR_LINE_SOURCES[year_key].name})"
                    f" — run pipeline_sandbox/historic_members/ocr_2012_register.py"
                )
                continue
        else:
            lines = extract_raw_lines(pdf_path)

        # 1b. Split any lines where a member name is embedded mid-line
        lines = split_embedded_names(lines)

        # 1c. Repair OCR rot of the category-1 'Occupations' marker ('l./I./i.' → '1.')
        # so older registers (e.g. 2016) group correctly instead of failing the gate.
        lines = repair_ocr_category_markers(lines)

        # 1c-bis. Insert the missing space in space-less category markers
        # ('1.Occupational Income' → '1. Occupational Income') — the 2003
        # register's layout; vocabulary-gated, no-op elsewhere.
        lines = repair_missing_category_space(lines)

        # 1d. OCR-only: repair a category-number marker whose '.' rotted to ',/;/:'
        # ('9, Contracts' → '9. Contracts'). Gated to OCR years so clean years can't
        # regress on body text that happens to start with a digit + category word.
        if is_ocr:
            lines = repair_ocr_category_numbers(lines)

        # 2. Group
        grouped = group_lines(lines, CATEGORIES_PATTERN, MEMBER_NAME_PATTERN)

        # 3. Parse
        members = parse_members(grouped, MEMBER_NAME_PATTERN)
        print(f"  Parsed {len(members)} members")

        # 4. Write intermediate JSON, load into Polars
        json_path = MEMBERS_DIR / f"{pdf_path.stem}_{case.lower()}.json"
        with open(json_path, "w", encoding="utf-8") as f:
            f.write(
                orjson.dumps(members, option=orjson.OPT_INDENT_2 | orjson.OPT_NON_STR_KEYS, default=str).decode("utf-8")
            )
        df = pl.read_json(json_path)

        # 5. Clean — pass numeric year so the rogue-row filter works correctly
        df = clean_interests(df, numeric_year)

        # 6. Join
        if case == "DAIL":
            df = join_master_list(df, MASTER_TD_PATH, MINISTER_PATH, HISTORIC_TD_PATH)
        else:
            df = join_master_list(df, MASTER_SEANAD_PATH, historic_path=HISTORIC_SEANAD_PATH)

        # 6b. Parse-quality gate — a scanned/OCR'd register mangles names and
        # matches the roster at ~0%. Skip (loudly) rather than emit garbage that
        # pollutes the year filter. Born-digital years clear this easily (~0.9+).
        n_registered = df.filter(pl.col("registration_status") == "registered").select("unique_member_code").n_unique()
        match_rate = quality_match_rate(n_registered, len(members))
        if not passes_quality_gate(n_registered, len(members)):
            print(
                f"  SKIP {year_key}: roster match {match_rate:.0%} < {QUALITY_MATCH_THRESHOLD:.0%}"
                f" — likely scanned/OCR'd ({n_registered}/{len(members)} matched), not ingesting"
            )
            continue
        print(f"  roster match {match_rate:.0%} ({n_registered}/{len(members)})")
        if case == "DAIL":
            dail_years.append(year_key)
        else:
            seanad_years.append(year_key)

        # 7. Save per-year CSV — consistent name used by combine_years
        save_output(df, f"{case.lower()}_member_interests_grouped_{year_key}.csv")

        # Keep Seanad intermediate JSONs for manual mismatch inspection; delete Dáil ones.
        if case == "DAIL" and os.path.exists(json_path) or case == "SEANAD":
            # os.remove(json_path)
            print(f"  Kept intermediate JSON for inspection: {json_path.name}")

    # 8. Combine dail and seanad years separately (different schemas)
    print("\n--- Combining years ---")
    if dail_years:
        combine_years(SILVER_DIR, dail_years, "dail")
    if seanad_years:
        combine_years(SILVER_DIR, seanad_years, "seanad")

    # 9. Clean up per-year scratch CSVs now that the combined outputs (CSV + parquet)
    # are written. Only clean if both combined files exist — otherwise leave the
    # per-year files so a failed run can be diagnosed from the inputs.
    for case, year_list in [("dail", dail_years), ("seanad", seanad_years)]:
        combined_csv = SILVER_DIR / f"{case}_member_interests_combined.csv"
        combined_parquet = SILVER_DIR / "parquet" / f"{case}_member_interests_combined.parquet"
        if year_list and combined_csv.exists() and combined_parquet.exists():
            for scratch in SILVER_DIR.glob(f"{case}_member_interests_grouped_*.csv"):
                scratch.unlink()
                print(f"  Cleaned scratch: {scratch.name}")

    print("\n=== Member interest pipeline complete ===")


if __name__ == "__main__":
    main()
