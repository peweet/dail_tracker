#!/usr/bin/env python3
"""
Iris Oifigiuil ETL — Polars rewrite (sandbox).

Behaviour-preserving conversion of iris_pymupdf_etl_v2.py to Polars:

- PDF traversal stays in Python (PyMuPDF is sequential).
- build_records replaces the imperative state machine with cum_sum + group_by.
- All per-row regex / df.apply work moves to Polars expressions.
- pipe_join + per-row dict loops are removed; multi-tag fields use
  concat_list -> list.drop_nulls -> list.unique -> list.join.

Outputs (same names as v2):
- iris_pdf_audit.csv
- iris_raw_lines_pymupdf.csv
- iris_notice_events_all.csv
- iris_notice_events_clean.csv
- iris_notice_events_quarantined.csv
- iris_si_taxonomy.csv
- iris_member_interests_raw_pages.json
- iris_dataset_dimensions.json

Usage:
    python iris_oifigiuil_etl_polars.py "path/to/pdfs/*.pdf" --out-dir ./out
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
from pathlib import Path
from typing import Any

import fitz  # PyMuPDF
import polars as pl


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MONTHS = dict(
    zip(
        [
            "january", "february", "march", "april", "may", "june",
            "july", "august", "september", "october", "november", "december",
        ],
        range(1, 13),
    )
)

# Footer line printed at the bottom of every page, e.g.:
#   "All notices and advertisements are published in Iris Oifigiúil ..."
# Fada is dropped in some issues — accept both spellings.
BOILERPLATE_PATTERN = r"All notices and advertisements are published in Iris Oifigi[uú]il"

# Publication chrome that sits on every page; stripped before record assembly.
# Examples of lines these match:
#   "IRIS OIFIGIÚIL, 12th April 2024"          ← masthead
#   "Number 30"                                 ← issue number band
#   "493"                                       ← bare page number
#   "Published by Authority"                    ← masthead subtitle
#   "Tuesday, 12 April 2024"                    ← issue date band
#   "Wt. 1234. 500. 4/24."                      ← printer's weight footer
HEADER_PATTERNS = [
    r"^IRIS OIFIGI[ÚU]IL,?\s+",
    r"^Number\s+\d+\b",
    r"^\d{2,5}$",
    r"^Published by Authority$",
    r"^(Tuesday|Friday),?\s+\d",
    r"^This publication is registered for transmission",
    r"^For places in Britain",
    r"^Wt\.",
]
HEADER_RE = "(?i)" + "|".join(f"(?:{p})" for p in HEADER_PATTERNS)

# Lines that open a new notice on their own — record breaks even when the
# underscore delimiter is missing (older issues sometimes omit it).
# Example matches:
#   "S.I. No. 142 of 2024."                     ← statutory instrument
#   "THE HIGH COURT"                            ← court notice (winding-up etc.)
#   "AN tORDÚ FOIRBEARTÁIN AGUS DLEACHTANNA"    ← Irish ministerial order
#   "FISHERIES MANAGEMENT NOTICE NO. 5 OF 2024" ← fisheries notice
#   "FÓGRA POIBLÍ"                              ← Irish public notice label
STRONG_START_PATTERNS = [
    r"^S\.I\. No\. \d+ of \d{4}\.?",
    r"^IRISH STANDARDS$",
    r"^IN THE MATTER OF$",
    r"^THE HIGH COURT$",
    r"^IN THE HIGH COURT",
    r"^AN tORD[ÚU] ",
    r"^AGREEMENTS WHICH ENTERED INTO FORCE",
    r"^FISHERIES MANAGEMENT NOTICE NO\.",
    r"^IRISH STATE SAVINGS$",
    r"^F[ÓO]GRA\b",
    r"^NOTICE\b",
]
STRONG_START_RE = "(?i)" + "|".join(f"(?:{p})" for p in STRONG_START_PATTERNS)

# Run of 4+ underscores — the canonical between-notice separator.
# Looks like: "_______________________________" (typically ~30+ underscores).
DELIMITER_RE = r"^_{4,}$"

# Parses the date band on page 1, e.g.:
#   "Tuesday, 12th April, 2024" = groups: ("Tuesday", "12", "April", "2024")
# parse_issue_date() then formats this as ISO "2024-04-12".
ISSUE_DATE_RE = re.compile(
    r"(Tuesday|Friday),?\s+(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+),?\s+(\d{4})", re.I
)
# Parses "Number 30" on page 1 = 30.
ISSUE_NUMBER_RE = re.compile(r"\bNumber\s+(\d+)\b", re.I)


# Multi-tag rules: a single SI commonly matches several flags (e.g. amendment + commencement).
# Order is the priority for `si_operation_primary` (list.first after dedup).
# Worked example — title "Social Welfare (Consolidated Claims, Payments and
# Control) (Amendment) (No. 5) Regulations 2024" hits:
#   amendment            (token "AMENDMENT")
#   appointment_assignment (substring "APPOINTMENT" inside "PAYMENTS"... no — but "ASSIGNMENT" if present)
#   scheme_or_benefit    (token "PAYMENT")
# So si_operation_flags = "amendment|scheme_or_benefit" and si_operation_primary = "amendment".
SI_OPERATION_RULES = [
    ("amendment", ["AMENDMENT", "AMENDS", "AMENDING"]),
    ("commencement", ["COMMENCEMENT", "COME INTO OPERATION", "APPOINTS THE"]),
    ("revocation", ["REVOCATION", "REVOKE", "REVOKES", "REVOKED"]),
    ("designation", ["DESIGNATION", "DESIGNATE", "DESIGNATES"]),
    ("restrictive_measures_or_sanctions", ["RESTRICTIVE MEASURES", "SANCTIONS"]),
    ("delegation_of_functions", ["DELEGATION OF MINISTERIAL FUNCTIONS", "FUNCTIONS OF THE MINISTER", "AIRE A THARMLIGEAN"]),
    ("appointment_assignment", ["APPOINTMENT", "ASSIGNMENT", "ASSIGN", "BUANSANNADH", "CHEAPADH"]),
    ("transfer_or_name_change", ["TRANSFER OF DEPARTMENTAL ADMINISTRATION", "ALTERATION OF NAME", "TITLE OF MINISTER"]),
    ("fees_levies_charges", ["FEE", "FEES", "LEVY", "LEVIES", "CHARGE", "CHARGES"]),
    ("licensing", ["LICENCE", "LICENCES", "LICENSING"]),
    ("exemption_or_non_application", ["EXEMPTION", "EXEMPT", "NON-APPLICATION", "DISAPPLICATION"]),
    ("prescription_or_prescribed", ["PRESCRIBED", "PRESCRIPTION"]),
    ("establishment", ["ESTABLISHMENT", "ESTABLISH"]),
    ("scheme_or_benefit", ["SCHEME", "BENEFIT", "ALLOWANCE", "PAYMENT"]),
    ("quota_or_open_season", ["QUOTA", "OPEN SEASONS", "CONTROL OF FISHING"]),
    ("superannuation_pension", ["SUPERANNUATION", "PENSION", "PENSIONS"]),
    ("court_procedure", ["RULES OF THE SUPERIOR COURTS", "DISTRICT COURT", "CIRCUIT COURT", "COURTS"]),
    ("statistics_collection", ["STATISTICS", "INQUIRY"]),
]

# Multi-domain rules: SIs spanning several domains are normal — all matches are kept.
# Worked example — a "Sea-Fisheries (Quota Management) Regulations 2024" hits:
#   agriculture_food_marine (token "MARINE")
#   fisheries               (tokens "FISH", "SEA-FISHERIES")
# si_policy_domains = "agriculture_food_marine|fisheries", primary = "agriculture_food_marine"
# (primary is just list.first, i.e. dict-insertion order, NOT a strength score).
POLICY_DOMAIN_RULES = {
    "finance_banking_tax": ["FINANCE", "CENTRAL BANK", "INSURANCE", "INVESTMENT", "MARKETS IN FINANCIAL", "LEVY", "TAX", "CASH INFRASTRUCTURE"],
    "justice_security_courts": ["JUSTICE", "CRIMINAL", "TERRORIST", "GARDA", "COURTS", "SUPERIOR COURTS", "DISTRICT COURT", "CIRCUIT COURT", "PRISONS"],
    "health_medicines_care": ["HEALTH", "MEDICINAL", "COVID", "VACCINE", "DESIGNATED CENTRES", "IONISING RADIATION", "NURS", "MEDICAL", "PHARMACY", "PHARMACEUTICAL"],
    "housing_planning_local_gov": ["HOUSING", "PLANNING", "RENT PRESSURE", "RESIDENTIAL TENANCIES", "LOCAL GOVERNMENT", "LAND DEVELOPMENT"],
    "environment_climate_energy": ["ENVIRONMENT", "CLIMATE", "ENERGY", "RENEWABLE", "AIR POLLUTION", "SOLID FUELS", "EMISSIONS", "WILDLIFE", "WILD MAMMALS", "DEER"],
    "transport_maritime_roads": ["TRANSPORT", "ROAD TRAFFIC", "SPECIAL SPEED LIMIT", "MERCHANT SHIPPING", "MARITIME", "VEHICLE", "MOTOR INSURANCE", "RAILWAY", "AVIATION"],
    "agriculture_food_marine": ["AGRICULTURE", "FOOD", "PLANT HEALTH", "FORESTRY", "ANIMAL", "VETERINARY", "MARINE"],
    "fisheries": ["FISH", "FISHERIES", "SALMON", "MACKEREL", "HERRING", "SEA-FISHERIES"],
    "social_protection_welfare": ["SOCIAL WELFARE", "SUPPLEMENTARY WELFARE", "ALLOWANCE", "BENEFIT", "PAYMENTS AND CONTROL"],
    "education_research_universities": ["EDUCATION", "UNIVERSITY", "UCD", "HIGHER EDUCATION", "RESEARCH"],
    "children_equality_disability_integration": ["CHILD", "CHILDMINDING", "DISABILITY", "EQUALITY", "INTEGRATION", "TEMPORARY PROTECTION BENEFICIARIES"],
    "migration_international_protection": ["INTERNATIONAL PROTECTION", "REFUGEE", "ASYLUM", "DISPLACED PERSONS", "UKRAINE TEMPORARY PROTECTION"],
    "communications_digital_online": ["COMMUNICATIONS", "ELECTRONIC COMMUNICATIONS", "ONLINE", "TELECOMMUNICATIONS", "WIRELESS TELEGRAPHY", "DIGITAL"],
    "public_service_governance": ["PUBLIC SERVICE", "MINISTERIAL FUNCTIONS", "SPECIAL ADVISER", "OVERSIGHT", "AUDIT COMMISSION", "APPOINTMENT"],
    "statistics_data_collection": ["STATISTICS", "INQUIRY"],
    "foreign_affairs_international": ["FOREIGN AFFAIRS", "AGREEMENTS", "TREATY", "LIBYA", "TÜRKIYE", "UKRAINE"],
    "culture_tourism_sport": ["CULTURE", "ARTS", "GAELTACHT"],
}


# ---------------------------------------------------------------------------
# PDF traversal — Python (PyMuPDF is sequential)
# ---------------------------------------------------------------------------

def parse_issue_date(text: str) -> tuple[str | None, str | None]:
    m = ISSUE_DATE_RE.search(text)
    if not m:
        return None, None
    day = int(m.group(2))
    mon = MONTHS.get(m.group(3).lower())
    year = int(m.group(4))
    if mon:
        return f"{year:04d}-{mon:02d}-{day:02d}", m.group(0)
    return None, m.group(0)


def parse_issue_number(text: str) -> int | None:
    m = ISSUE_NUMBER_RE.search(text)
    return int(m.group(1)) if m else None


def extract_page_blocks(page: fitz.Page) -> list[dict[str, Any]]:
    blocks = []
    page_dict = page.get_text("dict")
    for block_id, block in enumerate(page_dict.get("blocks", [])):
        if "lines" not in block:
            continue
        block_lines = []
        spans_payload = []
        for line_id, line in enumerate(block.get("lines", [])):
            line_text = "".join(s.get("text", "") for s in line.get("spans", [])).strip()
            if line_text:
                block_lines.append(line_text)
            for span_id, span in enumerate(line.get("spans", [])):
                text = span.get("text", "")
                if not text.strip():
                    continue
                spans_payload.append({
                    "line_id": line_id,
                    "span_id": span_id,
                    "text": text,
                    "bbox": [round(float(v), 2) for v in span.get("bbox", [])],
                    "font": span.get("font"),
                    "size": span.get("size"),
                    "flags": span.get("flags"),
                })
        if block_lines:
            blocks.append({
                "block_id": block_id,
                "bbox": [round(float(v), 2) for v in block.get("bbox", [])],
                "text": "\n".join(block_lines),
                "spans": spans_payload,
            })
    return blocks


def extract_lines_raw(pdf_path: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    # Returns (rows, meta).
    # Each row is one visible PDF line, e.g.:
    #   {"source_file": "iris_2024_30.pdf", "page_number": 4, "block_id": 2,
    #    "line_id": 0, "line_order": 0, "x0": 56.7, "y0": 144.0,
    #    "x1": 521.3, "y1": 156.4, "raw_line": "S.I. No. 142 of 2024.",
    #    "sizes": [10.0, 10.0], "fonts": ["TimesNewRoman-Bold"],
    #    "issue_date": "2024-04-12", "issue_number": 30, ...}
    # meta is a per-PDF audit record, e.g.:
    #   {"source_file": "iris_2024_30.pdf", "valid_pdf": True, "pages": 24,
    #    "total_chars": 38120, "issue_date": "2024-04-12", "issue_number": 30,
    #    "valid_iris_issue": True, ...}
    source_file = os.path.basename(pdf_path)
    try:
        doc = fitz.open(pdf_path)
    except Exception as e:
        return [], {"source_file": source_file, "valid_pdf": False, "error": str(e)}

    first_text = doc[0].get_text("text") if doc.page_count else ""
    issue_date, issue_date_text = parse_issue_date(first_text)
    issue_number = parse_issue_number(first_text)
    total_chars = 0
    image_count = 0
    page_text_char_counts: list[int] = []
    rows: list[dict[str, Any]] = []
    print(f"  Extracting {source_file} (pages: {doc.page_count}, issue_date: {issue_date}, issue_number: {issue_number})...")
    for pno, page in enumerate(doc, start=1):
        page_text = page.get_text("text")
        page_text_char_counts.append(len(page_text))
        total_chars += len(page_text)
        image_count += len(page.get_images(full=True))
        page_dict = page.get_text("dict")
        line_order = 0

        for block_id, block in enumerate(page_dict.get("blocks", [])):
            if "lines" not in block:
                continue
            for line_id, line in enumerate(block["lines"]):
                spans = line.get("spans", [])
                raw_line = "".join(s.get("text", "") for s in spans).strip()
                if not raw_line:
                    continue
                bbox = tuple(float(v) for v in line["bbox"])
                rows.append({
                    "source_file": source_file,
                    "page_number": pno,
                    "block_id": block_id,
                    "line_id": line_id,
                    "line_order": line_order,
                    "x0": bbox[0],
                    "y0": bbox[1],
                    "x1": bbox[2],
                    "y1": bbox[3],
                    "raw_line": raw_line,
                    "sizes": [s.get("size") for s in spans if s.get("size") is not None],
                    "fonts": [s.get("font", "") for s in spans if s.get("font")],
                    "issue_date": issue_date,
                    "issue_date_text": issue_date_text,
                    "issue_number": issue_number,
                })
                line_order += 1

    char_counts = page_text_char_counts or [0]
    meta = {
        "source_file": source_file,
        "valid_pdf": True,
        "pages": doc.page_count,
        "total_chars": total_chars,
        "images": image_count,
        "issue_date": issue_date,
        "issue_date_text": issue_date_text,
        "issue_number": issue_number,
        "min_page_chars": min(char_counts),
        "max_page_chars": max(char_counts),
        "mean_page_chars": round(sum(char_counts) / len(char_counts), 1),
        "valid_iris_issue": bool(issue_date and issue_number and "IRIS" in first_text.upper()),
    }
    doc.close()
    return rows, meta


def find_member_interest_page_ranges(pdf_path: str) -> list[dict[str, Any]]:
    # Side-channel: member-interest pages are dumped as raw JSON and intentionally
    # NOT folded into the structured record stream — their layout is too varied
    # to classify with the same rules.
    #
    # Two extract_types are produced:
    #   "section_29_member_interest_notice"  — single-member declarations under
    #       the Ethics in Public Office Act, opened by "Name of Member concerned: ..."
    #   "register_of_interests_supplement"   — the annual full register, runs
    #       from "REGISTER OF INTERESTS OF MEMBERS" / "CLÁR LEASA" pages until
    #       the next publication boilerplate footer page.
    #
    # Example output entry:
    #   {"source_file": "iris_2024_30.pdf", "issue_date": "2024-04-12",
    #    "extract_type": "section_29_member_interest_notice",
    #    "start_page": 8, "end_page": 9,
    #    "detected_member_names": ["Joan Murphy, T.D."],
    #    "pages": [ {"page_number": 8, "raw_text": "...", "blocks": [...]} ]}
    source_file = os.path.basename(pdf_path)
    try:
        doc = fitz.open(pdf_path)
    except Exception:
        return []

    first_text = doc[0].get_text("text") if doc.page_count else ""
    issue_date, _ = parse_issue_date(first_text)
    issue_number = parse_issue_number(first_text)

    page_texts = [page.get_text("text") for page in doc]
    marker_pages: list[int] = []
    full_register_starts: list[int] = []

    for idx, text in enumerate(page_texts):
        upper = text.upper()
        if "NAME OF MEMBER CONCERNED:" in upper:
            marker_pages.append(idx)
        if "REGISTER OF INTERESTS OF MEMBERS" in upper or "CLÁR LEASA" in upper or "CLAR LEASA" in upper:
            full_register_starts.append(idx)
        elif "ETHICS IN PUBLIC OFFICE" in upper and "SECTION 29" in upper:
            marker_pages.append(idx)

    ranges: list[tuple[int, int, str]] = []
    for idx in marker_pages:
        end = idx
        if idx + 1 < len(page_texts):
            nxt = page_texts[idx + 1].upper()
            if any(k in nxt for k in ("NAME OF MEMBER CONCERNED:", "ETHICS IN PUBLIC OFFICE", "ELECTORAL ACT 1997")):
                end = idx + 1
        ranges.append((idx, end, "section_29_member_interest_notice"))

    # Register supplement runs from its start page until the next publication
    # boilerplate page (or the end of the document).
    boilerplate = re.compile(BOILERPLATE_PATTERN, re.I)
    for start in full_register_starts:
        end = len(page_texts) - 1
        for j in range(start, len(page_texts)):
            if boilerplate.search(page_texts[j]):
                end = max(start, j - 1)
                break
        ranges.append((start, end, "register_of_interests_supplement"))

    ranges = sorted(set(ranges), key=lambda x: (x[0], x[1], x[2]))
    merged: list[tuple[int, int, str]] = []
    for s, e, t in ranges:
        if merged and s <= merged[-1][1] + 1 and t == merged[-1][2]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], e), t)
        else:
            merged.append((s, e, t))

    extracts: list[dict[str, Any]] = []
    name_re = re.compile(r"Name of Member concerned:\s*(.+)")
    for s, e, t in merged:
        pages_payload = []
        detected_names: list[str] = []
        for pidx in range(s, e + 1):
            page = doc[pidx]
            raw_text = page.get_text("text")
            for m in name_re.finditer(raw_text):
                name = m.group(1).strip()
                if name:
                    detected_names.append(name)
            pages_payload.append({
                "page_number": pidx + 1,
                "raw_text": raw_text,
                "blocks": extract_page_blocks(page),
            })
        extracts.append({
            "source_file": source_file,
            "issue_date": issue_date,
            "issue_number": issue_number,
            "extract_type": t,
            "start_page": s + 1,
            "end_page": e + 1,
            "detected_member_names": sorted(set(detected_names)),
            "pages": pages_payload,
        })

    doc.close()
    return extracts


# ---------------------------------------------------------------------------
# Polars expression helpers (return Expr; not UDFs)
# ---------------------------------------------------------------------------

def norm_space_expr(c: pl.Expr) -> pl.Expr:
    # Normalises NBSPs, smart quotes, en/em dashes, and runs of whitespace.
    # Example:
    #   "  S.I. No. 142  of 2024 — “Amendment”  "
    #   = "S.I. No. 142 of 2024 - \"Amendment\""
    return (
        c.str.replace_all(" ", " ", literal=True)
         .str.replace_all(r"[–—]", "-")
         .str.replace_all(r"[‘’]", "'")
         .str.replace_all(r"[“”]", '"')
         .str.replace_all(r"[ \t]+", " ")
         .str.strip_chars()
    )


def si_form_expr(c: pl.Expr) -> pl.Expr:
    # Maps an uppercase SI title/body to its instrument form.
    # Order matters — earlier rules win:
    #   "DISTRICT COURT (CIVIL PROCEDURE) RULES 2024"          = "rules"
    #   "DUBLIN CITY COUNCIL (PARKING) BYE-LAWS 2024"          = "bye_law"
    #   "JUDICIAL APPOINTMENTS COMMISSION (FEES) SCHEME 2024"  = "scheme"
    #   "CODE OF PRACTICE FOR THE GOVERNANCE OF ..."           = "code_of_practice"
    #   "EUROPEAN UNION (FOOD ADDITIVES) (AMENDMENT) REGULATIONS 2024" = "regulations"
    #   "SOCIAL WELFARE ACT 2024 (COMMENCEMENT) ORDER 2024"    = "order"
    return (
        pl.when(c.str.contains("RULES")).then(pl.lit("rules"))
          .when(c.str.contains_any(["BYE-LAW", "BYELAW", "BYE LAW"])).then(pl.lit("bye_law"))
          .when(c.str.contains("SCHEME")).then(pl.lit("scheme"))
          .when(c.str.contains("CODE OF PRACTICE")).then(pl.lit("code_of_practice"))
          .when(c.str.contains("REGULATIONS")).then(pl.lit("regulations"))
          .when(c.str.contains("ORDER")).then(pl.lit("order"))
          .otherwise(pl.lit(None, dtype=pl.String))
    )


def tag_or_none(condition: pl.Expr, tag: str) -> pl.Expr:
    # Used with concat_list + drop_nulls + unique to build multi-tag string columns
    # without per-row Python loops. Pattern:
    #   pl.concat_list([
    #       tag_or_none(cond_a, "tag_a"),     # = "tag_a" or null per row
    #       tag_or_none(cond_b, "tag_b"),
    #   ]).list.drop_nulls().list.unique(maintain_order=True).list.join("|")
    # produces e.g. "tag_a|tag_b" when both fire, "tag_a" when only one, "" when none.
    return pl.when(condition).then(pl.lit(tag)).otherwise(pl.lit(None, dtype=pl.String))


# ---------------------------------------------------------------------------
# Bronze frame assembly
# ---------------------------------------------------------------------------

BRONZE_OUT_COLS = [
    "source_file", "page_number", "block_id", "line_id", "line_order",
    "bbox", "x0", "y0", "x1", "y1",
    "font_size_mean", "font_names",
    "raw_line", "normalized_line",
    "issue_date", "issue_date_text", "issue_number",
    "ignore_publication_boilerplate",
]


def build_bronze_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    # One row per visible PDF text line. Example output row (after this fn):
    #   source_file:   "iris_2024_30.pdf"
    #   page_number:   4    block_id: 2    line_id: 0    line_order: 0
    #   bbox:          "[56.7, 144.0, 521.3, 156.4]"
    #   font_size_mean: 10.0
    #   font_names:    "TimesNewRoman-Bold"
    #   raw_line:      "S.I. No. 142  of 2024."        ← may have NBSPs etc.
    #   normalized_line: "S.I. No. 142 of 2024."        ← cleaned
    #   issue_date: "2024-04-12"   issue_number: 30
    #   ignore_publication_boilerplate: false
    if not rows:
        return pl.DataFrame()
    df = pl.DataFrame(rows)
    df = df.sort(["source_file", "page_number", "block_id", "line_id", "line_order"])
    df = df.with_columns(
        bbox=pl.format(
            "[{}, {}, {}, {}]",
            pl.col("x0").round(2), pl.col("y0").round(2),
            pl.col("x1").round(2), pl.col("y1").round(2),
        ),
        font_size_mean=pl.col("sizes").list.mean(),
        font_names=pl.col("fonts").list.unique().list.sort().list.join(";"),
        normalized_line=norm_space_expr(pl.col("raw_line")),
    )
    # Once the boilerplate footer appears on a page, every line below it on that
    # page is ignored. cum_max over (file, page) is the vectorised "stop here".
    df = df.with_columns(
        ignore_publication_boilerplate=(
            pl.col("normalized_line").str.contains(BOILERPLATE_PATTERN)
              .cum_max()
              .over(["source_file", "page_number"])
        )
    )
    return df.select(BRONZE_OUT_COLS)


# ---------------------------------------------------------------------------
# Silver record building (cum_sum + group_by replaces state machine)
# ---------------------------------------------------------------------------

def build_records(bronze: pl.DataFrame) -> pl.DataFrame:
    # Aggregates bronze lines into one row per inferred notice. Worked example
    # (showing how 5 bronze lines collapse into 2 silver records):
    #
    #   line_order  normalized_line                   is_delim  is_strong  group_id
    #   0           "S.I. No. 142 of 2024."           F         T          1
    #   1           "Social Welfare (Amendment) ..."  F         F          1
    #   2           "________________________"        T         F          2
    #   3           "FÓGRA POIBLÍ"                    F         T          3
    #   4           "Notice is hereby given..."       F         F          3
    #
    # group_by(group_id) then yields:
    #   group_id=1: raw_text="S.I. No. 142 of 2024.\nSocial Welfare ..."
    #               opened_by="strong_start", split_reason="underscore_delimiter"
    #   group_id=3: raw_text="FÓGRA POIBLÍ\nNotice is hereby given..."
    #               opened_by="strong_start", split_reason="eof"
    # (group_id=2 is the underscore line; it gets filtered out as content==empty.)
    if bronze.is_empty():
        return pl.DataFrame()

    df = bronze.with_columns(
        is_header=pl.col("normalized_line").str.contains(HEADER_RE),
        is_delimiter=pl.col("normalized_line").str.contains(DELIMITER_RE),
        is_strong_start=pl.col("normalized_line").str.contains(STRONG_START_RE),
    ).with_columns(
        record_break=pl.col("is_delimiter") | pl.col("is_strong_start"),
        break_kind=(
            pl.when(pl.col("is_delimiter")).then(pl.lit("underscore_delimiter"))
              .when(pl.col("is_strong_start")).then(pl.lit("strong_start"))
              .otherwise(pl.lit(None, dtype=pl.String))
        ),
    ).sort(["source_file", "page_number", "block_id", "line_id", "line_order"])

    # Each break flips the counter, so all lines of one notice share group_id.
    # Replaces v2's per-line state machine.
    df = df.with_columns(
        group_id=pl.col("record_break").cast(pl.Int64).cum_sum().over("source_file")
    )

    opened_by = (
        df.group_by(["source_file", "group_id"], maintain_order=True)
          .agg(opened_by=pl.col("break_kind").first())
    )

    content = df.filter(
        ~pl.col("is_delimiter") &
        ~pl.col("is_header") &
        ~pl.col("ignore_publication_boilerplate") &
        (pl.col("normalized_line").str.len_chars() > 0)
    )

    records = (
        content.group_by(["source_file", "group_id"], maintain_order=True)
        .agg(
            raw_text=pl.col("normalized_line").str.join("\n"),
            start_page=pl.col("page_number").min(),
            end_page=pl.col("page_number").max(),
            start_block_id=pl.col("block_id").first(),
            end_block_id=pl.col("block_id").last(),
            start_line_id=pl.col("line_id").first(),
            end_line_id=pl.col("line_id").last(),
            line_count=pl.len(),
            bbox_x0=pl.col("x0").min(),
            bbox_y0=pl.col("y0").min(),
            bbox_x1=pl.col("x1").max(),
            bbox_y1=pl.col("y1").max(),
            issue_date=pl.col("issue_date").first(),
            issue_number=pl.col("issue_number").first(),
        )
        .filter(pl.col("raw_text").str.len_chars() > 0)
    )

    # split_reason for record g = whatever opened group g+1 (i.e. what *ended* g);
    # "eof" if g is the last group in the file.
    next_opened = opened_by.rename({"group_id": "_next_gid", "opened_by": "_next_opened"})
    records = (
        records.with_columns(_next_gid=pl.col("group_id") + 1)
        .join(next_opened, on=["source_file", "_next_gid"], how="left")
        .with_columns(
            split_reason=pl.col("_next_opened").fill_null("eof"),
            bbox_union=pl.format(
                "[{}, {}, {}, {}]",
                pl.col("bbox_x0").round(2), pl.col("bbox_y0").round(2),
                pl.col("bbox_x1").round(2), pl.col("bbox_y1").round(2),
            ),
        )
        .drop(["_next_gid", "_next_opened", "group_id", "bbox_x0", "bbox_y0", "bbox_x1", "bbox_y1"])
    )

    return records.select([
        "source_file", "issue_date", "issue_number",
        "start_page", "end_page",
        "start_block_id", "start_line_id", "end_block_id", "end_line_id",
        "bbox_union", "raw_text", "split_reason", "line_count",
    ])


# ---------------------------------------------------------------------------
# Enrichment / classification / SI taxonomy (vectorised)
# ---------------------------------------------------------------------------

EVENTS_OUT_COLS = [
    "source_file", "issue_date", "issue_number",
    "start_page", "end_page",
    "start_block_id", "start_line_id", "end_block_id", "end_line_id",
    "bbox_union", "raw_text", "split_reason", "line_count",
    "notice_ref", "notice_section", "notice_number",
    "notice_category", "notice_subtype", "classification_flags",
    "si_number", "si_year",
    "title", "entity_name", "person_title_detected", "normalized_text",
    "extraction_confidence", "notes",
    "si_form", "si_operation_flags", "si_operation_primary",
    "si_eu_relationship", "si_policy_domains", "si_policy_domain_primary",
    "si_parent_legislation", "si_responsible_actor", "si_effective_date_text",
    "si_taxonomy_confidence", "si_taxonomy_notes",
    "eisb_url",
]


def enrich_records(records: pl.DataFrame) -> pl.DataFrame:
    if records.is_empty():
        return pl.DataFrame()

    # Extract SI number/year: "S.I. No. 142 of 2024" = si_number=142, si_year=2024.
    # Non-SI rows get nulls (cast strict=False).
    df = records.with_columns(
        text_upper=pl.col("raw_text").str.to_uppercase(),
        normalized_text=norm_space_expr(pl.col("raw_text").str.replace_all("\n", " ")),
        si_number=pl.col("raw_text").str.extract(
            r"(?i)\bS\.I\.\s*No\.\s*(\d+)\s+of\s+\d{4}", 1
        ).cast(pl.Int64, strict=False),
        si_year=pl.col("raw_text").str.extract(
            r"(?i)\bS\.I\.\s*No\.\s*\d+\s+of\s+(\d{4})", 1
        ).cast(pl.Int64, strict=False),
    )

    # notice_ref — Iris notices end with their bracketed reference, so take the
    # last match (earlier brackets are usually quotes or cross-references).
    # Examples:
    #   "...This Order applies. [FIN-1234]"     = notice_ref="FIN-1234"
    #                                            = notice_section="FIN", notice_number="1234"
    #   "...as defined in [12345A]"             = notice_ref="12345A"
    #                                            = notice_section=null, notice_number="12345A"
    df = df.with_columns(
        notice_ref=(
            pl.col("raw_text")
              .str.extract_all(r"\[(?:[A-Z]+-\d+[A-Z]?|\d+[A-Z]?)\]")
              .list.last()
              .str.strip_chars("[]")
        )
    ).with_columns(
        notice_section=(
            pl.when(pl.col("notice_ref").str.contains("-", literal=True))
              .then(pl.col("notice_ref").str.split("-").list.first())
              .otherwise(pl.lit(None, dtype=pl.String))
        ),
        notice_number=(
            pl.when(pl.col("notice_ref").str.contains("-", literal=True))
              .then(pl.col("notice_ref").str.split("-").list.last())
              .otherwise(pl.col("notice_ref"))
        ),
    )

    # title — SI heuristic via regex; non-SI heuristic via line-list manipulation.
    # SI example:
    #   raw_text = "S.I. No. 142 of 2024.\nSOCIAL WELFARE (CONSOLIDATED CLAIMS,\n
    #               PAYMENTS AND CONTROL) (AMENDMENT) (NO. 5)\nREGULATIONS 2024\n
    #               The Minister for Social Protection..."
    #   = title_si grabs lines between "S.I. No. ..." and "The Minister",
    #     joins+trims = "SOCIAL WELFARE (CONSOLIDATED CLAIMS, PAYMENTS AND
    #                    CONTROL) (AMENDMENT) (NO. 5) REGULATIONS 2024"
    # Non-SI example:
    #   raw_text = "[ABC-99]\nIN THE HIGH COURT\nCompanies Act 2014\nIn the matter of XYZ Limited"
    #   = title_non_si drops the "[ABC-99]" line and joins the next 4
    #     non-empty non-bracket lines with " | "
    #     = "IN THE HIGH COURT | Companies Act 2014 | In the matter of XYZ Limited"
    title_si = (
        pl.col("raw_text").str.extract(
            r"(?si)^S\.I\.\s*No\.[^\n]*\n([^\[]+?)(?:\n(?:The Minister|These Regulations|This Order|Copies of|Under the|The purpose|EXPLANATORY NOTE|\(This note)|\n\n|\z)",
            1,
        )
        .str.split("\n")
        .list.head(10)
        .list.eval(pl.element().str.strip_chars())
        .list.eval(pl.element().filter(pl.element().str.len_chars() > 0))
        .list.join(" ")
        .str.strip_chars(" .")
    )
    title_non_si = (
        pl.col("raw_text").str.split("\n")
          .list.eval(pl.element().str.strip_chars())
          .list.eval(pl.element().filter((pl.element().str.len_chars() > 0) & ~pl.element().str.starts_with("[")))
          .list.head(4)
          .list.join(" | ")
    )
    df = df.with_columns(
        is_si_record=pl.col("raw_text").str.contains(r"^S\.I\.\s*No\."),
    ).with_columns(
        title=pl.when(pl.col("is_si_record")).then(title_si).otherwise(title_non_si)
    ).with_columns(
        title=pl.when(pl.col("title").str.len_chars() > 0).then(pl.col("title")).otherwise(pl.lit(None, dtype=pl.String))
    )

    # ---- Classification (last-rule-wins overrides; early-return rules applied LAST) ----
    # Each block below sets (notice_category, notice_subtype) ONLY where its
    # condition fires; otherwise it keeps the previous value via .otherwise(pl.col(...)).
    # So the LAST matching rule wins — which is why ordering is load-bearing.
    #
    # Worked classification examples (final values after the whole chain):
    #   "S.I. No. 142 of 2024 ... AMENDMENT ..."
    #     = category="statutory_instrument", subtype="statutory_instrument_multi_axis"
    #   "FISHERIES MANAGEMENT NOTICE NO. 5 ... QUOTA ..."
    #     = category="fisheries_notice", subtype="quota_management"
    #   "IN THE HIGH COURT ... COMPANIES ACT 2014 ... LIQUIDATOR ... CREDITORS' VOLUNTARY..."
    #     = category="corporate_insolvency", subtype="creditors_voluntary_liquidation"
    #   "404 NOT FOUND"
    #     = category="invalid_source", subtype="404_or_non_issue" (early-return wins)
    t = pl.col("text_upper")

    df = df.with_columns(
        notice_category=pl.lit("other"),
        notice_subtype=pl.lit("unknown"),
    )

    # IRISH STANDARDS
    df = df.with_columns(
        notice_category=pl.when(t.str.contains("IRISH STANDARDS")).then(pl.lit("standards")).otherwise(pl.col("notice_category")),
        notice_subtype=pl.when(t.str.contains("IRISH STANDARD REVOCATIONS")).then(pl.lit("irish_standards_and_revocations"))
                       .when(t.str.contains("IRISH STANDARDS")).then(pl.lit("irish_standards"))
                       .otherwise(pl.col("notice_subtype")),
    )

    # SI
    has_si = t.str.contains(r"\bS\.I\.\s+NO\.\s*\d+\s+OF\s+\d{4}")
    df = df.with_columns(
        notice_category=pl.when(has_si).then(pl.lit("statutory_instrument")).otherwise(pl.col("notice_category")),
        notice_subtype=pl.when(has_si).then(pl.lit("statutory_instrument_multi_axis")).otherwise(pl.col("notice_subtype")),
    )

    # FISHERIES
    has_fisheries = t.str.contains("FISHERIES MANAGEMENT NOTICE")
    df = df.with_columns(
        notice_category=pl.when(has_fisheries).then(pl.lit("fisheries_notice")).otherwise(pl.col("notice_category")),
        notice_subtype=pl.when(has_fisheries & t.str.contains("QUOTA")).then(pl.lit("quota_management"))
                       .when(has_fisheries).then(pl.lit("fisheries_management_notice"))
                       .otherwise(pl.col("notice_subtype")),
    )

    # BANKRUPTCY
    has_bankrupt = t.str.contains_any(["BANKRUPT", "ADJUDICATED BANKRUPT"])
    df = df.with_columns(
        notice_category=pl.when(has_bankrupt).then(pl.lit("bankruptcy")).otherwise(pl.col("notice_category")),
        notice_subtype=pl.when(has_bankrupt).then(pl.lit("bankruptcy_adjudication")).otherwise(pl.col("notice_subtype")),
    )

    # MVL/CVL detection — corporate insolvency disambiguation.
    # Iris notices use formulaic phrases that distinguish:
    #   "members' voluntary liquidation"   = solvent wind-up      (MVL)
    #   "creditors' voluntary liquidation" = insolvent wind-up    (CVL)
    #   "by reason of its liabilities" / "cannot continue in business" = CVL
    # has_companies_act gates the precise subtype; without that anchor we still
    # tag liquidator-only notices but with subtype "liquidation_unspecified".
    explicit_mvl = (
        t.str.contains(r"MEMBERS?['\s]+VOLUNTARY\s+(?:LIQUIDATION|WINDING|WINDING UP)")
        | t.str.contains(r"\(IN\s+MEMBERS?['\s]+VOLUNTARY LIQUIDATION\)")
    )
    explicit_cvl = (
        t.str.contains(r"CREDITORS?['\s]+VOLUNTARY\s+(?:LIQUIDATION|WINDING|WINDING UP)")
        | t.str.contains_any(["BY REASON OF ITS LIABILITIES", "CANNOT CONTINUE IN BUSINESS"])
    )
    has_companies_act = t.str.contains_any(["COMPANIES ACT 2014", "COMPANIES ACTS 2014", "THE COMPANIES ACTS"])
    has_liquidator_kw = t.str.contains_any(["LIQUIDATOR", "VOLUNTARY LIQUIDATION", "VOLUNTARY WINDING"])

    df = df.with_columns(
        notice_category=(
            pl.when(has_companies_act & t.str.contains("RECEIVER")).then(pl.lit("corporate_insolvency"))
              .when(has_companies_act & t.str.contains_any(["PROCESS ADVISER", "RESCUE PROCESS"])).then(pl.lit("corporate_rescue"))
              .when(has_companies_act & t.str.contains("HIGH COURT") & t.str.contains_any(["WIND", "LIQUIDATOR"])).then(pl.lit("corporate_insolvency"))
              .when(has_companies_act & explicit_cvl).then(pl.lit("corporate_insolvency"))
              .when(has_companies_act & explicit_mvl).then(pl.lit("corporate_insolvency"))
              .when(has_companies_act & t.str.contains_any(["VOLUNTARY LIQUIDATION", "VOLUNTARY WINDING", "LIQUIDATOR"])).then(pl.lit("corporate_insolvency"))
              .when(has_companies_act).then(pl.lit("corporate_notice"))
              .when(~has_companies_act & has_liquidator_kw).then(pl.lit("corporate_insolvency"))
              .otherwise(pl.col("notice_category"))
        ),
        notice_subtype=(
            pl.when(has_companies_act & t.str.contains("RECEIVER")).then(pl.lit("receivership"))
              .when(has_companies_act & t.str.contains_any(["PROCESS ADVISER", "RESCUE PROCESS"])).then(pl.lit("scarp_process_adviser"))
              .when(has_companies_act & t.str.contains("HIGH COURT") & t.str.contains_any(["WIND", "LIQUIDATOR"])).then(pl.lit("court_winding_up"))
              .when(has_companies_act & explicit_cvl).then(pl.lit("creditors_voluntary_liquidation"))
              .when(has_companies_act & explicit_mvl).then(pl.lit("members_voluntary_liquidation"))
              .when(has_companies_act & t.str.contains_any(["VOLUNTARY LIQUIDATION", "VOLUNTARY WINDING", "LIQUIDATOR"])).then(pl.lit("voluntary_liquidation_unspecified"))
              .when(has_companies_act).then(pl.lit("companies_act_notice"))
              .when(~has_companies_act & has_liquidator_kw & explicit_cvl).then(pl.lit("creditors_voluntary_liquidation"))
              .when(~has_companies_act & has_liquidator_kw & explicit_mvl).then(pl.lit("members_voluntary_liquidation"))
              .when(~has_companies_act & has_liquidator_kw).then(pl.lit("liquidation_unspecified"))
              .otherwise(pl.col("notice_subtype"))
        ),
    )

    # AGREEMENTS ENTERED INTO FORCE
    has_agree = t.str.contains("AGREEMENTS WHICH ENTERED INTO FORCE")
    df = df.with_columns(
        notice_category=pl.when(has_agree).then(pl.lit("international_agreement_notice")).otherwise(pl.col("notice_category")),
        notice_subtype=pl.when(has_agree).then(pl.lit("agreement_entered_into_force")).otherwise(pl.col("notice_subtype")),
    )

    # SPECIAL ADVISER / generic appointment fallback.
    # has_sa is a HARD override (always wins, even over earlier rules) because
    # special-adviser appointments are politically significant and we never want
    # them filed as "other".
    # has_appt is a SOFT fallback — only takes effect when category is still
    # "other", so a "Companies Act ... appointment of receiver" stays as
    # corporate_insolvency.
    has_sa = t.str.contains_any(["APPOINTMENT OF SPECIAL ADVISER", "APPOINTMENT OF SPECIAL ADVISERS", "COMHAIRLEOIR"])
    has_appt = t.str.contains_any(["APPOINTMENT", "RE-APPOINTMENT", "APPOINTED", "REAPPOINTED", "A CHEAPADH", "A ATHCHEAPADH", "BUANSANNADH"])
    df = df.with_columns(
        # special-adviser hard override; otherwise generic appointment only when category is still "other"
        notice_category=(
            pl.when(has_sa).then(pl.lit("public_appointment"))
              .when(~has_sa & has_appt & (pl.col("notice_category") == "other")).then(pl.lit("public_appointment"))
              .otherwise(pl.col("notice_category"))
        ),
        notice_subtype=(
            pl.when(has_sa).then(pl.lit("special_adviser_appointment"))
              .when(~has_sa & has_appt & (pl.col("notice_category") == "other")).then(pl.lit("appointment_or_assignment"))
              .otherwise(pl.col("notice_subtype"))
        ),
    )

    # MEMBER INTERESTS
    has_mi = t.str.contains_any(["ETHICS IN PUBLIC OFFICE", "REGISTER OF INTERESTS", "NAME OF MEMBER CONCERNED:"])
    df = df.with_columns(
        notice_category=pl.when(has_mi).then(pl.lit("member_interests")).otherwise(pl.col("notice_category")),
        notice_subtype=pl.when(has_mi).then(pl.lit("raw_member_interest_extract_required")).otherwise(pl.col("notice_subtype")),
    )

    # ICAV
    has_icav = t.str.contains_any(["ICAV ACT 2015", "VOLUNTARY STRIKE OFF"])
    df = df.with_columns(
        notice_category=pl.when(has_icav).then(pl.lit("investment_vehicle_register_notice")).otherwise(pl.col("notice_category")),
        notice_subtype=pl.when(has_icav).then(pl.lit("icav_voluntary_strike_off")).otherwise(pl.col("notice_subtype")),
    )

    # LOCAL AUTHORITY BYE-LAWS / SPEED LIMIT
    is_council = t.str.contains_any(["COUNTY COUNCIL", "CITY COUNCIL"])
    is_byelaw = t.str.contains_any(["BYE-LAWS", "BYE LAWS", "SPECIAL SPEED LIMIT"])
    df = df.with_columns(
        notice_category=pl.when(is_council & is_byelaw).then(pl.lit("local_authority_notice")).otherwise(pl.col("notice_category")),
        notice_subtype=pl.when(is_council & is_byelaw & t.str.contains("SPEED LIMIT")).then(pl.lit("speed_limit_bye_law"))
                       .when(is_council & is_byelaw).then(pl.lit("local_bye_law"))
                       .otherwise(pl.col("notice_subtype")),
    )

    # DORMANT ACCOUNTS
    has_dormant = t.str.contains_any(["DORMANT ACCOUNTS", "CUNTAIS DÍOMHAOIN", "GUNTAS DÍOMHAOIN"])
    df = df.with_columns(
        notice_category=pl.when(has_dormant).then(pl.lit("financial_public_notice")).otherwise(pl.col("notice_category")),
        notice_subtype=pl.when(has_dormant).then(pl.lit("dormant_accounts_notice")).otherwise(pl.col("notice_subtype")),
    )

    # RTFO
    has_rtfo = t.str.contains("RENEWABLE TRANSPORT FUEL OBLIGATION") & t.str.contains("DETERMINATION NOTICE")
    df = df.with_columns(
        notice_category=pl.when(has_rtfo).then(pl.lit("sectoral_regulatory_notice")).otherwise(pl.col("notice_category")),
        notice_subtype=pl.when(has_rtfo).then(pl.lit("renewable_transport_fuel_obligation")).otherwise(pl.col("notice_subtype")),
    )

    # Early-return overrides — applied LAST so they win over everything
    has_admin = t.str.contains("ALL NOTICES AND ADVERTISEMENTS ARE PUBLISHED")
    has_404 = t.str.contains("404 NOT FOUND")
    df = df.with_columns(
        notice_category=pl.when(has_admin).then(pl.lit("publication_admin")).otherwise(pl.col("notice_category")),
        notice_subtype=pl.when(has_admin).then(pl.lit("boilerplate")).otherwise(pl.col("notice_subtype")),
    ).with_columns(
        notice_category=pl.when(has_404).then(pl.lit("invalid_source")).otherwise(pl.col("notice_category")),
        notice_subtype=pl.when(has_404).then(pl.lit("404_or_non_issue")).otherwise(pl.col("notice_subtype")),
    )

    # Classification flags — semicolon-joined multi-tag string, e.g.:
    #   "eu_derived_or_eu_related;person_title_detected"
    # Each tag is independent of (category, subtype) and surfaces orthogonal
    # signals downstream UIs use for filtering.
    eu_si_markers = (pl.col("notice_category") == "statutory_instrument") & t.str.contains_any(
        ["EUROPEAN UNION", "EUROPEAN COMMUNITIES", "DIRECTIVE (EU)", "REGULATION (EU)"]
    )
    has_person_title_marker = (
        pl.col("raw_text").str.contains(r"\b(?:MR|MS|MRS|DR)\.?\s+[A-Z]")
        | pl.col("raw_text").str.contains(r"\sT\.?D\b")
    )

    df = df.with_columns(
        classification_flags=(
            pl.concat_list([
                tag_or_none(eu_si_markers, "eu_derived_or_eu_related"),
                tag_or_none(has_companies_act, "companies_act_2014_exclusion_from_si"),
                tag_or_none(t.str.contains_any(["FÓGRA", "FOGRA"]), "irish_language_notice_or_notice_label"),
                tag_or_none(has_person_title_marker, "person_title_detected"),
                tag_or_none(pl.col("notice_category") == "member_interests", "quarantine_raw_json_extract"),
                tag_or_none(has_admin, "ignore"),
                tag_or_none(has_404, "exclude_file"),
            ])
            .list.drop_nulls()
            .list.unique(maintain_order=True)
            .list.join(";")
        )
    )

    # entity_name — extract the company name from corporate notices.
    # entity_pat matches a whole line containing a company-form keyword:
    #   "ACME WIDGETS LIMITED"                              = match
    #   "BRIGHT IDEAS DAC"                                  = match
    #   "GREEN ENERGY ICAV"                                 = match
    # bad_pat then drops false positives (statute references, addresses, etc.):
    #   "COMPANIES ACT 2014"                                = rejected (statute)
    #   "DUBLIN, IRELAND"                                   = rejected (address)
    #   "VOLUNTARY LIQUIDATION OF ACME LIMITED"             = rejected (descriptor)
    # For SI rows, entity_name=title (the regulation IS the entity).
    # For others, prefer the extracted company name, fall back to title.
    entity_pat = (
        r"(?im)^[^\n]*?\b(?:LIMITED|LTD|DAC|DESIGNATED ACTIVITY COMPANY|PLC|PUBLIC LIMITED COMPANY|"
        r"ICAV|UNLIMITED COMPANY|CLG|COMPANY LIMITED BY GUARANTEE)\b[^\n]*$"
    )
    bad_pat = (
        r"(?i)COMPANIES ACT|GOVERNMENT PUBLICATIONS|SOLICITOR|LIQUIDATOR|DUBLIN|"
        r"IN THE MATTER|VOLUNTARY LIQUIDATION|WINDING"
    )
    df = df.with_columns(
        _entity_candidate=(
            pl.col("raw_text").str.extract_all(entity_pat)
              .list.eval(pl.element().filter(~pl.element().str.contains(bad_pat)))
              .list.eval(pl.element().str.strip_chars())
              .list.first()
        )
    ).with_columns(
        entity_name=(
            pl.when(pl.col("notice_category") == "statutory_instrument")
              .then(pl.col("title"))
              .otherwise(pl.coalesce([pl.col("_entity_candidate"), pl.col("title")]))
        )
    ).drop("_entity_candidate")

    # person_title_detected — full extracted titles, deduplicated, "; "-joined.
    # Examples:
    #   "...signed by Mr Joseph O'Brien T.D., Minister..."
    #     = "Mr Joseph O'Brien T.D.; Minister Joseph O'Brien"
    #   "...the Honourable Ms Justice Mary Smith..."
    #     = "Ms Justice Mary Smith"
    # Empty strings are coerced to null so downstream `is_not_null()` checks work.
    df = df.with_columns(
        person_title_detected=(
            pl.col("raw_text").str.extract_all(
                r"\b(?:Mr|Ms|Mrs|Dr|Minister|Deputy)\.?\s+[A-ZÁÉÍÓÚ][A-Za-zÁÉÍÓÚáéíóú'\-]+(?:\s+[A-ZÁÉÍÓÚ][A-Za-zÁÉÍÓÚáéíóú'\-]+){1,4}(?:,?\s*T\.?D\.?|,?\s*TD)?"
            )
            .list.unique(maintain_order=True)
            .list.join("; ")
        )
    ).with_columns(
        person_title_detected=(
            pl.when(pl.col("person_title_detected").str.len_chars() > 0)
              .then(pl.col("person_title_detected"))
              .otherwise(pl.lit(None, dtype=pl.String))
        )
    )

    # extraction_confidence — additive score capped at 0.99.
    # Examples:
    #   classified row + has notice_ref + SI with si_number  = 0.95 + 0.02 + 0.02 = 0.99
    #   classified row only                                  = 0.95
    #   "other" + nothing else                               = 0.40 (will be quarantined)
    df = df.with_columns(
        extraction_confidence=(
            pl.when(pl.col("notice_category") != "other").then(pl.lit(0.95)).otherwise(pl.lit(0.40))
            + pl.when(pl.col("notice_ref").is_not_null()).then(pl.lit(0.02)).otherwise(pl.lit(0.0))
            + pl.when(
                (pl.col("notice_category") == "statutory_instrument") & pl.col("si_number").is_not_null()
            ).then(pl.lit(0.02)).otherwise(pl.lit(0.0))
        )
    ).with_columns(
        extraction_confidence=pl.min_horizontal([pl.col("extraction_confidence"), pl.lit(0.99)]).round(2)
    )

    df = df.with_columns(
        notes=pl.when(pl.col("notice_category") == "other")
                .then(pl.lit("Needs taxonomy rule or manual review"))
                .otherwise(pl.lit(""))
    )

    # ---- SI taxonomy ----
    # Strip purchase/contact boilerplate so supplier addresses don't pollute
    # domain or actor matching downstream. Example removed tail:
    #   "Copies of this Order may be purchased from the Government Publications
    #    Office, Phone: 046-9742000, Price: €5.00"
    # Without stripping, "FINANCE" in the supplier address would falsely match
    # the finance_banking_tax domain.
    boil_split = (
        r"(?si)(?:Copies of (?:the above|this Statutory Instrument|the Regulations|this Order) may be purchased|"
        r"Copies of this Order may be obtained|Phone:\s*046|Price:?\s*€).*"
    )
    df = df.with_columns(
        _si_text_core=pl.col("raw_text").str.replace(boil_split, "")
    ).with_columns(
        _si_core_upper=pl.col("_si_text_core").str.to_uppercase(),
        _title_upper=pl.col("title").fill_null("").str.to_uppercase(),
    )

    # SI form: title takes priority — body often quotes the parent instrument's
    # form (e.g. "Regulations" inside an Order), which would mis-classify.
    df = df.with_columns(
        si_form=pl.coalesce([
            si_form_expr(pl.col("_title_upper")),
            si_form_expr(pl.col("_si_core_upper")),
        ])
    )

    # SI operations — list of detected flags (raw, before default fallback).
    # For an SI titled "... (AMENDMENT) (COMMENCEMENT) Regulations 2024" this
    # produces _si_ops_raw = ["amendment", "commencement"], primary "amendment"
    # (the order from SI_OPERATION_RULES wins via list.first after dedup).
    op_exprs = [
        tag_or_none(
            pl.col("_title_upper").str.contains_any(toks)
            | pl.col("_si_core_upper").str.contains_any(toks),
            flag,
        )
        for flag, toks in SI_OPERATION_RULES
    ]
    df = df.with_columns(
        _si_ops_raw=pl.concat_list(op_exprs).list.drop_nulls().list.unique(maintain_order=True)
    )

    # Default to ["substantive_or_base_instrument"] if no operation matched —
    # preserves v2 semantics so downstream queries don't see nulls here.
    default_op = pl.concat_list([pl.lit("substantive_or_base_instrument")])
    df = df.with_columns(
        _si_ops_list=pl.when(pl.col("_si_ops_raw").list.len() == 0)
                       .then(default_op)
                       .otherwise(pl.col("_si_ops_raw"))
    ).with_columns(
        si_operation_flags=pl.col("_si_ops_list").list.join("|"),
        si_operation_primary=pl.col("_si_ops_list").list.first(),
    )

    # Policy domains
    domain_exprs = [
        tag_or_none(
            pl.col("_title_upper").str.contains_any(toks)
            | pl.col("_si_core_upper").str.contains_any(toks),
            domain,
        )
        for domain, toks in POLICY_DOMAIN_RULES.items()
    ]
    df = df.with_columns(
        _si_domains=pl.concat_list(domain_exprs).list.drop_nulls().list.unique(maintain_order=True)
    ).with_columns(
        si_policy_domains=pl.col("_si_domains").list.join("|"),
        si_policy_domain_primary=pl.col("_si_domains").list.first(),
    )

    # EU relationship — five orthogonal markers, joined with "|".
    # Examples:
    #   Title "... RESTRICTIVE MEASURES (UKRAINE) ... COUNCIL REGULATION (EU) ..."
    #     "eu_restrictive_measures|eu_title_or_legal_basis|eu_instrument_referenced"
    #   Title "EUROPEAN UNION (FOOD ADDITIVES) (AMENDMENT) REGULATIONS"
    #     "eu_title_or_legal_basis"
    #   Pure-domestic SI   =  "none_detected"
    tu = pl.col("_title_upper")
    cu = pl.col("_si_core_upper")
    eu_exprs = [
        tag_or_none(
            (tu.str.contains("RESTRICTIVE MEASURES") | cu.str.contains("RESTRICTIVE MEASURES"))
            & (tu.str.contains_any(["EUROPEAN UNION", "COUNCIL REGULATION"]) | cu.str.contains_any(["EUROPEAN UNION", "COUNCIL REGULATION"])),
            "eu_restrictive_measures",
        ),
        tag_or_none(
            tu.str.contains_any(["FOR THE PURPOSE OF GIVING FULL EFFECT", "GIVING FULL EFFECT"])
            | cu.str.contains_any(["FOR THE PURPOSE OF GIVING FULL EFFECT", "GIVING FULL EFFECT"]),
            "eu_full_effect",
        ),
        tag_or_none(
            tu.str.contains_any(["FOR THE PURPOSE OF GIVING FURTHER EFFECT", "GIVING FURTHER EFFECT"])
            | cu.str.contains_any(["FOR THE PURPOSE OF GIVING FURTHER EFFECT", "GIVING FURTHER EFFECT"]),
            "eu_further_effect",
        ),
        tag_or_none(
            tu.str.contains_any(["EUROPEAN UNION", "EUROPEAN COMMUNITIES"])
            | cu.str.contains_any(["EUROPEAN UNION", "EUROPEAN COMMUNITIES"]),
            "eu_title_or_legal_basis",
        ),
        tag_or_none(
            tu.str.contains_any(["DIRECTIVE (EU)", "COUNCIL DIRECTIVE", "REGULATION (EU)", "COUNCIL REGULATION"])
            | cu.str.contains_any(["DIRECTIVE (EU)", "COUNCIL DIRECTIVE", "REGULATION (EU)", "COUNCIL REGULATION"]),
            "eu_instrument_referenced",
        ),
    ]
    df = df.with_columns(
        _si_eu=pl.concat_list(eu_exprs).list.drop_nulls().list.unique(maintain_order=True)
    ).with_columns(
        si_eu_relationship=pl.when(pl.col("_si_eu").list.len() > 0)
                             .then(pl.col("_si_eu").list.join("|"))
                             .otherwise(pl.lit("none_detected"))
    )

    # Parent acts — every "<Title-Case Words> Act <year>" run, plus a special
    # case for the European Communities Act 1972 (the catch-all enabling act
    # for transposing EU instruments).
    # Example body:
    #   "...made under section 3 of the Social Welfare Consolidation Act 2005
    #    and the European Communities Act, 1972..."
    #   = si_parent_legislation = "Social Welfare Consolidation Act 2005|European Communities Act, 1972"
    df = df.with_columns(
        si_parent_legislation=(
            pl.col("_si_text_core").str.extract_all(r"\b[A-Z][A-Za-z& ,()\-'/]+? Act \d{4}\b")
              .list.concat(pl.col("_si_text_core").str.extract_all(r"(?i)\bEuropean Communities Act,?\s*1972\b"))
              .list.eval(pl.element().str.replace_all(r"[ \t]+", " "))
              .list.eval(pl.element().str.strip_chars(" .,;"))
              .list.unique(maintain_order=True)
              .list.join("|")
        )
    )

    # Effective date text — captures the *phrase*, not a parsed date (date
    # parsing is left to downstream consumers since the phrasing is varied).
    # Example matches:
    #   "comes into operation on 1 May 2024"
    #   "with effect from the 15th day of March 2024"
    #   "appoints the 1st day of April 2024 as the day"
    #   "for the period commencing on 1 January 2025"
    df = df.with_columns(
        si_effective_date_text=(
            pl.col("_si_text_core").str.extract_all(
                r"(?i)(?:come(?:s)? into operation on\s+[^\.\n]+|with effect from\s+[^\.\n]+|appoints?\s+the\s+[^\.\n]+?\s+as\s+the\s+day|for the period commencing on\s+[^\.\n]+)"
            )
            .list.eval(pl.element().str.replace_all(r"[ \t]+", " "))
            .list.eval(pl.element().str.strip_chars())
            .list.unique(maintain_order=True)
            .list.join("|")
        )
    )

    # Responsible actor — the body that issued the SI.
    # Example matches (joined with "|" if multiple):
    #   "The Minister for Social Protection"
    #   "The Minister of State at the Department of Finance"
    #   "The Taoiseach"
    #   "The Government"
    #   "The Commission for Communications Regulation"
    df = df.with_columns(
        si_responsible_actor=(
            pl.col("_si_text_core").str.extract_all(
                r"(?:The Minister for [^,\n]+|[A-Z][A-Za-z& ]+, Minister for [^,\n]+|The Commission for Communications Regulation|University College Dublin, National University of Ireland, Dublin|The Taoiseach|The Government|The Minister of State at the Department of [^,\n]+)"
            )
            .list.eval(pl.element().str.replace_all(r"[ \t]+", " "))
            .list.eval(pl.element().str.strip_chars())
            .list.unique(maintain_order=True)
            .list.join("|")
        )
    )

    # SI taxonomy confidence and notes.
    # Confidence starts at 0.82 and subtracts penalties for missing axes:
    #   no si_form          -0.20
    #   no operation flags  -0.15
    #   no policy domain    -0.15
    # Then clipped to [0.10, 0.99]. So a fully-tagged SI scores 0.82; one with
    # only form detected scores ~0.52; an SI with no signals at all scores 0.32.
    # si_taxonomy_notes is a "; "-joined string of caveats, e.g.:
    #   "Multi-operation SI; do not treat subtype as mutually exclusive;
    #    Multi-domain SI; preserve all domain tags"
    df = df.with_columns(
        si_taxonomy_confidence=(
            pl.lit(0.82)
            - pl.when(pl.col("si_form").is_null()).then(pl.lit(0.20)).otherwise(pl.lit(0.0))
            - pl.when(pl.col("_si_ops_raw").list.len() == 0).then(pl.lit(0.15)).otherwise(pl.lit(0.0))
            - pl.when(pl.col("_si_domains").list.len() == 0).then(pl.lit(0.15)).otherwise(pl.lit(0.0))
        ).clip(0.10, 0.99).round(2),
        si_taxonomy_notes=(
            pl.concat_list([
                tag_or_none(pl.col("si_form").is_null(), "SI form not detected from title/text"),
                tag_or_none(pl.col("_si_ops_raw").list.len() == 0, "No operation flags detected; may be substantive/base regulation"),
                tag_or_none(pl.col("_si_domains").list.len() == 0, "No policy domain detected"),
                tag_or_none(pl.col("_si_ops_raw").list.len() > 1, "Multi-operation SI; do not treat subtype as mutually exclusive"),
                tag_or_none(pl.col("_si_domains").list.len() > 1, "Multi-domain SI; preserve all domain tags"),
            ]).list.drop_nulls().list.join("; ")
        ),
    )

    # SI taxonomy is computed unconditionally for vectorisation, then masked off
    # on non-SI rows — checking notice_category mid-pipeline would require
    # row-wise branching that Polars can't elide.
    si_cols = [
        "si_form", "si_operation_flags", "si_operation_primary",
        "si_eu_relationship", "si_policy_domains", "si_policy_domain_primary",
        "si_parent_legislation", "si_responsible_actor", "si_effective_date_text",
        "si_taxonomy_confidence", "si_taxonomy_notes",
    ]
    df = df.with_columns([
        pl.when(pl.col("notice_category") == "statutory_instrument")
          .then(pl.col(c))
          .otherwise(pl.lit(None))
          .alias(c)
        for c in si_cols
    ])

    df = df.with_columns(
        eisb_url=pl.when(
            (pl.col("notice_category") == "statutory_instrument")
            & pl.col("si_year").is_not_null()
            & pl.col("si_number").is_not_null()
        )
        .then(pl.format(
            "https://www.irishstatutebook.ie/eli/{}/si/{}/made/en/html",
            pl.col("si_year"),
            pl.col("si_number"),
        ))
        .otherwise(pl.lit(None, dtype=pl.String))
    )

    # Drop intermediates and project to canonical schema
    return df.drop([
        "text_upper", "is_si_record",
        "_si_text_core", "_si_core_upper", "_title_upper",
        "_si_ops_raw", "_si_ops_list", "_si_domains", "_si_eu",
    ]).select(EVENTS_OUT_COLS)


# ---------------------------------------------------------------------------
# Quarantine (single vectorised when/then)
# ---------------------------------------------------------------------------

def add_quarantine(events: pl.DataFrame, threshold: float) -> pl.DataFrame:
    if events.is_empty():
        return events
    # when/then order encodes priority: the first matching reason wins.
    # Categorical reasons (other / member_interests / invalid_source) take
    # precedence over numeric heuristics (low confidence, oversized records).
    #
    # Possible quarantine_reason values and what triggers them:
    #   "unclassified_other"                       — rules failed, needs review
    #   "member_interests_raw_json_only"           — handled by side-channel JSON
    #   "invalid_source"                           — 404 page, not a real issue
    #   "low_confidence"                           — extraction_confidence < threshold (default 0.75)
    #   "very_large_record_possible_split_failure" — line_count > 120 (likely two notices fused)
    #   "multi_page_record_review"                 — record spans 3+ pages
    #   "multiple_notice_refs_in_record"           — 3+ distinct [XXX-NNN] refs
    # is_quarantined is just `quarantine_reason IS NOT NULL`.
    return (
        events.with_columns(
            _ref_count=pl.col("raw_text").str.extract_all(r"\[[A-Z]+-\d+[A-Z]?\]").list.unique().list.len()
        )
        .with_columns(
            quarantine_reason=(
                pl.when(pl.col("notice_category") == "other").then(pl.lit("unclassified_other"))
                  .when(pl.col("notice_category") == "member_interests").then(pl.lit("member_interests_raw_json_only"))
                  .when(pl.col("notice_category") == "invalid_source").then(pl.lit("invalid_source"))
                  .when(pl.col("extraction_confidence") < threshold).then(pl.lit("low_confidence"))
                  .when(pl.col("line_count") > 120).then(pl.lit("very_large_record_possible_split_failure"))
                  .when((pl.col("end_page") - pl.col("start_page")) >= 3).then(pl.lit("multi_page_record_review"))
                  .when(pl.col("_ref_count") >= 3).then(pl.lit("multiple_notice_refs_in_record"))
                  .otherwise(pl.lit(None, dtype=pl.String))
            )
        )
        .with_columns(is_quarantined=pl.col("quarantine_reason").is_not_null())
        .drop("_ref_count")
    )


# ---------------------------------------------------------------------------
# IO / orchestration
# ---------------------------------------------------------------------------

# Columns stripped from every gold-layer write. Bronze keeps everything
# (geometry + fonts are line-level provenance and belong in bronze only).
# - bbox / x0/y0/x1/y1 / bbox_union: layout-engine internals
# - font_size_mean / font_name(s):    layout-engine internals
# - split_reason:                     internal flag for record-break attribution
# - end_line_id:                      bronze-line index, redundant in gold
GOLD_DROP_COLS = [
    "bbox", "bbox_union",
    "x0", "y0", "x1", "y1",
    "bbox_x0", "bbox_y0", "bbox_x1", "bbox_y1",
    "font_size_mean", "font_name", "font_names",
    "split_reason",
    "end_line_id",
]

# Columns whose contents may carry embedded \n from the original PDF. CSV
# viewers (Excel, IDE previews) render those as fake row-breaks, making
# inspection painful even though the CSV is technically correct. Replacing
# \n with " // " collapses each gold row to one physical line per data row.
GOLD_FLATTEN_TEXT_COLS = (
    "raw_text",
    "person_title_detected",
    "si_parent_legislation",
    "si_effective_date_text",
)


def shape_for_gold(df: pl.DataFrame) -> pl.DataFrame:
    """Strip bronze-only columns and flatten multi-line text cells for gold writes.
    Safe on empty frames and on frames that lack any of the targeted columns."""
    if df.is_empty():
        return df
    out = df.drop([c for c in GOLD_DROP_COLS if c in df.columns])
    flatten_exprs = [
        pl.col(c).str.replace_all("\r\n", "\n").str.replace_all("\n", " // ").alias(c)
        for c in GOLD_FLATTEN_TEXT_COLS
        if c in out.columns
    ]
    return out.with_columns(flatten_exprs) if flatten_exprs else out


# Backwards-compatible alias — earlier code referenced `drop_geom_font_cols`.
drop_geom_font_cols = shape_for_gold


def write_dimensions(out_dir: str, dfs: dict[str, pl.DataFrame], member_extracts: list[dict[str, Any]], paths: list[str]) -> None:
    dims = {
        "source_pdf_count": len(paths),
        "bronze_grain": "one row per extracted visible text line from PyMuPDF page.get_text('dict'); uniquely identified by source_file, page_number, block_id, line_id, line_order, with bbox and font metadata",
        "silver_notice_grain": "one row per inferred notice/event record split by underscore delimiter and strong-start markers; records can span pages and may require quarantine review",
        "member_interest_json_grain": "one JSON extract per detected member-interest page range; each extract contains page-level raw_text and block/span bbox payloads",
        "dataframes": {
            name: {
                "rows": int(df.height),
                "columns": int(df.width),
                "column_names": list(df.columns),
            }
            for name, df in dfs.items()
        },
        "member_interest_extract_count": len(member_extracts),
        "member_interest_page_count": sum(len(e.get("pages", [])) for e in member_extracts),
    }
    Path(out_dir, "iris_dataset_dimensions.json").write_text(
        json.dumps(dims, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def run(paths: list[str], out_dir: str, confidence_threshold: float = 0.75) -> None:
    os.makedirs(out_dir, exist_ok=True)

    all_rows: list[dict[str, Any]] = []
    metas: list[dict[str, Any]] = []
    member_extracts: list[dict[str, Any]] = []

    total_pdfs = len(paths)
    print(f"[1/5] Extracting text from {total_pdfs} PDFs...")
    for idx, path in enumerate(paths, start=1):
        print(f"[{idx}/{total_pdfs}] {os.path.basename(path)}")
        rows, meta = extract_lines_raw(path)
        metas.append(meta)
        all_rows.extend(rows)
        mi_extracts = find_member_interest_page_ranges(path)
        if mi_extracts:
            print(f"  Found {len(mi_extracts)} member-interest extract range(s)")
        member_extracts.extend(mi_extracts)
    print(f"  Collected {len(all_rows)} raw lines across {total_pdfs} PDFs ({len(member_extracts)} member-interest extracts)")

    print(f"[2/5] Building bronze frame from {len(all_rows)} lines...")
    bronze = build_bronze_frame(all_rows)
    print(f"  Bronze: {bronze.height} rows, {bronze.width} cols")

    print("[3/5] Building silver records (cum_sum group_by)...")
    records = build_records(bronze) if not bronze.is_empty() else pl.DataFrame()
    print(f"  Records: {records.height} rows")

    print("[4/5] Enriching records (classification + SI taxonomy)...")
    events = enrich_records(records) if not records.is_empty() else pl.DataFrame()
    print(f"  Enriched events: {events.height} rows")

    print(f"[5/5] Applying quarantine flags (threshold={confidence_threshold})...")
    events = add_quarantine(events, confidence_threshold) if not events.is_empty() else events
    if not events.is_empty():
        q_count = int(events.filter(pl.col("is_quarantined")).height)
        print(f"  Quarantined: {q_count} / {events.height} rows")

    audit = pl.DataFrame(metas) if metas else pl.DataFrame()

    if events.is_empty():
        clean = events
        quarantine = events
        si_tax = events
    else:
        clean = events.filter(~pl.col("is_quarantined"))
        quarantine = events.filter(pl.col("is_quarantined"))
        si_tax = events.filter(pl.col("notice_category") == "statutory_instrument")

    audit.write_csv(os.path.join(out_dir, "iris_pdf_audit.csv"))
    bronze.write_csv(os.path.join(out_dir, "iris_raw_lines_pymupdf.csv"))
    # Gold-layer writes drop bronze-only columns and flatten multi-line text
    # so each row occupies a single physical CSV line.
    events_gold = shape_for_gold(events)
    clean_gold = shape_for_gold(clean)
    quarantine_gold = shape_for_gold(quarantine)
    si_tax_gold = shape_for_gold(si_tax)
    events_gold.write_csv(os.path.join(out_dir, "iris_notice_events_all.csv"))
    clean_gold.write_csv(os.path.join(out_dir, "iris_notice_events_clean.csv"))
    quarantine_gold.write_csv(os.path.join(out_dir, "iris_notice_events_quarantined.csv"))
    si_tax_gold.write_csv(os.path.join(out_dir, "iris_si_taxonomy.csv"))

    Path(out_dir, "iris_member_interests_raw_pages.json").write_text(
        json.dumps(member_extracts, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    write_dimensions(
        out_dir,
        {
            "pdf_audit": audit,
            "bronze_raw_lines": bronze,
            "notice_events_all": events_gold,
            "notice_events_clean": clean_gold,
            "notice_events_quarantined": quarantine_gold,
            "si_taxonomy": si_tax_gold,
        },
        member_extracts,
        paths,
    )

    print(f"Wrote {audit.height} PDF audit rows")
    print(f"Wrote {bronze.height} bronze raw line rows")
    print(f"Wrote {events.height} notice/event rows")
    print(f"Wrote {clean.height} clean rows")
    print(f"Wrote {quarantine.height} quarantined rows")
    print(f"Wrote {si_tax.height} SI taxonomy rows")
    print(f"Wrote {len(member_extracts)} member-interest raw JSON extracts")


# Default input — every Iris PDF in bronze. When the script is run with no
# arguments (e.g. clicked from an IDE) we fall back to this so we always
# process the full historical corpus rather than erroring out.
DEFAULT_INPUT_GLOB = str(
    Path(__file__).resolve().parents[1] / "data" / "bronze" / "iris_oifigiuil" / "*.pdf"
)
DEFAULT_OUT_DIR = str(Path(__file__).resolve().parent / "out")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "paths",
        nargs="*",
        default=[DEFAULT_INPUT_GLOB],
        help=f"PDF files or glob patterns (default: {DEFAULT_INPUT_GLOB})",
    )
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    parser.add_argument("--confidence-threshold", type=float, default=0.75)
    args = parser.parse_args()

    paths: list[str] = []
    for pat in args.paths:
        matches = glob.glob(pat)
        paths.extend(matches if matches else [pat])
    paths = list(dict.fromkeys(paths))
    # Drop the 146-byte 404 stubs — they're not real PDFs and crash fitz.
    paths = [p for p in paths if not (os.path.exists(p) and os.path.getsize(p) < 5_000)]
    if not paths:
        parser.error(f"No PDFs matched any of: {args.paths}")
    print(f"Processing {len(paths)} PDFs -> {args.out_dir}")
    run(paths, args.out_dir, args.confidence_threshold)


if __name__ == "__main__":
    main()
