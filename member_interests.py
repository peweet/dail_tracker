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
import polars as pl
import regex

import normalise_join_key
from config import GOLD_DIR, INTERESTS_PDF_DIR, MEMBERS_DIR, SILVER_DIR

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PDF_PATHS: dict[str, pathlib.Path] = {
    # Seanad
    "2020_seanad": INTERESTS_PDF_DIR / "2021-03-16_register-of-members-interests-seanad-eireann_en.pdf",
    "2021_seanad": INTERESTS_PDF_DIR / "2022-02-25_register-of-members-interests-seanad-eireann_en.pdf",
    "2022_seanad": INTERESTS_PDF_DIR / "2023-02-24_register-of-members-interests-seanad-eireann_en.pdf",
    "2023_seanad": INTERESTS_PDF_DIR / "2024-02-27_register-of-members-interests-seanad-eireann-2023_en.pdf",
    "2024_seanad": INTERESTS_PDF_DIR / "2025-02-27_register-of-member-s-interests-seanad-eireann-2024_en.pdf",
    "2025_seanad": INTERESTS_PDF_DIR / "2026-03-10_register-of-member-s-interests-seanad-eireann-2025_en.pdf",
    # DAIL
    "2020_dail": INTERESTS_PDF_DIR / "2021-02-25_register-of-members-interests-dail-eireann_en.pdf",
    "2021_dail": INTERESTS_PDF_DIR / "2022-02-16_register-of-members-interests-dail-eireann_en.pdf",
    "2022_dail": INTERESTS_PDF_DIR / "2023-02-22_register-of-member-s-interests-dail-eireann-2022_en.pdf",
    "2023_dail": INTERESTS_PDF_DIR / "2024-02-21_register-of-member-s-interests-dail-eireann-2023_en.pdf",
    "2024_dail": INTERESTS_PDF_DIR / "2025-02-27_register-of-member-s-interests-dail-eireann-2024_en.pdf",
    "2025_dail": INTERESTS_PDF_DIR / "2026-02-25_register-of-member-s-interests-dail-eireann-2025_en.pdf",
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

MASTER_TD_PATH = SILVER_DIR / "flattened_members.csv"
MASTER_SEANAD_PATH = SILVER_DIR / "flattened_seanad_members.csv"
MINISTER_PATH = GOLD_DIR / "enriched_td_attendance.csv"


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
        lines = [l for l in lines if l.strip() and not PAGE_FOOTER_RE.search(l)]
        text_boxes.append(lines)

    flat = [item for sublist in text_boxes for item in sublist]
    return flat[header_skip : len(flat) - footer_skip]


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
            r"(Occupations|Shares|Directorships|Land|Gifts|Property supplied or lent or a Service supplied|Travel Facilities|Remunerated Position|Contracts)",
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
        pl.col("interest_code").replace(INTEREST_CODE_MAP, default=pl.col("interest_code")).alias("interest_category")
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
        pl.when((pl.col('interest_code') == "1") & (pl.col('interest_description_cleaned') != "No interests declared")).then(pl.lit(True)).otherwise(pl.lit(False)).alias("is_occupation")
    ).with_columns(
        pl.when((pl.col('interest_code') == "1") & (pl.col('interest_description_cleaned') != "No interests declared")).then(pl.col('interest_description_cleaned')).otherwise(pl.lit("N/A")).alias("occupation_description")
    )

    return df


# ---------------------------------------------------------------------------
# Join
# ---------------------------------------------------------------------------
def join_master_list(
    df: pl.DataFrame,
    master_path: pathlib.Path,
    minister_path: pathlib.Path | None = None,
) -> pl.DataFrame:
    """Join cleaned interests against the master member list and optional ministerial office lookup."""
    df = normalise_join_key.normalise_df_td_name(df, "join_key")

    master = (
        pl.read_csv(master_path)
        .select(["unique_member_code", "first_name", "last_name", "constituency_name", "full_name", "party", "ministerial_office", 'year_elected'])
        .unique()
    )
    master = master.with_columns(pl.concat_str(pl.col(["first_name", "last_name"])).alias("join_key"))
    master = normalise_join_key.normalise_df_td_name(master, "join_key")

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
        frames.append(pl.read_csv(path).with_columns(year_declared=pl.lit(numeric_year)))
    combined = (
        pl.concat(frames)
        .filter(
            ~pl.col("interest_code")
            .cast(pl.String)
            .is_in(["2019", "2020", "2021", "2022", "2023", "2024", "2025", "2026"])
        )
        .sort(["unique_member_code", "year_declared", "interest_code"])
    )
    out_path = silver_dir / f"{case}_member_interests_combined.csv"
    combined.write_csv(out_path)
    parquet_dir = silver_dir / "parquet"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = parquet_dir / f"{case}_member_interests_combined.parquet"
    combined.write_parquet(parquet_path)
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

        # 1. Extract
        lines = extract_raw_lines(pdf_path)

        # 1b. Split any lines where a member name is embedded mid-line
        lines = split_embedded_names(lines)

        # 2. Group
        grouped = group_lines(lines, CATEGORIES_PATTERN, MEMBER_NAME_PATTERN)

        # 3. Parse
        members = parse_members(grouped, MEMBER_NAME_PATTERN)
        print(f"  Parsed {len(members)} members")

        # 4. Write intermediate JSON, load into Polars
        json_path = MEMBERS_DIR / f"{pdf_path.stem}_{case.lower()}.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(members, f, indent=4, ensure_ascii=False)
        df = pl.read_json(json_path)

        # 5. Clean — pass numeric year so the rogue-row filter works correctly
        df = clean_interests(df, numeric_year)

        # 6. Join
        if case == "DAIL":
            df = join_master_list(df, MASTER_TD_PATH, MINISTER_PATH)
            dail_years.append(year_key)
        else:
            df = join_master_list(df, MASTER_SEANAD_PATH)
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

    print("\n=== Member interest pipeline complete ===")


if __name__ == "__main__":
    main()
