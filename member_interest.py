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
The result is a structured dataset of members and their declared interests, suitable for downstream analysis.

Planned refactor: break into self-contained functions and run through main().
See stub definitions below — implement and test one at a time.
# RISK: each function boundary is a good place to add an assertion or a small unit test
#       before wiring them together in main(). Recommended order: extract → group → parse → clean → join → combine.
# OPPORTUNITY: once refactored, pipeline.py can import and call main() directly,
#              or call individual steps for partial re-runs (e.g. re-clean without re-extracting).
"""

import fitz  # PyMuPDF
import pathlib
import json
import re
import regex
import os
import polars as pl
import normalise_join_key
from config import DATA_DIR, MEMBERS_DIR

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SILVER_DIR = DATA_DIR / "silver"

PDF_PATHS: dict[str, pathlib.Path] = {
    #Seanad
    "2020_seanad": MEMBERS_DIR / "pdf_member_interest" / "2021-03-16_register-of-members-interests-seanad-eireann_en.pdf",
    "2021_seanad": MEMBERS_DIR / "pdf_member_interest" / "2022-02-25_register-of-members-interests-seanad-eireann_en.pdf",
    "2022_seanad": MEMBERS_DIR / "pdf_member_interest" / "2023-02-24_register-of-members-interests-seanad-eireann_en.pdf",
    "2023_seanad": MEMBERS_DIR / "pdf_member_interest" / "2024-02-27_register-of-members-interests-seanad-eireann-2023_en.pdf",
    "2024_seanad": MEMBERS_DIR / "pdf_member_interest" / "2025-02-27_register-of-member-s-interests-seanad-eireann-2024_en.pdf",
    "2025_seanad": MEMBERS_DIR / "pdf_member_interest" / "2026-03-10_register-of-member-s-interests-seanad-eireann-2025_en.pdf",
    #DAIL
    "2020_dail": MEMBERS_DIR / "pdf_member_interest" / "2021-02-25_register-of-members-interests-dail-eireann_en.pdf",
    "2021_dail": MEMBERS_DIR / "pdf_member_interest" / "2022-02-16_register-of-members-interests-dail-eireann_en.pdf",
    "2022_dail": MEMBERS_DIR / "pdf_member_interest" / "2023-02-22_register-of-member-s-interests-dail-eireann-2022_en.pdf",
    "2023_dail": MEMBERS_DIR / "pdf_member_interest" / "2024-02-21_register-of-member-s-interests-dail-eireann-2023_en.pdf",
    "2024_dail": MEMBERS_DIR / "pdf_member_interest" / "2025-02-27_register-of-member-s-interests-dail-eireann-2024_en.pdf",
    "2025_dail": MEMBERS_DIR / "pdf_member_interest" / "2026-02-25_register-of-member-s-interests-dail-eireann-2025_en.pdf",
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

MASTER_TD_PATH = DATA_DIR / "silver" / "flattened_members.csv"
MASTER_SEANAD_PATH = DATA_DIR / "silver" / "flattened_seanad_members.csv"
MINISTER_PATH = MEMBERS_DIR / "enriched_td_attendance.csv"


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------

def extract_raw_lines(pdf_path: pathlib.Path, header_skip: int = 8, footer_skip: int = 5) -> list[str]:
    """Open PDF, flatten all pages into a single list of non-blank lines, trim headers/footers."""
    doc = fitz.open(pdf_path)
    print(f"Processing: {pdf_path.name} ({doc.page_count} pages)")
    text_boxes = []
    for page in doc:
        text = page.get_text(option="text").strip()
        text_boxes.append(text.splitlines(False))

    flat = [item for sublist in text_boxes for item in sublist]
    result = list(filter(str.strip, flat))
    return result[header_skip: len(result) - footer_skip]


# ---------------------------------------------------------------------------
# Group
# ---------------------------------------------------------------------------

def group_lines(
    lines: list[str],
    categories: re.Pattern,
    member_name: regex.Pattern,
) -> list[str]:
    """Concatenate continuation lines onto their parent block; return one string per block."""
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
    """Walk grouped blocks and build [{name, interests}] — one dict per TD."""
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


# ---------------------------------------------------------------------------
# Clean
# ---------------------------------------------------------------------------

def clean_interests(df: pl.DataFrame, year: int) -> pl.DataFrame:
    """Apply all Polars cleaning steps and return a cleaned DataFrame."""
    df = df.explode("interests")
    df = df.with_columns(
        pl.col("name")
        .str.split(by=r"\s{2,}", literal=False)
        .alias("name_and_constituency")
    )
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
        .str.replace_all(
            r"(Etc|including property|Property supplied |or lent  or a Service supplied  )", ""
        )
        .str.replace_all(
            r"(Occupations|Shares|Directorships|Land|Gifts|Property supplied or lent or a Service supplied|Travel Facilities|Remunerated Position|Contracts)",
            "",
        )
        .str.replace_all(r'"Etc\b', "")
        .str.strip_chars_start()
        .str.replace_all(r"\s+", " ")
        .str.replace_all(r"[.…]+", "")
        .str.replace_all(r"^\s+", "")
        .str.replace_all(r"\b[1-9]\b", "")
        .str.replace_all(r"\s*\(\)\s*", " ")
        .str.replace_all(r" {2,}", " ")
        .str.replace_all(r'["""]', "")
        .str.replace(r"(or lent or a Service supplied |or lent or a service supplied No interests declared|or lent or a service supplied No interests declared)", "No interests declared")
        .str.replace_all(r"or lent\s*Nil?\s*or a Service supplied\s*Nil?", "Nil")
        .str.replace_all(r" {2,}", " ")
        .str.replace(r"^\(\)\s*", "")
        .str.replace(r'^"', "")
        .str.replace(r'^"', "")
        .str.strip_chars_start("-:;,. ")
        .str.strip_chars_start("etc")
        .str.replace(r'"$', "")
        .str.replace_all(r"^(│Dr.|dr|dr.|prof|mr|mrs|ms|miss|bl)\s+", "")
        .str.replace(r"Nil|Níl|Níl aon rud|etc Níl aon rud|Neamh-fheidhme|Neamh-infheidhme|Neamh infheidhme|Infheidhme", "No interests declared")
        .str.replace(r"No interests declared\s+\d{2}$", "No interests declared")
        .str.replace(r"No interests declared\s+\d{3}$", "No interests declared")
        .str.replace(r"(lord:|lord)", "Landlord:")
        .str.strip_chars()
        .alias("interest_description_cleaned")
    )

    df = df.with_columns(
        pl.col("interest_code")
        .replace(INTEREST_CODE_MAP, default=pl.col("interest_code"))
        .alias("interest_category")
    )

    df = df.with_columns(
        pl.col("full_name").list.get(0).alias("last_name"),
        pl.col("full_name").list.get(1).alias("first_name"),
    ).drop("full_name")

    df = df.with_columns(
        pl.concat_str(pl.col(["first_name", "last_name"])).alias("join_key")
    ).filter(
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

    df = df.with_columns(
        pl.col("split_interests").str.strip_chars().alias("interest_description_cleaned")
    ).drop("split_interests")

    df = df.with_columns(
        pl.when(
            (~pl.col("interest_description_cleaned").is_in(
                ["ceased", "no rental income", "I own no land or residences"]
            ))
            & (
                pl.col("interest_description_cleaned")
                .str.contains("let|rented|ARP|Léasóir|etc Lessor|Rental|rent received|Leasóir|Léasóir|lord|letting|renting|rental|HAP|RAS Scheme|lessor|Lessor|lord:")
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
    df = df.with_columns(
        pl.concat_str(pl.col(["first_name", "last_name"])).alias("join_key")
    )
    df = df.with_columns(
        pl.when(
            (pl.col("interest_code") == "4") & (pl.col("is_landlord") == True)
            | (
                (pl.col("interest_code") == "4")
                & (pl.col("interest_description_cleaned") != "No interests declared")
            )
        )
        .then(pl.lit("TRUE"))
        .otherwise(pl.lit("FALSE"))
        .alias("is_property_owner")
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

    master = pl.read_csv(master_path).select(
        ["unique_member_code", "first_name", "last_name", "constituency_name", "full_name", "party"]
    ).unique()
    master = master.with_columns(
        pl.concat_str(pl.col(["first_name", "last_name"])).alias("join_key")
    )
    master = normalise_join_key.normalise_df_td_name(master, "join_key")

    result = (
        master.join(df, on="join_key", how="left")
        .with_columns(
            pl.when(pl.col("interest_code").is_null())
            .then(pl.lit("unregistered"))
            .otherwise(pl.lit("registered"))
            .alias("registration_status")
        )
        .with_columns(
            pl.col("unique_member_code").str.extract(r"\b\d{4}\b", 0).alias("year_elected")
        )
        .drop("join_key")
    )
    drop_cols = ["first_name_right", "interests", "name", "interest_description_raw", "last_name_right"]
    if minister_path is not None:
        ministers = pl.read_csv(minister_path).select(
            ["unique_member_code", "ministerial_office_filled"]
        ).unique(subset=["unique_member_code"])
        result = result.join(ministers, on="unique_member_code", how="left")
    result = result.drop([c for c in drop_cols if c in result.columns])
    result = result.with_columns(
        pl.col("interest_flag").sum().over("unique_member_code").alias("interest_count")
    ).drop("interest_flag")
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
        frames.append(
            pl.read_csv(path).with_columns(year_declared=pl.lit(numeric_year))
        )
    combined = (
        pl.concat(frames)
        .filter(
            ~pl.col("interest_code").cast(pl.String).is_in(["2019", "2020", "2021", "2022", "2023", "2024", "2025", "2026"])
        )
        .sort(["unique_member_code", "year_declared", "interest_code"])
    )
    out_path = silver_dir / f"{case}_member_interests_combined.csv"
    combined.write_csv(out_path)
    print(f"Saved combined: {out_path}")
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

        # Clean up intermediate JSON
        if os.path.exists(json_path):
            os.remove(json_path)
            print(f"  Removed intermediate JSON: {json_path.name}")

    # 8. Combine dail and seanad years separately (different schemas)
    print("\n--- Combining years ---")
    if dail_years:
        combine_years(SILVER_DIR, dail_years, "dail")
    if seanad_years:
        combine_years(SILVER_DIR, seanad_years, "seanad")

    print("\n=== Member interest pipeline complete ===")


if __name__ == "__main__":
    main()



# import fitz  # PyMuPDF
# import pathlib
# import json
# import re
# import regex
# import os
# import polars as pl
# import normalise_join_key
# from config import DATA_DIR, MEMBERS_DIR


# ---------------------------------------------------------------------------
# CURRENT SCRIPT (to be replaced by main() once refactor is complete)
# ---------------------------------------------------------------------------
#double check years and file paths below before running — it's easy to accidentally mix up 2024 and 2025 PDFs, which will silently produce wrong outputs (e.g. 2024 CSV with 2025 data, which will then be filtered out in the combine step, resulting in an empty combined file).
# member_interest_2014= MEMBERS_DIR / "pdf_member_interest" / "2015-03-11_register-of-members-interests-dail-eireann_en.pdf"
# member_interest_2015= MEMBERS_DIR / "pdf_member_interest" / "2016-03-01_register-of-members-interests-dail-eireann_en.pdf"
# member_interest_2016 = MEMBERS_DIR / "pdf_member_interest" / "2017-03-10_register-of-members-interests-dail-eireann_en.pdf"
# member_interest_2017=MEMBERS_DIR / "pdf_member_interest" / "2018-02-14_register-of-members-interests-dail-eireann_en.pdf"
# member_interest_2018= MEMBERS_DIR / "pdf_member_interest" / "2019-02-13_register-of-members-interests-dail-eireann_en.pdf"

# member_interest_2019= MEMBERS_DIR / "pdf_member_interest" / "2020-03-03_register-of-members-interests-dail-eireann_en.pdf"
# member_interest_2020 = MEMBERS_DIR / "pdf_member_interest" / "2021-02-25_register-of-members-interests-dail-eireann_en.pdf"


# member_interest_2020 = MEMBERS_DIR / "pdf_member_interest" / "2021-02-25_register-of-members-interests-dail-eireann_en.pdf"
# member_interest_2021 = MEMBERS_DIR / "pdf_member_interest" / "2022-02-16_register-of-members-interests-dail-eireann_en.pdf"
# member_interest_2022 = MEMBERS_DIR / "pdf_member_interest" / "2023-02-22_register-of-member-s-interests-dail-eireann-2022_en.pdf"
# member_interest_2023 = MEMBERS_DIR / "pdf_member_interest" / "2024-02-21_register-of-member-s-interests-dail-eireann-2023_en.pdf"
# member_interest_2024 = MEMBERS_DIR / "pdf_member_interest" / "2025-02-27_register-of-member-s-interests-dail-eireann-2024_en.pdf"
# member_interest_2025 = MEMBERS_DIR / "pdf_member_interest" / "2026-02-25_register-of-member-s-interests-dail-eireann-2025_en.pdf"
# member_interest = [
# # member_interest_2014,
# # member_interest_2015,
# # member_interest_2016,
# # member_interest_2017,
# # member_interest_2018,
# # member_interest_2019, 
# member_interest_2020, member_interest_2021, member_interest_2022, member_interest_2023, member_interest_2024, member_interest_2025]

# for member_interest_pdf in member_interest:
#     year = 2020 if member_interest_pdf == member_interest_2020 else 2021 if member_interest_pdf == member_interest_2021 else 2022 if member_interest_pdf == member_interest_2022 else 2023 if member_interest_pdf == member_interest_2023 else 2024 if member_interest_pdf == member_interest_2024 else 2025

#     categories = re.compile(r"^\d+\.\s")       # "1. ", "2. " etc.
#     member_name = regex.compile(r"^\p{Lu}[\p{Lu}'\-]*(?:\s\p{Lu}[\p{Lu}'\-]*)*,\s") # 

#     print("Starting to process member interest PDF...")
#     doc = fitz.open(member_interest_pdf)
#     print(f"Processing file: {member_interest_pdf} with {doc.page_count} pages...")

#     # Extract and flatten all text lines.
#     # The strip method removes leading/trailing whitespace from extracted text.
#     # The splitlines method (keepends=False) splits text into lines, removing newline characters.
#     # This gives us a list of lines to process further, grouping by member and their interests.
#     text_boxes = []
#     for page in doc:
#         text = page.get_text(option="text")
#         text = text.strip()
#         lines = text.splitlines(False)
#         text_boxes.append(lines)

#     # Flatten the list of lists into a single list of lines.
#     # Necessary because text extraction gives a list of lines per page —
#     # flattening lets us apply grouping logic in one pass across all pages.
#     flat = []
#     for sublist in text_boxes:
#         for item in sublist:
#             flat.append(item)

#     # Remove empty/whitespace-only lines and trim header/footer.
#     # Slice removes the first 8 lines (headers) and last 5 lines (footers/disclaimers)
#     # based on manual inspection of the PDF structure, ensuring only member interest
#     # content is processed.
#     result = list(filter(str.strip, flat))
#     result = result[8:-5]

#     # Group fragmented lines together.
#     # If a line matches a category pattern ("1. ", "2. ") or a member name ("SMITH, John"),
#     # it signals the start of a new block — save the current group and start a new one.
#     # Otherwise, concatenate the line to the current group (continuation of previous entry).
#     # This handles member interests split across multiple lines in the PDF.
#     grouped = []
#     current = ""

#     for line in result:
#         if categories.match(line) or member_name.match(line):
#             if current.strip():  # Save current group before starting a new one
#                 grouped.append(current.strip())
#             current = line
#         else:
#             # Continuation of current member's interests — concatenate
#             current = current + " " + line

#     # Append any remaining text — the last member's interests won't have been
#     # added yet since there's no subsequent entry to trigger the append in the loop.
#     if current.strip():
#         grouped.append(current.strip())

#     # Structure into members with their interests.
#     # Each member_name match signals a new TD entry.
#     # All subsequent lines until the next member name are that TD's interests.
#     # TODO: can flatten_service.py replace this grouping logic? Similar problem of
#     #       grouping fragmented lines — could reuse logic and make this more maintainable.
#     members = []
#     current_member = None
#     for line in grouped:
#         # A line matching the member name pattern signals the start of a new entry.
#         if member_name.match(line):
#             # Save the previous member entry before starting a new one.
#             if current_member:
#                 members.append(current_member)
#             # Start a new member dict with name and empty interests list.
#             current_member = {"name": line, "interests": []}
#         # Non-matching lines are interests belonging to the current member.
#         elif current_member is not None:
#             current_member["interests"].append(line)
#     # After the loop, add the last member being built (no subsequent entry to trigger append).
#     if current_member:
#         members.append(current_member)

#     # Save intermediate JSON output — use .stem so the filename is just the PDF name,
#     # not the full path object serialised as a string.
#     output_path = MEMBERS_DIR / f"{member_interest_pdf.stem}.json"
#     with open(output_path, "w", encoding="utf-8") as f:
#         json.dump(members, f, indent=4, ensure_ascii=False)

#     df = pl.read_json(output_path)
#     df = df.explode("interests")
#     df = df.with_columns(
#         pl.col('name').str.split(
#             by=r"\s{2,}", 
#             literal=False
#             ).alias('name_and_constituency'))

#     df = df.with_columns(
#         pl.col('name_and_constituency').list.get(0).alias('name'),
#         pl.col('name_and_constituency').list.get(1).alias('constituency')
#     ).drop('name_and_constituency')
#     df = df.with_columns(
#         pl.col('constituency').str.strip_chars("()"),
#         pl.col('name').str.split(
#             by=r",", 
#             literal=True
#             ).alias('full_name'),
#         pl.col('interests').str.splitn(
#             by=r".", 
#             n=2).alias('interests_code')
#     ).unnest("interests_code")
#     df = df.rename({'field_0': 'interest_code', 
#                     'field_1': 'interest_description_raw'})
#     df = df.with_columns(
#         pl.col('interests')
#         .str.strip_chars_start('" ')
#         .str.strip_chars_end('"')
#         .str.replace_all(r"\xa0", " ")
#         .str.replace_all(
#             r"(Etc|including property|Property supplied |or lent  or a Service supplied  )",
#             ""
#         )
#         #TODO: handle characters at the start of a string ': ', '- ', ' 84'
#         .str.replace_all(
#             r"(Occupations|Shares|Directorships|Land|Gifts|Property supplied or lent or a Service supplied|Travel Facilities|Remunerated Position|Contracts)",
#             ""
#         ).str.replace_all(r'"Etc\b', ""
#         ).str.strip_chars_start()
#         .str.replace_all(r"\s+", " ")
#         .str.replace_all(r"[.…]+", "")
#         .str.replace_all(r"^\s+", "")
#         .str.replace_all(r"\b[1-9]\b", "")
#         .str.replace_all(r"\s*\(\)\s*", " ")
#         .str.replace_all(r" {2,}", " ")
#         .str.replace_all(r'["""]', "")
#         .str.replace_all(r'or lent or a Service supplied ', '')
#         .str.replace_all(r"or lent\s*Nil?\s*or a Service supplied\s*Nil?", "Nil")
#         .str.replace_all(r" {2,}", " ") # 
#         .str.replace(r"^\(\)\s*", "")
#         .str.replace(r'^"', "")
#         .str.replace(r'^"', "")
#         .str.replace(r'"$', "")
#         .str.replace_all(r"^(│Dr.|dr|dr.|prof|mr|mrs|ms|miss|bl)\s+", "")
#         .str.replace(r"Nil|Neamh-fheidhme|Neamh-infheidhme|Neamh infheidhme|Infheidhme", "No interests declared")
#         .str.replace(r"No interests declared\s+\d{2}$", "No interests declared")
#         .str.replace(r"No interests declared\s+\d{3}$", "No interests declared")
#         .str.replace(r"(lord:|lord)", "Landlord:")
#         .str.strip_chars()
#         .alias('interest_description_cleaned')
#     )
#     df = df.with_columns(
#         pl.when(pl.col('interest_code') == "1").then(pl.lit("Occupations"))
#         .when(pl.col('interest_code')   == "2").then(pl.lit("Shares"))
#         .when(pl.col('interest_code')   == "3").then(pl.lit("Directorships"))
#         .when(pl.col('interest_code')   == "4").then(pl.lit("Land (including property)"))
#         .when(pl.col('interest_code')   == "5").then(pl.lit("Gifts"))
#         .when(pl.col('interest_code')   == "6").then(pl.lit("Property supplied or lent or a Service supplied"))
#         .when(pl.col('interest_code')   == "7").then(pl.lit("Travel Facilities"))
#         .when(pl.col('interest_code')   == "8").then(pl.lit("Remunerated Position"))
#         .when(pl.col('interest_code')   == "9").then(pl.lit("Contracts"))
#         .otherwise(pl.col('interest_code'))
#         .alias('interest_category')
#     )
#     df = df.with_columns(
#         pl.col('full_name').list.get(0).alias('last_name'),
#         pl.col('full_name').list.get(1).alias('first_name')
#     ).drop('full_name')
#     df = df.with_columns(
#         pl.concat_str(pl.col(['first_name', 'last_name'])).alias('join_key')
#     # Dynamic filter — rogue rows carry the register year as their interest_category
#     # (e.g. a stray "2024" line in the 2024 PDF parses to interest_code="2024", category="2024").
#     # Using str(year) makes this correct for both files instead of hardcoding "2025".
#     ).filter(
#         pl.col('interest_description_raw') != str(year),
#         pl.col('interest_description_cleaned') != str(year),
#         pl.col('interest_category') != str(year)
#     )
#     # Split multi-interest rows on ";" for categories that commonly contain several entries.
#     # Categories 1, 2, 3, 4, 9 are split; others are wrapped in a list as-is.
#     df = df.with_columns(
#         pl.when(pl.col("interest_code") == "1"
#         ).then(pl.col("interest_description_cleaned").str.split(";"))
#         .when(pl.col("interest_code") == "2"
#         ).then(pl.col("interest_description_cleaned").str.split(";"))
#         .when(pl.col("interest_code") == "3"
#         ).then(pl.col("interest_description_cleaned").str.split(";"))
#         .when(pl.col("interest_code") == "4"
#         ).then(pl.col("interest_description_cleaned").str.split(";"))
#         .when(pl.col("interest_code") == "9"
#         ).then(pl.col("interest_description_cleaned").str.split(";"))
#         .otherwise(pl.concat_list("interest_description_cleaned"))
#         .alias("split_interests")
#     ).explode("split_interests")
#     df = df.with_columns(
#         pl.col("split_interests").str.strip_chars().alias("interest_description_cleaned")
#     ).drop("split_interests")
#     # Tag rows as landlord where the description mentions letting/rental activity,
#     # excluding rows that simply say "ceased" or similar non-letting phrases.
#     df = df.with_columns(
#         pl.when(
#             (~pl.col('interest_description_cleaned').is_in(["ceased", "no rental income", "I own no land or residences"])) &
#             (pl.col('interest_description_cleaned')
#              .str.contains('let|rented|ARP|Léasóir|Rental|rent received|Leasóir|lord|letting|renting|rental|HAP|RAS Scheme|lessor|Lessor|lord:')
#              )).then(pl.lit('true')).otherwise(pl.lit('false')).alias('is_landlord')
#     )
#     df = df.with_columns(
#         pl.col('interest_description_cleaned')
#         .str.replace('or lent No interests declared or a Service supplied', 'No interests declared')
#     )
#     df = df.with_columns(
#         pl.when(pl.col('interest_description_cleaned') != 'No interests declared')
#           .then(1)
#           .otherwise(0)
#           .alias('interest_flag')
#     )
#     df = df.with_columns(
#         pl.concat_str(pl.col(['first_name', 'last_name'])).alias('join_key')
#     )

#     df = df.with_columns(
#     pl.when((pl.col("interest_code") == "4") &
#             (pl.col('is_landlord') == True) |
#             ((pl.col("interest_code") == "4") & (pl.col('interest_description_cleaned')!='No interests declared'))
#             ).then(
#                 pl.lit('TRUE')
#                 ).otherwise(pl.lit("FALSE")
#                 ).alias('is_property_owner'))
#     df = normalise_join_key.normalise_df_td_name(df, 'join_key')
#     # Join against master TD list to attach unique_member_code, party, constituency.
#     # TODO: move hardcoded paths to config.py so pipeline.py has a single source of truth.

#     master_td_list = pl.read_csv(DATA_DIR / 'silver' / 'flattened_members.csv')
#     master_td_list = master_td_list.select(
#         ['unique_member_code', 
#         'first_name', 
#         'last_name', 
#         'constituency_name', 
#         'full_name', 
#         'party']
#     ).unique()
#     master_td_list = master_td_list.with_columns(
#         pl.concat_str(pl.col(['first_name', 'last_name'])).alias('join_key')
#     )
#     master_td_list = normalise_join_key.normalise_df_td_name(master_td_list, 'join_key')
#     # Left join from master list so all TDs appear even if they filed no interests.
#     # registration_status distinguishes those with no matched rows (unregistered).
#     # year_elected is extracted from the unique_member_code string (contains a 4-digit year).
#     # TODO: package the year_elected extraction into a utility function — used elsewhere too.
#     registered_unregistered = master_td_list.join(df, on='join_key', how='left').with_columns(
#         pl.when(pl.col('interest_code').is_null())
#           .then(pl.lit('unregistered'))
#           .otherwise(pl.lit('registered'))
#           .alias('registration_status')
#     ).with_columns(
#         pl.col('unique_member_code').str.extract(r"\b\d{4}\b", 0).alias('year_elected')
#     ).drop('join_key')
    
#     is_minister_or_not = pl.read_csv(f"{MEMBERS_DIR}/enriched_td_attendance.csv")
#     is_minister_or_not = is_minister_or_not.select(
#         ['unique_member_code', 'ministerial_office_filled']
#     ).unique(subset=['unique_member_code'])

#     registered_unregistered = registered_unregistered.join(
#         is_minister_or_not, on='unique_member_code', how='left'
#     ).drop('first_name_right', 'interests', 'name', 'interest_description_raw', 'last_name_right')
    
#     # Add interest count per member (excluding "No interests declared")
#     registered_unregistered = registered_unregistered.with_columns(
#         pl.col('interest_flag').sum().over('unique_member_code').alias('interest_count')
#     ).drop('interest_flag')
    
#     registered_unregistered.write_csv(MEMBERS_DIR / f"member_interests_grouped_{year}.csv")

#     print(f"Processed {len(members)} members")
#     print(f"Output saved to {output_path}")

#     # Clean up intermediate JSON — not needed once CSV is written.
#     if os.path.exists(output_path):
#         # os.remove(output_path)
#         print('JSON file deleted successfully.')

# # Combine both year CSVs after the loop.
# # year_declared is added here rather than during processing so the individual CSVs
# # stay clean and year-agnostic (easier to re-run one year without touching the other).

# # df14 = pl.read_csv(MEMBERS_DIR / "member_interests_grouped_2014.csv").with_columns(year_declared=pl.lit(2014))
# # df15 = pl.read_csv(MEMBERS_DIR / "member_interests_grouped_2015.csv").with_columns(year_declared=pl.lit(2015))
# # df16 = pl.read_csv(MEMBERS_DIR / "member_interests_grouped_2016.csv").with_columns(year_declared=pl.lit(2016))
# # df17 = pl.read_csv(MEMBERS_DIR / "member_interests_grouped_2017.csv").with_columns(year_declared=pl.lit(2017))
# # df18 = pl.read_csv(MEMBERS_DIR / "member_interests_grouped_2018.csv").with_columns(year_declared=pl.lit(2018))
# # df19 = pl.read_csv(MEMBERS_DIR / "member_interests_grouped_2019.csv").with_columns(year_declared=pl.lit(2019))
# df20 = pl.read_csv(MEMBERS_DIR / "member_interests_grouped_2020.csv").with_columns(year_declared=pl.lit(2020))
# df21 = pl.read_csv(MEMBERS_DIR / "member_interests_grouped_2021.csv").with_columns(year_declared=pl.lit(2021))
# df22 = pl.read_csv(MEMBERS_DIR / "member_interests_grouped_2022.csv").with_columns(year_declared=pl.lit(2022))
# df23 = pl.read_csv(MEMBERS_DIR / "member_interests_grouped_2023.csv").with_columns(year_declared=pl.lit(2023))
# df24 = pl.read_csv(MEMBERS_DIR / "member_interests_grouped_2024.csv").with_columns(year_declared=pl.lit(2024))
# df25 = pl.read_csv(MEMBERS_DIR / "member_interests_grouped_2025.csv").with_columns(year_declared=pl.lit(2025))
# df21 = pl.read_csv(MEMBERS_DIR / "member_interests_grouped_2021.csv").with_columns(year_declared=pl.lit(2021))
# df_20 = pl.read_csv(MEMBERS_DIR / "member_interests_grouped_2020.csv").with_columns(year_declared=pl.lit(2020))
# combined = ( 
#     # df16, df14, df15,df17,df18,df19,df19,
#     pl.concat([df20, df21, df22, df23, df24, df25])
#     # interest_code is a string column throughout — must compare to strings, not integers.
#     # "2026" guards against badly parsed rowns ie 2026 taken by accident that may appear in the 2025 PDF.
#     .filter(~pl.col("interest_code").is_in([2019,2020, 2021, 2022, 2023, 2024, 2025, 2026]))
#     .sort(["unique_member_code", "year_declared", "interest_code"])
# )
# #4	No interests declared	Land (including property)
# combined.write_csv(MEMBERS_DIR / "member_interests_combined.csv")

# if __name__ == "__main__":
#     print("Member interest extraction complete. CSV and JSON files created successfully.")
