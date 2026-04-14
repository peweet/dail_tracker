import fitz  # PyMuPDF
import pathlib
import json
import re
import os
import polars as pl
import normalise_join_key 
from config import MEMBERS_DIR
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
"""
member_interest = MEMBERS_DIR / "pdf_member_interest" / "2026-02-25_register-of-member-s-interests-dail-eireann-2025_en.pdf"
categories = re.compile(r"^\d+\.\s")       # "1. ", "2. " etc.
member_name = re.compile(r"^[A-Z]{2,},\s")  # "ARDAGH, Catherine"

print("Starting to process member interest PDF...")
doc = fitz.open(member_interest)
print(f"Processing file: {member_interest} with {doc.page_count} pages...")

# Extract and flatten all text lines
text_boxes = []
for page in doc:
    text = page.get_text(option="text")
    # The strip method removes any leading or trailing whitespace characters from the extracted text, which can help clean up the data and make it easier to process further. This is especially useful when dealing with text extracted from PDFs, as there can often be extra spaces or newline characters that are not relevant to the actual content we want to analyze.
    text = text.strip()
    # The splitlines method with keepends=False (which is the default) splits the text into lines and removes the newline characters. This gives us a list of lines that we can then process further to group them by member and their interests.
    lines = text.splitlines(False)
    text_boxes.append(lines)

flat = []
# Flatten the list of lists into a single list of lines
# This is necessary because the text extraction gives us a list of lines for each page, and we want to process all lines together to group them by member and their interests. By flattening the list of lists into a single list, we can then apply our grouping logic in one pass through the data.
# The nested loops here iterate through each sublist of lines (which corresponds to a page) and then through each line in that sublist, appending it to the flat list. This results in a single list of all lines from all pages, which we can then filter and process further.
for sublist in text_boxes:
    for item in sublist:
        flat.append(item)

# Remove empty/whitespace-only lines and trim header/footer
result = list(filter(str.strip, flat))
#slice the result to remove the first 8 lines (which are headers) and the last 5 lines (which are footers/disclaimers), based on manual inspection of the PDF structure. This ensures that we are only processing the relevant content that contains the member interests, and not any extraneous text that may be present in the headers or footers of the PDF.
result = result[8:-5]

# Group fragmented lines together
grouped = []
current = ""

for line in result:
    # The logic here is to check if the current line matches either the category pattern (e.g., "1. ", "2. ") or the member name pattern (e.g., "SMITH, John"). 
    # If it does, this indicates the start of a new category or member entry, *
    # and we should start a new group. If it does not match either pattern, 
    # it is assumed to be a continuation of the previous line (i.e., part of the same member's interests), and we concatenate it to the current group. 
    # This way, we can handle cases where a member's interests are split across multiple lines in the PDF, 
    # and ensure that they are grouped together correctly in our final output.
    if categories.match(line) or member_name.match(line):
        if current.strip(): # If current has content, add it to grouped before starting a new one. This ensures that we don't lose any member's interests when we encounter the next member name or category.
            grouped.append(current.strip())
        current = line
    else:
        # If the line does not match a new category or member name, it is assumed to be a 
        # continuation of the current member's interests, 
        # and we concatenate it to the current string. 
        # This allows us to handle cases where a member's interests 
        # are split across multiple lines in the PDF, and ensures that 
        #  they are grouped together correctly in our final output.
        current = current + " " + line

# Append any remaining text
# If the last line was part of a member's interests, it would not have been added to grouped yet, so we check if current has content and add it if so.
if current.strip():
    # This ensures that the last member's interests are included in the final output, even if there is no subsequent member name or category to trigger the append in the loop.
    grouped.append(current.strip())

# Structure into members with their interests
members = []
current_member = None
#TODO can the flatten_service.py replace the below logic for grouping the lines into member entries and their interests? It seems like a similar problem of grouping fragmented lines together, and we could potentially reuse some of the logic from there to make this more robust and easier to maintain. For example, we could use a similar approach of checking for patterns that indicate the start of a new member entry (e.g., a line that matches the member name pattern), and then grouping subsequent lines as interests until we encounter the next member entry. This would allow us to handle cases where a member's interests are split across multiple lines in the PDF, and ensure that they are grouped together correctly in our final output.
for line in grouped:
    # If the line matches the member name pattern (e.g., "SMITH, John"),
    # this signals the start of a new member's entry.
    if member_name.match(line):
        # If we were already building a member entry, save it to the list before starting a new one.
        if current_member:
            members.append(current_member)
        # Start a new member dictionary with the name and an empty interests list.
        current_member = {"name": line, "interests": []}
    # If the line does not match a member name, it is assumed to be an interest
    # belonging to the current member. Add it to their interests list.
    elif current_member is not None:
        current_member["interests"].append(line)

# After the loop, if there is a member being built, add it to the list (handles the last member)
if current_member:
    members.append(current_member)
# Save output
output_path = MEMBERS_DIR / "member_interests_grouped.json"
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(members, f, indent=4, ensure_ascii=False)

df = pl.read_json(output_path)
df = df.explode("interests")

df = df.with_columns(
    pl.col('name').str.split(by=r"\s{2,}", literal=False).alias('name_and_constituency'))

df = df.with_columns(
    pl.col('name_and_constituency').list.get(0).alias('name'),
    pl.col('name_and_constituency').list.get(1).alias('constituency')
).drop('name_and_constituency')
df = df.with_columns(  pl.col('constituency').str.strip_chars("()"),
                       pl.col('name').str.split(by=r",", literal=True).alias('full_name'),
                       pl.col('interests').str.splitn(by=r".", n=2).alias('interests_code'
                        )
).unnest("interests_code")
df = df.rename({'field_0':'interest_code', 'field_1':'interest_description_raw'})
df = df.with_columns(
    pl.col('interests')
    .str.strip_chars_start('" ')
    .str.strip_chars_end('"')
    .str.replace_all(r"\xa0", " ")
    .str.replace_all(
        r"(Etc|including property|Property supplied |or lent  or a Service supplied  )",
        ""
    ).str.replace_all(
        r"(Occupations|Shares|Directorships|Land|Gifts|Property supplied or lent or a Service supplied|Travel Facilities|Remunerated Position|Contracts)",
        ""
    ).str.replace_all(r'"Etc\b', ""
    ).str.strip_chars_start()
    .str.replace_all(r"\s+", " ")
    .str.replace_all(r"[.…]+", "")
    .str.replace_all(r"^\s+", "")
    .str.replace_all(r"\b[1-9]\b", "")             
    .str.replace_all(r"\s*\(\)\s*", " ")             
    .str.replace_all(r" {2,}", " ")
    .str.replace_all(r'["""]', "")
    .str.replace_all(r'or lent or a Service supplied ', '')
    .str.replace_all(r"or lent\s*Nil?\s*or a Service supplied\s*Nil?", "Nil")
    .str.replace_all(r" {2,}", " ")
    .str.replace(r"^\(\)\s*", "")
    .str.replace(r'^"', "")
    .str.replace(r'^"', "")
    .str.replace(r'"$', "")
    .str.replace(r"Nil", "No interests declared")
    .str.strip_chars()
    .alias('interest_description_cleaned')
)
df = df.with_columns(
    pl.when(pl.col('interest_code') == "1").then(pl.lit("Occupations"))
    .when(pl.col('interest_code')   == "2").then(pl.lit("Shares"))
    .when(pl.col('interest_code')   == "3").then(pl.lit("Directorships"))
    .when(pl.col('interest_code')   == "4").then(pl.lit("Land (including property)"))
    .when(pl.col('interest_code')   == "5").then(pl.lit("Gifts"))
    .when(pl.col('interest_code')   == "6").then(pl.lit("Property supplied or lent or a Service supplied"))
    .when(pl.col('interest_code')   == "7").then(pl.lit("Travel Facilities"))
    .when(pl.col('interest_code')   == "8").then(pl.lit("Remunerated Position"))
    .when(pl.col('interest_code')   == "9").then(pl.lit("Contracts")).otherwise(pl.col('interest_code')
    ).alias('interest_category')
)
df = df.with_columns(
    pl.col('full_name').list.get(0).alias('last_name'),
    pl.col('full_name').list.get(1).alias('first_name')
).drop('full_name')
df = df.with_columns(
    pl.concat_str(
    pl.col(['first_name', 'last_name'])
    ).alias('join_key')
    ).filter(pl.col('interest_description_raw') !="2025", 
             pl.col('interest_description_cleaned') !="2025", 
             pl.col('interest_category') !="2025")

df = normalise_join_key.normalise_df_td_name(df, 'join_key')
enrich_data = pl.read_csv('members/enriched_td_attendance.csv')
enrich_data = enrich_data.select(['join_key', 'unique_member_code', 'party', 'year_elected']).unique()
df = df.join(enrich_data, on='join_key', how='left').drop("name", "join_key", "interests").drop('interest_description_raw')
df.write_csv(MEMBERS_DIR / "member_interests_grouped.csv")
print(f"Processed {len(members)} members")
print(f"Output saved to {output_path}")

if os.path.exists(MEMBERS_DIR / "member_interests_grouped.json"):
    os.remove(MEMBERS_DIR / "member_interests_grouped.json")
    print('JSON files deleted successfully.')
    
if __name__ == "__main__":
    print("Member interest extraction complete. CSV and JSON files created successfully.")