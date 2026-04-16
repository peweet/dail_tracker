# # import fitz  # PyMuPDF
# # import pathlib
# # import json
# # import re
# # import regex
# # import os
# # import polars as pl
# # import normalise_join_key 
# # from config import MEMBERS_DIR
# # """
# # member_interest.py
# # ------------------
# # Extracts, cleans, and structures the Register of Members' Interests from a PDF file.
# # This script processes the messy, unstructured PDF text, groups lines into member entries and their interests,
# # and outputs a normalized CSV and JSON. The main challenge is that the PDF contains a lot of superfluous data
# # (headers, footers, disclaimers, and line breaks in awkward places), so the approach is to aggressively cut away
# # the fluff and only keep the core data (member names and their interests). This is much easier and more robust
# # than trying to extract the relevant data directly from a very messy and inconsistent structure.

# # Regexes are used to:
# # - Identify category headers (e.g., "1. ", "2. ")
# # - Identify member name lines (e.g., "SMITH, John")
# # Cleaning steps:
# # - Remove empty lines, headers, and footers
# # - Group lines by member and interest category
# # - Normalize and split out names and interests for further analysis
# # The result is a structured dataset of members and their declared interests, suitable for downstream analysis.
# # """
# # member_interest_2024 = MEMBERS_DIR / "pdf_member_interest" / "2025-02-27_register-of-member-s-interests-dail-eireann-2024_en.pdf"
# # member_interest_2025 = MEMBERS_DIR / "pdf_member_interest" / "2026-02-25_register-of-member-s-interests-dail-eireann-2025_en.pdf"
# # # member_interest_2025 = MEMBERS_DIR / "pdf_member_interest" / "2026-02-25_register-of-member-s-interests-dail-eireann-2025_en.pdf"
# # member_interest = [member_interest_2024, member_interest_2025]
# # #https://claude.ai/chat/42d612f3-f698-4eba-b834-e0b7285d0356
# # #next steps
# # # def members_interest_pdf_cleaner(df)-> None:
# # # def members_interest_pdf_cleaner():
# # # """Cleans and structures the member interest data extracted from the PDF, and saves the cleaned data to CSV files."""
# # for member_interest_pdf in member_interest:
# #     categories = re.compile(r"^\d+\.\s")       # "1. ", "2. " etc.
# #     member_name = regex.compile(r"^\p{Lu}[\p{Lu}'\-]*(?:\s\p{Lu}[\p{Lu}'\-]*)*,\s")
# #     print("Starting to process member interest PDF...")
# #     doc = fitz.open(member_interest_pdf)
# # print(f"Processing file: {member_interest_pdf} with {doc.page_count} pages...")

# # # Extract and flatten all text lines
# # text_boxes = []
# # for page in doc:
# #     text = page.get_text(option="text")
# #     # The strip method removes any leading or trailing whitespace characters from the extracted text, which can help clean up the data and make it easier to process further. This is especially useful when dealing with text extracted from PDFs, as there can often be extra spaces or newline characters that are not relevant to the actual content we want to analyze.
# #     text = text.strip()
# #     # The splitlines method with keepends=False (which is the default) splits the text into lines and removes the newline characters. This gives us a list of lines that we can then process further to group them by member and their interests.
# #     lines = text.splitlines(False)
# #     text_boxes.append(lines)

# # flat = []
# # # Flatten the list of lists into a single list of lines
# # # This is necessary because the text extraction gives us a list of lines for each page, and we want to process all lines together to group them by member and their interests. By flattening the list of lists into a single list, we can then apply our grouping logic in one pass through the data.
# # # The nested loops here iterate through each sublist of lines (which corresponds to a page) and then through each line in that sublist, appending it to the flat list. This results in a single list of all lines from all pages, which we can then filter and process further.
# # for sublist in text_boxes:
# #     for item in sublist:
# #         flat.append(item)

# # # Remove empty/whitespace-only lines and trim header/footer
# # result = list(filter(str.strip, flat))
# # #slice the result to remove the first 8 lines (which are headers) and the last 5 lines (which are footers/disclaimers), based on manual inspection of the PDF structure. This ensures that we are only processing the relevant content that contains the member interests, and not any extraneous text that may be present in the headers or footers of the PDF.
# # result = result[8:-5]

# # # Group fragmented lines together
# # grouped = []
# # current = ""

# # for line in result:
# #     # The logic here is to check if the current line matches either the category pattern (e.g., "1. ", "2. ") or the member name pattern (e.g., "SMITH, John"). 
# #     # If it does, this indicates the start of a new category or member entry, *
# #     # and we should start a new group. If it does not match either pattern, 
# #     # it is assumed to be a continuation of the previous line (i.e., part of the same member's interests), and we concatenate it to the current group. 
# #     # This way, we can handle cases where a member's interests are split across multiple lines in the PDF, 
# #     # and ensure that they are grouped together correctly in our final output.
# #     if categories.match(line) or member_name.match(line):
# #         if current.strip(): # If current has content, add it to grouped before starting a new one. This ensures that we don't lose any member's interests when we encounter the next member name or category.
# #             grouped.append(current.strip())
# #         current = line
# #     else:
# #         # If the line does not match a new category or member name, it is assumed to be a 
# #         # continuation of the current member's interests, 
# #         # and we concatenate it to the current string. 
# #         # This allows us to handle cases where a member's interests 
# #         # are split across multiple lines in the PDF, and ensures that 
# #         #  they are grouped together correctly in our final output.
# #         current = current + " " + line

# # # Append any remaining text
# # # If the last line was part of a member's interests, it would not have been added to grouped yet, so we check if current has content and add it if so.
# # if current.strip():
# #     # This ensures that the last member's interests are included in the final output, even if there is no subsequent member name or category to trigger the append in the loop.
# #     grouped.append(current.strip())

# # # Structure into members with their interests
# # members = []
# # current_member = None
# # #TODO can the flatten_service.py replace the below logic for grouping the lines into member entries and their interests? It seems like a similar problem of grouping fragmented lines together, and we could potentially reuse some of the logic from there to make this more robust and easier to maintain. For example, we could use a similar approach of checking for patterns that indicate the start of a new member entry (e.g., a line that matches the member name pattern), and then grouping subsequent lines as interests until we encounter the next member entry. This would allow us to handle cases where a member's interests are split across multiple lines in the PDF, and ensure that they are grouped together correctly in our final output.
# # for line in grouped:
# #     # If the line matches the member name pattern (e.g., "SMITH, John"),
# #     # this signals the start of a new member's entry.
# #     if member_name.match(line):
# #         # If we were already building a member entry, save it to the list before starting a new one.
# #         if current_member:
# #             members.append(current_member)
# #         # Start a new member dictionary with the name and an empty interests list.
# #         current_member = {"name": line, "interests": []}
# #     # If the line does not match a member name, it is assumed to be an interest
# #     # belonging to the current member. Add it to their interests list.
# #     elif current_member is not None:
# #         current_member["interests"].append(line)

# # # After the loop, if there is a member being built, add it to the list (handles the last member)
# # if current_member:
# #     members.append(current_member)
# # # Save output
# # output_path = MEMBERS_DIR / f"{member_interest_pdf}.json"
# # with open(output_path, "w", encoding="utf-8") as f:
# #     json.dump(members, f, indent=4, ensure_ascii=False)

# # df = pl.read_json(output_path)
# # df = df.explode("interests")
# # df = df.with_columns(
# #     pl.col('name').str.split(by=r"\s{2,}", literal=False).alias('name_and_constituency'))

# # df = df.with_columns(
# #     pl.col('name_and_constituency').list.get(0).alias('name'),
# #     pl.col('name_and_constituency').list.get(1).alias('constituency')
# # ).drop('name_and_constituency')
# # df = df.with_columns(  pl.col('constituency').str.strip_chars("()"),
# #                        pl.col('name').str.split(by=r",", literal=True).alias('full_name'),
# #                        pl.col('interests').str.splitn(by=r".", n=2).alias('interests_code'
# #                         )
# # ).unnest("interests_code")
# # df = df.rename({'field_0':'interest_code', 'field_1':'interest_description_raw'})
# # df = df.with_columns(
# #     pl.col('interests')
# #     .str.strip_chars_start('" ')
# #     .str.strip_chars_end('"')
# #     .str.replace_all(r"\xa0", " ")
# #     .str.replace_all(
# #         r"(Etc|including property|Property supplied |or lent  or a Service supplied  )",
# #         ""
# #     )
# #     .str.replace_all(
# #         r"(Occupations|Shares|Directorships|Land|Gifts|Property supplied or lent or a Service supplied|Travel Facilities|Remunerated Position|Contracts)",
# #         ""
# #     ).str.replace_all(r'"Etc\b', ""
# #     ).str.strip_chars_start()
# #     .str.replace_all(r"\s+", " ")
# #     .str.replace_all(r"[.…]+", "")
# #     .str.replace_all(r"^\s+", "")
# #     .str.replace_all(r"\b[1-9]\b", "")             
# #     .str.replace_all(r"\s*\(\)\s*", " ")             
# #     .str.replace_all(r" {2,}", " ")
# #     .str.replace_all(r'["""]', "")
# #     .str.replace_all(r'or lent or a Service supplied ', '')
# #     .str.replace_all(r"or lent\s*Nil?\s*or a Service supplied\s*Nil?", "Nil")
# #     .str.replace_all(r" {2,}", " ")
# #     .str.replace(r"^\(\)\s*", "")
# #     .str.replace(r'^"', "")
# #     .str.replace(r'^"', "")
# #     .str.replace(r'"$', "")
# #     .str.replace_all(r"^(│Dr.|dr|dr.|prof|mr|mrs|ms|miss|bl)\s+", "")
# #     .str.replace(r"Nil|Neamh-fheidhme|Neamh-infheidhme", "No interests declared")
# #     .str.strip_chars()
# #     .alias('interest_description_cleaned')
# # )
# # df = df.with_columns(
# #     pl.when(pl.col('interest_code') == "1").then(pl.lit("Occupations"))
# #     .when(pl.col('interest_code')   == "2").then(pl.lit("Shares"))
# #     .when(pl.col('interest_code')   == "3").then(pl.lit("Directorships"))
# #     .when(pl.col('interest_code')   == "4").then(pl.lit("Land (including property)"))
# #     .when(pl.col('interest_code')   == "5").then(pl.lit("Gifts"))
# #     .when(pl.col('interest_code')   == "6").then(pl.lit("Property supplied or lent or a Service supplied"))
# #     .when(pl.col('interest_code')   == "7").then(pl.lit("Travel Facilities"))
# #     .when(pl.col('interest_code')   == "8").then(pl.lit("Remunerated Position"))
# #     .when(pl.col('interest_code')   == "9").then(pl.lit("Contracts")).otherwise(pl.col('interest_code')
# #     ).alias('interest_category')
# # )

# # df = df.with_columns(
# #     pl.col('full_name').list.get(0).alias('last_name'),
# #     pl.col('full_name').list.get(1).alias('first_name')
# # ).drop('full_name')
# # df = df.with_columns(
# #     pl.concat_str(
# #     pl.col(['first_name', 'last_name'])
# #     ).alias('join_key')
# #     ).filter(pl.col('interest_description_raw') !="2025", 
# #              pl.col('interest_description_cleaned') !="2025", 
# #              pl.col('interest_category') !="2025")

# # df = df.with_columns(
# #     pl.when(pl.col("interest_code") == "1"
# #     ).then(pl.col("interest_description_cleaned")
# #            .str.split(";")
# #     ).when(pl.col("interest_code") == "2"
# #     ).then(pl.col("interest_description_cleaned")
# #            .str.split(";"))
# #     .when(pl.col("interest_code") == "3"   
# #     ).then(pl.col("interest_description_cleaned")
# #            .str.split(";"))
# #     .when(pl.col("interest_code") == "4"
# #     ).then(pl.col("interest_description_cleaned")
# #            .str.split(";"))
# #     .when(pl.col("interest_code") == "9"
# #     ).then(pl.col("interest_description_cleaned")
# #            .str.split(";"))
# #     .otherwise(pl.concat_list("interest_description_cleaned")
# #                )
# #     .alias("split_interests")
# # )
# # df = df.explode("split_interests")
# # df = df.with_columns(
# #     pl.col("split_interests")
# #     .str.strip_chars()
# #     .alias("interest_description_cleaned")
# # ).drop("split_interests")
# # df = df.with_columns(
# #     pl.when(
# #         (~pl.col('interest_description_cleaned'
# #                  ).is_in(["ceased"])) &
# #         (pl.col('interest_description_cleaned')
# #         .str.contains('let|rented|ARP|Léasóir|Rental|rent received|Leasóir|lord|letting|renting|rental|HAP|RAS Scheme|lessor|Lessor|lord:')
# #         )).then(pl.lit('true')
# #         ).otherwise(pl.lit('false')
# #                     ).alias('is_landlord')
# # )
# # df = df.with_columns(pl.col('interest_description_cleaned')
# #         .str.replace('or lent No interests declared or a Service supplied', 'No interests declared'))
# # df = df.with_columns(
# #     pl.concat_str(
# #         pl.col(['first_name', 'last_name'])
# #     ).alias('join_key')
# # )
# # df = normalise_join_key.normalise_df_td_name(df, 'join_key')
# # master_td_list = pl.read_csv("C://Users//pglyn//PycharmProjects//dail_extractor//members//flattened_members.csv")

# # master_td_list = master_td_list.select(
# #     ['unique_member_code', 
# #     'first_name', 'last_name', 
# #     'member_constituency', 
# #     'full_name', 
# #     'party']
# # ).unique()
# # master_td_list = master_td_list.with_columns(
# #     pl.concat_str(
# #         pl.col(['first_name', 'last_name'])
# #         ).alias('join_key')
# # )
# # master_td_list = normalise_join_key.normalise_df_td_name(master_td_list, 'join_key')
# # registered_unregistered = master_td_list.join(
# #     df,
# #     on='join_key',
# #     how='left'
# # ).with_columns(
# #     pl.when(pl.col('interest_code').is_null())
# #       .then(pl.lit('unregistered'))
# #       .otherwise(pl.lit('registered'))
# #       .alias('registration_status')
# # ).with_columns(pl.col('unique_member_code')
# #                                        .str.extract(r"\b\d{4}\b", 0) #TODO: package this into a utility function since it is used elsewhere
# #                                        .alias('year_elected')
# #                                        ).drop('join_key')
# # is_minister_or_not = pl.read_csv("C:\\Users\\pglyn\\PycharmProjects\\dail_extractor\\members\\enriched_td_attendance.csv")
# # is_minister_or_not =is_minister_or_not.select(['unique_member_code', 'ministerial_office_filled']).unique(subset=['unique_member_code'])
# # registered_unregistered = registered_unregistered.join(is_minister_or_not, on='unique_member_code', how='left'
# #                         ).drop(
# #                             'first_name_right', 
# #                             'interests', 
# #                             'name', 
# #                             'interest_description_raw', 
# #                             'last_name_right'
# #                             )
# # registered_unregistered.write_csv(MEMBERS_DIR / "member_interests_grouped_2024.csv")

# # df24= pl.read_csv("members//member_interests_grouped_2024.csv")
# # df24 = df24.with_columns(year_declared=pl.lit(2024))

# # df25= pl.read_csv("members//member_interests_grouped_2024.csv")
# # df25 = df25.with_columns(year_declared=pl.lit(2025)
# #                         ).filter((pl.col("interest_code") != 2025) & 
# #                                  (pl.col("interest_code") != 2024))
# # combined = (
# #     pl.concat([df24, df25])
# #     .sort(["unique_member_code", "year_declared", "interest_code"])
# # )

# # combined.write_csv(MEMBERS_DIR /"member_interests_combined.csv")
# # print(f"Processed {len(members)} members")
# # print(f"Output saved to {output_path}")

# # if os.path.exists(MEMBERS_DIR / "member_interests_grouped_2024.json"):
# #     # os.remove(MEMBERS_DIR / "member_interests_grouped_2025.json")
# #     print('JSON files deleted successfully.')
    
# # if __name__ == "__main__":
# #     print("Member interest extraction complete. CSV and JSON files created successfully.")

# import fitz  # PyMuPDF
# import pathlib
# import json
# import re
# import regex
# import os
# import polars as pl
# import normalise_join_key 
# from config import MEMBERS_DIR

# member_interest_2024 = MEMBERS_DIR / "pdf_member_interest" / "2025-02-27_register-of-member-s-interests-dail-eireann-2024_en.pdf"
# member_interest_2025 = MEMBERS_DIR / "pdf_member_interest" / "2026-02-25_register-of-member-s-interests-dail-eireann-2025_en.pdf"
# member_interest = [member_interest_2024, member_interest_2025]

# for member_interest_pdf in member_interest:
#     # FIX 1: direct comparison — no fragile string matching on filename
#     year = 2024 if member_interest_pdf == member_interest_2024 else 2025
#     categories = re.compile(r"^\d+\.\s")
#     member_name = regex.compile(r"^\p{Lu}[\p{Lu}'\-]*(?:\s\p{Lu}[\p{Lu}'\-]*)*,\s")
#     print("Starting to process member interest PDF...")
#     doc = fitz.open(member_interest_pdf)
#     print(f"Processing file: {member_interest_pdf} with {doc.page_count} pages...")

#     text_boxes = []
#     for page in doc:
#         text = page.get_text(option="text")
#         text = text.strip()
#         lines = text.splitlines(False)
#         text_boxes.append(lines)

#     flat = []
#     for sublist in text_boxes:
#         for item in sublist:
#             flat.append(item)

#     result = list(filter(str.strip, flat))
#     result = result[8:-5]

#     grouped = []
#     current = ""

#     for line in result:
#         if categories.match(line) or member_name.match(line):
#             if current.strip():
#                 grouped.append(current.strip())
#             current = line
#         else:
#             current = current + " " + line
#     if current.strip():
#         grouped.append(current.strip())

#     members = []
#     current_member = None

#     for line in grouped:
#         if member_name.match(line):
#             if current_member:
#                 members.append(current_member)
#             current_member = {"name": line, "interests": []}
#         elif current_member is not None:
#             current_member["interests"].append(line)

#     if current_member:
#         members.append(current_member)

#     # FIX 2: use .stem so the filename is clean, not the full path object
#     output_path = MEMBERS_DIR / f"{member_interest_pdf.stem}.json"
#     with open(output_path, "w", encoding="utf-8") as f:
#         json.dump(members, f, indent=4, ensure_ascii=False)

#     df = pl.read_json(output_path)
#     df = df.explode("interests")
#     df = df.with_columns(
#         pl.col('name').str.split(by=r"\s{2,}", literal=False).alias('name_and_constituency'))

#     df = df.with_columns(
#         pl.col('name_and_constituency').list.get(0).alias('name'),
#         pl.col('name_and_constituency').list.get(1).alias('constituency')
#     ).drop('name_and_constituency')
#     df = df.with_columns(
#         pl.col('constituency').str.strip_chars("()"),
#         pl.col('name').str.split(by=r",", literal=True).alias('full_name'),
#         pl.col('interests').str.splitn(by=r".", n=2).alias('interests_code')
#     ).unnest("interests_code")
#     df = df.rename({'field_0':'interest_code', 'field_1':'interest_description_raw'})
#     df = df.with_columns(
#         pl.col('interests')
#         .str.strip_chars_start('" ')
#         .str.strip_chars_end('"')
#         .str.replace_all(r"\xa0", " ")
#         .str.replace_all(
#             r"(Etc|including property|Property supplied |or lent  or a Service supplied  )",
#             ""
#         )
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
#         .str.replace_all(r" {2,}", " ")
#         .str.replace(r"^\(\)\s*", "")
#         .str.replace(r'^"', "")
#         .str.replace(r'^"', "")
#         .str.replace(r'"$', "")
#         .str.replace_all(r"^(│Dr.|dr|dr.|prof|mr|mrs|ms|miss|bl)\s+", "")
#         .str.replace(r"Nil|Neamh-fheidhme|Neamh-infheidhme", "No interests declared")
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
#         .when(pl.col('interest_code')   == "9").then(pl.lit("Contracts")).otherwise(pl.col('interest_code')
#         ).alias('interest_category')
#     )
#     df = df.with_columns(
#         pl.col('full_name').list.get(0).alias('last_name'),
#         pl.col('full_name').list.get(1).alias('first_name')
#     ).drop('full_name')
#     df = df.with_columns(
#         pl.concat_str(pl.col(['first_name', 'last_name'])).alias('join_key')
#     # FIX 2: dynamic filter — rogue rows carry the register year as their category
#     ).filter(
#         pl.col('interest_description_raw') != str(year),
#         pl.col('interest_description_cleaned') != str(year),
#         pl.col('interest_category') != str(year)
#     )
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
#     )
#     df = df.explode("split_interests")
#     df = df.with_columns(
#         pl.col("split_interests").str.strip_chars().alias("interest_description_cleaned")
#     ).drop("split_interests")
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
#         pl.concat_str(pl.col(['first_name', 'last_name'])).alias('join_key')
#     )
#     df = normalise_join_key.normalise_df_td_name(df, 'join_key')

#     master_td_list = pl.read_csv("C://Users//pglyn//PycharmProjects//dail_extractor//members//flattened_members.csv")
#     master_td_list = master_td_list.select(
#         ['unique_member_code', 'first_name', 'last_name', 'member_constituency', 'full_name', 'party']
#     ).unique()
#     master_td_list = master_td_list.with_columns(
#         pl.concat_str(pl.col(['first_name', 'last_name'])).alias('join_key')
#     )
#     master_td_list = normalise_join_key.normalise_df_td_name(master_td_list, 'join_key')

#     registered_unregistered = master_td_list.join(df, on='join_key', how='left').with_columns(
#         pl.when(pl.col('interest_code').is_null())
#           .then(pl.lit('unregistered'))
#           .otherwise(pl.lit('registered'))
#           .alias('registration_status')
#     ).with_columns(
#         pl.col('unique_member_code').str.extract(r"\b\d{4}\b", 0).alias('year_elected')
#     ).drop('join_key')

#     is_minister_or_not = pl.read_csv("C:\\Users\\pglyn\\PycharmProjects\\dail_extractor\\members\\enriched_td_attendance.csv")
#     is_minister_or_not = is_minister_or_not.select(
#         ['unique_member_code', 'ministerial_office_filled']
#     ).unique(subset=['unique_member_code'])

#     registered_unregistered = registered_unregistered.join(
#         is_minister_or_not, on='unique_member_code', how='left'
#     ).drop('first_name_right', 'interests', 'name', 'interest_description_raw', 'last_name_right')

#     registered_unregistered.write_csv(MEMBERS_DIR / f"member_interests_grouped_{year}.csv")

#     print(f"Processed {len(members)} members")
#     print(f"Output saved to {output_path}")

#     if os.path.exists(output_path):
#         os.remove(output_path)
#         print('JSON file deleted successfully.')

# # Combine both CSVs after the loop
# df24 = pl.read_csv(MEMBERS_DIR / "member_interests_grouped_2024.csv").with_columns(year_declared=pl.lit(2024))
# # FIX 4: df25 was reading from the 2024 CSV — corrected to 2025
# df25 = pl.read_csv(MEMBERS_DIR / "member_interests_grouped_2025.csv").with_columns(year_declared=pl.lit(2025))

# combined = (
#     pl.concat([df24, df25])
#     # FIX 3: interest_code is a string column — comparing to integers never matched anything
#     .filter(~pl.col("interest_code").is_in([2024, 2025, 2026]))
#     .sort(["unique_member_code", "year_declared", "interest_code"])
# )
# combined.write_csv(MEMBERS_DIR / "member_interests_combined.csv")

# if __name__ == "__main__":
#     print("Member interest extraction complete. CSV and JSON files created successfully.")
import fitz  # PyMuPDF
import pathlib
import json
import re
import regex
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

Planned refactor: break into self-contained functions and run through main().
See stub definitions below — implement and test one at a time.
# RISK: each function boundary is a good place to add an assertion or a small unit test
#       before wiring them together in main(). Recommended order: extract → group → parse → clean → join → combine.
# OPPORTUNITY: once refactored, pipeline.py can import and call main() directly,
#              or call individual steps for partial re-runs (e.g. re-clean without re-extracting).
"""

# ---------------------------------------------------------------------------
# PLANNED FUNCTION STUBS
# Implement and test these one at a time — do NOT wire into main() until each
# is independently verified. Type hints are provided as a guide.
# ---------------------------------------------------------------------------

# def extract_raw_lines(pdf_path: pathlib.Path, header_skip: int = 8, footer_skip: int = 5) -> list[str]:
#     """
#     Opens the PDF, extracts all text lines, flattens pages into a single list,
#     removes blank lines, and trims the fixed header/footer slice.
#     Returns a flat list of cleaned strings ready for grouping.
#     # RISK: header_skip / footer_skip are derived from manual inspection of the 2024 PDF.
#     #       If the 2025 PDF has a different number of header/footer lines this will silently
#     #       drop real data or keep junk. Worth printing result[:10] and result[-5:] after
#     #       each new PDF to sanity-check the slice.
#     # TEST: assert len(result) > 0
#     #       assert not any(line.strip() == "" for line in result)
#     """
#     pass

# def group_lines(lines: list[str], categories: re.Pattern, member_name: regex.Pattern) -> list[str]:
#     """
#     Iterates over the flat line list and concatenates continuation lines
#     (lines that don't start a new category or member) onto the previous group.
#     Returns a list of grouped strings, one per category-block or member-name.
#     # RISK: continuation logic depends entirely on the two regexes being correct.
#     #       A false positive (a line wrongly matching member_name) will split a block early.
#     #       A false negative (a real member not matching) will merge two members together.
#     # TEST: assert all grouped entries are either a member name or start with a digit + ". "
#     #       spot-check a known multi-line entry (e.g. a long address) to confirm it wasn't split
#     """
#     pass

# def parse_members(grouped: list[str], member_name: regex.Pattern) -> list[dict]:
#     """
#     Walks the grouped lines and builds a list of dicts: {"name": str, "interests": list[str]}.
#     Each dict represents one TD and their declared interest strings.
#     # RISK: if the last member in the file has no interests (edge case), the dict will have
#     #       an empty interests list — downstream explode() will drop the row silently.
#     # TEST: assert len(members) == expected_td_count (174 for both 2024 and 2025)
#     #       assert all("interests" in m for m in members)
#     """
#     pass

# def clean_interests(df: pl.DataFrame, year: int) -> pl.DataFrame:
#     """
#     Applies all Polars cleaning steps: strip chars, replace patterns, map interest codes
#     to category labels, split multi-interest rows, tag landlords, filter rogue year rows.
#     Returns a cleaned DataFrame ready for joining.
#     # RISK: the regex chain is long and order-dependent — a change to one replace_all can
#     #       affect the output of a later one. If you add a new rule, add it at the end and
#     #       verify with a known dirty string first.
#     # RISK: str.replace(r"Nil|Neamh-fheidhme|Neamh-infheidhme", "No interests declared")
#     #       only replaces the FIRST match per cell (use replace_all if that's ever an issue).
#     # OPPORTUNITY: this is the best candidate for parameterised unit tests —
#     #              feed a small hand-crafted DataFrame of known dirty strings and assert outputs.
#     # TYPE NOTE: year: int is used as str(year) inside the filter — make that explicit in the body.
#     """
#     pass

# def join_master_list(df: pl.DataFrame, master_path: pathlib.Path, minister_path: pathlib.Path) -> pl.DataFrame:
#     """
#     Joins the cleaned interests DataFrame against the master TD list and the
#     ministerial office lookup. Adds registration_status and year_elected columns.
#     Drops intermediate columns not needed downstream.
#     Returns the final per-year DataFrame ready to write to CSV.
#     # RISK: the join is on a normalised join_key — if normalise_df_td_name() handles
#     #       a name differently between the two DataFrames, the join will silently produce
#     #       nulls (registration_status = 'unregistered') for that TD.
#     # TEST: after the join, assert df.filter(pl.col("unique_member_code").is_null()).is_empty()
#     #       i.e. every row should have matched a TD in the master list.
#     # OPPORTUNITY: hardcoded absolute paths (C://Users//pglyn/...) belong in config.py
#     #              so pipeline.py doesn't need to know about them.
#     """
#     pass

# def combine_years(members_dir: pathlib.Path) -> pl.DataFrame:
#     """
#     Reads both year-specific CSVs, adds year_declared column, filters rogue interest_code
#     values, concatenates and sorts. Writes member_interests_combined.csv.
#     Returns the combined DataFrame.
#     # RISK: reads from disk — if either CSV is missing or empty (e.g. a failed run),
#     #       this will raise or silently produce a half-combined file.
#     # TEST: assert combined.filter(pl.col("year_declared") == 2024).height > 0
#     #       assert combined.filter(pl.col("year_declared") == 2025).height > 0
#     #       assert combined.filter(pl.col("interest_code").is_in(["2024","2025","2026"])).is_empty()
#     """
#     pass

# def main() -> None:
#     """
#     Orchestrates the full pipeline: extract → group → parse → clean → join → combine.
#     Called by pipeline.py or directly via `python member_interest.py`.
#     Wire functions in here only once each is individually tested.
#     # OPPORTUNITY: add a --year CLI arg (argparse) so pipeline.py can re-run a single year
#     #              without reprocessing both PDFs every time.
#     """
#     pass

# ---------------------------------------------------------------------------
# CURRENT SCRIPT (to be replaced by main() once refactor is complete)
# ---------------------------------------------------------------------------

member_interest_2024 = MEMBERS_DIR / "pdf_member_interest" / "2025-02-27_register-of-member-s-interests-dail-eireann-2024_en.pdf"
member_interest_2025 = MEMBERS_DIR / "pdf_member_interest" / "2026-02-25_register-of-member-s-interests-dail-eireann-2025_en.pdf"
member_interest = [member_interest_2024, member_interest_2025]

for member_interest_pdf in member_interest:
    # Direct comparison — no fragile string matching on filename
    year = 2024 if member_interest_pdf == member_interest_2024 else 2025

    categories = re.compile(r"^\d+\.\s")       # "1. ", "2. " etc.
    member_name = regex.compile(r"^\p{Lu}[\p{Lu}'\-]*(?:\s\p{Lu}[\p{Lu}'\-]*)*,\s")

    print("Starting to process member interest PDF...")
    doc = fitz.open(member_interest_pdf)
    print(f"Processing file: {member_interest_pdf} with {doc.page_count} pages...")

    # Extract and flatten all text lines.
    # The strip method removes leading/trailing whitespace from extracted text.
    # The splitlines method (keepends=False) splits text into lines, removing newline characters.
    # This gives us a list of lines to process further, grouping by member and their interests.
    text_boxes = []
    for page in doc:
        text = page.get_text(option="text")
        text = text.strip()
        lines = text.splitlines(False)
        text_boxes.append(lines)

    # Flatten the list of lists into a single list of lines.
    # Necessary because text extraction gives a list of lines per page —
    # flattening lets us apply grouping logic in one pass across all pages.
    flat = []
    for sublist in text_boxes:
        for item in sublist:
            flat.append(item)

    # Remove empty/whitespace-only lines and trim header/footer.
    # Slice removes the first 8 lines (headers) and last 5 lines (footers/disclaimers)
    # based on manual inspection of the PDF structure, ensuring only member interest
    # content is processed.
    result = list(filter(str.strip, flat))
    result = result[8:-5]

    # Group fragmented lines together.
    # If a line matches a category pattern ("1. ", "2. ") or a member name ("SMITH, John"),
    # it signals the start of a new block — save the current group and start a new one.
    # Otherwise, concatenate the line to the current group (continuation of previous entry).
    # This handles member interests split across multiple lines in the PDF.
    grouped = []
    current = ""

    for line in result:
        if categories.match(line) or member_name.match(line):
            if current.strip():  # Save current group before starting a new one
                grouped.append(current.strip())
            current = line
        else:
            # Continuation of current member's interests — concatenate
            current = current + " " + line

    # Append any remaining text — the last member's interests won't have been
    # added yet since there's no subsequent entry to trigger the append in the loop.
    if current.strip():
        grouped.append(current.strip())

    # Structure into members with their interests.
    # Each member_name match signals a new TD entry.
    # All subsequent lines until the next member name are that TD's interests.
    # TODO: can flatten_service.py replace this grouping logic? Similar problem of
    #       grouping fragmented lines — could reuse logic and make this more maintainable.
    members = []
    current_member = None

    for line in grouped:
        # A line matching the member name pattern signals the start of a new entry.
        if member_name.match(line):
            # Save the previous member entry before starting a new one.
            if current_member:
                members.append(current_member)
            # Start a new member dict with name and empty interests list.
            current_member = {"name": line, "interests": []}
        # Non-matching lines are interests belonging to the current member.
        elif current_member is not None:
            current_member["interests"].append(line)

    # After the loop, add the last member being built (no subsequent entry to trigger append).
    if current_member:
        members.append(current_member)

    # Save intermediate JSON output — use .stem so the filename is just the PDF name,
    # not the full path object serialised as a string.
    output_path = MEMBERS_DIR / f"{member_interest_pdf.stem}.json"
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
    df = df.with_columns(
        pl.col('constituency').str.strip_chars("()"),
        pl.col('name').str.split(by=r",", literal=True).alias('full_name'),
        pl.col('interests').str.splitn(by=r".", n=2).alias('interests_code')
    ).unnest("interests_code")
    df = df.rename({'field_0': 'interest_code', 'field_1': 'interest_description_raw'})

    df = df.with_columns(
        pl.col('interests')
        .str.strip_chars_start('" ')
        .str.strip_chars_end('"')
        .str.replace_all(r"\xa0", " ")
        .str.replace_all(
            r"(Etc|including property|Property supplied |or lent  or a Service supplied  )",
            ""
        )
        .str.replace_all(
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
        .str.replace_all(r"^(│Dr.|dr|dr.|prof|mr|mrs|ms|miss|bl)\s+", "")
        .str.replace(r"Nil|Neamh-fheidhme|Neamh-infheidhme", "No interests declared")
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
        .when(pl.col('interest_code')   == "9").then(pl.lit("Contracts"))
        .otherwise(pl.col('interest_code'))
        .alias('interest_category')
    )
    df = df.with_columns(
        pl.col('full_name').list.get(0).alias('last_name'),
        pl.col('full_name').list.get(1).alias('first_name')
    ).drop('full_name')
    df = df.with_columns(
        pl.concat_str(pl.col(['first_name', 'last_name'])).alias('join_key')
    # Dynamic filter — rogue rows carry the register year as their interest_category
    # (e.g. a stray "2024" line in the 2024 PDF parses to interest_code="2024", category="2024").
    # Using str(year) makes this correct for both files instead of hardcoding "2025".
    ).filter(
        pl.col('interest_description_raw') != str(year),
        pl.col('interest_description_cleaned') != str(year),
        pl.col('interest_category') != str(year)
    )

    # Split multi-interest rows on ";" for categories that commonly contain several entries.
    # Categories 1, 2, 3, 4, 9 are split; others are wrapped in a list as-is.
    df = df.with_columns(
        pl.when(pl.col("interest_code") == "1"
        ).then(pl.col("interest_description_cleaned").str.split(";"))
        .when(pl.col("interest_code") == "2"
        ).then(pl.col("interest_description_cleaned").str.split(";"))
        .when(pl.col("interest_code") == "3"
        ).then(pl.col("interest_description_cleaned").str.split(";"))
        .when(pl.col("interest_code") == "4"
        ).then(pl.col("interest_description_cleaned").str.split(";"))
        .when(pl.col("interest_code") == "9"
        ).then(pl.col("interest_description_cleaned").str.split(";"))
        .otherwise(pl.concat_list("interest_description_cleaned"))
        .alias("split_interests")
    )
    df = df.explode("split_interests")
    df = df.with_columns(
        pl.col("split_interests").str.strip_chars().alias("interest_description_cleaned")
    ).drop("split_interests")

    # Tag rows as landlord where the description mentions letting/rental activity,
    # excluding rows that simply say "ceased" or similar non-letting phrases.
    df = df.with_columns(
        pl.when(
            (~pl.col('interest_description_cleaned').is_in(["ceased", "no rental income", "I own no land or residences"])) &
            (pl.col('interest_description_cleaned')
             .str.contains('let|rented|ARP|Léasóir|Rental|rent received|Leasóir|lord|letting|renting|rental|HAP|RAS Scheme|lessor|Lessor|lord:')
             )).then(pl.lit('true')).otherwise(pl.lit('false')).alias('is_landlord')
    )
    df = df.with_columns(
        pl.col('interest_description_cleaned')
        .str.replace('or lent No interests declared or a Service supplied', 'No interests declared')
    )
    df = df.with_columns(
        pl.concat_str(pl.col(['first_name', 'last_name'])).alias('join_key')
    )
    df = normalise_join_key.normalise_df_td_name(df, 'join_key')

    # Join against master TD list to attach unique_member_code, party, constituency.
    # TODO: move hardcoded paths to config.py so pipeline.py has a single source of truth.
    master_td_list = pl.read_csv("C://Users//pglyn//PycharmProjects//dail_extractor//members//flattened_members.csv")
    master_td_list = master_td_list.select(
        ['unique_member_code', 'first_name', 'last_name', 'member_constituency', 'full_name', 'party']
    ).unique()
    master_td_list = master_td_list.with_columns(
        pl.concat_str(pl.col(['first_name', 'last_name'])).alias('join_key')
    )
    master_td_list = normalise_join_key.normalise_df_td_name(master_td_list, 'join_key')

    # Left join from master list so all TDs appear even if they filed no interests.
    # registration_status distinguishes those with no matched rows (unregistered).
    # year_elected is extracted from the unique_member_code string (contains a 4-digit year).
    # TODO: package the year_elected extraction into a utility function — used elsewhere too.
    registered_unregistered = master_td_list.join(df, on='join_key', how='left').with_columns(
        pl.when(pl.col('interest_code').is_null())
          .then(pl.lit('unregistered'))
          .otherwise(pl.lit('registered'))
          .alias('registration_status')
    ).with_columns(
        pl.col('unique_member_code').str.extract(r"\b\d{4}\b", 0).alias('year_elected')
    ).drop('join_key')

    is_minister_or_not = pl.read_csv("C:\\Users\\pglyn\\PycharmProjects\\dail_extractor\\members\\enriched_td_attendance.csv")
    is_minister_or_not = is_minister_or_not.select(
        ['unique_member_code', 'ministerial_office_filled']
    ).unique(subset=['unique_member_code'])

    registered_unregistered = registered_unregistered.join(
        is_minister_or_not, on='unique_member_code', how='left'
    ).drop('first_name_right', 'interests', 'name', 'interest_description_raw', 'last_name_right')

    registered_unregistered.write_csv(MEMBERS_DIR / f"member_interests_grouped_{year}.csv")

    print(f"Processed {len(members)} members")
    print(f"Output saved to {output_path}")

    # Clean up intermediate JSON — not needed once CSV is written.
    if os.path.exists(output_path):
        os.remove(output_path)
        print('JSON file deleted successfully.')

# Combine both year CSVs after the loop.
# year_declared is added here rather than during processing so the individual CSVs
# stay clean and year-agnostic (easier to re-run one year without touching the other).
df24 = pl.read_csv(MEMBERS_DIR / "member_interests_grouped_2024.csv").with_columns(year_declared=pl.lit(2024))
df25 = pl.read_csv(MEMBERS_DIR / "member_interests_grouped_2025.csv").with_columns(year_declared=pl.lit(2025))

combined = (
    pl.concat([df24, df25])
    # interest_code is a string column throughout — must compare to strings, not integers.
    # "2026" guards against publication-year artifacts that may appear in the 2025 PDF.
    .filter(~pl.col("interest_code").is_in([2024, 2025, 2026]))
    .sort(["unique_member_code", "year_declared", "interest_code"])
)
combined.write_csv(MEMBERS_DIR / "member_interests_combined.csv")

if __name__ == "__main__":
    print("Member interest extraction complete. CSV and JSON files created successfully.")
