import csv
import polars as pl
from utility.select_drop_rename_cols_mappings import lobbying_rename
import os
# Parse each non-empty line using the parse_line function
# Read the raw input file line by line
# Function to parse a single line from the input CSV, the lobbyist data is very messy and 
# the csv dialect changes which necessistates manual intervention. Pandas will not work effectively with this data even with different dialect settings, so we have to do it manually and then read the cleaned data with polars for further processing. The main issues with the raw data are inconsistent use of quotes, embedded commas in fields, and inconsistent line breaks, which makes it difficult to parse with standard CSV parsers without manual cleaning.
# To extract the faw data go to https://www.lobbying.ie/app/Organisation/Search?currentPage=0&pageSize=20&queryText=&subjectMatters=&subjectMatterAreas=&lobbyingActivities=&returnDateFrom=&returnDateTo=&period=&dpo=&client=&includeClients=false
# and click the CSV export option to export all the registered lobby organizations in the Republic of Ireland
def parse_line(line):
    # print(f"Parsing line: {line.strip()}")
     # Replace triple double-quotes with double double-quotes
    line =line.replace('\"\'', '\'')  # Replace triple double-quotes with double double-quotes
    stripped = line.strip().split('","') # Split the line on '","' to separate fields, which handles quoted fields with embedded commas
    stripped[0] = stripped[0].lstrip('"')  # Remove the leading quote from the first field
    stripped[-1] = stripped[-1].rstrip('"')  # Remove the trailing quote
    # print(f"Test: {stripped}")  # Remove the first two quotes from the first field
    return stripped
with open('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/raw/Lobbying_ie_organisation_results.csv', 'r', encoding='utf-8') as f:
    raw_lines = f.readlines()

def lobby_org_csv_sanitizer():
    rows = []
    for line in raw_lines:
        rows.append(parse_line(line))
    with open('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/cleaned.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerows(rows)
    #manually assign col names as the csv has no header row, and then persist the cleaned data to a new csv file for further processing with polars
    #additioanlly the rows are ragged and uneven which can cause further issues down the line, so we need to make sure all rows have the same number of columns before writing to the cleaned csv file, and if not, we can log the issue and skip those rows to avoid errors in the downstream processing. This is a common issue with messy CSV data where some rows may have missing or extra fields, which can cause parsing errors if not handled properly.
    column_names = ["lobby_issue_uri","name","address","county","country","phone_number","website","main_activities_of_organisation","person_responsible_name","person_responsible_email","person_responsible_telephone","email","company_registration_number","company_registered_name","company_registered_address","charity_regulation_number","chy_number"]
    df = pl.read_csv('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/cleaned.csv',  has_header=False,infer_schema=True, skip_lines=1)
    df.columns = column_names
    df.write_csv('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/cleaned_output.csv')
    os.remove('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/cleaned.csv')
    print("Lobbying organization CSV sanitized and cleaned successfully. Cleaned data saved to cleaned_output.csv")
lobby_org_csv_sanitizer()

#HOW TO EXTRACT THE LOBBYING DATA:
# eg: https://www.lobbying.ie/app/home/search?currentPage=0&pageSize=20&queryText=&subjectMatters=&subjectMatterAreas=&publicBodys=&jobTitles=11&returnDateFrom=01-02-2026&returnDateTo=08-04-2026&period=&dpo=&client=&responsible=&lobbyist=&lobbyistId=
#Do it in batches of 1000 and download it manually, the max date range is 12 months ie 2025-01 to 2026-01, and then stack the csvs together and do the cleaning and processing with polars. The lobbying data is not available via an API, so we have to download it manually in batches of 1000 records, which is the maximum allowed by the website for a single download. We can filter the data by job titles (e.g. TDs, Senators, Ministers, etc.) and date range to get more relevant data for our analysis. Once we have downloaded the raw CSV files, we can use the csv sanitizer to clean the data and persist it to a new CSV file for further processing with polars. This will allow us to extract meaningful insights from the lobbying data and analyze the lobbying activities and their impact on the politicians in Ireland.
#drop it in the raw folder in the lobbying directory, and then run the csv sanitizer to clean the data and persist it to a new csv file for further processing with polars. The csv sanitizer will handle the messy and inconsistent formatting of the raw data, and ensure that we have a clean and consistent dataset to work with for our analysis of the lobbying activities and their impact on the politicians. This is a crucial step in the data processing pipeline, as it allows us to extract meaningful insights from the lobbying data, and identify any potential patterns or trends in the lobbying activities that may be relevant for our analysis of the political landscape and potential conflicts of interest.
csvs_to_stack = []
for file in os.listdir("C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/raw"):
    if file.endswith(".csv") and not file.startswith("Lobbying_ie_organisation_results"):  # Ensure we only process the raw CSV files and not the cleaned output
        print(f"Processing file: {file}")
        df = pl.read_csv(f"C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/raw/{file}")
        df= df.rename(lobbying_rename)
        print(f"Number of rows in {file}: {df.height}")
        csvs_to_stack.append(df)
        
lobbying_df = pl.concat(csvs_to_stack, how="diagonal")
lobbying_df.write_csv('c:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/combined_lobbying_data.csv')  # Save the combined data to a new CSV file for further processing
print(f"Total number of rows in combined lobbying data: {lobbying_df.height}")
lobby_org = pl.read_csv("C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/cleaned_output.csv", infer_schema=4000)
lobby_org = lobby_org.select("lobby_issue_uri",
                             "name",
                             "website",
                             "main_activities_of_organisation",
                             "company_registration_number",
                             "company_registered_name"
                             )
#construct hyperlinks for the lobby orgs
lobby_org = lobby_org.with_columns(pl.col('name'
                                          ).str.to_lowercase(
                                          ).str.replace(" ", "-").str.replace(" ", "-").alias("name_for_link"))
lobby_org = lobby_org.with_columns(
    #create hyper link
    #https://www.lobbying.ie/organisation
    pl.format("https://www.lobbying.ie/organisation/{}/{}", 
    pl.col("lobby_issue_uri"), pl.col('name_for_link')).alias("lobby_org_link")
    )
lobby_org = lobby_org.drop("name_for_link"
                           ).select(
                               "lobby_issue_uri", 
                               "name", 
                               "main_activities_of_organisation", 
                               "website", 
                               "company_registration_number", 
                               "company_registered_name", 
                               "lobby_org_link"
                            )


split_df = df.with_columns(pl.col("dpo_lobbied").str.split("::").alias("lobbyists")
        ).explode("lobbyists").with_columns(
        pl.col("lobbyists").str.split("|").alias("parts")
    )
split_df = split_df.with_columns(
        pl.col("parts").list.get(0).alias("full_name"),
        pl.col("parts").list.get(1).alias("position"),
        pl.col("parts").list.get(2).alias("chamber"),
    ).drop("lobbyists", "parts", "dpo_lobbied", "lobby_enterprise_uri")

split_df = split_df.with_columns(
        pl.col("lobbying_activities"
        ).str.split("::").alias("activities_list")
        ).explode("activities_list").with_columns(
        pl.col("activities_list").str.split("|").alias("activities_parts")
    )
split_df = split_df.with_columns(
        pl.col("activities_parts").list.get(0).alias("action"),
        pl.col("activities_parts").list.get(1).alias("delivery"),
        pl.col("activities_parts").list.get(2).alias("members_targeted"),
    ).drop("activities_list", "activities_parts", "lobbying_activities")
df = split_df.select("full_name","position", "chamber")

segmented = df.group_by(["full_name", "chamber"]
    ).agg(pl.count().alias("segmented_count"))

# Total count by full_name
total = df.group_by("full_name"
        ).agg(pl.count().alias("total_count"))

combined = segmented.join(total, on="full_name")

# Join total count onto segmented count
most_lobbied_politician = combined.join(total, on="full_name")

#big bug on counts (look at test.py)
most_lobbied_politician.write_csv('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/most_lobbied_politicians.csv')
if not os.path.exists('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/lobby_break_down_by_politician.csv'):
    split_df.write_csv('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/lobby_break_down_by_politician.csv')
else: 
    print("total lobbying activities count by politician already exists, skipping creation of lobby_break_down_by_politician.csv to avoid overwriting existing file. If you want to update the file, please delete the existing file and run the code again.")
    most_prolific_lobbyist = lobbying_df.select(
            pl.col('primary_key'),
            pl.col("lobby_enterprise_uri"),
            pl.col("lobbyist_name"),
            pl.col("dpo_lobbied").str.split("::").list.len().alias("politicians_involved_count")
        )
    counts = most_prolific_lobbyist.group_by(
                        pl.col("lobbyist_name")
                        ).agg(pl.len().alias("lobby_requests_count"
            )).join(
                lobby_org.select(
                    pl.col("main_activities_of_organisation"),
                    pl.col("company_registration_number"),
                    pl.col("company_registered_name"),
                    pl.col("website"),
                    pl.col("name"), 
                    pl.col("lobby_org_link")), left_on="lobbyist_name", right_on="name", how="inner")
most_prolific_lobbyist = most_prolific_lobbyist.join(
                counts, on="lobbyist_name"
                ).sort(["politicians_involved_count", "lobby_requests_count"],  descending=True)
most_prolific_lobbyist.write_csv('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/lobby_count_details.csv')
os.remove('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/cleaned_output.csv')




#
# =============================================================================
# TODO: DPO (Designated Public Official) INTEGRATION
# =============================================================================
# This snippet should be placed after the split_df explode logic in 
# lobbying_processing.py (after line ~107). It filters, cleans, and normalises
# the DPO data so it can be joined onto enriched_td_attendance.csv, giving
# each TD a full lobbying profile alongside attendance, payments, and interests.
#
# The key insight from robmcelhinney/lobbyieng is that the raw dpo_lobbied
# field contains a lot of garbage entries that inflate counts. Filtering by
# recognised job titles and banning known junk values is essential before
# any aggregation.
# =============================================================================

# import normalise_join_key  # already imported in lobbying_processing.py

# # --- Step 1: Banned names list ---
# # These appear in the dpo_lobbied field but are not real individual DPOs.
# # They are generic references, corporate names pasted into the wrong column,
# # or placeholder text from the lobbying.ie export. Without filtering these
# # out, your "most lobbied politician" counts will be inflated.
# # Add to this list as you discover more junk entries in your data.
# BANNED_DPO_NAMES = [
#     "All TDs",
#     "All Galway West and Galway East TD;s",
#     "All Public Representatives.",
#     "ALL TDS of O.",
#     "Members of Government",
#     "Members of Oireachtas Committee on Children and Youth Affairs",
#     "Members of Oireachtas Health Committee",
#     "Dublin South West GE 2024 Candidates",
#     "(Vacant)",
# ]

# # --- Step 2: Recognised DPO job titles ---
# # The position field (pipe-delimited index 1) tells you what kind of
# # official was lobbied. Filter to the titles relevant to your analysis.
# # "TD" is the core target for your project, but Ministers and Ministers
# # of State are also TDs who hold additional office, so include them.
# VALID_TD_POSITIONS = [
#     "TD",
#     "Minister",
#     "Minister of State",
#     "Ceann Comhairle",
#     "Leas-Cheann Comhairle",
#     "Taoiseach",
#     "Tánaiste",
# ]
# # If you expand to the Seanad later, add: "Senator", "Cathaoirleach", etc.

# # --- Step 3: Clean the DPO name field ---
# # Strip titles, suffixes, and whitespace that prevent joins.
# # This mirrors what lobbyieng does in normalize_person_name().
# dpo_df = split_df.with_columns(
#     # Strip common prefixes that appear inconsistently in the lobbying data
#     pl.col("full_name")
#       .str.strip_chars()
#       .str.replace(r"^(Minister |Mr\.? |Ms\.? |Dr\.? |Dep\.? |Deputy )", "")
#       .str.replace(r",.*$", "")        # drop everything after a comma (e.g. "Harris, Simon, TD")
#       .str.replace(r"\s+TD$", "")       # strip trailing " TD" suffix
#       .str.strip_chars()
#       .alias("dpo_name_cleaned")
# )

# # --- Step 4: Filter out banned names and non-TD positions ---
# dpo_df = dpo_df.filter(
#     ~pl.col("dpo_name_cleaned").is_in(BANNED_DPO_NAMES)
#     & pl.col("dpo_name_cleaned").str.len_chars() > 3   # drop empty or single-char junk
#     & pl.col("position").is_in(VALID_TD_POSITIONS)
# )

# # --- Step 5: Normalise names for joining to enriched_td_attendance ---
# # Reuse the existing normalise_join_key module so the join key is consistent
# # across attendance, payments, member interests, and now lobbying.
# dpo_df = normalise_join_key.normalise_df_td_name(dpo_df, "dpo_name_cleaned")

# # --- Step 6: Extract lobbying method from activities ---
# # The delivery field (pipe index 1 from lobbying_activities) contains values
# # like "Email", "Meeting", "Telephone", "Letter", "Other". Keeping this
# # lets you analyse HOW politicians are being lobbied, not just by whom.
# # This is something lobbyieng surfaces in its per-official method breakdown.
# dpo_df = dpo_df.with_columns(
#     pl.col("delivery")
#       .str.strip_chars()
#       .str.to_titlecase()
#       .alias("lobbying_method")
# )

# # --- Step 7: Join lobbying data onto enriched TD dataset ---
# enriched = pl.read_csv("members/enriched_td_attendance.csv")
# enriched_cols = enriched.select(
#     "join_key", "unique_member_code", "party",
#     "member_constituency", "ministerial_office"
# ).unique()
#
# td_lobbying = dpo_df.join(enriched_cols, on="join_key", how="left")
#
# # Flag unmatched rows — these are DPOs whose names didn't normalise
# # correctly or who aren't in the current Dáil (e.g. former TDs still
# # appearing in historical lobbying returns). Investigate these manually.
# unmatched = td_lobbying.filter(pl.col("unique_member_code").is_null())
# if unmatched.height > 0:
#     print(f"WARNING: {unmatched.height} lobbying records could not be matched to a current TD.")
#     print(unmatched.select("dpo_name_cleaned", "position").unique())

# # --- Step 8: Aggregated counts per TD (corrected) ---
# # Your current code has a bug where segmented.join(total) then
# # most_lobbied_politician.join(total) double-joins the total_count column.
# # This version computes both in one pass.
# td_lobby_counts = td_lobbying.group_by("join_key").agg(
#     pl.col("dpo_name_cleaned").first().alias("td_name"),
#     pl.col("party").first(),
#     pl.col("member_constituency").first(),
#     pl.col("unique_member_code").first(),
#     pl.len().alias("total_lobby_contacts"),
#     pl.col("lobbyist_name").n_unique().alias("unique_lobbyists"),
#     pl.col("lobbying_method").value_counts().alias("method_breakdown"),
# ).sort("total_lobby_contacts", descending=True)

# # --- Step 9: Period-over-period "biggest movers" ---
# # This is the analysis lobbyieng does that you're currently missing.
# # It compares consecutive reporting periods to surface which TDs saw
# # the biggest spike or drop in lobbying contacts — the kind of insight
# # journalists want: "who suddenly became a target?"
# periods = td_lobbying.select("lobbying_period").unique().sort("lobbying_period")
# if periods.height >= 2:
#     latest_period = periods.row(-1)[0]
#     previous_period = periods.row(-2)[0]
#
#     latest_counts = (
#         td_lobbying
#         .filter(pl.col("lobbying_period") == latest_period)
#         .group_by("join_key")
#         .agg(pl.len().alias("latest_count"),
#              pl.col("dpo_name_cleaned").first().alias("td_name"))
#     )
#     previous_counts = (
#         td_lobbying
#         .filter(pl.col("lobbying_period") == previous_period)
#         .group_by("join_key")
#         .agg(pl.len().alias("previous_count"))
#     )
#     biggest_movers = (
#         latest_counts
#         .join(previous_counts, on="join_key", how="full", coalesce=True)
#         .with_columns(
#             pl.col("latest_count").fill_null(0),
#             pl.col("previous_count").fill_null(0),
#         )
#         .with_columns(
#             (pl.col("latest_count") - pl.col("previous_count")).alias("delta")
#         )
#         .sort("delta", descending=True)
#     )
#     print(f"Biggest movers between {previous_period} and {latest_period}:")
#     print(biggest_movers.head(10))
#     # biggest_movers.write_csv("lobbyist/biggest_movers_by_period.csv")

# # --- Step 10: Shared lobbyists between TD pairs (network analysis) ---
# # Which TDs are being lobbied by the exact same organisations?
# # This is the "shared_lobbyists" query from lobbyieng that surfaces
# # hidden connections — e.g. two TDs on different committees both being
# # targeted by the same lobby group.
# td_lobbyist_pairs = (
#     td_lobbying
#     .select("join_key", "dpo_name_cleaned", "lobbyist_name")
#     .unique()
# )
# shared = (
#     td_lobbyist_pairs.join(
#         td_lobbyist_pairs,
#         on="lobbyist_name",
#         suffix="_b"
#     )
#     .filter(pl.col("join_key") < pl.col("join_key_b"))  # avoid self-pairs and duplicates
#     .group_by(["dpo_name_cleaned", "dpo_name_cleaned_b"])
#     .agg(
#         pl.col("lobbyist_name").n_unique().alias("shared_lobbyist_count"),
#         pl.col("lobbyist_name").unique().alias("shared_lobbyist_names"),
#     )
#     .sort("shared_lobbyist_count", descending=True)
# )
# print("TD pairs with most shared lobbyists:")
# print(shared.head(10))
# # shared.write_csv("lobbyist/shared_lobbyists_between_tds.csv")

# # --- Step 11: Degree centrality (how connected is each TD in the lobby network) ---
# # How many distinct lobbyists target each TD? High centrality = high influence target.
# centrality = (
#     td_lobbying
#     .group_by("join_key")
#     .agg(
#         pl.col("dpo_name_cleaned").first().alias("td_name"),
#         pl.col("lobbyist_name").n_unique().alias("degree_centrality"),
#     )
#     .sort("degree_centrality", descending=True)
# )
# print("TD degree centrality (unique lobbyists per TD):")
# print(centrality.head(15))
# # centrality.write_csv("lobbyist/td_degree_centrality.csv")